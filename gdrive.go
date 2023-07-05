package gdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

var ErrFileExist = errors.New("file exist")

type Config struct {
	LocalFolderRoot  string
	RemoteFolderRoot string
	TotalMaxSize     int64 // in bytes
}

type GDrive struct {
	ctx            context.Context
	oauthConfig    *oauth2.Config
	config         *Config
	dao            Dao
	httpClient     *http.Client
	driveService   *drive.Service
	parentFolderID string
}

func New(ctx context.Context, credential json.RawMessage, config *Config, dao Dao, token *oauth2.Token) (*GDrive, error) {
	cfg, err := google.ConfigFromJSON(credential, drive.DriveFileScope)
	if err != nil {
		return nil, err
	}
	var httpClient *http.Client
	var driveService *drive.Service
	if token != nil {
		httpClient = cfg.Client(ctx, token)
		driveService, err = drive.NewService(ctx, option.WithHTTPClient(httpClient))
		if err != nil {
			return nil, err
		}
	}
	return &GDrive{
		ctx:          ctx,
		oauthConfig:  cfg,
		config:       config,
		dao:          dao,
		httpClient:   httpClient,
		driveService: driveService,
	}, nil
}

func (g *GDrive) Start() {
	t := time.NewTimer(time.Minute)
	for {
		select {
		case <-t.C:
			if g.shouldRemove() {
				t.Reset(time.Second)
			} else {
				t.Reset(time.Minute)
			}
		case <-g.ctx.Done():
			return
		}
	}
}

func (g *GDrive) Init() error {
	folderName := g.getFolderName(g.config.RemoteFolderRoot)
	files, err := g.driveService.Files.List().
		Q(fmt.Sprintf("mimeType = 'application/vnd.google-apps.folder' and name = '%s'", folderName)).
		Do()
	if err != nil {
		return err
	}
	found := false
	for _, f := range files.Files {
		if len(f.Parents) == 0 {
			found = true
			g.parentFolderID = f.Id
			break
		}
	}
	if !found {
		res, err := g.driveService.Files.Create(
			&drive.File{
				Name:     folderName,
				MimeType: "application/vnd.google-apps.folder",
			}).
			Do()
		if err != nil {
			return err
		}
		g.parentFolderID = res.Id
	}
	return nil
}

func (g *GDrive) GetLoginURL() string {
	return g.oauthConfig.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
}

func (g *GDrive) ExchangeOauthCode(code string) (*oauth2.Token, error) {
	token, err := g.oauthConfig.Exchange(g.ctx, code)
	if err != nil {
		return nil, err
	}
	g.httpClient = g.oauthConfig.Client(g.ctx, token)
	g.driveService, err = drive.NewService(g.ctx, option.WithHTTPClient(g.httpClient))
	if err != nil {
		return nil, err
	}
	return token, nil
}

func (g *GDrive) StoreFile(ctx context.Context, fileInsertInfo *FileInsertInfo) error {
	// check if file exist in local
	localPath := g.localFullPath(fileInsertInfo.Filepath)
	_, err := os.Stat(localPath)
	if err != nil && !os.IsNotExist(err) && !fileInsertInfo.Replace {
		return ErrFileExist
	}

	driveFile := g.getFileInCloud(ctx, fileInsertInfo.Filepath)
	if driveFile != nil && !fileInsertInfo.Replace {
		return ErrFileExist
	}

	// store it to google drive
	reader := bytes.NewReader(fileInsertInfo.FileBytes)
	res, err := g.uploadToCloud(ctx, fileInsertInfo.Filepath, reader, fileInsertInfo.Replace)
	if err != nil {
		return err
	}

	// store it to local folder
	err = g.storeFileToLocal(ctx, fileInsertInfo.Filepath, fileInsertInfo.FileBytes)
	if err != nil {
		return err
	}

	if g.dao != nil {
		g.dao.InsertOrUpdate(ctx, &FileInfo{FileID: res.Id, LastAccess: time.Now(), Filepath: fileInsertInfo.Filepath,
			Size: int64(len(fileInsertInfo.FileBytes)), MimeType: res.MimeType})
	}

	return nil
}

func (g *GDrive) TouchFile(ctx context.Context, filePathName string) error {
	localPath := g.localFullPath(filePathName)
	_, err := os.Stat(localPath)
	if err == nil {
		if g.dao != nil {
			g.dao.Touch(ctx, filePathName, time.Now())
		}
		return nil
	}
	files, err := g.driveService.Files.List().
		Q(fmt.Sprintf("name ='%s' and '%s' in parents and trashed = false",
			g.convertToGDrive(filePathName), g.parentFolderID)).
		Do()
	if err != nil {
		return err
	}
	if len(files.Files) == 0 {
		return errors.New("file not available on google drive")
	}
	resp, err := g.driveService.Files.Get(files.Files[0].Id).Download()
	if err != nil {
		return err
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	err = g.storeFileToLocal(ctx, filePathName, b)
	if err != nil {
		return err
	}
	if g.dao != nil {
		g.dao.InsertOrUpdate(ctx, &FileInfo{FileID: files.Files[0].Id, LastAccess: time.Now(), Filepath: filePathName,
			Size: int64(len(b)), MimeType: files.Files[0].MimeType})
	}
	return nil
}

func (g *GDrive) UploadAll(ctx context.Context) error {
	chanLimit := make(chan struct{}, 10)
	wg := &sync.WaitGroup{}
	filepath.Walk(g.config.LocalFolderRoot, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		wg.Add(1)
		go func(wg *sync.WaitGroup, limiter chan struct{}) {
			limiter <- struct{}{}
			defer func() {
				wg.Done()
				<-limiter
			}()
			rel, _ := filepath.Rel(g.config.LocalFolderRoot, path)
			f, err := os.Open(path)
			if err != nil {
				logrus.WithError(err).Error("unable to open file in upload all")
				return
			}
			b, err := io.ReadAll(f)
			if err != nil {
				logrus.WithError(err).Error("unable to read byte of the file in upload all")
			}
			logrus.WithField("path", path).Debug("uploading from upload all")
			reader := bytes.NewReader(b)
			res, err := g.uploadToCloud(ctx, rel, reader, false)
			if err != nil {
				logrus.WithError(err).Error("unable to store to google drive in upload all")
			}
			if g.dao != nil {
				g.dao.InsertOrUpdate(ctx, &FileInfo{FileID: res.Id, LastAccess: time.Now(), Filepath: rel, Size: int64(len(b)), MimeType: res.MimeType})
			}
		}(wg, chanLimit)
		return nil
	})
	wg.Wait()
	return nil
}

func (g *GDrive) uploadToCloud(ctx context.Context, filepathName string, reader io.Reader, replace bool) (*drive.File, error) {
	driveFile := g.getFileInCloud(ctx, filepathName)
	if driveFile != nil && !replace {
		return driveFile, nil
	}
	if driveFile == nil {
		return g.driveService.Files.Create(
			&drive.File{
				Name:    g.convertToGDrive(filepathName),
				Parents: []string{g.parentFolderID},
			}).
			Media(reader).
			Do()
	}
	return g.driveService.Files.Update(driveFile.Id, driveFile).Media(reader).Do()
}

func (g *GDrive) getFileInCloud(ctx context.Context, filepathName string) *drive.File {
	remoteName := g.convertToGDrive(filepathName)
	files, err := g.driveService.Files.List().
		Q(fmt.Sprintf("name ='%s' and '%s' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false",
			remoteName, g.parentFolderID)).
		Do()
	if err != nil {
		return nil
	}
	if len(files.Files) > 0 {
		return files.Files[0]
	}
	return nil
}

func (g *GDrive) storeFileToLocal(ctx context.Context, filePathName string, bytes []byte) error {
	localPath := g.localFullPath(filePathName)
	dir := filepath.Dir(localPath)
	_, err := os.Stat(dir)
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
	}
	err = os.WriteFile(localPath, bytes, 0666)
	if err != nil {
		return err
	}
	return nil
}

func (g *GDrive) localFileExist(filePathName string) bool {
	localPath := g.localFullPath(filePathName)
	_, err := os.Stat(localPath)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func (g *GDrive) localFullPath(pathName string) string {
	return path.Join(g.config.LocalFolderRoot, pathName)
}

func (g *GDrive) getFolderName(name string) string {
	return fmt.Sprintf("gdrive-%s", name)
}

func (g *GDrive) convertToGDrive(path string) string {
	return strings.ReplaceAll(path, "/", "#")
}

func (g *GDrive) shouldRemove() bool {
	if g.dao != nil {
		total, err := g.dao.TotalSize(g.ctx)
		if err != nil {
			logrus.WithError(err).Error("unable to get total size from dao")
			return false
		}
		if total > g.config.TotalMaxSize {
			logrus.WithField("total", total).WithField("maxSize", g.config.TotalMaxSize).Debug("total size exceeded")
			list, err := g.dao.QueryOldest(g.ctx, 10)
			if err != nil {
				logrus.WithError(err).Error("unable to query older from dao")
				return false
			}
			diff := total - g.config.TotalMaxSize
			var totalToRemove int64
			toRemove := []FileInfo{}
			for i := range list {
				totalToRemove += list[i].Size
				toRemove = append(toRemove, list[i])
				if totalToRemove > diff {
					break
				}
			}
			for _, rem := range toRemove {
				err := g.dao.Delete(g.ctx, rem.Filepath)
				if err != nil {
					logrus.WithError(err).Error("unable to remove from dao")
					return false
				}
				err = os.Remove(g.localFullPath(rem.Filepath))
				if err != nil {
					logrus.WithError(err).Error("unable to remove file")
					return false
				}
			}
			if diff > totalToRemove {
				return true
			}
		}
	}
	return false
}

// this only for testing
func (g *GDrive) deleteRootFolder(ctx context.Context) error {
	return g.driveService.Files.Delete(g.parentFolderID).Do()
}
