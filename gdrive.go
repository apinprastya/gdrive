package gdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

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
	files, err := g.driveService.Files.List().
		Q(fmt.Sprintf("mimeType = 'application/vnd.google-apps.folder' and name = '%s'",
			g.getFolderName(g.config.RemoteFolderRoot))).
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
		fmt.Println("creating remote folder")
		res, err := g.driveService.Files.Create(
			&drive.File{Name: g.getFolderName(g.config.RemoteFolderRoot),
				MimeType: "application/vnd.google-apps.folder"}).
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
	// store it to google drive
	reader := bytes.NewReader(fileInsertInfo.FileBytes)
	res, err := g.driveService.Files.Create(
		&drive.File{Name: g.convertToGDrive(fileInsertInfo.Filepath), Parents: []string{g.parentFolderID}}).
		Media(reader).
		Do()
	if err != nil {
		return err
	}

	// store it to local folder
	err = g.storeFile(ctx, fileInsertInfo.Filepath, fileInsertInfo.FileBytes)
	if err != nil {
		return err
	}

	if g.dao != nil {
		g.dao.Insert(ctx, &FileInfo{FileID: res.Id, LastAccess: time.Now(), Filepath: fileInsertInfo.Filepath,
			Size: res.Size, MimeType: res.MimeType})
	}

	return nil
}

func (g *GDrive) TouchFile(ctx context.Context, filePathName string) error {
	localPath := path.Join(g.config.LocalFolderRoot, filePathName)
	_, err := os.Stat(localPath)
	if err == nil {
		return nil
	}
	files, err := g.driveService.Files.List().
		Q(fmt.Sprintf("name = '%s' and '%s' in parents and trashed = false",
			g.convertToGDrive(filePathName), g.parentFolderID)).
		Do()
	if err != nil {
		return err
	}
	if len(files.Files) == 0 {
		return errors.New("file not available on google drive")
	}
	for _, f := range files.Files {
		fmt.Println(f.Name, f.Trashed)
	}
	resp, err := g.driveService.Files.Get(files.Files[0].Id).Download()
	if err != nil {
		return err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	err = g.storeFile(ctx, filePathName, b)
	if err != nil {
		return err
	}
	if g.dao != nil {
		g.dao.Insert(ctx, &FileInfo{FileID: files.Files[0].Id, LastAccess: time.Now(), Filepath: filePathName,
			Size: files.Files[0].Size, MimeType: files.Files[0].MimeType})
	}
	return nil
}

func (g *GDrive) storeFile(ctx context.Context, filePathName string, bytes []byte) error {
	localPath := path.Join(g.config.LocalFolderRoot, filePathName)
	dir := filepath.Dir(localPath)
	_, err := os.Stat(dir)
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(dir, os.ModeDir)
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

func (g *GDrive) getFolderName(name string) string {
	return fmt.Sprintf("apin-%s", name)
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
				err := g.dao.Delete(g.ctx, rem.FileID)
				if err != nil {
					logrus.WithError(err).Error("unable to remove from dao")
					return false
				}
				err = os.Remove(filepath.Join(g.config.LocalFolderRoot, rem.Filepath))
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
