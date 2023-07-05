package gdrive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"golang.org/x/oauth2"
)

type GDriveTestSuite struct {
	suite.Suite
	localFolder  string
	remoteFolder string
	instance     *GDrive
}

func (s *GDriveTestSuite) SetupSuite() {
	credentialFile := os.Getenv("CREDENTIAL_JSON")
	tokenFile := os.Getenv("TOKEN_JSON")

	s.Require().NotEmpty(credentialFile)
	s.Require().NotEmpty(tokenFile)

	s.localFolder = path.Join(os.TempDir(), "gdrive")
	s.remoteFolder = "roottest"
	instance, err := createInstance(credentialFile, tokenFile, &Config{
		LocalFolderRoot:  s.localFolder,
		RemoteFolderRoot: s.remoteFolder,
	}, nil)
	s.Require().NoError(err)
	s.Require().NotNil(instance)
	s.instance = instance
}

func (s *GDriveTestSuite) TearDownSuite() {
	err := s.instance.deleteRootFolder(context.Background())
	s.Require().NoError(err)
	err = os.RemoveAll(s.localFolder)
	s.Require().NoError(err)
}

func (s *GDriveTestSuite) TestStoreFile() {
	files := []string{"file number one", "file number two", "file number three"}
	paths := []string{"fileone.txt", "folder/filetwo.txt", "folder/filethree.txt"}
	for i := range files {
		err := s.instance.StoreFile(context.TODO(), &FileInsertInfo{Filepath: paths[i], FileBytes: []byte(files[i])})
		s.Require().NoError(err)
		s.Require().True(s.instance.localFileExist(paths[i]))
		cloudFile := s.instance.getFileInCloud(context.TODO(), paths[i])
		s.Require().NotNil(cloudFile)
	}
	cloudFile := s.instance.getFileInCloud(context.TODO(), "unknownfile.txt")
	s.Require().Nil(cloudFile)

	s.Run("file exist in local", func() {
		err := s.instance.StoreFile(context.TODO(), &FileInsertInfo{FileBytes: []byte(files[0]), Filepath: paths[0]})
		s.Require().Error(err)
		s.Require().True(errors.Is(err, ErrFileExist))
	})

	s.Run("file exist in cloud", func() {
		// remote the local file
		err := os.Remove(s.instance.localFullPath(paths[0]))
		s.Require().NoError(err)
		exist := s.instance.localFileExist(paths[0])
		s.Require().False(exist)
		err = s.instance.StoreFile(context.TODO(), &FileInsertInfo{FileBytes: []byte(files[0]), Filepath: paths[0]})
		s.Require().Error(err)
		s.Require().True(errors.Is(err, ErrFileExist))
	})

	s.Run("touch file", func() {
		exist := s.instance.localFileExist(paths[0])
		s.Require().False(exist)
		cloudFile := s.instance.getFileInCloud(context.TODO(), paths[0])
		s.Require().NotNil(cloudFile)
		err := s.instance.TouchFile(context.TODO(), paths[0])
		s.Require().Nil(err)
		exist = s.instance.localFileExist(paths[0])
		s.Require().True(exist)
	})
}

func (s *GDriveTestSuite) TestUploadAll() {
	files := []string{"file number one", "file number two", "file number three"}
	paths := []string{"fileone.txt", "folder/filetwo.txt", "folder/filethree.txt"}
	localFolder := path.Join(os.TempDir(), "gdrive_uploadall")
	remoteFolder := "roottestuploadall"
	err := os.MkdirAll(localFolder, os.ModePerm)
	s.Require().NoError(err)

	instance, err := createInstance(os.Getenv("CREDENTIAL_JSON"), os.Getenv("TOKEN_JSON"), &Config{
		LocalFolderRoot:  localFolder,
		RemoteFolderRoot: remoteFolder,
	}, nil)
	s.Require().NoError(err)
	s.Require().NotNil(instance)

	for i := range files {
		err := instance.storeFileToLocal(context.TODO(), paths[i], []byte(files[0]))
		s.Require().NoError(err)
	}

	err = instance.UploadAll(context.TODO())
	s.Require().NoError(err)

	var total int64
	for i := range paths {
		cloudFile := instance.getFileInCloud(context.TODO(), paths[i])
		s.Require().NotNil(cloudFile)
		total += cloudFile.Size
	}
	fmt.Println(total)

	err = instance.deleteRootFolder(context.TODO())
	s.Require().NoError(err)
	err = os.RemoveAll(localFolder)
	s.Require().NoError(err)
}

func (s *GDriveTestSuite) TestWorker() {
	files := []string{"file number one", "file number two", "file number three", "file number four", "file number five"}
	paths := []string{"fileone.txt", "folder/filetwo.txt", "folder/filethree.txt", "folder/filefour.txt", "folder/filefive.txt"}
	localFolder := path.Join(os.TempDir(), "gdrive_testworker")
	remoteFolder := "roottestworker"
	err := os.MkdirAll(localFolder, os.ModePerm)
	s.Require().NoError(err)

	memoryDao := NewMemoryDao()
	var maxSize int64 = 60
	instance, err := createInstance(os.Getenv("CREDENTIAL_JSON"), os.Getenv("TOKEN_JSON"), &Config{
		LocalFolderRoot:  localFolder,
		RemoteFolderRoot: remoteFolder,
		TotalMaxSize:     maxSize,
	}, memoryDao)
	s.Require().NoError(err)
	s.Require().NotNil(instance)

	var total int64
	for i := range files {
		err := instance.StoreFile(context.TODO(), &FileInsertInfo{
			FileBytes: []byte(files[0]),
			Filepath:  paths[i],
		})
		time.Sleep(500 * time.Millisecond)
		total += int64(len([]byte(files[0])))
		s.Require().NoError(err)
	}

	totalDao, err := memoryDao.TotalSize(context.TODO())
	s.Require().NoError(err)
	s.Require().Equal(total, totalDao)

	instance.shouldRemove()
	totalDao, err = memoryDao.TotalSize(context.TODO())
	s.Require().NoError(err)
	s.Require().True(totalDao < maxSize)

	fileExist := instance.localFileExist(paths[0])
	s.Require().False(fileExist)

	instance.shouldRemove()
	oldTotalDao := totalDao
	totalDao, err = memoryDao.TotalSize(context.TODO())
	s.Require().NoError(err)
	s.Require().Equal(totalDao, oldTotalDao)

	err = instance.TouchFile(context.TODO(), paths[0])
	s.Require().NoError(err)
	err = instance.TouchFile(context.TODO(), paths[1])
	s.Require().NoError(err)
	fileExist = instance.localFileExist(paths[0])
	s.Require().True(fileExist)
	fileExist = instance.localFileExist(paths[1])
	s.Require().True(fileExist)

	instance.shouldRemove()
	totalDao, err = memoryDao.TotalSize(context.TODO())
	s.Require().NoError(err)
	s.Require().True(totalDao < maxSize)

	fileExist = instance.localFileExist(paths[2])
	s.Require().False(fileExist)

	err = instance.deleteRootFolder(context.TODO())
	s.Require().NoError(err)
	err = os.RemoveAll(localFolder)
	s.Require().NoError(err)
}

func TestGDrive(t *testing.T) {
	suite.Run(t, new(GDriveTestSuite))
}

func getTokenFromFile(filepath string) *oauth2.Token {
	f, err := os.Open(filepath)
	if err != nil {
		return nil
	}
	defer f.Close()
	token := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(token)
	if err != nil {
		return nil
	}
	return token
}

func createInstance(credentialFile, tokenFile string, cfg *Config, dao Dao) (*GDrive, error) {
	credentialByte, err := os.ReadFile(credentialFile)
	if err != nil {
		return nil, err
	}
	token := getTokenFromFile(tokenFile)
	if tokenFile == "" {
		return nil, errors.New("token not available")
	}
	err = os.MkdirAll(cfg.LocalFolderRoot, os.ModePerm)
	if err != nil {
		return nil, err
	}

	instance, err := New(context.Background(), credentialByte, cfg, dao, token)
	if err != nil {
		return nil, err
	}

	err = instance.Init()
	return instance, err
}
