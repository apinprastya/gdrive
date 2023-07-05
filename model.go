package gdrive

import "time"

type FileInsertInfo struct {
	FileBytes []byte
	Filepath  string
	Replace   bool
}

type FileInfo struct {
	FileID     string
	LastAccess time.Time // time when the cache created
	Filepath   string
	Size       int64
	MimeType   string
}
