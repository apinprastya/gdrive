package gdrive

import (
	"context"
	"time"
)

type Dao interface {
	InsertOrUpdate(ctx context.Context, fileInfo *FileInfo) error
	Touch(ctx context.Context, filepathName string, date time.Time) error
	Delete(ctx context.Context, filepathName string) error
	TotalSize(ctx context.Context) (int64, error)
	QueryOldest(ctx context.Context, limit int) ([]FileInfo, error)
}
