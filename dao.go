package gdrive

import "context"

type Dao interface {
	Insert(ctx context.Context, fileInfo *FileInfo) error
	Touch(ctx context.Context, fileID string) error
	Delete(ctx context.Context, fileID string) error
	TotalSize(ctx context.Context) (int64, error)
	QueryOldest(ctx context.Context, limit int) ([]FileInfo, error)
}
