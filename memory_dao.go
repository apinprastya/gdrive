package gdrive

import (
	"context"
	"errors"
	"sync"
	"time"

	"gopkg.in/typ.v4/slices"
)

type Memory struct {
	mut  sync.Mutex
	data []FileInfo
}

func NewMemoryDao() *Memory {
	return &Memory{
		mut:  sync.Mutex{},
		data: []FileInfo{},
	}
}

func (m *Memory) InsertOrUpdate(ctx context.Context, fileInfo *FileInfo) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	idx := slices.IndexFunc(m.data, func(data FileInfo) bool { return data.Filepath == fileInfo.Filepath })
	if idx >= 0 {
		fileInfo.LastAccess = time.Now()
		m.data[idx] = *fileInfo
	} else {
		m.data = append(m.data, *fileInfo)
	}

	return nil
}

func (m *Memory) Touch(ctx context.Context, filepathName string, date time.Time) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	idx := slices.IndexFunc(m.data, func(data FileInfo) bool { return data.Filepath == filepathName })
	if idx >= 0 {
		m.data[idx].LastAccess = date
	}

	return nil
}

func (m *Memory) Delete(ctx context.Context, filepathName string) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	idx := slices.IndexFunc(m.data, func(data FileInfo) bool { return data.Filepath == filepathName })
	if idx < 0 {
		return errors.New("file not found")
	}
	slices.Remove(&m.data, idx)
	return nil
}

func (m *Memory) TotalSize(ctx context.Context) (int64, error) {
	m.mut.Lock()
	defer m.mut.Unlock()

	var total int64
	for i := range m.data {
		total += m.data[i].Size
	}

	return total, nil
}

func (m *Memory) QueryOldest(ctx context.Context, limit int) ([]FileInfo, error) {
	m.mut.Lock()
	defer m.mut.Unlock()

	slices.SortFunc(m.data, func(a, b FileInfo) bool { return a.LastAccess.Before(b.LastAccess) })
	retVal := []FileInfo{}
	for i := 0; i < limit; i++ {
		if i >= len(m.data) {
			break
		}
		retVal = append(retVal, m.data[i])
	}

	return retVal, nil
}
