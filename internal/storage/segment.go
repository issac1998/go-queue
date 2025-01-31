package storage

import (
	"encoding/binary"
	"os"
)

type Segment struct {
	BaseOffset  int64
	LogFile     *os.File
	IndexFile   *os.File
	MaxBytes    int64
	currentSize int64
}

// Append
func (s *Segment) Append(msg []byte) (offset int64, err error) {
	s.Lock()
	defer s.UnLock()

	offset = s.BaseOffset + s.currentSize
	binary.Write(s.LogFile, binary.BigEndian, int32(len(msg)))
	s.LogFile.Write(msg)

	// 每隔 1KB 写入索引条目
	if s.currentSize%1024 == 0 {
		pos := getCurrentFilePosition(s.LogFile)
		binary.Write(s.IndexFile, binary.BigEndian, offset)
		binary.Write(s.IndexFile, binary.BigEndian, pos)
	}

	s.currentSize += int64(len(msg) + 4)
	return offset, nil
}

func getCurrentFilePosition(*os.File) int64 {
	return 0
}

// Lock
func (s *Segment) Lock() {
}

// UnLock
func (s *Segment) UnLock() {
}
