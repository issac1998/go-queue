package storage

import (
	"encoding/binary"
	"os"
)

// internal/storage/segment.go
type Segment struct {
	BaseOffset  int64    // 当前段起始 Offset
	LogFile     *os.File // 日志文件（追加写入）
	IndexFile   *os.File // 稀疏索引（Offset → 文件位置）
	MaxBytes    int64    // 单个日志文件最大大小
	currentSize int64
}

// Append
func (s *Segment) Append(msg []byte) (offset int64, err error) {
	s.Lock()
	defer s.UnLock()

	offset = s.BaseOffset + s.currentSize
	// 写入日志文件（格式: Length + Data）
	binary.Write(s.LogFile, binary.BigEndian, int32(len(msg)))
	s.LogFile.Write(msg)

	// 每隔 1KB 写入索引条目
	if s.currentSize%1024 == 0 {
		pos := getCurrentFilePosition(s.LogFile)
		binary.Write(s.IndexFile, binary.BigEndian, offset)
		binary.Write(s.IndexFile, binary.BigEndian, pos)
	}

	s.currentSize += int64(len(msg) + 4) // 4 bytes for length header
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
