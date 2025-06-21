package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	IndexEntrySize = 16   // 8 bytes offset + 8 bytes position
	IndexInterval  = 1024 // 每1KB写入一个索引条目
)

// Segment
type Segment struct {
	BaseOffset int64
	EndOffset  int64
	MaxBytes   int64
	IsActive   bool

	LogFile       *os.File
	IndexFile     *os.File
	TimeIndexFile *os.File

	DataDir string

	Mu sync.RWMutex

	// cache
	IndexEntries []IndexEntry
	CurrentSize  int64
	LastSynced   int64

	WriteCount    int64
	ReadCount     int64
	LastWriteTime time.Time
	LastReadTime  time.Time
}

// IndexEntry
type IndexEntry struct {
	Offset   int64
	Position int64
	TimeMs   int64
}

// Append
func NewSegment(dir string, baseOffset int64, maxBytes int64) (*Segment, error) {
	// make or create dir and open file
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create directory failed: %v", err)
	}

	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
	timeIndexPath := filepath.Join(dir, fmt.Sprintf("%020d.timeindex", baseOffset))

	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file failed: %v", err)
	}

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("open index file failed: %v", err)
	}

	timeIndexFile, err := os.OpenFile(timeIndexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("open time index file failed: %v", err)
	}

	stat, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		indexFile.Close()
		timeIndexFile.Close()
		return nil, fmt.Errorf("get log file size failed: %v", err)
	}

	segment := &Segment{
		BaseOffset:    baseOffset,
		MaxBytes:      maxBytes,
		IsActive:      true,
		LogFile:       logFile,
		IndexFile:     indexFile,
		TimeIndexFile: timeIndexFile,
		DataDir:       dir,
		CurrentSize:   stat.Size(),
		IndexEntries:  make([]IndexEntry, 0),
		LastWriteTime: time.Now(),
		LastReadTime:  time.Now(),
	}

	// load index to mem
	if err := segment.loadIndex(); err != nil {
		segment.Close()
		return nil, fmt.Errorf("load index failed: %v", err)
	}

	return segment, nil

}

// loadIndex to mem
func (s *Segment) loadIndex() error {
	stat, err := s.IndexFile.Stat()
	if err != nil {
		return err
	}

	numEntries := stat.Size() / IndexEntrySize
	s.IndexEntries = make([]IndexEntry, 0, numEntries)

	for i := int64(0); i < numEntries; i++ {
		var entry IndexEntry
		if err := binary.Read(s.IndexFile, binary.BigEndian, &entry.Offset); err != nil {
			return err
		}
		if err := binary.Read(s.IndexFile, binary.BigEndian, &entry.Position); err != nil {
			return err
		}
		s.IndexEntries = append(s.IndexEntries, entry)
	}

	return nil
}

// Append
func (s *Segment) Append(msg []byte) (offset int64, err error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if s.CurrentSize+int64(len(msg)+4) > s.MaxBytes {
		return 0, errors.New("segment is full")
	}
	offset = s.BaseOffset + s.CurrentSize

	if err := binary.Write(s.LogFile, binary.BigEndian, int32(len(msg))); err != nil {
		return 0, fmt.Errorf("write message length failed: %v", err)
	}
	if _, err := s.LogFile.Write(msg); err != nil {
		return 0, fmt.Errorf("write message content failed: %v", err)
	}

	s.CurrentSize += int64(len(msg) + 4)
	s.WriteCount++

	// 每隔一定间隔写入索引条目
	if s.CurrentSize%IndexInterval == 0 {
		pos, err := s.LogFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("get current position failed: %v", err)
		}

		if err := binary.Write(s.IndexFile, binary.BigEndian, offset); err != nil {
			return 0, fmt.Errorf("write index offset failed: %v", err)
		}
		if err := binary.Write(s.IndexFile, binary.BigEndian, pos); err != nil {
			return 0, fmt.Errorf("write index position failed: %v", err)
		}

		s.IndexEntries = append(s.IndexEntries, IndexEntry{
			Offset:   offset,
			Position: pos,
		})
	}

	return offset, nil
}

// FindPosition find message position by offset
func (s *Segment) FindPosition(offset int64) (int64, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	if offset < s.BaseOffset || offset >= s.BaseOffset+s.CurrentSize {
		return 0, errors.New("offset out of range")
	}

	// binary search
	left := 0
	right := len(s.IndexEntries) - 1
	var nearestEntry IndexEntry
	var startPos int64

	// 如果没有索引条目，从头开始扫描
	if len(s.IndexEntries) == 0 {
		startPos = 0
	} else {
		for left <= right {
			mid := left + (right-left)/2
			if s.IndexEntries[mid].Offset == offset {
				return s.IndexEntries[mid].Position, nil
			} else if s.IndexEntries[mid].Offset < offset {
				nearestEntry = s.IndexEntries[mid]
				left = mid + 1
			} else {
				right = mid - 1
			}
		}

		if nearestEntry.Offset == 0 {
			startPos = s.BaseOffset
		} else {
			startPos = nearestEntry.Position
		}
	}

	// 从最近的索引位置开始线性扫描找到确切的 offset
	currentPos := startPos
	currentOffset := s.BaseOffset
	if nearestEntry.Offset != 0 {
		currentOffset = nearestEntry.Offset
	}

	for currentOffset < offset {
		lenBuf := make([]byte, 4)
		if _, err := s.LogFile.Seek(currentPos, io.SeekStart); err != nil {
			return 0, fmt.Errorf("seek failed: %v", err)
		}

		n, err := s.LogFile.Read(lenBuf)
		if err != nil || n != 4 {
			return 0, fmt.Errorf("failed to read message length at position %d: %v", currentPos, err)
		}

		msgSize := binary.BigEndian.Uint32(lenBuf)
		if msgSize == 0 {
			break
		}

		currentPos += 4 + int64(msgSize)
		currentOffset++

		if currentOffset == offset {
			return currentPos, nil
		}
	}

	return 0, fmt.Errorf("offset %d not found in segment", offset)
}

// ReadAt
func (s *Segment) ReadAt(pos int64, buf []byte) (int, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	if pos < 0 || pos >= s.CurrentSize {
		return 0, errors.New("invalid position")
	}

	if _, err := s.LogFile.Seek(pos, io.SeekStart); err != nil {
		return 0, fmt.Errorf("seek failed: %v", err)
	}

	n, err := io.ReadFull(s.LogFile, buf)
	if err != nil && err != io.EOF {
		return n, fmt.Errorf("read failed: %v", err)
	}

	return n, nil
}

// Close
func (s *Segment) Close() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	var errs []error

	if s.LogFile != nil {
		if err := s.LogFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close log file failed: %v", err))
		}
	}
	if s.IndexFile != nil {
		if err := s.IndexFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close index file failed: %v", err))
		}
	}
	if s.TimeIndexFile != nil {
		if err := s.TimeIndexFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close time index file failed: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("close segment failed: %v", errs)
	}
	return nil
}

// Sync
func (s *Segment) Sync() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if err := s.LogFile.Sync(); err != nil {
		return fmt.Errorf("sync log file failed: %v", err)
	}
	if err := s.IndexFile.Sync(); err != nil {
		return fmt.Errorf("sync index file failed: %v", err)
	}
	s.LastSynced = s.CurrentSize
	return nil
}
