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

	typederrors "github.com/issac1998/go-queue/internal/errors"
)

const (
	IndexEntrySize = 16   // 8 bytes offset + 8 bytes position
	IndexInterval  = 1024 // Write one index entry per 1KB
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

	MinTimestamp time.Time
	MaxTimestamp time.Time
}

// IndexEntry
type IndexEntry struct {
	Offset   int64
	Position int64
	TimeMs   int64
}

// NewSegment creates a new segment
func NewSegment(dir string, baseOffset int64, maxBytes int64) (*Segment, error) {
	// make or create dir and open file
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "create directory failed",
			Cause:   err,
		}
	}

	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
	timeIndexPath := filepath.Join(dir, fmt.Sprintf("%020d.timeindex", baseOffset))

	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "open log file failed",
			Cause:   err,
		}
	}

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logFile.Close()
		return nil, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "open index file failed",
			Cause:   err,
		}
	}

	timeIndexFile, err := os.OpenFile(timeIndexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "open time index file failed",
			Cause:   err,
		}
	}

	stat, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		indexFile.Close()
		timeIndexFile.Close()
		return nil, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "get log file size failed",
			Cause:   err,
		}
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
		return nil, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "load index failed",
			Cause:   err,
		}
	}

	// Update WriteCount based on loaded index entries
	if len(segment.IndexEntries) > 0 {
		// WriteCount should be equal to the number of index entries
		segment.WriteCount = int64(len(segment.IndexEntries))
		// Update EndOffset
		lastEntry := segment.IndexEntries[len(segment.IndexEntries)-1]
		segment.EndOffset = lastEntry.Offset
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

	// If the file is empty, return directly
	if stat.Size() == 0 {
		return nil
	}

	// Reset file pointer to the beginning
	if _, err := s.IndexFile.Seek(0, io.SeekStart); err != nil {
		return &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "seek index file failed",
			Cause:   err,
		}
	}

	// Read all index entries (only read offset and position)
	for i := int64(0); i < numEntries; i++ {
		var entry IndexEntry
		if err := binary.Read(s.IndexFile, binary.BigEndian, &entry.Offset); err != nil {
			return &typederrors.TypedError{
				Type:    typederrors.StorageError,
				Message: fmt.Sprintf("read offset failed at entry %d", i),
				Cause:   err,
			}
		}
		if err := binary.Read(s.IndexFile, binary.BigEndian, &entry.Position); err != nil {
			return &typederrors.TypedError{
				Type:    typederrors.StorageError,
				Message: fmt.Sprintf("read position failed at entry %d", i),
				Cause:   err,
			}
		}
		// TimeMs field is not read from the index file during loading because it was not stored in old versions
		entry.TimeMs = 0
		s.IndexEntries = append(s.IndexEntries, entry)

	}

	return nil
}

// Append appends a message to the segment
func (s *Segment) Append(msg []byte, timestamp time.Time) (offset int64, err error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if s.CurrentSize+int64(len(msg)+4) > s.MaxBytes {
		return 0, errors.New("segment is full")
	}

	offset = s.BaseOffset + s.WriteCount

	// Get current file position
	currentFilePos, err := s.LogFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "get file position failed",
			Cause:   err,
		}
	}

	// Write message length and content
	if err := binary.Write(s.LogFile, binary.BigEndian, int32(len(msg))); err != nil {
		return 0, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "write message length failed",
			Cause:   err,
		}
	}
	if _, err := s.LogFile.Write(msg); err != nil {
		return 0, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "write message content failed",
			Cause:   err,
		}
	}

	// Force write index entry (index is established for each message)
	if err := binary.Write(s.IndexFile, binary.BigEndian, offset); err != nil {
		return 0, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "write index offset failed",
			Cause:   err,
		}
	}
	if err := binary.Write(s.IndexFile, binary.BigEndian, currentFilePos); err != nil {
		return 0, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "write index position failed",
			Cause:   err,
		}
	}

	// Add to memory index
	entry := IndexEntry{
		Offset:   offset,
		Position: currentFilePos,
		TimeMs:   timestamp.UnixMilli(),
	}
	s.IndexEntries = append(s.IndexEntries, entry)

	// Update timestamp range
	if s.MinTimestamp.IsZero() || timestamp.Before(s.MinTimestamp) {
		s.MinTimestamp = timestamp
	}
	if timestamp.After(s.MaxTimestamp) {
		s.MaxTimestamp = timestamp
	}

	// Update counters and size
	s.WriteCount++
	s.CurrentSize += int64(len(msg) + 4)
	s.EndOffset = offset

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

	return 0, &typederrors.TypedError{
		Type:    typederrors.StorageError,
		Message: fmt.Sprintf("offset %d not found in segment", offset),
	}
}

// ReadAt reads data from a specified position
func (s *Segment) ReadAt(pos int64, buf []byte) (int, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	if pos < 0 {
		return 0, errors.New("invalid position")
	}

	if _, err := s.LogFile.Seek(pos, io.SeekStart); err != nil {
		return 0, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "seek failed",
			Cause:   err,
		}
	}

	// Read is better to use io.ReadFull to ensure full read
	n, err := s.LogFile.Read(buf)
	if err != nil && err != io.EOF {
		return n, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "read failed",
			Cause:   err,
		}
	}

	return n, nil
}

// Close closes the Segment
func (s *Segment) Close() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	var errs []error

	if s.LogFile != nil {
		if err := s.LogFile.Close(); err != nil {
			errs = append(errs, &typederrors.TypedError{
				Type:    typederrors.StorageError,
				Message: "close log file failed",
				Cause:   err,
			})
		}
	}
	if s.IndexFile != nil {
		if err := s.IndexFile.Close(); err != nil {
			errs = append(errs, &typederrors.TypedError{
				Type:    typederrors.StorageError,
				Message: "close index file failed",
				Cause:   err,
			})
		}
	}
	if s.TimeIndexFile != nil {
		if err := s.TimeIndexFile.Close(); err != nil {
			errs = append(errs, &typederrors.TypedError{
				Type:    typederrors.StorageError,
				Message: "close time index file failed",
				Cause:   err,
			})
		}
	}

	if len(errs) > 0 {
		return &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "close segment failed",
			Cause:   errs[0], // Use first error as cause
		}
	}
	return nil
}

// Sync synchronizes data to disk
func (s *Segment) Sync() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if err := s.LogFile.Sync(); err != nil {
		return &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "sync log file failed",
			Cause:   err,
		}
	}
	if err := s.IndexFile.Sync(); err != nil {
		return &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "sync index file failed",
			Cause:   err,
		}
	}
	s.LastSynced = s.CurrentSize
	return nil
}

// PurgeBefore removes index entries and data before the given time.
func (s *Segment) PurgeBefore(expireBefore time.Time) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if len(s.IndexEntries) == 0 {
		return nil
	}

	// Find the first entry to keep
	keepFromIndex := -1
	var newBaseOffset int64
	var newBasePosition int64

	for i, entry := range s.IndexEntries {
		if time.UnixMilli(entry.TimeMs).After(expireBefore) {
			keepFromIndex = i
			newBaseOffset = entry.Offset
			newBasePosition = entry.Position
			break
		}
	}

	// If all entries should be purged
	if keepFromIndex == -1 {
		// Clear all entries and reset segment
		s.IndexEntries = s.IndexEntries[:0]
		s.BaseOffset = s.EndOffset
		s.CurrentSize = 0
		s.MinTimestamp = time.Time{}
		s.MaxTimestamp = time.Time{}
		
		// Truncate files to remove all data
		if err := s.LogFile.Truncate(0); err != nil {
			return &typederrors.TypedError{
				Type:    typederrors.StorageError,
				Message: "failed to truncate log file",
				Cause:   err,
			}
		}
		
		if err := s.IndexFile.Truncate(0); err != nil {
			return &typederrors.TypedError{
				Type:    typederrors.StorageError,
				Message: "failed to truncate index file",
				Cause:   err,
			}
		}
		
		if err := s.TimeIndexFile.Truncate(0); err != nil {
			return &typederrors.TypedError{
				Type:    typederrors.StorageError,
				Message: "failed to truncate time index file",
				Cause:   err,
			}
		}
		
		return nil
	}

	// Keep entries from keepFromIndex onwards
	newEntries := make([]IndexEntry, len(s.IndexEntries)-keepFromIndex)
	copy(newEntries, s.IndexEntries[keepFromIndex:])
	
	// Adjust positions relative to new base
	for i := range newEntries {
		newEntries[i].Position -= newBasePosition
	}
	
	s.IndexEntries = newEntries
	s.BaseOffset = newBaseOffset

	// Update timestamps
	if len(s.IndexEntries) > 0 {
		s.MinTimestamp = time.UnixMilli(s.IndexEntries[0].TimeMs)
		s.MaxTimestamp = time.UnixMilli(s.IndexEntries[len(s.IndexEntries)-1].TimeMs)
	} else {
		s.MinTimestamp = time.Time{}
		s.MaxTimestamp = time.Time{}
	}

	// Compact the log file by removing data before newBasePosition
	if newBasePosition > 0 {
		if err := s.compactLogFile(newBasePosition); err != nil {
			return err
		}
	}

	// Update current size
	s.CurrentSize -= newBasePosition
	if s.CurrentSize < 0 {
		s.CurrentSize = 0
	}

	return nil
}

// compactLogFile removes data before the given position by shifting remaining data
func (s *Segment) compactLogFile(removeBeforePos int64) error {
	// Get current file size
	fileInfo, err := s.LogFile.Stat()
	if err != nil {
		return &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "failed to get log file info",
			Cause:   err,
		}
	}
	
	currentSize := fileInfo.Size()
	if removeBeforePos >= currentSize {
		// Nothing to compact
		return nil
	}

	// Create a temporary file for compaction
	tempFile, err := os.CreateTemp(s.DataDir, "segment_compact_*.tmp")
	if err != nil {
		return &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "failed to create temp file for compaction",
			Cause:   err,
		}
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Copy data from removeBeforePos to end of file
	_, err = s.LogFile.Seek(removeBeforePos, io.SeekStart)
	if err != nil {
		return &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "failed to seek in log file",
			Cause:   err,
		}
	}

	_, err = io.Copy(tempFile, s.LogFile)
	if err != nil {
		return &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "failed to copy data during compaction",
			Cause:   err,
		}
	}

	// Close and reopen log file for writing
	s.LogFile.Close()
	
	logFile, err := os.OpenFile(filepath.Join(s.DataDir, fmt.Sprintf("%020d.log", s.BaseOffset)), 
		os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "failed to reopen log file",
			Cause:   err,
		}
	}
	s.LogFile = logFile

	// Copy compacted data back to log file
	tempFile.Seek(0, io.SeekStart)
	_, err = io.Copy(s.LogFile, tempFile)
	if err != nil {
		return &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "failed to write compacted data",
			Cause:   err,
		}
	}

	// Truncate to remove any remaining old data
	newSize := currentSize - removeBeforePos
	err = s.LogFile.Truncate(newSize)
	if err != nil {
		return &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "failed to truncate log file after compaction",
			Cause:   err,
		}
	}

	return nil
}
