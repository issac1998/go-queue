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
	IndexInterval  = 1024 // Write one index entry per 1KB
)

// Segment represents a log segment containing message data and indexes
type Segment struct {
	// --- Basic metadata ---
	BaseOffset int64 // Starting offset of current Segment
	EndOffset  int64 // Ending offset of current Segment
	MaxBytes   int64 // Maximum capacity of Segment
	IsActive   bool  // Whether this is an active Segment

	// --- File handles ---
	LogFile       *os.File // Log file
	IndexFile     *os.File // Index file
	TimeIndexFile *os.File // Time index file

	DataDir string // Data directory

	// --- Concurrency control ---
	Mu sync.RWMutex

	// --- Memory cache ---
	IndexEntries []IndexEntry // Index entries in memory
	CurrentSize  int64        // Current log file size
	LastSynced   int64        // Last persisted position

	// --- Statistics ---
	WriteCount    int64     // Write count
	ReadCount     int64     // Read count
	LastWriteTime time.Time // Last write time
	LastReadTime  time.Time // Last read time

	MinTimestamp time.Time
	MaxTimestamp time.Time
}

// IndexEntry index entry
type IndexEntry struct {
	Offset   int64 // Message offset
	Position int64 // Position in log file
	TimeMs   int64 // Message timestamp (milliseconds)
}

// NewSegment creates a new segment
func NewSegment(dir string, baseOffset int64, maxBytes int64) (*Segment, error) {
	// Create directory (if not exists)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create directory failed: %v", err)
	}

	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
	timeIndexPath := filepath.Join(dir, fmt.Sprintf("%020d.timeindex", baseOffset))

	// Open log file
	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file failed: %v", err)
	}

	// Open index file
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("open index file failed: %v", err)
	}

	// Open time index file
	timeIndexFile, err := os.OpenFile(timeIndexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("open time index file failed: %v", err)
	}

	// Get current log file size
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

	// Load existing index
	if err := segment.loadIndex(); err != nil {
		segment.Close()
		return nil, fmt.Errorf("load index failed: %v", err)
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

// loadIndex loads index entries from the index file into memory
func (s *Segment) loadIndex() error {
	stat, err := s.IndexFile.Stat()
	if err != nil {
		return err
	}

	// Calculate the number of index entries (each entry is 16 bytes: offset+position)
	numEntries := stat.Size() / IndexEntrySize
	s.IndexEntries = make([]IndexEntry, 0, numEntries)

	// If the file is empty, return directly
	if stat.Size() == 0 {
		return nil
	}

	// Reset file pointer to the beginning
	if _, err := s.IndexFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek index file failed: %v", err)
	}

	// Read all index entries (only read offset and position)
	for i := int64(0); i < numEntries; i++ {
		var entry IndexEntry
		if err := binary.Read(s.IndexFile, binary.BigEndian, &entry.Offset); err != nil {
			return fmt.Errorf("read offset failed at entry %d: %v", i, err)
		}
		if err := binary.Read(s.IndexFile, binary.BigEndian, &entry.Position); err != nil {
			return fmt.Errorf("read position failed at entry %d: %v", i, err)
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

	// Check if exceeding maximum capacity
	if s.CurrentSize+int64(len(msg)+4) > s.MaxBytes {
		return 0, errors.New("segment is full")
	}

	// Calculate message offset (using message count, not byte count)
	offset = s.BaseOffset + s.WriteCount

	// Get current file position
	currentFilePos, err := s.LogFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("get file position failed: %v", err)
	}

	// Write message length and content
	if err := binary.Write(s.LogFile, binary.BigEndian, int32(len(msg))); err != nil {
		return 0, fmt.Errorf("write message length failed: %v", err)
	}
	if _, err := s.LogFile.Write(msg); err != nil {
		return 0, fmt.Errorf("write message content failed: %v", err)
	}

	// Force write index entry (index is established for each message)
	if err := binary.Write(s.IndexFile, binary.BigEndian, offset); err != nil {
		return 0, fmt.Errorf("write index offset failed: %v", err)
	}
	if err := binary.Write(s.IndexFile, binary.BigEndian, currentFilePos); err != nil {
		return 0, fmt.Errorf("write index position failed: %v", err)
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

// FindPosition finds the file position by offset
func (s *Segment) FindPosition(offset int64) (int64, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	// Check if offset is within range (using message count range)
	if offset < s.BaseOffset || offset > s.BaseOffset+s.WriteCount {
		return 0, errors.New("offset out of range")
	}

	// If no index entries, start from the beginning of the file
	if len(s.IndexEntries) == 0 {
		return 0, nil
	}

	// Precise index entry lookup
	for _, entry := range s.IndexEntries {
		if entry.Offset == offset {
			return entry.Position, nil
		}
	}

	// If no exact match found, find the index entry closest to and less than the target offset
	var bestEntry *IndexEntry
	for i := len(s.IndexEntries) - 1; i >= 0; i-- {
		if s.IndexEntries[i].Offset <= offset {
			bestEntry = &s.IndexEntries[i]
			break
		}
	}

	if bestEntry != nil {
		return bestEntry.Position, nil
	}

	// If no suitable index entry, start from the beginning of the file
	return 0, nil
}

// ReadAt reads data from a specified position
func (s *Segment) ReadAt(pos int64, buf []byte) (int, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	// Check if position is valid (pos should be file position, not compared to CurrentSize)
	if pos < 0 {
		return 0, errors.New("invalid position")
	}

	// Seek to the specified position
	if _, err := s.LogFile.Seek(pos, io.SeekStart); err != nil {
		return 0, fmt.Errorf("seek failed: %v", err)
	}

	// Read data
	n, err := s.LogFile.Read(buf)
	if err != nil && err != io.EOF {
		return n, fmt.Errorf("read failed: %v", err)
	}

	return n, nil
}

// Close closes the Segment
func (s *Segment) Close() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	var errs []error

	// Close all files
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

// Sync synchronizes data to disk
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

// PurgeBefore removes index entries and (optionally) data before the given time.
// This is a minimal stub; you should implement actual data deletion as needed.
func (s *Segment) PurgeBefore(expireBefore time.Time) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	newEntries := s.IndexEntries[:0]
	for _, entry := range s.IndexEntries {
		if time.UnixMilli(entry.TimeMs).After(expireBefore) {
			newEntries = append(newEntries, entry)
		}
	}
	s.IndexEntries = newEntries
	// Optionally, update MinTimestamp
	if len(s.IndexEntries) > 0 {
		s.MinTimestamp = time.UnixMilli(s.IndexEntries[0].TimeMs)
	} else {
		s.MinTimestamp = time.Time{}
	}
}
