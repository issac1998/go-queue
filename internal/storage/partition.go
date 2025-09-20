package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	typederrors "github.com/issac1998/go-queue/internal/errors"
)

var (
	ErrOffsetOutOfRange = errors.New("offset out of range")
	ErrPartitionClosed  = errors.New("partition is closed")
)

// PartitionConfig contains configuration for a partition
type PartitionConfig struct {
	MaxSegmentSize int64         // Maximum size per segment
	MaxIndexSize   int64         // Maximum index size
	RetentionTime  time.Duration // How long to keep data
	RetentionSize  int64         // Maximum total size
}

// Partition manages multiple segments for a topic partition
type Partition struct {
	DataDir string
	Config  *PartitionConfig

	mu       sync.RWMutex
	segments []*Segment
	active   *Segment

	// Metrics
	totalMessages int64
	totalBytes    int64
	closed        bool
}

// NewPartition creates a new partition
func NewPartition(dataDir string, config *PartitionConfig) (*Partition, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "failed to create partition directory",
			Cause:   err,
		}
	}

	p := &Partition{
		DataDir:  dataDir,
		Config:   config,
		segments: make([]*Segment, 0),
	}

	// Load existing segments
	if err := p.loadSegments(); err != nil {
		return nil, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "failed to load segments",
			Cause:   err,
		}
	}

	if len(p.segments) == 0 {
		if err := p.createNewSegment(0); err != nil {
			return nil, &typederrors.TypedError{
				Type:    typederrors.StorageError,
				Message: "failed to create initial segment",
				Cause:   err,
			}
		}
	}

	log.Printf("Created partition at %s with %d segments", dataDir, len(p.segments))
	return p, nil
}

// loadSegments loads existing segments from disk
func (p *Partition) loadSegments() error {
	files, err := os.ReadDir(p.DataDir)
	if err != nil {
		return err
	}

	var baseOffsets []int64
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".log" {
			var baseOffset int64
			if n, err := fmt.Sscanf(file.Name(), "%020d.log", &baseOffset); n == 1 && err == nil {
				baseOffsets = append(baseOffsets, baseOffset)
			}
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, baseOffset := range baseOffsets {
		segment, err := NewSegment(p.DataDir, baseOffset, p.Config.MaxSegmentSize)
		if err != nil {
			log.Printf("Failed to load segment at offset %d: %v", baseOffset, err)
			continue
		}
		p.segments = append(p.segments, segment)
	}

	// Set the last segment as active
	if len(p.segments) > 0 {
		p.active = p.segments[len(p.segments)-1]
	}

	return nil
}

// createNewSegment creates a new segment
func (p *Partition) createNewSegment(baseOffset int64) error {
	segment, err := NewSegment(p.DataDir, baseOffset, p.Config.MaxSegmentSize)
	if err != nil {
		return err
	}

	// Mark previous segment as inactive
	if p.active != nil {
		p.active.IsActive = false
	}

	p.segments = append(p.segments, segment)
	p.active = segment

	log.Printf("Created new segment at offset %d", baseOffset)
	return nil
}

// Append appends a message to the partition
func (p *Partition) Append(data []byte, timestamp time.Time) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, ErrPartitionClosed
	}

	if p.active == nil {
		return 0, errors.New("no active segment")
	}

	// Check if we need to create a new segment
	if p.active.CurrentSize+int64(len(data)+4) > p.Config.MaxSegmentSize {
		nextBaseOffset := p.active.BaseOffset + p.active.WriteCount
		if err := p.createNewSegment(nextBaseOffset); err != nil {
			return 0, &typederrors.TypedError{
				Type:    typederrors.StorageError,
				Message: "failed to create new segment",
				Cause:   err,
			}
		}
	}

	offset, err := p.active.Append(data, timestamp)
	if err != nil {
		return 0, err
	}

	p.totalMessages++
	p.totalBytes += int64(len(data))

	return offset, nil
}

// ReadAt reads a message at the specified offset
func (p *Partition) ReadAt(offset int64) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, ErrPartitionClosed
	}

	segment := p.findSegmentForOffset(offset)
	if segment == nil {
		return nil, ErrOffsetOutOfRange
	}

	position, err := segment.FindPosition(offset)
	if err != nil {
		return nil, err
	}

	lengthBuf := make([]byte, 4)
	if _, err := segment.ReadAt(position, lengthBuf); err != nil {
		return nil, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "failed to read message length",
			Cause:   err,
		}
	}

	messageLength := binary.BigEndian.Uint32(lengthBuf)
	if messageLength == 0 {
		return nil, errors.New("invalid message length")
	}

	messageBuf := make([]byte, messageLength)
	if _, err := segment.ReadAt(position+4, messageBuf); err != nil {
		return nil, &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "failed to read message data",
			Cause:   err,
		}
	}

	return messageBuf, nil
}

// findSegmentForOffset finds the segment that contains the given offset
func (p *Partition) findSegmentForOffset(offset int64) *Segment {
	for i := len(p.segments) - 1; i >= 0; i-- {
		segment := p.segments[i]
		if offset >= segment.BaseOffset && offset < segment.BaseOffset+segment.WriteCount {
			return segment
		}
	}
	return nil
}

// ReadRange reads messages in a range starting from offset
func (p *Partition) ReadRange(startOffset int64, maxBytes int32) ([][]byte, int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, startOffset, ErrPartitionClosed
	}

	var messages [][]byte
	currentOffset := startOffset
	totalBytes := int32(0)

	for totalBytes < maxBytes && len(messages) < 1000 { // Limit to 1000 messages per read
		messageData, err := p.ReadAt(currentOffset)
		if err != nil {
			if err == ErrOffsetOutOfRange {
				break
			}
			return messages, currentOffset, err
		}

		messages = append(messages, messageData)
		totalBytes += int32(len(messageData))
		currentOffset++
	}

	return messages, currentOffset, nil
}

// GetHighWaterMark returns the highest offset + 1 (next offset to be written)
func (p *Partition) GetHighWaterMark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.active == nil {
		return 0
	}

	return p.active.BaseOffset + p.active.WriteCount
}

// GetLowWaterMark returns the lowest available offset
func (p *Partition) GetLowWaterMark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.segments) == 0 {
		return 0
	}

	return p.segments[0].BaseOffset
}

// Sync forces all segments to sync to disk
func (p *Partition) Sync() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return ErrPartitionClosed
	}

	for _, segment := range p.segments {
		if err := segment.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// Close closes all segments
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	var errs []error
	for _, segment := range p.segments {
		if err := segment.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	p.closed = true
	p.active = nil

	if len(errs) > 0 {
		return &typederrors.TypedError{
			Type:    typederrors.StorageError,
			Message: "failed to close some segments",
			Cause:   fmt.Errorf("%v", errs),
		}
	}

	log.Printf("Closed partition at %s", p.DataDir)
	return nil
}

// GetMetrics returns partition metrics
func (p *Partition) GetMetrics() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return map[string]interface{}{
		"total_messages":  p.totalMessages,
		"total_bytes":     p.totalBytes,
		"segment_count":   len(p.segments),
		"high_water_mark": p.GetHighWaterMark(),
		"low_water_mark":  p.GetLowWaterMark(),
		"is_closed":       p.closed,
	}
}

// Cleanup removes old segments based on retention policy
func (p *Partition) Cleanup() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPartitionClosed
	}

	now := time.Now()
	var segmentsToRemove []*Segment

	// Find segments that are expired
	for i, segment := range p.segments {
		// Don't remove the active segment
		if segment == p.active {
			continue
		}

		// Check time-based retention
		if p.Config.RetentionTime > 0 && now.Sub(segment.MaxTimestamp) > p.Config.RetentionTime {
			segmentsToRemove = append(segmentsToRemove, segment)
			continue
		}

		// Check size-based retention (keep at least one segment)
		if p.Config.RetentionSize > 0 && len(p.segments) > 1 {
			totalSize := int64(0)
			for j := i; j < len(p.segments); j++ {
				totalSize += p.segments[j].CurrentSize
			}
			if totalSize > p.Config.RetentionSize {
				segmentsToRemove = append(segmentsToRemove, segment)
			}
		}
	}

	// Remove expired segments
	for _, segment := range segmentsToRemove {
		if err := p.removeSegment(segment); err != nil {
			log.Printf("Failed to remove segment: %v", err)
		}
	}

	return nil
}

// removeSegment removes a segment from the partition
func (p *Partition) removeSegment(segmentToRemove *Segment) error {
	// Close the segment
	if err := segmentToRemove.Close(); err != nil {
		return err
	}

	baseOffset := segmentToRemove.BaseOffset
	logPath := filepath.Join(p.DataDir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(p.DataDir, fmt.Sprintf("%020d.index", baseOffset))
	timeIndexPath := filepath.Join(p.DataDir, fmt.Sprintf("%020d.timeindex", baseOffset))

	os.Remove(logPath)
	os.Remove(indexPath)
	os.Remove(timeIndexPath)

	for i, segment := range p.segments {
		if segment == segmentToRemove {
			p.segments = append(p.segments[:i], p.segments[i+1:]...)
			break
		}
	}

	log.Printf("Removed segment at offset %d", baseOffset)
	return nil
}
