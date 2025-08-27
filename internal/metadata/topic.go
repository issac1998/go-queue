package metadata

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/storage"
)

const (
	DefaultSegmentSize = 1 << 30
	DefaultPartitions  = 1
)

// Topic defines a topic
type Topic struct {
	Name       string
	Partitions map[int32]*Partition
	Config     *TopicConfig
	mu         sync.RWMutex
}

// Partition defines a partition
type Partition struct {
	ID         int32
	Topic      string
	DataDir    string
	Segments   map[int]*storage.Segment
	ActiveSeg  *storage.Segment
	MaxSegSize int64
	Mu         sync.RWMutex
}

// NewPartition creates a new partition
func NewPartition(id int32, topic string, sysConfig *Config) (*Partition, error) {
	dataDir := filepath.Join(sysConfig.DataDir, topic, fmt.Sprintf("partition-%d", id))
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create partition dir failed: %v", err)
	}

	seg, err := storage.NewSegment(dataDir, 0, sysConfig.SegmentSize)
	if err != nil {
		return nil, fmt.Errorf("create segment failed: %v", err)
	}

	p := &Partition{
		ID:         id,
		Topic:      topic,
		DataDir:    dataDir,
		Segments:   map[int]*storage.Segment{0: seg},
		ActiveSeg:  seg,
		MaxSegSize: sysConfig.SegmentSize,
		Mu:         sync.RWMutex{},
	}
	return p, nil
}

// Append appends a message to the partition
func (p *Partition) Append(msg []byte) (int64, error) {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	var activeSegment *storage.Segment
	for _, seg := range p.Segments {
		activeSegment = seg
		break
		// TODO:decide to write into the last or random segment
	}

	offset, err := activeSegment.Append(msg, time.Now())
	if err != nil {
		if err.Error() == "segment is full" {
			newSegment, err := storage.NewSegment(
				p.DataDir,
				activeSegment.BaseOffset+activeSegment.CurrentSize,
				DefaultSegmentSize,
			)
			if err != nil {
				return 0, fmt.Errorf("create new segment failed: %v", err)
			}

			p.Segments[len(p.Segments)] = newSegment
			activeSegment = newSegment

			// retry
			offset, err = activeSegment.Append(msg, time.Now())
			if err != nil {
				return 0, fmt.Errorf("append to new segment failed: %v", err)
			}
		} else {
			return 0, err
		}
	}

	return offset, nil
}

// Read
func (p *Partition) Read(offset int64, maxBytes int32) ([][]byte, int64, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	var targetSegment *storage.Segment
	for _, segment := range p.Segments {
		if offset >= segment.BaseOffset && offset < segment.BaseOffset+segment.WriteCount {
			targetSegment = segment
			break
		}
	}

	if targetSegment == nil {
		return nil, 0, fmt.Errorf("offset %d not found", offset)
	}

	pos, err := targetSegment.FindPosition(offset)
	if err != nil {
		return nil, 0, err
	}

	messages, nextOffset, err := readMessagesFromSegment(targetSegment, pos, int64(maxBytes))
	if err != nil {
		return nil, 0, err
	}

	return messages, nextOffset, nil
}

// readMessagesFromSegment
func readMessagesFromSegment(segment *storage.Segment, startPos int64, maxBytes int64) ([][]byte, int64, error) {
	var messages [][]byte
	currentPos := startPos
	totalBytes := int64(0)
	messageCount := int64(0)
	// read unitils maxBytes or EOF
	for totalBytes < maxBytes {
		lenBuf := make([]byte, 4)
		n, err := segment.ReadAt(currentPos, lenBuf)
		if err != nil || n < 4 {
			break
		}

		msgSize := int64(binary.BigEndian.Uint32(lenBuf))
		currentPos += 4

		if msgSize <= 0 || msgSize > maxBytes-totalBytes {
			break
		}

		msgBuf := make([]byte, msgSize)
		n, err = segment.ReadAt(currentPos, msgBuf)
		if err != nil || int64(n) < msgSize {
			break
		}

		messages = append(messages, msgBuf)
		currentPos += msgSize
		totalBytes += msgSize + 4
		messageCount++
	}

	// TODO:Test logic below
	var nextOffset int64
	if messageCount > 0 {
		// 找到起始offset，然后加上读取的消息数量
		startOffset := int64(-1)
		for _, entry := range segment.IndexEntries {
			if entry.Position == startPos {
				startOffset = entry.Offset
				break
			}
		}

		if startOffset >= 0 {
			nextOffset = startOffset + messageCount
		} else {
			// 如果找不到精确的起始位置，基于segment基础offset计算
			nextOffset = segment.BaseOffset + messageCount
		}
	} else {
		// 没有读取到消息，返回末尾offset
		nextOffset = segment.BaseOffset + segment.WriteCount
	}

	return messages, nextOffset, nil
}

// Close closes the partition and its segments
func (p *Partition) Close() error {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	var errs []error
	for _, segment := range p.Segments {
		if err := segment.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("close partition failed: %v", errs)
	}
	return nil
}
