package metadata

import (
	"encoding/binary"
	"fmt"
	"go-queue/internal/storage"
	"io"
	"path/filepath"
	"sync"
)

const (
	DefaultSegmentSize = 1 << 30
	DefaultPartitions  = 1
)

// Topic defines
type Topic struct {
	Name       string
	Partitions []*Partition
}

// Partition defines
type Partition struct {
	ID       int32
	Topic    string
	Segments []*storage.Segment
	Leader   string   
	Replicas []string 
	Isr      []string 
	Mu       sync.RWMutex
	DataDir  string
}

var (
	topics     = make(map[string]*Topic)
	topicsLock sync.RWMutex
)

// CreateTopic defines
func CreateTopic(name string, numPartitions int32, dataDir string) (*Topic, error) {
	topicsLock.Lock()
	defer topicsLock.Unlock()

	if _, exists := topics[name]; exists {
		return nil, fmt.Errorf("topic %s already exists", name)
	}

	topic := &Topic{
		Name:       name,
		Partitions: make([]*Partition, numPartitions),
	}

	for i := int32(0); i < numPartitions; i++ {
		partition, err := createPartition(name, i, dataDir)
		if err != nil {
			return nil, fmt.Errorf("create partition %d failed: %v", i, err)
		}
		topic.Partitions[i] = partition
	}

	topics[name] = topic
	return topic, nil
}

// GetTopic defines
func GetTopic(name string) (*Topic, error) {
	topicsLock.RLock()
	defer topicsLock.RUnlock()

	topic, exists := topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", name)
	}
	return topic, nil
}

func createPartition(topic string, id int32, dataDir string) (*Partition, error) {
	partitionDir := filepath.Join(dataDir, topic, fmt.Sprintf("partition-%d", id))

	segment, err := storage.NewSegment(partitionDir, 0, DefaultSegmentSize)
	if err != nil {
		return nil, fmt.Errorf("create initial segment failed: %v", err)
	}

	return &Partition{
		ID:       id,
		Topic:    topic,
		Segments: []*storage.Segment{segment},
		DataDir:  partitionDir,
	}, nil
}

// GetPartition defins
func GetPartition(topic string, partitionID int32) (*Partition, error) {
	t, err := GetTopic(topic)
	if err != nil {
		return nil, err
	}

	if partitionID < 0 || int(partitionID) >= len(t.Partitions) {
		return nil, fmt.Errorf("invalid partition ID: %d", partitionID)
	}

	return t.Partitions[partitionID], nil
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

// Append 
func (p *Partition) Append(msg []byte) (int64, error) {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	activeSegment := p.Segments[len(p.Segments)-1]

	offset, err := activeSegment.Append(msg)
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

			p.Segments = append(p.Segments, newSegment)
			activeSegment = newSegment

			// retry
			offset, err = activeSegment.Append(msg)
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
		if offset >= segment.BaseOffset && offset < segment.BaseOffset+segment.CurrentSize {
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

	for totalBytes < maxBytes {
		lenBuf := make([]byte, 4)
		if _, err := segment.ReadAt(currentPos, lenBuf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, err
		}
		msgSize := int64(binary.BigEndian.Uint32(lenBuf))
		currentPos += 4

		if msgSize <= 0 || msgSize > maxBytes-totalBytes {
			return nil, 0, fmt.Errorf("invalid message length: %d", msgSize)
		}

		msgBuf := make([]byte, msgSize)
		if _, err := segment.ReadAt(currentPos, msgBuf); err != nil {
			return nil, 0, err
		}
		messages = append(messages, msgBuf)
		currentPos += int64(msgSize)
		totalBytes += int64(msgSize) + 4
	}

	return messages, segment.BaseOffset + currentPos, nil
}
