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

func NewPartition(id int32, topic string, sysConfig *Config) (*Partition, error) {
	// 构建分区数据目录
	dataDir := filepath.Join(sysConfig.DataDir, topic, fmt.Sprintf("partition-%d", id))
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create partition dir failed: %v", err)
	}

	// 创建第一个 segment
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

var (
	topics     = make(map[string]*Topic)
	topicsLock sync.RWMutex
)

// CreateTopic creates a new topic
func CreateTopic(name string, numPartitions int32, dataDir string) (*Topic, error) {
	topicsLock.Lock()
	defer topicsLock.Unlock()

	if _, exists := topics[name]; exists {
		return nil, fmt.Errorf("topic %s already exists", name)
	}

	topic := &Topic{
		Name:       name,
		Partitions: make(map[int32]*Partition),
	}

	for i := int32(0); i < numPartitions; i++ {
		partition, err := createPartition(name, i, dataDir)
		if err != nil {
			return nil, fmt.Errorf("create partition %d failed: %v", i, err)
		}
		topic.Partitions[i] = partition
	}

	// 保存主题
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
	// 创建分区目录
	partitionDir := filepath.Join(dataDir, topic, fmt.Sprintf("partition-%d", id))

	// 创建初始 Segment
	segment, err := storage.NewSegment(partitionDir, 0, DefaultSegmentSize)
	if err != nil {
		return nil, fmt.Errorf("create initial segment failed: %v", err)
	}

	return &Partition{
		ID:       id,
		Topic:    topic,
		Segments: map[int]*storage.Segment{0: segment},
		DataDir:  partitionDir,
	}, nil
}

// GetPartition defins
func GetPartition(topic string, partitionID int32) (*Partition, error) {
	// 获取主题
	t, err := GetTopic(topic)
	if err != nil {
		return nil, err
	}

	// 检查分区ID是否有效
	if partitionID < 0 || int(partitionID) >= len(t.Partitions) {
		return nil, fmt.Errorf("invalid partition ID: %d", partitionID)
	}

	return t.Partitions[partitionID], nil
}

// Close 关闭分区
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

// Append 追加消息到分区
func (p *Partition) Append(msg []byte) (int64, error) {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	// 获取当前活跃的 Segment
	var activeSegment *storage.Segment
	for _, seg := range p.Segments {
		activeSegment = seg
		break // 取第一个，简化实现
	}

	// 尝试追加消息
	offset, err := activeSegment.Append(msg, time.Now())
	if err != nil {
		if err.Error() == "segment is full" {
			// 创建新的 Segment
			newSegment, err := storage.NewSegment(
				p.DataDir,
				activeSegment.BaseOffset+activeSegment.CurrentSize,
				DefaultSegmentSize,
			)
			if err != nil {
				return 0, fmt.Errorf("create new segment failed: %v", err)
			}

			// 添加到分区
			nextID := len(p.Segments)
			p.Segments[nextID] = newSegment
			activeSegment = newSegment

			// 重试追加
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

// Read 从分区读取消息
func (p *Partition) Read(offset int64, maxBytes int32) ([][]byte, int64, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	// 查找包含目标 offset 的 Segment (使用消息计数范围)
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

	// 查找消息位置
	pos, err := targetSegment.FindPosition(offset)
	if err != nil {
		return nil, 0, err
	}

	// 读取消息
	messages, nextOffset, err := readMessagesFromSegment(targetSegment, pos, int64(maxBytes))
	if err != nil {
		return nil, 0, err
	}

	return messages, nextOffset, nil
}

// readMessagesFromSegment 从 Segment 读取消息
func readMessagesFromSegment(segment *storage.Segment, startPos int64, maxBytes int64) ([][]byte, int64, error) {
	var messages [][]byte
	currentPos := startPos
	totalBytes := int64(0)
	messageCount := int64(0)

	for totalBytes < maxBytes {
		// 读取消息长度
		lenBuf := make([]byte, 4)
		n, err := segment.ReadAt(currentPos, lenBuf)
		if err != nil || n < 4 {
			// 到达文件末尾或者读取不足
			break
		}

		msgSize := int64(binary.BigEndian.Uint32(lenBuf))
		currentPos += 4

		// 检查消息长度是否有效
		if msgSize <= 0 {
			// 遇到无效消息长度，停止读取
			break
		}
		if msgSize > maxBytes-totalBytes {
			// 剩余空间不足，停止读取
			break
		}

		// 读取消息内容
		msgBuf := make([]byte, msgSize)
		n, err = segment.ReadAt(currentPos, msgBuf)
		if err != nil || int64(n) < msgSize {
			// 读取失败或不足，停止读取
			break
		}

		messages = append(messages, msgBuf)
		currentPos += msgSize
		totalBytes += msgSize + 4
		messageCount++
	}

	// 返回下一个消息的offset
	// 如果读取到了消息，nextOffset应该是最后一条消息的offset + 1
	// 如果没有读取到消息，返回当前的segment末尾offset
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
		// 没有读取到消息，返回当前的写入位置
		nextOffset = segment.BaseOffset + segment.WriteCount
	}

	return messages, nextOffset, nil
}
