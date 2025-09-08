package client

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"math/big"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// Producer message producer
type Producer struct {
	client      *Client
	partitioner Partitioner
}

// Partitioner defines the interface for partition selection strategies
type Partitioner interface {
	Partition(message *ProduceMessage, numPartitions int32) (int32, error)
}

// PartitionStrategy represents different partitioning strategies
type PartitionStrategy int

const (
	PartitionStrategyManual PartitionStrategy = iota
	PartitionStrategyRoundRobin
	PartitionStrategyRandom
	PartitionStrategyHash
)

// NewProducer creates a new producer with default manual partitioning
func NewProducer(client *Client) *Producer {
	return &Producer{
		client:      client,
		partitioner: &ManualPartitioner{},
	}
}

// NewProducerWithStrategy creates a new producer with specified partitioning strategy
func NewProducerWithStrategy(client *Client, strategy PartitionStrategy) *Producer {
	var partitioner Partitioner

	switch strategy {
	case PartitionStrategyRoundRobin:
		partitioner = &RoundRobinPartitioner{}
	case PartitionStrategyRandom:
		partitioner = &RandomPartitioner{}
	case PartitionStrategyHash:
		partitioner = &HashPartitioner{}
	default:
		partitioner = &ManualPartitioner{}
	}

	return &Producer{
		client:      client,
		partitioner: partitioner,
	}
}

// ProduceMessage single message structure
type ProduceMessage struct {
	Topic     string
	Partition int32
	Key       []byte
	Value     []byte
}

// ProduceResult production result
type ProduceResult struct {
	Topic     string
	Partition int32
	Offset    int64
	Error     error
}

// Send sends a single message
func (p *Producer) Send(msg ProduceMessage) (*ProduceResult, error) {
	return p.SendBatch([]ProduceMessage{msg})
}

// SendBatch sends messages in batch
func (p *Producer) SendBatch(messages []ProduceMessage) (*ProduceResult, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("message list cannot be empty")
	}

	topic := messages[0].Topic
	for _, msg := range messages {
		if msg.Topic != topic {
			return nil, fmt.Errorf("batch messages must belong to the same topic")
		}
	}

	partition, err := p.selectPartition(&messages[0])
	if err != nil {
		return nil, fmt.Errorf("failed to select partition: %v", err)
	}

	for i := range messages {
		messages[i].Partition = partition
	}

	return p.sendToPartitionLeader(topic, partition, messages)
}

// selectPartition selects appropriate partition for a message
func (p *Producer) selectPartition(msg *ProduceMessage) (int32, error) {
	if msg.Partition >= 0 {
		return msg.Partition, nil
	}

	topicMeta, err := p.client.getTopicMetadata(msg.Topic)
	if err != nil {
		return 0, fmt.Errorf("failed to get topic metadata: %v", err)
	}

	numPartitions := int32(len(topicMeta.Partitions))
	return p.partitioner.Partition(msg, numPartitions)
}

// ManualPartitioner uses manually specified partitions
type ManualPartitioner struct{}

func (mp *ManualPartitioner) Partition(message *ProduceMessage, numPartitions int32) (int32, error) {
	if message.Partition < 0 {
		return 0, fmt.Errorf("partition must be specified for manual partitioning")
	}
	if message.Partition >= numPartitions {
		return 0, fmt.Errorf("partition %d exceeds available partitions %d", message.Partition, numPartitions)
	}
	return message.Partition, nil
}

// RoundRobinPartitioner distributes messages across partitions in round-robin fashion
type RoundRobinPartitioner struct {
	counter int32
}

func (rrp *RoundRobinPartitioner) Partition(message *ProduceMessage, numPartitions int32) (int32, error) {
	if numPartitions <= 0 {
		return 0, fmt.Errorf("invalid number of partitions: %d", numPartitions)
	}

	partition := rrp.counter % numPartitions
	rrp.counter++
	return partition, nil
}

// RandomPartitioner randomly selects a partition
type RandomPartitioner struct{}

func (rp *RandomPartitioner) Partition(message *ProduceMessage, numPartitions int32) (int32, error) {
	if numPartitions <= 0 {
		return 0, fmt.Errorf("invalid number of partitions: %d", numPartitions)
	}

	randomNum, err := rand.Int(rand.Reader, big.NewInt(int64(numPartitions)))
	if err != nil {
		return 0, fmt.Errorf("failed to generate random number: %v", err)
	}

	return int32(randomNum.Int64()), nil
}

// HashPartitioner uses hash of message key to determine partition
type HashPartitioner struct{}

func (hp *HashPartitioner) Partition(message *ProduceMessage, numPartitions int32) (int32, error) {
	if numPartitions <= 0 {
		return 0, fmt.Errorf("invalid number of partitions: %d", numPartitions)
	}

	// If no key is provided, use random partitioning
	if len(message.Key) == 0 {
		randomNum, err := rand.Int(rand.Reader, big.NewInt(int64(numPartitions)))
		if err != nil {
			return 0, fmt.Errorf("failed to generate random number: %v", err)
		}
		return int32(randomNum.Int64()), nil
	}

	// Hash the key
	hasher := fnv.New32a()
	hasher.Write(message.Key)
	hash := hasher.Sum32()

	return int32(hash) % numPartitions, nil
}

// sendToPartitionLeader sends messages directly to the partition leader
func (p *Producer) sendToPartitionLeader(topic string, partition int32, messages []ProduceMessage) (*ProduceResult, error) {
	conn, err := p.client.connectForDataOperation(topic, partition, true)
	if err != nil {
		p.client.refreshTopicMetadata(topic)
		return nil, fmt.Errorf("failed to connect to partition leader: %v", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(p.client.timeout))

	requestData, err := p.buildProduceRequest(topic, partition, messages)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	requestType := protocol.ProduceRequestType
	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		return nil, fmt.Errorf("failed to send request type: %v", err)
	}

	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to send request data: %v", err)
	}

	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return nil, fmt.Errorf("failed to read response length: %v", err)
	}

	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}

	// TODO: refreshMetadata if it's a follower error

	result, err := p.parseProduceResponse(topic, partition, responseData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	return result, nil
}

// buildProduceRequest builds produce request
func (p *Producer) buildProduceRequest(topic string, partition int32, messages []ProduceMessage) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ProtocolVersion)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int16(len(topic))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(topic); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, partition); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int8(protocol.CompressionNone)); err != nil {
		return nil, err
	}

	messagesBuf := new(bytes.Buffer)
	for _, msg := range messages {
		if err := binary.Write(messagesBuf, binary.BigEndian, int32(len(msg.Value))); err != nil {
			return nil, err
		}
		if _, err := messagesBuf.Write(msg.Value); err != nil {
			return nil, err
		}
	}

	if err := binary.Write(buf, binary.BigEndian, int32(messagesBuf.Len())); err != nil {
		return nil, err
	}

	if _, err := buf.Write(messagesBuf.Bytes()); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// parseProduceResponse parses produce response
func (p *Producer) parseProduceResponse(topic string, partition int32, data []byte) (*ProduceResult, error) {
	buf := bytes.NewReader(data)

	var baseOffset int64
	if err := binary.Read(buf, binary.BigEndian, &baseOffset); err != nil {
		return nil, fmt.Errorf("failed to read base offset: %v", err)
	}

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return nil, fmt.Errorf("failed to read error code: %v", err)
	}

	result := &ProduceResult{
		Topic:     topic,
		Partition: partition,
		Offset:    baseOffset,
	}

	if errorCode != 0 {
		result.Error = fmt.Errorf("server error, error code: %d", errorCode)
	}

	return result, nil
}
