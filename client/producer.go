package client

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math/big"
	"net"
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
	ProducerID     string
	SequenceNumber int64
	AsyncIO        bool
	Topic          string
	Partition      int32
	Key            []byte
	Value          []byte
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
	partition := messages[0].Partition

	for _, msg := range messages {
		if msg.Topic != topic {
			return nil, fmt.Errorf("batch messages must belong to the same topic")
		}
		if msg.Partition != partition {
			return nil, fmt.Errorf("batch messages must belong to the same topic and partition")
		}
	}

	if partition <= 0 {
		var err error
		partition, err = p.selectPartition(&messages[0])
		if err != nil {
			return nil, fmt.Errorf("failed to select partition: %v", err)
		}
		producerID := p.client.GetProducerID()
		stateManager := p.client.GetDeduplicatorManager()

		for i := range messages {
			messages[i].Partition = partition
			messages[i].AsyncIO = p.client.config.EnableAsyncIO
			if p.client.IsdeduplicatorEnabled() {
				messages[i].ProducerID = producerID
				messages[i].SequenceNumber = stateManager.GetNextSequenceNumber(producerID, messages[i].Partition)
			}

		}
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
	metadata, err := p.client.getTopicMetadata(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic metadata: %v", err)
	}

	partitionMeta, exists := metadata.Partitions[partition]
	if !exists {
		return nil, fmt.Errorf("partition %d not found for topic %s", partition, topic)
	}

	requestData, err := p.buildProduceRequest(topic, partition, messages)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	var result *ProduceResult
	if p.client.config.EnableConnectionPool && p.client.connectionPool != nil {
		result, err = p.sendWithConnectionPool(partitionMeta.Leader, requestData)
	} else {
		result, err = p.sendWithDirectConnection(partitionMeta.Leader, requestData)
	}
	if err != nil {
		return nil, err
	}

	result.Topic = topic
	result.Partition = partition
	return result, nil
}

func (p *Producer) sendWithConnectionPool(brokerAddr string, requestData []byte) (*ProduceResult, error) {
	if p.client.config.EnableAsyncIO && p.client.asyncIO != nil {
		return p.sendWithAsyncConnection(brokerAddr, requestData)
	}

	conn, err := p.client.connectionPool.GetConnection(brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection from pool: %v", err)
	}
	defer conn.Return()

	return p.sendSynchronously(conn, requestData)
}

func (p *Producer) sendWithAsyncConnection(brokerAddr string, requestData []byte) (*ProduceResult, error) {
	handler := func(responseData []byte) (interface{}, error) {
		return p.parseProduceResponse(responseData)
	}

	callback := func(result interface{}, err error) {
		if err != nil {
			// TODO:保存错误信息，类似死信队列
			// 需要记录requestData,topic,partition
			return
		}
	}

	err := p.client.AsyncRequestWithCallback(brokerAddr, protocol.ProduceRequestType, requestData, handler, callback)
	if err != nil {
		return p.sendWithConnectionPoolFallback(brokerAddr, requestData)
	}
	return &ProduceResult{}, nil
}

func (p *Producer) sendWithConnectionPoolFallback(brokerAddr string, requestData []byte) (*ProduceResult, error) {
	if p.client.connectionPool == nil {
		return nil, fmt.Errorf("no connection pool available for fallback")
	}

	conn, err := p.client.connectionPool.GetConnection(brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("fallback connection pool failed: %v", err)
	}
	defer conn.Return()

	return p.sendSynchronously(conn, requestData)
}

func (p *Producer) sendWithDirectConnection(brokerAddr string, requestData []byte) (*ProduceResult, error) {
	conn, err := protocol.ConnectToSpecificBroker(brokerAddr, p.client.timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %v", err)
	}
	defer conn.Close()

	return p.sendSynchronously(conn, requestData)
}

func (p *Producer) sendSynchronously(conn net.Conn, requestData []byte) (*ProduceResult, error) {
	conn.SetDeadline(time.Now().Add(p.client.timeout))

	requestType := protocol.ProduceRequestType
	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		return nil, fmt.Errorf("failed to send request type: %v", err)
	}

	// Send data length first, then the actual data
	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return nil, fmt.Errorf("failed to send data length: %v", err)
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

	return p.parseProduceResponse(responseData)
}

// buildProduceRequest builds produce request
func (p *Producer) buildProduceRequest(topic string, partition int32, messages []ProduceMessage) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int32(len(topic))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(topic); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, partition); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int32(len(messages))); err != nil {
		return nil, err
	}

	for _, msg := range messages {
		producerIDLen := int32(len(msg.ProducerID))
		if err := binary.Write(buf, binary.BigEndian, producerIDLen); err != nil {
			return nil, err
		}
		if producerIDLen > 0 {
			if _, err := buf.WriteString(msg.ProducerID); err != nil {
				return nil, err
			}
		}

		if err := binary.Write(buf, binary.BigEndian, msg.SequenceNumber); err != nil {
			return nil, err
		}

		asyncIOByte := byte(0)
		if msg.AsyncIO {
			asyncIOByte = byte(1)
		}
		if err := binary.Write(buf, binary.BigEndian, asyncIOByte); err != nil {
			return nil, err
		}

		keyLen := int32(0)
		if msg.Key != nil {
			keyLen = int32(len(msg.Key))
		}
		if err := binary.Write(buf, binary.BigEndian, keyLen); err != nil {
			return nil, err
		}
		if keyLen > 0 {
			if _, err := buf.Write(msg.Key); err != nil {
				return nil, err
			}
		}

		if err := binary.Write(buf, binary.BigEndian, int32(len(msg.Value))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(msg.Value); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// ProduceResponseFromBroker represents the JSON response from broker
type ProduceResponseFromBroker struct {
	Topic     string                    `json:"topic"`
	Partition int32                     `json:"partition"`
	Results   []ProduceResultFromBroker `json:"results"`
	ErrorCode int16                     `json:"error_code"`
	Error     string                    `json:"error,omitempty"`
}

// ProduceResultFromBroker represents individual message result from broker
type ProduceResultFromBroker struct {
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
	Error     string `json:"error,omitempty"`
}

func (p *Producer) parseProduceResponse(data []byte) (*ProduceResult, error) {
	var brokerResponse ProduceResponseFromBroker
	if err := json.Unmarshal(data, &brokerResponse); err == nil {
		if brokerResponse.ErrorCode != 0 || brokerResponse.Error != "" {
			return &ProduceResult{
				Error: fmt.Errorf("broker error (code %d): %s", brokerResponse.ErrorCode, brokerResponse.Error),
			}, nil
		}

		if len(brokerResponse.Results) == 0 {
			return &ProduceResult{
				Error: fmt.Errorf("no results in response"),
			}, nil
		}

		firstResult := brokerResponse.Results[0]
		result := &ProduceResult{
			Offset: firstResult.Offset,
		}

		if firstResult.Error != "" {
			result.Error = fmt.Errorf("%s", firstResult.Error)
		}

		return result, nil
	}

	buf := bytes.NewReader(data)
	result := &ProduceResult{}

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return nil, fmt.Errorf("failed to parse response: not JSON and not valid binary format: %v", err)
	}

	if errorCode != 0 {
		var errorMsgLen int32
		if err := binary.Read(buf, binary.BigEndian, &errorMsgLen); err != nil {
			return nil, fmt.Errorf("failed to read error message length: %v", err)
		}

		errorMsg := make([]byte, errorMsgLen)
		if _, err := buf.Read(errorMsg); err != nil {
			return nil, fmt.Errorf("failed to read error message: %v", err)
		}

		result.Error = fmt.Errorf("server error (code %d): %s", errorCode, string(errorMsg))
		return result, nil
	}

	buf = bytes.NewReader(data)
	var baseOffset int64
	if err := binary.Read(buf, binary.BigEndian, &baseOffset); err != nil {
		return nil, fmt.Errorf("failed to read base offset: %v", err)
	}
	result.Offset = baseOffset

	binary.Read(buf, binary.BigEndian, &errorCode)

	return result, nil
}
