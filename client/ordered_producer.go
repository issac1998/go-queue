package client

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// OrderedProducer producer
type OrderedProducer struct {
	client      *Client
	partitioner *OrderedPartitioner
}

// OrderedPartitioner pratitioner
type OrderedPartitioner struct {
	messageGroupPartitions map[string]int32
}

// NewOrderedPartitioner create new ordered partitioner
func NewOrderedPartitioner() *OrderedPartitioner {
	return &OrderedPartitioner{
		messageGroupPartitions: make(map[string]int32),
	}
}

// SelectPartitionForMessageGroup select partition for message group
func (op *OrderedPartitioner) SelectPartitionForMessageGroup(messageGroup string, numPartitions int32) int32 {
	if partition, exists := op.messageGroupPartitions[messageGroup]; exists {
		return partition
	}

	partition := op.hashMessageGroup(messageGroup, numPartitions)
	op.messageGroupPartitions[messageGroup] = partition
	return partition
}

func (op *OrderedPartitioner) hashMessageGroup(messageGroup string, numPartitions int32) int32 {
	if numPartitions <= 0 {
		return 0
	}

	h := fnv.New64a()
	h.Write([]byte(fmt.Sprintf("%s", messageGroup)))
	return int32(h.Sum64()) % (numPartitions)
}

// OrderedMessage message
type OrderedMessage struct {
	Key          []byte            `json:"key,omitempty"`
	Value        []byte            `json:"value"`
	Headers      map[string]string `json:"headers,omitempty"`
	MessageGroup string            `json:"message_group"`
}

// OrderedProduceRequest request
type OrderedProduceRequest struct {
	Topic    string           `json:"topic"`
	Messages []OrderedMessage `json:"messages"`
}

// OrderedProduceResult result
type OrderedProduceResult struct {
	Topic               string                               `json:"topic"`
	PartitionResponses  map[string]*PartitionProduceResponse `json:"partition_responses"`
	PartitionErrors     map[string]string                    `json:"partition_errors"`
	TotalMessages       int                                  `json:"total_messages"`
	SuccessfulMessages  int                                  `json:"successful_messages"`
	OverallErrorCode    int16                                `json:"overall_error_code"`
	MessageGroupRouting map[string]int32                     `json:"message_group_routing"`
}

// PartitionProduceResponse partition produce response
type PartitionProduceResponse struct {
	Partition int32                  `json:"partition"`
	Results   []MessageProduceResult `json:"results"`
	ErrorCode int16                  `json:"error_code"`
	Error     string                 `json:"error,omitempty"`
}

// MessageProduceResult message
type MessageProduceResult struct {
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Error     string    `json:"error,omitempty"`
}

// NewOrderedProducer create ordered producer
func NewOrderedProducer(client *Client) *OrderedProducer {
	return &OrderedProducer{
		client:      client,
		partitioner: NewOrderedPartitioner(),
	}
}

// SendOrderedMessages send ordered messages
func (op *OrderedProducer) SendOrderedMessages(topic string, messages []OrderedMessage) (*OrderedProduceResult, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages to send")
	}

	request := OrderedProduceRequest{
		Topic:    topic,
		Messages: messages,
	}

	return op.sendOrderedProduceRequest(request)
}

// SendSingleOrderedMessage send single ordered message
func (op *OrderedProducer) SendSingleOrderedMessage(topic string, messageGroup string, key []byte, value []byte) (*OrderedProduceResult, error) {
	message := OrderedMessage{
		Key:          key,
		Value:        value,
		MessageGroup: messageGroup,
		Headers:      make(map[string]string),
	}

	return op.SendOrderedMessages(topic, []OrderedMessage{message})
}

// SendMessageGroupBatch send message group batch
func (op *OrderedProducer) SendMessageGroupBatch(topic string, messageGroup string, messages []OrderedMessage) (*OrderedProduceResult, error) {
	for i := range messages {
		messages[i].MessageGroup = messageGroup
	}

	return op.SendOrderedMessages(topic, messages)
}

// sendOrderedProduceRequest
func (op *OrderedProducer) sendOrderedProduceRequest(request OrderedProduceRequest) (*OrderedProduceResult, error) {
	topicMeta, err := op.client.getTopicMetadata(request.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic metadata: %w", err)
	}

	numPartitions := int32(len(topicMeta.Partitions))
	if numPartitions == 0 {
		return nil, fmt.Errorf("topic %s has no partitions", request.Topic)
	}

	partitionGroups := make(map[int32][]OrderedMessage)
	messageGroupRouting := make(map[string]int32)

	for _, msg := range request.Messages {
		partition := op.partitioner.SelectPartitionForMessageGroup(msg.MessageGroup, numPartitions)
		partitionGroups[partition] = append(partitionGroups[partition], msg)
		messageGroupRouting[msg.MessageGroup] = partition
	}

	result := &OrderedProduceResult{
		Topic:               request.Topic,
		PartitionResponses:  make(map[string]*PartitionProduceResponse),
		PartitionErrors:     make(map[string]string),
		TotalMessages:       len(request.Messages),
		MessageGroupRouting: messageGroupRouting,
	}

	successCount := 0
	for partition, messages := range partitionGroups {
		partitionResult, err := op.sendToPartition(request.Topic, partition, messages)
		partitionKey := fmt.Sprintf("%d", partition)

		if err != nil {
			result.PartitionErrors[partitionKey] = err.Error()
			result.OverallErrorCode = -1
		} else {
			result.PartitionResponses[partitionKey] = partitionResult
			successCount += len(messages)
		}
	}

	result.SuccessfulMessages = successCount
	return result, nil
}

func (op *OrderedProducer) sendToPartition(topic string, partition int32, messages []OrderedMessage) (*PartitionProduceResponse, error) {
	conn, err := op.client.connectForDataOperation(topic, partition, true)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to partition %d leader: %w", partition, err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(op.client.timeout))

	if err := binary.Write(conn, binary.BigEndian, protocol.OrderedProduceRequestType); err != nil {
		return nil, fmt.Errorf("failed to write request type: %w", err)
	}

	partitionRequest := OrderedProduceRequest{
		Topic:    topic,
		Messages: messages,
	}

	requestData, err := json.Marshal(partitionRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return nil, fmt.Errorf("failed to write request length: %w", err)
	}

	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to write request data: %w", err)
	}

	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}

	responseData := make([]byte, responseLen)
	if _, err := conn.Read(responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}

	var response PartitionProduceResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &response, nil
}
