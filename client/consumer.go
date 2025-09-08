package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// Consumer message consumer
type Consumer struct {
	client           *Client
	subscribedTopics []string
	topicOffsets     map[string]map[int32]int64
	mu               sync.RWMutex
}

// NewConsumer creates a new consumer
func NewConsumer(client *Client) *Consumer {
	return &Consumer{
		client:           client,
		subscribedTopics: make([]string, 0),
		topicOffsets:     make(map[string]map[int32]int64),
	}
}

// FetchRequest fetch request
type FetchRequest struct {
	Topic     string
	Partition int32
	Offset    int64
	MaxBytes  int32
}

// Message message structure
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Value     []byte
}

// FetchResult fetch result
type FetchResult struct {
	Topic      string
	Partition  int32
	Messages   []Message
	NextOffset int64
	Error      error
}

// BatchFetchRequest represents a batch fetch request
type BatchFetchRequest struct {
	Topic     string
	Partition int32
	Ranges    []FetchRange
}

// FetchRange represents a single range in a batch fetch request
type FetchRange struct {
	Offset   int64
	MaxBytes int32
	MaxCount int32
}

// BatchFetchResult represents the result of a batch fetch
type BatchFetchResult struct {
	Topic     string
	Partition int32
	Results   []FetchRangeResult
	Error     error
}

// FetchRangeResult represents the result of a single fetch range
type FetchRangeResult struct {
	Messages   []Message
	NextOffset int64
	Error      error
}

// Fetch fetches messages
func (c *Consumer) Fetch(req FetchRequest) (*FetchResult, error) {
	if req.MaxBytes <= 0 {
		req.MaxBytes = 1024 * 1024
	}

	return c.fetchFromPartition(req)
}

// FetchFrom fetches messages starting from specified offset
func (c *Consumer) FetchFrom(topic string, partition int32, offset int64) (*FetchResult, error) {
	return c.Fetch(FetchRequest{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		MaxBytes:  1024 * 1024,
	})
}

// BatchFetch fetches multiple message ranges in a single request
func (c *Consumer) BatchFetch(req BatchFetchRequest) (*BatchFetchResult, error) {
	if len(req.Ranges) == 0 {
		return &BatchFetchResult{
			Topic:     req.Topic,
			Partition: req.Partition,
			Results:   []FetchRangeResult{},
			Error:     fmt.Errorf("empty batch request"),
		}, nil
	}

	if len(req.Ranges) == 1 {
		singleResult, err := c.Fetch(FetchRequest{
			Topic:     req.Topic,
			Partition: req.Partition,
			Offset:    req.Ranges[0].Offset,
			MaxBytes:  req.Ranges[0].MaxBytes,
		})

		if err != nil {
			return &BatchFetchResult{
				Topic:     req.Topic,
				Partition: req.Partition,
				Results:   []FetchRangeResult{},
				Error:     err,
			}, nil
		}

		return &BatchFetchResult{
			Topic:     req.Topic,
			Partition: req.Partition,
			Results: []FetchRangeResult{
				{
					Messages:   singleResult.Messages,
					NextOffset: singleResult.NextOffset,
					Error:      singleResult.Error,
				},
			},
		}, nil
	}

	return c.batchFetchFromPartition(req)
}

// FetchMultipleRanges is a convenience method for fetching multiple ranges
func (c *Consumer) FetchMultipleRanges(topic string, partition int32, ranges []FetchRange) (*BatchFetchResult, error) {
	return c.BatchFetch(BatchFetchRequest{
		Topic:     topic,
		Partition: partition,
		Ranges:    ranges,
	})
}

// buildFetchRequest builds fetch request
func (c *Consumer) buildFetchRequest(req FetchRequest) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ProtocolVersion)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int16(len(req.Topic))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(req.Topic); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, req.Partition); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, req.Offset); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, req.MaxBytes); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// parseFetchResponse parses fetch response
func (c *Consumer) parseFetchResponse(topic string, partition int32, requestOffset int64, data []byte) (*FetchResult, error) {
	buf := bytes.NewReader(data)

	result := &FetchResult{
		Topic:     topic,
		Partition: partition,
		Messages:  make([]Message, 0),
	}

	var topicLen int16
	if err := binary.Read(buf, binary.BigEndian, &topicLen); err != nil {
		return nil, fmt.Errorf("failed to read topic length: %v", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(buf, topicBytes); err != nil {
		return nil, fmt.Errorf("failed to read topic: %v", err)
	}

	var responsePartition int32
	if err := binary.Read(buf, binary.BigEndian, &responsePartition); err != nil {
		return nil, fmt.Errorf("failed to read partition: %v", err)
	}

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return nil, fmt.Errorf("failed to read error code: %v", err)
	}

	if errorCode != 0 {
		result.Error = fmt.Errorf("server error, error code: %d", errorCode)
		return result, nil
	}

	if err := binary.Read(buf, binary.BigEndian, &result.NextOffset); err != nil {
		return nil, fmt.Errorf("failed to read next offset: %v", err)
	}

	var messageCount int32
	if err := binary.Read(buf, binary.BigEndian, &messageCount); err != nil {
		return nil, fmt.Errorf("failed to read message count: %v", err)
	}

	currentOffset := requestOffset // Start from the requested offset
	for i := int32(0); i < messageCount; i++ {
		var msgLen int32
		if err := binary.Read(buf, binary.BigEndian, &msgLen); err != nil {
			return nil, fmt.Errorf("failed to read message %d length: %v", i, err)
		}

		if msgLen < 0 || msgLen > 1024*1024 { // Max 1MB per message
			return nil, fmt.Errorf("invalid message length: %d", msgLen)
		}

		msgData := make([]byte, msgLen)
		if _, err := io.ReadFull(buf, msgData); err != nil {
			return nil, fmt.Errorf("failed to read message %d content: %v", i, err)
		}

		message := Message{
			Topic:     topic,
			Partition: partition,
			Offset:    currentOffset,
			Value:     msgData,
		}
		result.Messages = append(result.Messages, message)
		currentOffset++
	}

	return result, nil
}

// fetchFromPartition fetches messages directly from the partition leader or follower
func (c *Consumer) fetchFromPartition(req FetchRequest) (*FetchResult, error) {
	conn, err := c.client.connectForDataOperation(req.Topic, req.Partition, false)
	if err != nil {
		// TODO: only do refresh if it's a follower error
		c.client.refreshTopicMetadata(req.Topic)
		return nil, fmt.Errorf("failed to connect to partition leader or follower: %v", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(c.client.timeout))

	requestData, err := c.buildFetchRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	requestType := protocol.FetchRequestType
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

	result, err := c.parseFetchResponse(req.Topic, req.Partition, req.Offset, responseData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	return result, nil
}

// batchFetchFromPartition fetches messages for multiple ranges from a single partition
func (c *Consumer) batchFetchFromPartition(req BatchFetchRequest) (*BatchFetchResult, error) {
	conn, err := c.client.connectForDataOperation(req.Topic, req.Partition, false)
	if err != nil {
		c.client.refreshTopicMetadata(req.Topic)
		return nil, fmt.Errorf("failed to connect to partition leader or follower: %v", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(c.client.timeout))

	requestData, err := c.buildBatchFetchRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to build batch request: %v", err)
	}

	requestType := protocol.BatchFetchRequestType
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

	result, err := c.parseBatchFetchResponse(req.Topic, req.Partition, responseData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse batch response: %v", err)
	}

	return result, nil
}

// buildBatchFetchRequest builds a batch fetch request
func (c *Consumer) buildBatchFetchRequest(req BatchFetchRequest) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ProtocolVersion)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int16(len(req.Topic))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(req.Topic); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, req.Partition); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int32(len(req.Ranges))); err != nil {
		return nil, err
	}

	for _, r := range req.Ranges {
		if err := binary.Write(buf, binary.BigEndian, r.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, r.MaxBytes); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, r.MaxCount); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// parseBatchFetchResponse parses a batch fetch response
func (c *Consumer) parseBatchFetchResponse(topic string, partition int32, data []byte) (*BatchFetchResult, error) {
	buf := bytes.NewReader(data)

	result := &BatchFetchResult{
		Topic:     topic,
		Partition: partition,
		Results:   make([]FetchRangeResult, 0),
	}

	var topicLen int16
	if err := binary.Read(buf, binary.BigEndian, &topicLen); err != nil {
		return nil, fmt.Errorf("failed to read topic length: %v", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(buf, topicBytes); err != nil {
		return nil, fmt.Errorf("failed to read topic: %v", err)
	}

	var responsePartition int32
	if err := binary.Read(buf, binary.BigEndian, &responsePartition); err != nil {
		return nil, fmt.Errorf("failed to read partition: %v", err)
	}

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return nil, fmt.Errorf("failed to read error code: %v", err)
	}

	if errorCode != 0 {
		result.Error = fmt.Errorf("server error, error code: %d", errorCode)
		return result, nil
	}

	var rangeCount int32
	if err := binary.Read(buf, binary.BigEndian, &rangeCount); err != nil {
		return nil, fmt.Errorf("failed to read range count: %v", err)
	}

	for i := int32(0); i < rangeCount; i++ {
		var rangeResult FetchRangeResult
		var rangeOffset int64
		if err := binary.Read(buf, binary.BigEndian, &rangeOffset); err != nil {
			return nil, fmt.Errorf("failed to read range %d offset: %v", i, err)
		}

		var rangeNextOffset int64
		if err := binary.Read(buf, binary.BigEndian, &rangeNextOffset); err != nil {
			return nil, fmt.Errorf("failed to read range %d next offset: %v", i, err)
		}

		var rangeErrorCode int16
		if err := binary.Read(buf, binary.BigEndian, &rangeErrorCode); err != nil {
			return nil, fmt.Errorf("failed to read range %d error code: %v", i, err)
		}

		if rangeErrorCode != 0 {
			rangeResult.Error = fmt.Errorf("server error in range %d, error code: %d", i, rangeErrorCode)
		} else {
			// For simplicity, set next offset directly (can be improved to parse messages)
			rangeResult.NextOffset = rangeNextOffset
			rangeResult.Messages = []Message{} // TODO: Implement proper message parsing for batch ranges
		}
		result.Results = append(result.Results, rangeResult)
	}

	return result, nil
}

// SubscribePartition simple consumer subscription for a single partition (starting from latest position)
func (c *Consumer) SubscribePartition(ctx context.Context, topic string, partition int32, handler func(Message) error) error {
	offset := int64(0)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		result, err := c.FetchFrom(topic, partition, offset)
		if err != nil {
			return fmt.Errorf("failed to fetch messages: %v", err)
		}

		if result.Error != nil {
			return fmt.Errorf("server error: %v", result.Error)
		}

		for _, msg := range result.Messages {
			if err := handler(msg); err != nil {
				return fmt.Errorf("failed to process message: %v", err)
			}
			offset = msg.Offset + 1
		}

		if len(result.Messages) == 0 {
			time.Sleep(100 * time.Millisecond)
		} // Update offset to next position
		if result.NextOffset > offset {
			offset = result.NextOffset
		}
	}
}

// Subscribe subscribes to topics for automatic consumption
func (c *Consumer) Subscribe(topics []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.subscribedTopics = topics

	// Initialize offsets for new topics
	for _, topic := range topics {
		if c.topicOffsets[topic] == nil {
			c.topicOffsets[topic] = make(map[int32]int64)
		}
	}

	return nil
}

// Unsubscribe unsubscribes from topics
func (c *Consumer) Unsubscribe(topics []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, topicToRemove := range topics {
		for i, subscribedTopic := range c.subscribedTopics {
			if subscribedTopic == topicToRemove {
				c.subscribedTopics = append(c.subscribedTopics[:i], c.subscribedTopics[i+1:]...)
				break
			}
		}
		delete(c.topicOffsets, topicToRemove)
	}

	return nil
}

// GetSubscription returns currently subscribed topics
func (c *Consumer) GetSubscription() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]string, len(c.subscribedTopics))
	copy(result, c.subscribedTopics)
	return result
}

// Poll polls messages from subscribed topics
func (c *Consumer) Poll(timeout time.Duration) ([]*Message, error) {
	c.mu.RLock()
	topics := make([]string, len(c.subscribedTopics))
	copy(topics, c.subscribedTopics)
	c.mu.RUnlock()

	if len(topics) == 0 {
		return nil, fmt.Errorf("no topics subscribed")
	}

	var allMessages []*Message

	for _, topic := range topics {
		messages, err := c.pollTopic(topic, timeout)
		if err != nil {
			continue
		}
		allMessages = append(allMessages, messages...)
	}

	return allMessages, nil
}

// pollTopic polls messages from a specific topic
func (c *Consumer) pollTopic(topic string, timeout time.Duration) ([]*Message, error) {
	admin := NewAdmin(c.client)
	topicInfo, err := admin.GetTopicInfo(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic info for %s: %w", topic, err)
	}

	var allMessages []*Message

	for i := int32(0); i < topicInfo.Partitions; i++ {
		partitionID := i
		c.mu.Lock()
		if c.topicOffsets[topic] == nil {
			c.topicOffsets[topic] = make(map[int32]int64)
		}
		offset := c.topicOffsets[topic][partitionID]
		c.mu.Unlock()

		fetchResult, err := c.Fetch(FetchRequest{
			Topic:     topic,
			Partition: partitionID,
			Offset:    offset,
			MaxBytes:  1024 * 1024,
		})
		if err != nil {
			continue
		}
		if fetchResult.Error != nil {
			continue
		}

		for _, msg := range fetchResult.Messages {
			allMessages = append(allMessages, &Message{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Value:     msg.Value,
			})

			c.mu.Lock()
			c.topicOffsets[topic][partitionID] = msg.Offset + 1
			c.mu.Unlock()
		}
	}

	return allMessages, nil
}

// Seek sets the offset for a specific topic partition
func (c *Consumer) Seek(topic string, partition int32, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.topicOffsets[topic] == nil {
		c.topicOffsets[topic] = make(map[int32]int64)
	}
	c.topicOffsets[topic][partition] = offset

	return nil
}
