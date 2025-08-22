package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// Consumer message consumer
type Consumer struct {
	client *Client
}

// NewConsumer creates a new consumer
func NewConsumer(client *Client) *Consumer {
	return &Consumer{
		client: client,
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

// Fetch fetches messages
func (c *Consumer) Fetch(req FetchRequest) (*FetchResult, error) {
	if req.MaxBytes <= 0 {
		req.MaxBytes = 1024 * 1024 
	}

	requestData, err := c.buildFetchRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	responseData, err := c.client.sendRequest(protocol.FetchRequestType, requestData)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	result, err := c.parseFetchResponse(req.Topic, req.Partition, req.Offset, responseData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	return result, nil
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

// Subscribe simple consumer subscription (starting from latest position)
func (c *Consumer) Subscribe(ctx context.Context, topic string, partition int32, handler func(Message) error) error {
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
