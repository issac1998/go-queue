package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	// Request type constants
	ProduceRequestType     = 0
	FetchRequestType       = 1
	CreateTopicRequestType = 2

	// Protocol version
	ProtocolVersion = 1

	// Compression types
	CompressionNone = 0
)

// Producer message producer
type Producer struct {
	client *Client
}

// NewProducer creates a new producer
func NewProducer(client *Client) *Producer {
	return &Producer{
		client: client,
	}
}

// ProduceMessage single message structure
type ProduceMessage struct {
	Topic     string
	Partition int32
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

	// All messages must belong to the same topic and partition
	topic := messages[0].Topic
	partition := messages[0].Partition
	for _, msg := range messages {
		if msg.Topic != topic || msg.Partition != partition {
			return nil, fmt.Errorf("batch messages must belong to the same topic and partition")
		}
	}

	// Build request data
	requestData, err := p.buildProduceRequest(topic, partition, messages)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	// Send request
	responseData, err := p.client.sendRequest(ProduceRequestType, requestData)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	// Parse response
	result, err := p.parseProduceResponse(topic, partition, responseData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	return result, nil
}

// buildProduceRequest builds produce request
func (p *Producer) buildProduceRequest(topic string, partition int32, messages []ProduceMessage) ([]byte, error) {
	buf := new(bytes.Buffer)

	// 1. Protocol version
	if err := binary.Write(buf, binary.BigEndian, int16(ProtocolVersion)); err != nil {
		return nil, err
	}

	// 2. Topic length and content
	if err := binary.Write(buf, binary.BigEndian, int16(len(topic))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(topic); err != nil {
		return nil, err
	}

	// 3. Partition
	if err := binary.Write(buf, binary.BigEndian, partition); err != nil {
		return nil, err
	}

	// 4. Compression type
	if err := binary.Write(buf, binary.BigEndian, int8(CompressionNone)); err != nil {
		return nil, err
	}

	// 5. Build message set
	messagesBuf := new(bytes.Buffer)
	for _, msg := range messages {
		// Message length
		if err := binary.Write(messagesBuf, binary.BigEndian, int32(len(msg.Value))); err != nil {
			return nil, err
		}
		// Message content
		if _, err := messagesBuf.Write(msg.Value); err != nil {
			return nil, err
		}
	}

	// 6. Write message set size
	if err := binary.Write(buf, binary.BigEndian, int32(messagesBuf.Len())); err != nil {
		return nil, err
	}

	// 7. Write message set content
	if _, err := buf.Write(messagesBuf.Bytes()); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// parseProduceResponse parses produce response
func (p *Producer) parseProduceResponse(topic string, partition int32, data []byte) (*ProduceResult, error) {
	buf := bytes.NewReader(data)

	// Read BaseOffset
	var baseOffset int64
	if err := binary.Read(buf, binary.BigEndian, &baseOffset); err != nil {
		return nil, fmt.Errorf("failed to read base offset: %v", err)
	}

	// Read ErrorCode
	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return nil, fmt.Errorf("failed to read error code: %v", err)
	}

	result := &ProduceResult{
		Topic:     topic,
		Partition: partition,
		Offset:    baseOffset,
	}

	// Check error code
	if errorCode != 0 {
		result.Error = fmt.Errorf("server error, error code: %d", errorCode)
	}

	return result, nil
}
