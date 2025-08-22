package client

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/issac1998/go-queue/internal/protocol"
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

	topic := messages[0].Topic
	partition := messages[0].Partition
	for _, msg := range messages {
		if msg.Topic != topic || msg.Partition != partition {
			return nil, fmt.Errorf("batch messages must belong to the same topic and partition")
		}
	}

	requestData, err := p.buildProduceRequest(topic, partition, messages)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	responseData, err := p.client.sendRequest(protocol.ProduceRequestType, requestData)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

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
