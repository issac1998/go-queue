package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

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

	return p.sendToPartitionLeader(topic, partition, messages)
}

// sendToPartitionLeader sends messages directly to the partition leader
func (p *Producer) sendToPartitionLeader(topic string, partition int32, messages []ProduceMessage) (*ProduceResult, error) {
	conn, err := p.client.connectForDataOperation(topic, partition, true)
	if err != nil {
		// TODO: only do refresh if it's a follower error
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
