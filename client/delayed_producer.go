package client

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// DelayLevel represents predefined delay levels
type DelayLevel int32

const (
	DelayLevel1s  DelayLevel = 1
	DelayLevel5s  DelayLevel = 2
	DelayLevel10s DelayLevel = 3
	DelayLevel30s DelayLevel = 4
	DelayLevel1m  DelayLevel = 5
	DelayLevel2m  DelayLevel = 6
	DelayLevel3m  DelayLevel = 7
	DelayLevel4m  DelayLevel = 8
	DelayLevel5m  DelayLevel = 9
	DelayLevel6m  DelayLevel = 10
	DelayLevel7m  DelayLevel = 11
	DelayLevel8m  DelayLevel = 12
	DelayLevel9m  DelayLevel = 13
	DelayLevel10m DelayLevel = 14
	DelayLevel20m DelayLevel = 15
	DelayLevel30m DelayLevel = 16
	DelayLevel1h  DelayLevel = 17
	DelayLevel2h  DelayLevel = 18
)

// DelayedProducer handles delayed message production
type DelayedProducer struct {
	client *Client
}

// NewDelayedProducer creates a new delayed message producer
func NewDelayedProducer(client *Client) *DelayedProducer {
	return &DelayedProducer{
		client: client,
	}
}

// DelayedProduceRequest represents a delayed message produce request
type DelayedProduceRequest struct {
	Topic       string     `json:"topic"`
	Partition   int32      `json:"partition"`
	Key         []byte     `json:"key"`
	Value       []byte     `json:"value"`
	DelayLevel  DelayLevel `json:"delay_level"`
	DelayTime   int64      `json:"delay_time"`
	DeliverTime int64      `json:"deliver_time"`
}

// DelayedProduceResponse represents the response from delayed message produce
type DelayedProduceResponse struct {
	MessageID   string `json:"message_id"`
	DeliverTime int64  `json:"deliver_time"`
	ErrorCode   int32  `json:"error_code"`
	Error       string `json:"error"`
}

// DelayedMessage represents a delayed message
type DelayedMessage struct {
	ID          string     `json:"id"`
	Topic       string     `json:"topic"`
	Partition   int32      `json:"partition"`
	Key         []byte     `json:"key"`
	Value       []byte     `json:"value"`
	DelayLevel  DelayLevel `json:"delay_level"`
	DelayTime   int64      `json:"delay_time"`
	DeliverTime int64      `json:"deliver_time"`
	Status      int32      `json:"status"`
	CreatedAt   int64      `json:"created_at"`
	RetryCount  int32      `json:"retry_count"`
}

// ProduceDelayed sends a delayed message using predefined delay level
func (dp *DelayedProducer) ProduceDelayed(topic string, partition int32, key []byte, value []byte, delayLevel DelayLevel) (*DelayedProduceResponse, error) {
	request := &DelayedProduceRequest{
		Topic:      topic,
		Partition:  partition,
		Key:        key,
		Value:      value,
		DelayLevel: delayLevel,
	}

	return dp.sendDelayedProduceRequest(request)
}

// ProduceDelayedAt sends a message to be delivered at specific time
func (dp *DelayedProducer) ProduceDelayedAt(topic string, partition int32, key []byte, value []byte, deliverTime int64) (*DelayedProduceResponse, error) {
	request := &DelayedProduceRequest{
		Topic:       topic,
		Partition:   partition,
		Key:         key,
		Value:       value,
		DelayLevel:  0, // DelayLevel is 0 when using DeliverTime
		DelayTime:   0,
		DeliverTime: deliverTime,
	}

	return dp.sendDelayedProduceRequest(request)
}

// ProduceDelayedAfter sends a message to be delivered after specified duration
func (dp *DelayedProducer) ProduceDelayedAfter(topic string, partition int32, key []byte, value []byte, delayTime time.Duration) (*DelayedProduceResponse, error) {
	request := &DelayedProduceRequest{
		Topic:       topic,
		Partition:   partition,
		Key:         key,
		Value:       value,
		DelayLevel:  0, // DelayLevel is 0 when using DelayTime
		DelayTime:   delayTime.Milliseconds(),
		DeliverTime: 0, // Calculated by server
	}

	return dp.sendDelayedProduceRequest(request)
}

// sendDelayedProduceRequest sends the delayed produce request to broker
func (dp *DelayedProducer) sendDelayedProduceRequest(request *DelayedProduceRequest) (*DelayedProduceResponse, error) {
	requestData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	responseData, err := dp.client.sendMetaRequest(protocol.DelayedProduceRequestType, requestData)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	var response DelayedProduceResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if response.ErrorCode != 0 {
		return &response, fmt.Errorf("delayed produce failed: %s", response.Error)
	}

	return &response, nil
}

// QueryDelayedMessage queries the status of a delayed message
func (dp *DelayedProducer) QueryDelayedMessage(messageID string) (*DelayedMessage, error) {
	// Construct query request
	type QueryRequest struct {
		MessageID string `json:"message_id"`
	}

	request := &QueryRequest{
		MessageID: messageID,
	}

	// Serialize request
	requestData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request and get response
	responseData, err := dp.client.sendMetaRequest(protocol.DelayedMessageQueryRequestType, requestData)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	// Parse response
	type QueryResponse struct {
		Message   *DelayedMessage `json:"message"`
		ErrorCode int32           `json:"error_code"`
		Error     string          `json:"error"`
	}

	var response QueryResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	// Check error
	if response.ErrorCode != 0 {
		return nil, fmt.Errorf("query failed: %s", response.Error)
	}

	return response.Message, nil
}

// CancelDelayedMessage cancels a delayed message
func (dp *DelayedProducer) CancelDelayedMessage(messageID string) error {
	// Construct cancel request
	type CancelRequest struct {
		MessageID string `json:"message_id"`
	}

	request := &CancelRequest{
		MessageID: messageID,
	}

	// Serialize request
	requestData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request and get response
	responseData, err := dp.client.sendMetaRequest(protocol.DelayedMessageCancelRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}

	// Parse response
	type CancelResponse struct {
		MessageID string `json:"message_id"`
		ErrorCode int32  `json:"error_code"`
		Error     string `json:"error"`
	}

	var response CancelResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %v", err)
	}

	// Check error
	if response.ErrorCode != 0 {
		return fmt.Errorf("cancel failed: %s", response.Error)
	}

	return nil
}
