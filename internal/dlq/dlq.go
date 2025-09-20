package dlq

import (
	"time"

	"github.com/issac1998/go-queue/client"
)

// DLQ provides a unified interface for dead letter queue operations
type DLQ struct {
	client *DLQClient
}

// New creates a new DLQ instance with default configuration
func New(clientInstance *client.Client) (*DLQ, error) {
	config := DefaultDLQConfig()
	dlqClient, err := NewDLQClient(clientInstance, config)
	if err != nil {
		return nil, err
	}

	return &DLQ{
		client: dlqClient,
	}, nil
}

// NewWithConfig creates a new DLQ instance with custom configuration
func NewWithConfig(clientInstance *client.Client, config *DLQConfig) (*DLQ, error) {
	dlqClient, err := NewDLQClient(clientInstance, config)
	if err != nil {
		return nil, err
	}

	return &DLQ{
		client: dlqClient,
	}, nil
}

// HandleFailure handles a message processing failure
func (d *DLQ) HandleFailure(msg *client.Message, consumerGroup, consumerID, errorMsg string) error {
	return d.client.HandleMessageFailure(msg, consumerGroup, consumerID, errorMsg)
}

// ShouldRetry checks if a message should be retried
func (d *DLQ) ShouldRetry(topic string, partition int32, offset int64, consumerGroup string) bool {
	return d.client.ShouldRetryMessage(topic, partition, offset, consumerGroup)
}

// GetRetryDelay returns the delay before next retry
func (d *DLQ) GetRetryDelay(topic string, partition int32, offset int64, consumerGroup string) time.Duration {
	return d.client.GetRetryDelay(topic, partition, offset, consumerGroup)
}

// GetMessages retrieves messages from dead letter queue
func (d *DLQ) GetMessages(topic string, limit int) ([]*DeadLetterMessage, error) {
	return d.client.GetDLQMessages(topic, limit)
}

// GetMessagesByTimeRange retrieves DLQ messages within a time range
func (d *DLQ) GetMessagesByTimeRange(topic string, start, end time.Time, limit int) ([]*DeadLetterMessage, error) {
	return d.client.GetDLQMessagesByTimeRange(topic, start, end, limit)
}

// GetMessagesByConsumerGroup retrieves DLQ messages for a specific consumer group
func (d *DLQ) GetMessagesByConsumerGroup(topic, consumerGroup string, limit int) ([]*DeadLetterMessage, error) {
	return d.client.GetDLQMessagesByConsumerGroup(topic, consumerGroup, limit)
}

// Reprocess attempts to reprocess a message from DLQ
func (d *DLQ) Reprocess(dlqMsg *DeadLetterMessage) error {
	return d.client.ReprocessDLQMessage(dlqMsg)
}

// ReprocessBatch reprocesses multiple messages from DLQ
func (d *DLQ) ReprocessBatch(dlqMessages []*DeadLetterMessage) []error {
	return d.client.ReprocessDLQMessages(dlqMessages)
}

// GetStats returns DLQ statistics
func (d *DLQ) GetStats() *DLQStats {
	return d.client.GetDLQStats()
}

// GetRetryStates returns current retry states for monitoring
func (d *DLQ) GetRetryStates() map[string]*MessageRetryState {
	return d.client.GetRetryStates()
}

// Cleanup removes expired retry states
func (d *DLQ) Cleanup() {
	d.client.CleanupExpiredRetries()
}

// Stop stops the DLQ instance
func (d *DLQ) Stop() {
	d.client.Stop()
}

// Consumer wraps a regular consumer with DLQ functionality
type Consumer struct {
	consumer      *client.Consumer
	dlq           *DLQ
	consumerGroup string
	consumerID    string
}

// NewConsumer creates a new DLQ-enabled consumer
func NewConsumer(clientInstance *client.Client, consumerGroup, consumerID string) (*Consumer, error) {
	dlq, err := New(clientInstance)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		consumer:      client.NewConsumer(clientInstance),
		dlq:           dlq,
		consumerGroup: consumerGroup,
		consumerID:    consumerID,
	}, nil
}

// NewConsumerWithConfig creates a new DLQ-enabled consumer with custom config
func NewConsumerWithConfig(clientInstance *client.Client, consumerGroup, consumerID string, config *DLQConfig) (*Consumer, error) {
	dlq, err := NewWithConfig(clientInstance, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		consumer:      client.NewConsumer(clientInstance),
		dlq:           dlq,
		consumerGroup: consumerGroup,
		consumerID:    consumerID,
	}, nil
}

// Fetch fetches messages with DLQ support
func (c *Consumer) Fetch(req client.FetchRequest) (*client.FetchResult, error) {
	return c.consumer.Fetch(req)
}

// ProcessMessage processes a message with automatic DLQ handling
func (c *Consumer) ProcessMessage(msg *client.Message, processor func(*client.Message) error) error {
	// Check if this message should be retried
	if c.dlq.ShouldRetry(msg.Topic, msg.Partition, msg.Offset, c.consumerGroup) {
		// Wait for retry delay
		delay := c.dlq.GetRetryDelay(msg.Topic, msg.Partition, msg.Offset, c.consumerGroup)
		if delay > 0 {
			time.Sleep(delay)
		}
	}

	// Process the message
	err := processor(msg)
	if err != nil {
		// Handle failure through DLQ
		return c.dlq.HandleFailure(msg, c.consumerGroup, c.consumerID, err.Error())
	}

	return nil
}

// Stop stops the consumer
func (c *Consumer) Stop() {
	c.dlq.Stop()
}
