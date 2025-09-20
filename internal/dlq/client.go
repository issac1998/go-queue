package dlq

import (
	"fmt"
	"time"

	"github.com/issac1998/go-queue/client"
	typederrors "github.com/issac1998/go-queue/internal/errors"
)

// DLQClient provides high-level API for dead letter queue operations
type DLQClient struct {
	manager  *Manager
	client   *client.Client
	producer *client.Producer
	consumer *client.Consumer
}

// NewDLQClient creates a new DLQ client
func NewDLQClient(clientInstance *client.Client, config *DLQConfig) (*DLQClient, error) {
	manager, err := NewManager(clientInstance, config)
	if err != nil {
		return nil, typederrors.NewTypedError(typederrors.GeneralError, "failed to create DLQ manager", err)
	}

	return &DLQClient{
		manager:  manager,
		client:   clientInstance,
		producer: client.NewProducer(clientInstance),
		consumer: client.NewConsumer(clientInstance),
	}, nil
}

// HandleMessageFailure handles a failed message processing
func (d *DLQClient) HandleMessageFailure(msg *client.Message, consumerGroup, consumerID, errorMsg string) error {
	failureInfo := &FailureInfo{
		ConsumerGroup: consumerGroup,
		ConsumerID:    consumerID,
		ErrorMessage:  errorMsg,
		FailureTime:   time.Now(),
	}

	return d.manager.HandleFailedMessage(msg, failureInfo)
}

// ShouldRetryMessage checks if a message should be retried
func (d *DLQClient) ShouldRetryMessage(topic string, partition int32, offset int64, consumerGroup string) bool {
	return d.manager.ShouldRetryMessage(topic, partition, offset, consumerGroup)
}

// GetRetryDelay returns the delay before next retry
func (d *DLQClient) GetRetryDelay(topic string, partition int32, offset int64, consumerGroup string) time.Duration {
	return d.manager.GetRetryDelay(topic, partition, offset, consumerGroup)
}

// GetDLQMessages retrieves messages from dead letter queue
func (d *DLQClient) GetDLQMessages(topic string, limit int) ([]*DeadLetterMessage, error) {
	return d.manager.GetDLQMessages(topic, limit)
}

// GetRetryStates returns current retry states for monitoring
func (d *DLQClient) GetRetryStates() map[string]*MessageRetryState {
	return d.manager.GetRetryStates()
}

// GetDLQStats returns DLQ statistics
func (d *DLQClient) GetDLQStats() *DLQStats {
	return d.manager.GetDLQStats()
}

// CleanupExpiredRetries removes expired retry states
func (d *DLQClient) CleanupExpiredRetries() {
	d.manager.CleanupExpiredRetries()
}

// ReprocessDLQMessage attempts to reprocess a message from DLQ
func (d *DLQClient) ReprocessDLQMessage(dlqMsg *DeadLetterMessage) error {
	// Create original message structure
	originalMsg := &client.Message{
		Topic:     dlqMsg.OriginalTopic,
		Partition: dlqMsg.OriginalPartition,
		Offset:    dlqMsg.OriginalOffset,
		Value:     dlqMsg.OriginalValue,
	}

	// Send back to original topic
	produceMsg := client.ProduceMessage{
		Topic:     originalMsg.Topic,
		Partition: originalMsg.Partition,
		Value:     originalMsg.Value,
	}

	_, err := d.producer.Send(produceMsg)
	if err != nil {
		return typederrors.NewTypedError(typederrors.GeneralError, "failed to reprocess DLQ message", err)
	}

	return nil
}

// ReprocessDLQMessages reprocesses multiple messages from DLQ
func (d *DLQClient) ReprocessDLQMessages(dlqMessages []*DeadLetterMessage) []error {
	errors := make([]error, 0)

	for _, dlqMsg := range dlqMessages {
		if err := d.ReprocessDLQMessage(dlqMsg); err != nil {
			errors = append(errors, typederrors.NewTypedError(typederrors.GeneralError, 
			fmt.Sprintf("failed to reprocess message from topic %s partition %d offset %d", 
				dlqMsg.OriginalTopic, dlqMsg.OriginalPartition, dlqMsg.OriginalOffset), err))
		}
	}

	return errors
}

// ListDLQTopics returns all DLQ topics
func (d *DLQClient) ListDLQTopics() ([]string, error) {
	// This would require metadata API to list all topics and filter DLQ topics
	// For now, return empty slice as this requires broker metadata support
	return []string{}, nil
}

// DeleteDLQMessage marks a DLQ message as processed (logical deletion)
// In a real implementation, this might involve updating message status or moving to processed topic
func (d *DLQClient) DeleteDLQMessage(dlqMsg *DeadLetterMessage) error {
	// For now, this is a no-op as we don't have a deletion mechanism
	// In production, you might:
	// 1. Move message to a "processed" topic
	// 2. Update message status in a separate metadata store
	// 3. Use compaction with null values
	return nil
}

// GetDLQMessagesByTimeRange retrieves DLQ messages within a time range
func (d *DLQClient) GetDLQMessagesByTimeRange(topic string, start, end time.Time, limit int) ([]*DeadLetterMessage, error) {
	// Get all DLQ messages first
	allMessages, err := d.GetDLQMessages(topic, limit*2) // Get more to filter
	if err != nil {
		return nil, err
	}

	// Filter by time range
	var filteredMessages []*DeadLetterMessage
	for _, msg := range allMessages {
		if msg.DLQTimestamp.After(start) && msg.DLQTimestamp.Before(end) {
			filteredMessages = append(filteredMessages, msg)
			if len(filteredMessages) >= limit {
				break
			}
		}
	}

	return filteredMessages, nil
}

// GetDLQMessagesByConsumerGroup retrieves DLQ messages for a specific consumer group
func (d *DLQClient) GetDLQMessagesByConsumerGroup(topic, consumerGroup string, limit int) ([]*DeadLetterMessage, error) {
	// Get all DLQ messages first
	allMessages, err := d.GetDLQMessages(topic, limit*2) // Get more to filter
	if err != nil {
		return nil, err
	}

	// Filter by consumer group
	var filteredMessages []*DeadLetterMessage
	for _, msg := range allMessages {
		if msg.ConsumerGroup == consumerGroup {
			filteredMessages = append(filteredMessages, msg)
			if len(filteredMessages) >= limit {
				break
			}
		}
	}

	return filteredMessages, nil
}

// Stop stops the DLQ client and underlying manager
func (d *DLQClient) Stop() {
	d.manager.Stop()
}