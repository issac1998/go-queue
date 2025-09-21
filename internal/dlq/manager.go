package dlq

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/issac1998/go-queue/client"
	typederrors "github.com/issac1998/go-queue/internal/errors"
)

// Manager manages dead letter queue operations
type Manager struct {
	client      *client.Client
	producer    *client.Producer
	config      *DLQConfig
	retryStates map[string]*MessageRetryState
	mu          sync.RWMutex
	stopCh      chan struct{}
}

// NewManager creates a new DLQ manager
func NewManager(clientInstance *client.Client, config *DLQConfig) (*Manager, error) {
	if clientInstance == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}
	if config == nil {
		config = DefaultDLQConfig()
	}

	m := &Manager{
		client:      clientInstance,
		producer:    client.NewProducer(clientInstance),
		config:      config,
		retryStates: make(map[string]*MessageRetryState),
		stopCh:      make(chan struct{}),
	}

	return m, nil
}

// HandleFailedMessage processes a failed message according to DLQ policy
func (m *Manager) HandleFailedMessage(originalMsg *client.Message, failureInfo *FailureInfo) error {
	if !m.config.Enabled {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Generate unique key for this message
	key := fmt.Sprintf("%s-%d-%d-%s", originalMsg.Topic, originalMsg.Partition, originalMsg.Offset, failureInfo.ConsumerGroup)
	retryState, exists := m.retryStates[key]

	if !exists {
		// First failure for this message
		retryState = &MessageRetryState{
			OriginalTopic:     originalMsg.Topic,
			OriginalPartition: originalMsg.Partition,
			OriginalOffset:    originalMsg.Offset,
			ConsumerGroup:     failureInfo.ConsumerGroup,
			RetryCount:        0,
			MaxRetries:        m.config.RetryPolicy.MaxRetries,
			FirstFailureTime:  failureInfo.FailureTime,
			RetryPolicy:       m.config.RetryPolicy,
			RetryAttempts:     make([]RetryAttempt, 0),
		}
		m.retryStates[key] = retryState
	}

	// Update retry state
	retryState.LastFailureTime = failureInfo.FailureTime

	if retryState.ShouldRetry() {
		retryState.RetryCount++
		
		retryState.NextRetryTime = retryState.CalculateNextRetryTime()
		
		attempt := RetryAttempt{
			AttemptNumber: retryState.RetryCount,
			AttemptTime:   failureInfo.FailureTime,
			Error:         failureInfo.ErrorMessage,
			NextRetryTime: retryState.NextRetryTime,
		}
		retryState.RetryAttempts = append(retryState.RetryAttempts, attempt)
	} else {
		retryState.RetryCount++
		
		attempt := RetryAttempt{
			AttemptNumber: retryState.RetryCount,
			AttemptTime:   failureInfo.FailureTime,
			Error:         failureInfo.ErrorMessage,
		}
		retryState.RetryAttempts = append(retryState.RetryAttempts, attempt)
		
		// Send to dead letter queue
		if err := m.sendToDeadLetterQueue(originalMsg, failureInfo, retryState); err != nil {
			return typederrors.NewTypedError(typederrors.GeneralError, "failed to send message to DLQ", err)
		}

		delete(m.retryStates, key)
	}

	return nil
}

// ShouldRetryMessage checks if a message should be retried
func (m *Manager) ShouldRetryMessage(topic string, partition int32, offset int64, consumerGroup string) bool {
	if !m.config.Enabled {
		return false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("%s-%d-%d-%s", topic, partition, offset, consumerGroup)
	retryState, exists := m.retryStates[key]

	return exists && retryState.ShouldRetry()
}

// GetRetryDelay returns the delay before next retry for a message
func (m *Manager) GetRetryDelay(topic string, partition int32, offset int64, consumerGroup string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("%s-%d-%d-%s", topic, partition, offset, consumerGroup)
	retryState, exists := m.retryStates[key]

	if !exists {
		return 0
	}

	now := time.Now()
	if now.Before(retryState.NextRetryTime) {
		return retryState.NextRetryTime.Sub(now)
	}

	return 0
}

// sendToDeadLetterQueue sends a message to the dead letter queue
func (m *Manager) sendToDeadLetterQueue(originalMsg *client.Message, failureInfo *FailureInfo, retryState *MessageRetryState) error {
	dlqTopic := originalMsg.Topic + m.config.TopicSuffix

	// Create dead letter message
	dlqMessage := &DeadLetterMessage{
		OriginalTopic:     originalMsg.Topic,
		OriginalPartition: originalMsg.Partition,
		OriginalOffset:    originalMsg.Offset,
		OriginalValue:     originalMsg.Value,
		DLQTopic:          dlqTopic,
		DLQTimestamp:      time.Now(),
		ConsumerGroup:     failureInfo.ConsumerGroup,
		ConsumerID:        failureInfo.ConsumerID,
		FailureReason:     failureInfo.ErrorMessage,
		RetryCount:        retryState.RetryCount,
		MaxRetries:        retryState.MaxRetries,
		FirstFailureTime:  retryState.FirstFailureTime,
		LastFailureTime:   retryState.LastFailureTime,
		Headers:           make(map[string]string),
	}

	// Add metadata headers
	dlqMessage.Headers["dlq.original.topic"] = originalMsg.Topic
	dlqMessage.Headers["dlq.original.partition"] = fmt.Sprintf("%d", originalMsg.Partition)
	dlqMessage.Headers["dlq.original.offset"] = fmt.Sprintf("%d", originalMsg.Offset)
	dlqMessage.Headers["dlq.failure.reason"] = failureInfo.ErrorMessage
	dlqMessage.Headers["dlq.retry.count"] = fmt.Sprintf("%d", retryState.RetryCount)
	dlqMessage.Headers["dlq.consumer.group"] = failureInfo.ConsumerGroup
	dlqMessage.Headers["dlq.consumer.id"] = failureInfo.ConsumerID

	// Serialize DLQ message
	dlqData, err := json.Marshal(dlqMessage)
	if err != nil {
		return typederrors.NewTypedError(typederrors.GeneralError, "failed to serialize DLQ message", err)
	}

	// Send to DLQ topic
	produceMessage := client.ProduceMessage{
		Topic:     dlqTopic,
		Partition: 0, // Use partition 0 for DLQ messages
		Value:     dlqData,
	}

	_, err = m.producer.Send(produceMessage)
	if err != nil {
		return typederrors.NewTypedError(typederrors.GeneralError, fmt.Sprintf("failed to send message to DLQ topic %s", dlqTopic), err)
	}

	return nil
}

// GetRetryStates returns current retry states (for monitoring)
func (m *Manager) GetRetryStates() map[string]*MessageRetryState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a copy to avoid race conditions
	states := make(map[string]*MessageRetryState)
	for k, v := range m.retryStates {
		states[k] = v
	}
	return states
}

// GetDLQMessages retrieves messages from the dead letter queue
func (m *Manager) GetDLQMessages(topic string, limit int) ([]*DeadLetterMessage, error) {
	dlqTopic := topic + m.config.TopicSuffix

	// Create consumer for DLQ topic
	consumer := client.NewConsumer(m.client)

	// Fetch messages from DLQ topic
	result, err := consumer.Fetch(client.FetchRequest{
		Topic:     dlqTopic,
		Partition: 0,
		Offset:    0,
		MaxBytes:  int32(limit * 1024), // Estimate 1KB per message
	})
	if err != nil {
		return nil, typederrors.NewTypedError(typederrors.GeneralError, "failed to fetch DLQ messages", err)
	}

	// Parse messages
	var dlqMessages []*DeadLetterMessage
	for _, msg := range result.Messages {
		var dlqMsg DeadLetterMessage
		if err := json.Unmarshal(msg.Value, &dlqMsg); err != nil {
			continue // Skip invalid messages
		}
		dlqMessages = append(dlqMessages, &dlqMsg)

		if len(dlqMessages) >= limit {
			break
		}
	}

	return dlqMessages, nil
}

// CleanupExpiredRetries removes expired retry states
func (m *Manager) CleanupExpiredRetries() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	expiredKeys := make([]string, 0)

	for key, state := range m.retryStates {
		// Remove states that are older than retention period
		if now.Sub(state.FirstFailureTime) > m.config.RetentionPeriod {
			expiredKeys = append(expiredKeys, key)
		}
	}

	for _, key := range expiredKeys {
		delete(m.retryStates, key)
	}
}

// GetDLQStats returns statistics about the dead letter queue
func (m *Manager) GetDLQStats() *DLQStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := &DLQStats{
		TotalMessages: int64(len(m.retryStates)),
	}

	// Calculate time-based statistics
	now := time.Now()
	hourAgo := now.Add(-1 * time.Hour)
	dayAgo := now.Add(-24 * time.Hour)

	for _, state := range m.retryStates {
		if state.LastFailureTime.After(hourAgo) {
			stats.MessagesLastHour++
		}
		if state.LastFailureTime.After(dayAgo) {
			stats.MessagesLastDay++
		}

		// Track oldest and newest messages
		if stats.OldestMessage.IsZero() || state.FirstFailureTime.Before(stats.OldestMessage) {
			stats.OldestMessage = state.FirstFailureTime
		}
		if stats.NewestMessage.IsZero() || state.LastFailureTime.After(stats.NewestMessage) {
			stats.NewestMessage = state.LastFailureTime
		}
	}

	return stats
}

// Stop stops the DLQ manager
func (m *Manager) Stop() {
	close(m.stopCh)
}