package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// TransactionalConsumerConfig transactional consumer config
type TransactionalConsumerConfig struct {
	GroupConsumerConfig
	IdempotentStorageConfig IdempotentStorageConfig
	TransactionTimeout      time.Duration
	BatchSize               int
	EnableIdempotent        bool
}

// IdempotentStorageConfig idempotent storage config
type IdempotentStorageConfig struct {
	StorageType     string
	MaxRecords      int
	CleanupInterval time.Duration
	DatabaseURL     string
}

// TransactionalConsumer transactional consumer
type TransactionalConsumer struct {
	groupConsumer      *GroupConsumer
	transactionManager *ConsumerTransactionManager
	idempotentManager  *IdempotentManager
	config             TransactionalConsumerConfig
	mu                 sync.RWMutex
	closed             bool
}

// NewTransactionalConsumer create transactional consumer
func NewTransactionalConsumer(client *Client, config TransactionalConsumerConfig) (*TransactionalConsumer, error) {
	if config.TransactionTimeout == 0 {
		config.TransactionTimeout = 30 * time.Second
	}
	if config.BatchSize == 0 {
		config.BatchSize = 100
	}
	if config.IdempotentStorageConfig.MaxRecords == 0 {
		config.IdempotentStorageConfig.MaxRecords = 10000
	}
	if config.IdempotentStorageConfig.CleanupInterval == 0 {
		config.IdempotentStorageConfig.CleanupInterval = 5 * time.Minute
	}

	groupConsumer := NewGroupConsumer(client, config.GroupConsumerConfig)

	ManagerConfig := ConsumerManagerConfig{
		GroupID:         config.GroupID,
		ConsumerID:      config.ConsumerID,
		DefaultTimeout:  config.TransactionTimeout,
		CleanupInterval: 5 * time.Minute,
	}
	transactionManager := NewConsumerTransactionManager(client, ManagerConfig)

	var idempotentManager *IdempotentManager
	if config.EnableIdempotent {
		var storage IdempotentStorage
		switch config.IdempotentStorageConfig.StorageType {
		case "database":
			// TODO: database/kv store
			return nil, fmt.Errorf("database storage not implemented yet")
		default:
			storage = NewMemoryIdempotentStorage(
				config.IdempotentStorageConfig.MaxRecords,
				config.IdempotentStorageConfig.CleanupInterval,
			)
		}
		idempotentManager = NewIdempotentManager(storage)
	}

	tc := &TransactionalConsumer{
		groupConsumer:      groupConsumer,
		transactionManager: transactionManager,
		idempotentManager:  idempotentManager,
		config:             config,
	}

	return tc, nil
}

// JoinGroup join group
func (tc *TransactionalConsumer) JoinGroup() error {
	return tc.groupConsumer.JoinGroup()
}

// LeaveGroup leave group
func (tc *TransactionalConsumer) LeaveGroup() error {
	return tc.groupConsumer.LeaveGroup()
}

// Subscribe subscribe topics
func (tc *TransactionalConsumer) Subscribe(topics []string) error {
	return tc.groupConsumer.Subscribe(topics)
}

// Unsubscribe unsubscribe topics
func (tc *TransactionalConsumer) Unsubscribe(topics []string) error {
	return tc.groupConsumer.Unsubscribe(topics)
}

// ConsumeTransactionally consume messages transactionally
func (tc *TransactionalConsumer) ConsumeTransactionally(ctx context.Context, handler func([]*ConsumeMessage) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		tc.mu.RLock()
		if tc.closed {
			tc.mu.RUnlock()
			return fmt.Errorf("consumer is closed")
		}
		tc.mu.RUnlock()

		messages, err := tc.groupConsumer.Poll(5 * time.Second)
		if err != nil {
			log.Printf("Failed to poll messages: %v", err)
			continue
		}

		if len(messages) == 0 {
			continue
		}

		err = tc.processBatch(ctx, messages, handler)
		if err != nil {
			log.Printf("Failed to process batch: %v", err)
			// continue process next batch
		}
	}
}

// ConsumeTransactionallyWithManualCommit consume messages transactionally with manual commit
func (tc *TransactionalConsumer) ConsumeTransactionallyWithManualCommit(ctx context.Context, handler func([]*ConsumeMessage, *ConsumerTransaction) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		tc.mu.RLock()
		if tc.closed {
			tc.mu.RUnlock()
			return fmt.Errorf("consumer is closed")
		}
		tc.mu.RUnlock()

		messages, err := tc.groupConsumer.Poll(5 * time.Second)
		if err != nil {
			log.Printf("Failed to poll messages: %v", err)
			continue
		}

		if len(messages) == 0 {
			continue
		}

		transaction, err := tc.transactionManager.BeginTransaction(ctx)
		if err != nil {
			log.Printf("Failed to begin transaction: %v", err)
			continue
		}

		filteredMessages, err := tc.filterDuplicateMessages(messages)
		if err != nil {
			log.Printf("Failed to filter duplicate messages: %v", err)
			tc.transactionManager.AbortTransaction(ctx, transaction.ID)
			continue
		}

		if len(filteredMessages) == 0 {
			// all messages are duplicate, commit transaction
			tc.transactionManager.CommitTransaction(ctx, transaction.ID)
			continue
		}

		// call user handler function
		err = handler(filteredMessages, transaction)
		if err != nil {
			log.Printf("Handler failed: %v", err)
			tc.transactionManager.AbortTransaction(ctx, transaction.ID)
			continue
		}

	}
}

// CommitTransaction commit transaction
func (tc *TransactionalConsumer) CommitTransaction(ctx context.Context, transactionID string) error {
	return tc.transactionManager.CommitTransaction(ctx, transactionID)
}

// AbortTransaction abort transaction
func (tc *TransactionalConsumer) AbortTransaction(ctx context.Context, transactionID string) error {
	return tc.transactionManager.AbortTransaction(ctx, transactionID)
}

// processBatch process message batch
func (tc *TransactionalConsumer) processBatch(ctx context.Context, messages []*ConsumeMessage, handler func([]*ConsumeMessage) error) error {
	// begin transaction
	transaction, err := tc.transactionManager.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	defer func() {
		// ensure transaction is processed
		if transaction.GetState() == ConsumerTransactionStateInProgress {
			tc.transactionManager.AbortTransaction(ctx, transaction.ID)
		}
	}()

	// filter duplicate messages
	filteredMessages, err := tc.filterDuplicateMessages(messages)
	if err != nil {
		return fmt.Errorf("failed to filter duplicate messages: %v", err)
	}

	if len(filteredMessages) == 0 {
		// all messages are duplicate, commit transaction
		return tc.transactionManager.CommitTransaction(ctx, transaction.ID)
	}

	err = handler(filteredMessages)
	if err != nil {
		return fmt.Errorf("handler failed: %v", err)
	}

	if tc.config.EnableIdempotent {
		err = tc.idempotentManager.BatchMarkProcessed(filteredMessages, transaction.ID)
		if err != nil {
			return fmt.Errorf("failed to mark messages as processed: %v", err)
		}
	}

	err = tc.commitOffsets(filteredMessages, transaction)
	if err != nil {
		return fmt.Errorf("failed to commit offsets: %v", err)
	}

	return tc.transactionManager.CommitTransaction(ctx, transaction.ID)
}

// filterDuplicateMessages filter duplicate messages
func (tc *TransactionalConsumer) filterDuplicateMessages(messages []*ConsumeMessage) ([]*ConsumeMessage, error) {
	if !tc.config.EnableIdempotent || tc.idempotentManager == nil {
		return messages, nil
	}

	filteredMessages := make([]*ConsumeMessage, 0, len(messages))

	for _, msg := range messages {
		messageID := tc.idempotentManager.GenerateMessageID(msg)
		processed, err := tc.idempotentManager.IsProcessed(messageID)
		if err != nil {
			return nil, fmt.Errorf("failed to check if message is processed: %v", err)
		}

		if !processed {
			filteredMessages = append(filteredMessages, msg)
		}
	}

	return filteredMessages, nil
}

// commitOffsets commit offsets
func (tc *TransactionalConsumer) commitOffsets(messages []*ConsumeMessage, transaction *ConsumerTransaction) error {
	offsetMap := make(map[string]map[int32]int64)

	for _, msg := range messages {
		if offsetMap[msg.Topic] == nil {
			offsetMap[msg.Topic] = make(map[int32]int64)
		}

		if msg.Offset+1 > offsetMap[msg.Topic][msg.Partition] {
			offsetMap[msg.Topic][msg.Partition] = msg.Offset + 1
		}
	}

	for topic, partitions := range offsetMap {
		for partition, offset := range partitions {
			err := tc.groupConsumer.CommitOffset(topic, partition, offset, "")
			if err != nil {
				return fmt.Errorf("failed to commit offset for %s-%d: %v", topic, partition, err)
			}

			transaction.AddOffsetCommit(topic, partition, offset)
		}
	}

	return nil
}

// GetAssignment get assignment
func (tc *TransactionalConsumer) GetAssignment() map[string][]int32 {
	return tc.groupConsumer.GetAssignment()
}

// IsRebalancing check if rebalancing
func (tc *TransactionalConsumer) IsRebalancing() bool {
	return tc.groupConsumer.IsRebalancing()
}

// WaitForRebalanceComplete wait for rebalance complete
func (tc *TransactionalConsumer) WaitForRebalanceComplete(timeout time.Duration) error {
	return tc.groupConsumer.WaitForRebalanceComplete(timeout)
}

// GetCurrentTransaction get current transaction
func (tc *TransactionalConsumer) GetCurrentTransaction() *ConsumerTransaction {
	return tc.transactionManager.GetCurrentTransaction()
}

// GetTransaction get transaction
func (tc *TransactionalConsumer) GetTransaction(transactionID string) (*ConsumerTransaction, bool) {
	return tc.transactionManager.GetTransaction(transactionID)
}

// Close close transactional consumer
func (tc *TransactionalConsumer) Close() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.closed {
		return nil
	}

	tc.closed = true

	// close components
	var errs []error

	if err := tc.groupConsumer.LeaveGroup(); err != nil {
		errs = append(errs, fmt.Errorf("failed to leave group: %v", err))
	}

	if err := tc.transactionManager.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close transaction manager: %v", err))
	}

	if tc.idempotentManager != nil {
		if err := tc.idempotentManager.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close idempotent manager: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	return nil
}
