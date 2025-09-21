package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// AbortHandler defines the interface for handling transaction abort operations
// This handler is called when a transaction is aborted to allow custom cleanup logic
type AbortHandler interface {
	// OnAbort is called when a transaction is being aborted
	// It receives the transaction ID and the messages that were processed in the transaction
	// This allows the application to perform custom rollback operations
	OnAbort(ctx context.Context, transactionID string, processedMessages []*ConsumeMessage) error
}

// TransactionalConsumerConfig transactional consumer config
type TransactionalConsumerConfig struct {
	GroupConsumerConfig
	IdempotentStorageConfig IdempotentStorageConfig
	TransactionTimeout      time.Duration
	BatchSize               int
	EnableIdempotent        bool
	AbortHandler            AbortHandler // Optional handler for transaction abort operations
}

// IdempotentStorageConfig idempotent storage config
type IdempotentStorageConfig struct {
	StorageType     string
	MaxRecords      int
	CleanupInterval time.Duration
	DatabaseURL     string
	PebbleDBPath    string // Path for PebbleDB storage
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
		GroupID:        config.GroupID,
		ConsumerID:     config.ConsumerID,
		DefaultTimeout: config.TransactionTimeout,
	}
	transactionManager := NewConsumerTransactionManager(client, groupConsumer, ManagerConfig)

	var idempotentManager *IdempotentManager
	if config.EnableIdempotent {
		var storage IdempotentStorage

		if config.IdempotentStorageConfig.PebbleDBPath == "" {
			config.IdempotentStorageConfig.PebbleDBPath = "./data/idempotent"
		}
		pebbleStorage, err := NewPebbleIdempotentStorage(config.IdempotentStorageConfig.PebbleDBPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create pebble storage: %w", err)
		}
		storage = pebbleStorage

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
func (tc *TransactionalConsumer) ConsumeTransactionally(ctx context.Context, handler func(*ConsumeMessage) error, commitHandler func(msg *ConsumeMessage) error, rollbackHandler func(msg *ConsumeMessage) error) error {
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
		// process batch
		err = tc.processBatch(ctx, messages, handler, commitHandler, rollbackHandler)
		if err != nil {
			log.Printf("Failed to process batch: %v", err)
		}
	}
}

// CommitTransaction commit transaction
func (tc *TransactionalConsumer) CommitTransaction(ctx context.Context, transactionID string, commitHandler func(msg *ConsumeMessage) error) error {
	return tc.transactionManager.CommitTransaction(ctx, transactionID, commitHandler)
}

// AbortTransaction abort transaction and rollback local state
func (tc *TransactionalConsumer) AbortTransaction(ctx context.Context, transactionID string, rollbackHandler func(msg *ConsumeMessage) error) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Get the transaction before aborting
	transaction, exists := tc.transactionManager.GetTransaction(transactionID)
	if !exists {
		return fmt.Errorf("transaction not found: %s", transactionID)
	}

	// Rollback local transaction state
	transaction.mu.Lock()
	defer transaction.mu.Unlock()

	transaction.ProcessedMessages = nil

	transaction.OffsetCommits = make(map[string]map[int32]int64)

	return tc.transactionManager.AbortTransaction(ctx, transactionID, rollbackHandler)
}

func (tc *TransactionalConsumer) processBatch(ctx context.Context, messages []*ConsumeMessage, handler func(*ConsumeMessage) error, rollbackHandler func(msg *ConsumeMessage) error, commitHandler func(msg *ConsumeMessage) error) error {
	transaction, err := tc.transactionManager.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	defer func() {
		if transaction.GetState() == ConsumerTransactionStateInProgress {
			tc.transactionManager.AbortTransaction(ctx, transaction.ID, rollbackHandler)
		}
	}()
	offsetMap := make(map[string]map[int32]int64)

	for _, msg := range messages {
		if offsetMap[msg.Topic] == nil {
			offsetMap[msg.Topic] = make(map[int32]int64)
		}
		// there should not be offset gap, which is guaranteed by broker
		if msg.Offset+1 > offsetMap[msg.Topic][msg.Partition] {
			offsetMap[msg.Topic][msg.Partition] = msg.Offset + 1
		}

		if tc.config.EnableIdempotent && tc.idempotentManager == nil {
			messageID := tc.idempotentManager.GenerateMessageID(msg)
			processed, err := tc.idempotentManager.IsProcessed(messageID)
			if err != nil {
				return fmt.Errorf("failed to check if message is processed: %v", err)
			}

			if processed {
				continue
			}
		}
		err := handler(msg)
		if err != nil {
			return fmt.Errorf("handler failed: %v", err)
		}
		tc.transactionManager.processedMessages[transaction.ID] = append(tc.transactionManager.processedMessages[transaction.ID], msg)
	}

	// Idempotent(duplicate message)
	if tc.config.EnableIdempotent {
		err = tc.idempotentManager.BatchMarkProcessed(tc.transactionManager.processedMessages[transaction.ID], transaction.ID)
		if err != nil {
			return fmt.Errorf("failed to mark messages as processed: %v", err)
		}
	}

	for topic, partitions := range offsetMap {
		for partition, offset := range partitions {
			transaction.AddOffsetCommit(topic, partition, offset)
		}
	}

	return tc.transactionManager.CommitTransaction(ctx, transaction.ID, commitHandler)
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
