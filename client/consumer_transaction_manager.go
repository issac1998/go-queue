package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type ConsumerTransactionState int32

const (
	ConsumerTransactionStateBegin ConsumerTransactionState = iota
	ConsumerTransactionStateInProgress
	ConsumerTransactionStateCommitting
	ConsumerTransactionStateCommitted
	ConsumerTransactionStateAborting
	ConsumerTransactionStateAborted
)

func (s ConsumerTransactionState) String() string {
	switch s {
	case ConsumerTransactionStateBegin:
		return "BEGIN"
	case ConsumerTransactionStateInProgress:
		return "IN_PROGRESS"
	case ConsumerTransactionStateCommitting:
		return "COMMITTING"
	case ConsumerTransactionStateCommitted:
		return "COMMITTED"
	case ConsumerTransactionStateAborting:
		return "ABORTING"
	case ConsumerTransactionStateAborted:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

// ConsumerTransaction defines txn
type ConsumerTransaction struct {
	ID                string
	GroupID           string
	ConsumerID        string
	State             ConsumerTransactionState
	StartTime         time.Time
	LastUpdateTime    time.Time
	Timeout           time.Duration
	ProcessedMessages []*ProcessedRecord
	OffsetCommits     map[string]map[int32]int64 // topic -> partition -> offset
	mu                sync.RWMutex
}

// NewConsumerTransaction create new txn
func NewConsumerTransaction(id, groupID, consumerID string, timeout time.Duration) *ConsumerTransaction {
	return &ConsumerTransaction{
		ID:                id,
		GroupID:           groupID,
		ConsumerID:        consumerID,
		State:             ConsumerTransactionStateBegin,
		StartTime:         time.Now(),
		LastUpdateTime:    time.Now(),
		Timeout:           timeout,
		ProcessedMessages: make([]*ProcessedRecord, 0),
		OffsetCommits:     make(map[string]map[int32]int64),
	}
}

// AddProcessedMessage add processed meassge
func (ct *ConsumerTransaction) AddProcessedMessage(record *ProcessedRecord) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	ct.ProcessedMessages = append(ct.ProcessedMessages, record)
	ct.LastUpdateTime = time.Now()
}

// AddOffsetCommit commit offset
func (ct *ConsumerTransaction) AddOffsetCommit(topic string, partition int32, offset int64) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	if ct.OffsetCommits[topic] == nil {
		ct.OffsetCommits[topic] = make(map[int32]int64)
	}
	ct.OffsetCommits[topic][partition] = offset
	ct.LastUpdateTime = time.Now()
}

// UpdateState update txn
func (ct *ConsumerTransaction) UpdateState(state ConsumerTransactionState) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	ct.State = state
	ct.LastUpdateTime = time.Now()
}

// IsExpired check expiration
func (ct *ConsumerTransaction) IsExpired() bool {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	return time.Since(ct.StartTime) > ct.Timeout
}

// GetState get txn state
func (ct *ConsumerTransaction) GetState() ConsumerTransactionState {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	return ct.State
}

// ConsumerTransactionManager consumer txn manager
type ConsumerTransactionManager struct {
	client             *Client
	groupConsumer      *GroupConsumer // 添加 GroupConsumer 字段
	groupID            string
	consumerID         string
	transactions       map[string]*ConsumerTransaction
	currentTransaction *ConsumerTransaction
	defaultTimeout     time.Duration
	mu                 sync.RWMutex
	stopCleanup        chan struct{}
	processedMessages  map[string][]*ConsumeMessage
}

// ConsumerManagerConfig manager config
type ConsumerManagerConfig struct {
	GroupID        string
	ConsumerID     string
	DefaultTimeout time.Duration
}

// NewConsumerTransactionManager create manager
func NewConsumerTransactionManager(client *Client, groupConsumer *GroupConsumer, config ConsumerManagerConfig) *ConsumerTransactionManager {
	if config.DefaultTimeout == 0 {
		config.DefaultTimeout = 30 * time.Second
	}

	ctm := &ConsumerTransactionManager{
		client:            client,
		groupConsumer:     groupConsumer, // 设置 GroupConsumer
		groupID:           config.GroupID,
		consumerID:        config.ConsumerID,
		transactions:      make(map[string]*ConsumerTransaction),
		defaultTimeout:    config.DefaultTimeout,
		stopCleanup:       make(chan struct{}),
		processedMessages: make(map[string][]*ConsumeMessage),
	}

	return ctm
}

// BeginTransaction start txn
func (ctm *ConsumerTransactionManager) BeginTransaction(ctx context.Context) (*ConsumerTransaction, error) {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()

	transactionID := fmt.Sprintf("%s-%s-%d", ctm.groupID, ctm.consumerID, time.Now().UnixNano())

	transaction := NewConsumerTransaction(transactionID, ctm.groupID, ctm.consumerID, ctm.defaultTimeout)
	transaction.UpdateState(ConsumerTransactionStateBegin)

	ctm.transactions[transactionID] = transaction
	ctm.currentTransaction = transaction

	log.Printf("Started transaction %s locally (no broker request)", transactionID)
	return transaction, nil
}

// CommitTransaction commit txn
func (ctm *ConsumerTransactionManager) CommitTransaction(ctx context.Context, transactionID string, commitHandler func(msg *ConsumeMessage) error) error {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()

	transaction, exists := ctm.transactions[transactionID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", transactionID)
	}

	if transaction.IsExpired() {
		return fmt.Errorf("transaction expired: %s", transactionID)
	}

	transaction.UpdateState(ConsumerTransactionStateCommitting)

	// Execute commit handler for processed messages
	if commitHandler != nil {
		if messages, exists := ctm.processedMessages[transactionID]; exists {
			for _, msg := range messages {
				if err := commitHandler(msg); err != nil {
					transaction.UpdateState(ConsumerTransactionStateInProgress)
					return fmt.Errorf("commit handler failed for message: %v", err)
				}
			}
		}
	}

	transaction.mu.RLock()
	for topic, partitions := range transaction.OffsetCommits {
		for partition, offset := range partitions {
			if groupConsumer := ctm.getGroupConsumer(); groupConsumer != nil {
				if err := groupConsumer.CommitOffset(topic, partition, offset, fmt.Sprintf("tx:%s", transactionID)); err != nil {
					transaction.mu.RUnlock()
					transaction.UpdateState(ConsumerTransactionStateInProgress)
					return fmt.Errorf("failed to commit offset for topic %s partition %d: %v", topic, partition, err)
				}
			}
		}
	}
	transaction.mu.RUnlock()

	transaction.UpdateState(ConsumerTransactionStateCommitted)
	delete(ctm.processedMessages, transactionID)

	return nil
}

func (ctm *ConsumerTransactionManager) getGroupConsumer() *GroupConsumer {
	return ctm.groupConsumer
}

// AbortTransaction abort transaction
func (ctm *ConsumerTransactionManager) AbortTransaction(ctx context.Context, transactionID string, rollbackHandler func(msg *ConsumeMessage) error) error {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()

	transaction, exists := ctm.transactions[transactionID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", transactionID)
	}

	if transaction.IsExpired() {
		return fmt.Errorf("transaction expired: %s", transactionID)
	}

	transaction.UpdateState(ConsumerTransactionStateAborting)

	// Execute rollback handler for processed messages
	if rollbackHandler != nil {
		if messages, exists := ctm.processedMessages[transactionID]; exists {
			for _, msg := range messages {
				if err := rollbackHandler(msg); err != nil {
					log.Printf("Rollback handler failed for message: %v", err)
				}
			}
		}
	}

	transaction.UpdateState(ConsumerTransactionStateAborted)
	delete(ctm.processedMessages, transactionID)

	log.Printf("Transaction %s aborted successfully (local cleanup only)", transactionID)
	return nil
}

// GetCurrentTransaction get current transaction
func (ctm *ConsumerTransactionManager) GetCurrentTransaction() *ConsumerTransaction {
	ctm.mu.RLock()
	defer ctm.mu.RUnlock()

	return ctm.currentTransaction
}

// GetTransaction get transaction
func (ctm *ConsumerTransactionManager) GetTransaction(transactionID string) (*ConsumerTransaction, bool) {
	ctm.mu.RLock()
	defer ctm.mu.RUnlock()

	transaction, exists := ctm.transactions[transactionID]
	return transaction, exists
}

// Close close transaction manager
func (ctm *ConsumerTransactionManager) Close() error {

	select {
	case ctm.stopCleanup <- struct{}{}:
	default:
	}

	return nil
}
