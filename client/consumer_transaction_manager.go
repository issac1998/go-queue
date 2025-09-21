package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
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
	client              *Client
	groupID             string
	consumerID          string
	transactions        map[string]*ConsumerTransaction
	currentTransaction  *ConsumerTransaction
	defaultTimeout      time.Duration
	mu                  sync.RWMutex
	cleanupTicker       *time.Ticker
	stopCleanup         chan struct{}
}

// ConsumerTransactionManagerConfig manager config
type ConsumerTransactionManagerConfig struct {
	GroupID           string
	ConsumerID        string
	DefaultTimeout    time.Duration
	CleanupInterval   time.Duration
}

// NewConsumerTransactionManager create manager 
func NewConsumerTransactionManager(client *Client, config ConsumerTransactionManagerConfig) *ConsumerTransactionManager {
	if config.DefaultTimeout == 0 {
		config.DefaultTimeout = 30 * time.Second
	}
	if config.CleanupInterval == 0 {
		config.CleanupInterval = 5 * time.Minute
	}
	
	ctm := &ConsumerTransactionManager{
		client:         client,
		groupID:        config.GroupID,
		consumerID:     config.ConsumerID,
		transactions:   make(map[string]*ConsumerTransaction),
		defaultTimeout: config.DefaultTimeout,
		stopCleanup:    make(chan struct{}),
	}
	
	ctm.cleanupTicker = time.NewTicker(config.CleanupInterval)
	go ctm.runCleanup()
	
	return ctm
}

// BeginTransaction start txn
func (ctm *ConsumerTransactionManager) BeginTransaction(ctx context.Context) (*ConsumerTransaction, error) {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()
	
	if ctm.currentTransaction != nil {
		state := ctm.currentTransaction.GetState()
		if state == ConsumerTransactionStateBegin || state == ConsumerTransactionStateInProgress {
			return nil, fmt.Errorf("transaction already in progress: %s", ctm.currentTransaction.ID)
		}
	}
	
	transactionID := fmt.Sprintf("%s-%s-%d", ctm.groupID, ctm.consumerID, time.Now().UnixNano())
	
	transaction := NewConsumerTransaction(transactionID, ctm.groupID, ctm.consumerID, ctm.defaultTimeout)
	
	err := ctm.sendBeginTransactionRequest(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	
	transaction.UpdateState(ConsumerTransactionStateInProgress)
	ctm.transactions[transactionID] = transaction
	ctm.currentTransaction = transaction
	
	return transaction, nil
}

// CommitTransaction commit txn
func (ctm *ConsumerTransactionManager) CommitTransaction(ctx context.Context, transactionID string) error {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()
	
	transaction, exists := ctm.transactions[transactionID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", transactionID)
	}
	
	if transaction.GetState() != ConsumerTransactionStateInProgress {
		return fmt.Errorf("transaction not in progress: %s, state: %s", transactionID, transaction.GetState())
	}
	
	transaction.UpdateState(ConsumerTransactionStateCommitting)
	
	err := ctm.sendCommitTransactionRequest(ctx, transaction)
	if err != nil {
		transaction.UpdateState(ConsumerTransactionStateInProgress)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	
	transaction.UpdateState(ConsumerTransactionStateCommitted)
	// compelte
	if ctm.currentTransaction != nil && ctm.currentTransaction.ID == transactionID {
		ctm.currentTransaction = nil
	}
	
	return nil
}

// AbortTransaction abort transaction
func (ctm *ConsumerTransactionManager) AbortTransaction(ctx context.Context, transactionID string) error {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()
	
	transaction, exists := ctm.transactions[transactionID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", transactionID)
	}
	
	state := transaction.GetState()
	if state != ConsumerTransactionStateInProgress && state != ConsumerTransactionStateCommitting {
		return fmt.Errorf("transaction cannot be aborted: %s, state: %s", transactionID, state)
	}
	
	transaction.UpdateState(ConsumerTransactionStateAborting)
	
	err := ctm.sendAbortTransactionRequest(ctx, transaction)
	if err != nil {
		transaction.UpdateState(ConsumerTransactionStateInProgress)
		return fmt.Errorf("failed to abort transaction: %v", err)
	}
	
	transaction.UpdateState(ConsumerTransactionStateAborted)
	
	
	if ctm.currentTransaction != nil && ctm.currentTransaction.ID == transactionID {
		ctm.currentTransaction = nil
	}
	
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
	if ctm.cleanupTicker != nil {
		ctm.cleanupTicker.Stop()
	}
	
	select {
	case ctm.stopCleanup <- struct{}{}:
	default:
	}
	
	return nil
}

// sendBeginTransactionRequest send begin transaction request
func (ctm *ConsumerTransactionManager) sendBeginTransactionRequest(ctx context.Context, transaction *ConsumerTransaction) error {
	requestData, err := ctm.buildBeginTransactionRequest(transaction)
	if err != nil {
		return fmt.Errorf("failed to build start transaction request: %w", err)
	}

	responseData, err := ctm.client.sendMetaRequest(protocol.ConsumerBeginTransactionRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send start transaction request: %w", err)
	}

	return ctm.parseBeginTransactionResponse(responseData)
}

func (ctm *ConsumerTransactionManager) sendCommitTransactionRequest(ctx context.Context, transaction *ConsumerTransaction) error {
	requestData, err := ctm.buildCommitTransactionRequest(transaction)
	if err != nil {
		return fmt.Errorf("failed to build commit transaction request: %w", err)
	}

	responseData, err := ctm.client.sendMetaRequest(protocol.ConsumerCommitTransactionRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send commit transaction request: %w", err)
	}

	return ctm.parseCommitTransactionResponse(responseData)
}

func (ctm *ConsumerTransactionManager) sendAbortTransactionRequest(ctx context.Context, transaction *ConsumerTransaction) error {
	requestData, err := ctm.buildAbortTransactionRequest(transaction)
	if err != nil {
		return fmt.Errorf("failed to build abort transaction request: %w", err)
	}

	responseData, err := ctm.client.sendMetaRequest(protocol.ConsumerAbortTransactionRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send abort transaction request: %w", err)
	}

	return ctm.parseAbortTransactionResponse(responseData)
}

func (ctm *ConsumerTransactionManager) buildBeginTransactionRequest(transaction *ConsumerTransaction) ([]byte, error) {
	request := map[string]interface{}{
		"transaction_id": transaction.ID,
		"consumer_id":    ctm.consumerID,
		"group_id":       ctm.groupID,
		"timeout_ms":     int64(transaction.Timeout / time.Millisecond),
	}
	return json.Marshal(request)
}

func (ctm *ConsumerTransactionManager) buildCommitTransactionRequest(transaction *ConsumerTransaction) ([]byte, error) {
	request := map[string]interface{}{
		"transaction_id": transaction.ID,
		"consumer_id":    ctm.consumerID,
		"group_id":       ctm.groupID,
	}
	return json.Marshal(request)
}

func (ctm *ConsumerTransactionManager) buildAbortTransactionRequest(transaction *ConsumerTransaction) ([]byte, error) {
	request := map[string]interface{}{
		"transaction_id": transaction.ID,
		"consumer_id":    ctm.consumerID,
		"group_id":       ctm.groupID,
	}
	return json.Marshal(request)
}

func (ctm *ConsumerTransactionManager) parseBeginTransactionResponse(responseData []byte) error {
	var response map[string]interface{}
	if err := json.Unmarshal(responseData, &response); err != nil {
		return fmt.Errorf("failed to parse start transaction response: %w", err)
	}

	if errorCode, exists := response["error_code"]; exists {
		if code, ok := errorCode.(float64); ok && code != 0 {
			errorMsg := "unknown error"
			if msg, exists := response["error_message"]; exists {
				if msgStr, ok := msg.(string); ok {
					errorMsg = msgStr
				}
			}
			return fmt.Errorf("failed to start transaction: %s (error code: %.0f)", errorMsg, code)
		}
	}

	return nil
}

func (ctm *ConsumerTransactionManager) parseCommitTransactionResponse(responseData []byte) error {
	var response map[string]interface{}
	if err := json.Unmarshal(responseData, &response); err != nil {
		return fmt.Errorf("failed to parse commit transaction response: %w", err)
	}

	if errorCode, exists := response["error_code"]; exists {
		if code, ok := errorCode.(float64); ok && code != 0 {
			errorMsg := "unknown error"
			if msg, exists := response["error_message"]; exists {
				if msgStr, ok := msg.(string); ok {
					errorMsg = msgStr
				}
			}
			return fmt.Errorf("failed to commit transaction: %s (error code: %.0f)", errorMsg, code)
		}
	}

	return nil
}

func (ctm *ConsumerTransactionManager) parseAbortTransactionResponse(responseData []byte) error {
	var response map[string]interface{}
	if err := json.Unmarshal(responseData, &response); err != nil {
		return fmt.Errorf("failed to parse abort transaction response: %w", err)
	}

	if errorCode, exists := response["error_code"]; exists {
		if code, ok := errorCode.(float64); ok && code != 0 {
			errorMsg := "unknown error"
			if msg, exists := response["error_message"]; exists {
				if msgStr, ok := msg.(string); ok {
					errorMsg = msgStr
				}
			}
			return fmt.Errorf("failed to abort transaction: %s (error code: %.0f)", errorMsg, code)
		}
	}

	return nil
}

func (ctm *ConsumerTransactionManager) runCleanup() {
	for {
		select {
		case <-ctm.cleanupTicker.C:
			ctm.cleanupExpiredTransactions()
		case <-ctm.stopCleanup:
			return
		}
	}
}

func (ctm *ConsumerTransactionManager) cleanupExpiredTransactions() {
	ctm.mu.Lock()
	defer ctm.mu.Unlock()
	
	for transactionID, transaction := range ctm.transactions {
		if transaction.IsExpired() {
			state := transaction.GetState()
			if state == ConsumerTransactionStateCommitted || state == ConsumerTransactionStateAborted {
				delete(ctm.transactions, transactionID)
			} else {
				transaction.UpdateState(ConsumerTransactionStateAborted)
				if ctm.currentTransaction != nil && ctm.currentTransaction.ID == transactionID {
					ctm.currentTransaction = nil
				}
			}
		}
	}
}