package transaction

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// TransactionManager manages transaction lifecycle and state
type TransactionManager struct {
	storage       HalfMessageStorage
	expiryManager *ExpiryManager
	mu            sync.RWMutex

	defaultTimeout   time.Duration
	maxCheckCount    int
	checkInterval    time.Duration
	maxCheckInterval time.Duration

	producerGroupCheckers map[string]*DefaultTransactionChecker

	enableRaft  bool
	raftGroupID uint64

	errorHandler *TransactionErrorHandler
	metrics      *TransactionErrorMetrics

	queryService *TransactionQueryService

	stopChan chan struct{}
}

// NewTransactionManager creates transaction manager with default configuration
func NewTransactionManager() *TransactionManager {
	return NewTransactionManagerWithConfig(DefaultManagerConfig())
}

func NewTransactionManagerWithConfig(config *ManagerConfig) *TransactionManager {
	if err := config.Validate(); err != nil {
		log.Fatalf("Invalid transaction manager config: %v", err)
	}

	storage, err := NewPebbleHalfMessageStorage(config.StoragePath)
	if err != nil {
		log.Fatalf("Failed to initialize half message storage: %v", err)
	}

	expiryManager := NewExpiryManager(storage, &ExpiryManagerConfig{
		CheckInterval: config.ExpiryCheckInterval,
		Logger:        config.Logger,
	})

	tm := &TransactionManager{
		storage:               storage,
		expiryManager:         expiryManager,
		defaultTimeout:        config.DefaultTimeout,
		maxCheckCount:         config.MaxCheckCount,
		checkInterval:         config.CheckInterval,
		maxCheckInterval:      config.MaxCheckInterval,
		producerGroupCheckers: make(map[string]*DefaultTransactionChecker),
		enableRaft:            config.EnableRaft,
		raftGroupID:           config.RaftGroupID,
		errorHandler:          NewTransactionErrorHandler(config.Logger),
		metrics:               NewTransactionErrorMetrics(config.Logger),
		stopChan:              make(chan struct{}),
	}

	go tm.startTransactionChecker()

	tm.expiryManager.Start()

	return tm
}

// PrepareTransaction prepares a new transaction
func (tm *TransactionManager) PrepareTransaction(req *TransactionPrepareRequest) (*TransactionPrepareResponse, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if storedMsg, err := tm.storage.Get(string(req.TransactionID)); err == nil && storedMsg != nil {
		return &TransactionPrepareResponse{
			TransactionID: req.TransactionID,
			ErrorCode:     protocol.ErrorInvalidRequest,
			Error:         "transaction already exists",
		}, nil
	}

	timeout := tm.defaultTimeout
	if req.Timeout > 0 {
		timeout = time.Duration(req.Timeout) * time.Millisecond
	}

	_, exists := tm.producerGroupCheckers[req.ProducerGroup]
	if !exists {
		return &TransactionPrepareResponse{
			TransactionID: req.TransactionID,
			ErrorCode:     protocol.ErrorInvalidRequest,
			Error:         fmt.Sprintf("producer group not registered: %s", req.ProducerGroup),
		}, nil
	}

	halfMessage := &HalfMessage{
		TransactionID: req.TransactionID,
		Topic:         req.Topic,
		Partition:     req.Partition,
		Key:           req.Key,
		Value:         req.Value,
		Headers:       req.Headers,
		ProducerGroup: req.ProducerGroup,
		CreatedAt:     time.Now(),
		Timeout:       timeout,
		State:         StatePrepared,
		CheckCount:    0,
		LastCheck:     time.Time{},
	}

	expireTime := time.Now().Add(timeout)
	if err := tm.storage.Store(string(req.TransactionID), halfMessage, expireTime); err != nil {
		return &TransactionPrepareResponse{
			TransactionID: req.TransactionID,
			ErrorCode:     protocol.ErrorInternalError,
			Error:         fmt.Sprintf("failed to store half message: %v", err),
		}, nil
	}

	return &TransactionPrepareResponse{
		TransactionID: req.TransactionID,
		ErrorCode:     protocol.ErrorNone,
	}, nil
}

// CommitTransaction commits a transaction
func (tm *TransactionManager) CommitTransaction(transactionID TransactionID) (*TransactionCommitResponse, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	storedMsg, err := tm.storage.Get(string(transactionID))
	if err != nil {
		return &TransactionCommitResponse{
			TransactionID: transactionID,
			ErrorCode:     protocol.ErrorInvalidRequest,
			Error:         "transaction not found",
		}, nil
	}

	storedMsg.HalfMessage.State = StateCommit

	if err := tm.storage.Delete(string(transactionID)); err != nil {
		log.Printf("Failed to delete committed transaction %s: %v", transactionID, err)
	}

	log.Printf("Transaction committed: %s", transactionID)

	return &TransactionCommitResponse{
		TransactionID: transactionID,
		Offset:        -1,
		Timestamp:     time.Now(),
		ErrorCode:     protocol.ErrorNone,
	}, nil
}

// RollbackTransaction rolls back a transaction
func (tm *TransactionManager) RollbackTransaction(transactionID TransactionID) (*TransactionRollbackResponse, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if err := tm.storage.Delete(string(transactionID)); err != nil {
		log.Printf("Failed to delete rolled back transaction %s: %v", transactionID, err)
	}

	return &TransactionRollbackResponse{
		TransactionID: transactionID,
		ErrorCode:     protocol.ErrorNone,
	}, nil
}

func (tm *TransactionManager) GetHalfMessage(transactionID TransactionID) (*HalfMessage, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	storedMsg, err := tm.storage.Get(string(transactionID))
	if err != nil || storedMsg == nil {
		return nil, false
	}

	return storedMsg.HalfMessage, true
}

// GetAllHalfMessages get all half messages
func (tm *TransactionManager) GetAllHalfMessages() map[TransactionID]*HalfMessage {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	result := make(map[TransactionID]*HalfMessage)

	futureTime := time.Now().Add(365 * 24 * time.Hour)
	storedMsgs, err := tm.storage.GetExpiredMessages(futureTime)
	if err != nil {
		log.Printf("Failed to get all half messages: %v", err)
		return result
	}

	for _, storedMsg := range storedMsgs {
		result[TransactionID(storedMsg.TransactionID)] = storedMsg.HalfMessage
	}

	return result
}

// RegisterProducerGroup registers a producer group with the transaction manager
func (tm *TransactionManager) RegisterProducerGroup(group, callbackAddr string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.producerGroupCheckers[group]; !exists {
		checker := NewDefaultTransactionChecker()
		tm.producerGroupCheckers[group] = checker
		log.Printf("Created new checker for producer group: %s", group)
	}

	checker := tm.producerGroupCheckers[group]
	checker.RegisterProducerGroup(group, callbackAddr)
	return nil
}

// UnregisterProducerGroup unregisters a producer group from the transaction manager
func (tm *TransactionManager) UnregisterProducerGroup(producerGroup string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	checker, exists := tm.producerGroupCheckers[producerGroup]
	if !exists {
		return fmt.Errorf("producer group not found: %s", producerGroup)
	}

	checker.UnregisterProducerGroup(producerGroup)

	delete(tm.producerGroupCheckers, producerGroup)
	log.Printf("Unregistered producer group: %s", producerGroup)

	return nil
}

// GetRegisteredProducerGroups returns all registered producer groups
func (tm *TransactionManager) GetRegisteredProducerGroups() map[string]string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	result := make(map[string]string)
	for group, checker := range tm.producerGroupCheckers {
		groups := checker.GetRegisteredProducerGroups()
		for g, addr := range groups {
			result[g] = addr
		}
		if len(groups) == 0 {
			result[group] = ""
		}
	}

	return result
}

func (tm *TransactionManager) startTransactionChecker() {
	ticker := time.NewTicker(tm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.checkTimeoutTransactions()
		case <-tm.stopChan:
			return
		}
	}
}

func (tm *TransactionManager) checkTimeoutTransactions() {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	expiredMsgs, err := tm.storage.GetExpiredMessages(time.Now())
	if err != nil {
		log.Printf("Failed to get expired messages: %v", err)
		return
	}

	for _, storedMsg := range expiredMsgs {
		halfMessage := storedMsg.HalfMessage

		if halfMessage.CheckCount >= tm.maxCheckCount {
			log.Printf("Transaction %s exceeded max check count, removing", halfMessage.TransactionID)
			if err := tm.storage.Delete(storedMsg.TransactionID); err != nil {
				log.Printf("Failed to delete expired transaction %s: %v", halfMessage.TransactionID, err)
			}
			continue
		}

		checker, exists := tm.producerGroupCheckers[halfMessage.ProducerGroup]
		if !exists {
			log.Printf("No checker found for producer group: %s", halfMessage.ProducerGroup)
			continue
		}

		// 执行回查
		go func(txnID TransactionID, hm *HalfMessage) {
			state := checker.CheckTransactionState(txnID, *hm)
			tm.handleCheckResult(txnID, state)
		}(halfMessage.TransactionID, halfMessage)

		// 更新检查次数和时间
		halfMessage.CheckCount++
		halfMessage.LastCheck = time.Now()

		// 重新存储更新后的消息
		expireTime := time.Unix(0, storedMsg.ExpireTime*int64(time.Millisecond))
		if err := tm.storage.Store(storedMsg.TransactionID, halfMessage, expireTime); err != nil {
			log.Printf("Failed to update half message %s: %v", halfMessage.TransactionID, err)
		}
	}
}

func (tm *TransactionManager) calculateCheckInterval(checkCount int) time.Duration {
	interval := time.Duration(checkCount) * tm.checkInterval
	if interval > tm.maxCheckInterval {
		return tm.maxCheckInterval
	}
	return interval
}

func (tm *TransactionManager) handleCheckResult(transactionID TransactionID, state TransactionState) {
	switch state {
	case StateCommit:
		_, err := tm.CommitTransaction(transactionID)
		if err != nil {
			log.Printf("Failed to commit transaction %s: %v", transactionID, err)
		}
	case StateRollback:
		_, err := tm.RollbackTransaction(transactionID)
		if err != nil {
			log.Printf("Failed to rollback transaction %s: %v", transactionID, err)
		}
	case StatePrepared:
		// 继续等待
		log.Printf("Transaction %s still in prepared state", transactionID)
	default:
		log.Printf("Unknown transaction state for %s: %v", transactionID, state)
	}
}

// Stop stops the transaction manager
func (tm *TransactionManager) Stop() {
	close(tm.stopChan)

	// 停止过期管理器
	if tm.expiryManager != nil {
		tm.expiryManager.Stop()
	}

	// 关闭存储
	if tm.storage != nil {
		if err := tm.storage.Close(); err != nil {
			log.Printf("Failed to close storage: %v", err)
		}
	}
}

// StoreHalfMessage stores a half message for a transaction
func (tm *TransactionManager) StoreHalfMessage(transactionID TransactionID, message *HalfMessage) error {
	startTime := time.Now()

	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 计算过期时间
	expireTime := message.CreatedAt.Add(message.Timeout)

	// 存储到PebbleDB
	err := tm.storage.Store(string(transactionID), message, expireTime)
	if err != nil {
		tm.metrics.RecordError("storage_error", "store_half_message", message.ProducerGroup)
		return err
	}

	tm.metrics.RecordSuccess(string(transactionID), "store_half_message", time.Since(startTime))
	return nil
}

// GetTransactionStatus retrieves the status of a transaction
func (tm *TransactionManager) GetTransactionStatus(transactionID TransactionID) (map[string]interface{}, error) {
	if tm.queryService != nil {
		// Use query service for Raft-enabled mode
		ctx := context.Background()
		response, err := tm.queryService.GetHalfMessage(ctx, tm.raftGroupID, string(transactionID))
		if err != nil {
			return nil, err
		}

		if !response.Success {
			return nil, fmt.Errorf("query failed: %s", response.Error)
		}

		return map[string]interface{}{
			"transaction_id": transactionID,
			"status":         "found",
			"data":           response.Data,
		}, nil
	}

	// 使用新的存储层
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	storedMsg, err := tm.storage.Get(string(transactionID))
	if err != nil {
		return nil, fmt.Errorf("transaction not found: %s", transactionID)
	}

	return map[string]interface{}{
		"transaction_id": string(transactionID),
		"state":          storedMsg.Status,
		"created_at":     time.Unix(0, storedMsg.CreatedTime*int64(time.Millisecond)),
		"expire_time":    time.Unix(0, storedMsg.ExpireTime*int64(time.Millisecond)),
		"half_message":   storedMsg.HalfMessage,
	}, nil
}

// GetExpiredTransactions retrieves all expired transactions
func (tm *TransactionManager) GetExpiredTransactions() ([]interface{}, error) {
	if tm.queryService != nil {
		// Use query service for Raft-enabled mode
		ctx := context.Background()
		response, err := tm.queryService.GetExpiredTransactions(ctx, tm.raftGroupID)
		if err != nil {
			return nil, err
		}

		if !response.Success {
			return nil, fmt.Errorf("query failed: %s", response.Error)
		}

		// Extract expired transactions from response data
		if data, ok := response.Data.(map[string]interface{}); ok {
			if expiredTxns, ok := data["expired_transactions"].([]interface{}); ok {
				return expiredTxns, nil
			}
		}

		return []interface{}{}, nil
	}

	// 使用新的存储层
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	storedMsgs, err := tm.storage.GetExpiredMessages(time.Now())
	if err != nil {
		return nil, err
	}

	var expired []interface{}
	for _, storedMsg := range storedMsgs {
		expired = append(expired, storedMsg)
	}

	return expired, nil
}
