package transaction

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// PartitionTransactionManager manages transactions at the partition level
type PartitionTransactionManager struct {
	mu sync.RWMutex

	defaultTimeout   time.Duration
	maxCheckCount    int
	checkInterval    time.Duration
	maxCheckInterval time.Duration

	// Producer group checker mapping
	producerGroupCheckers map[string]*DefaultTransactionChecker

	router *TransactionRouter

	// Raft proposer for executing transaction commands
	raftProposer RaftProposer

	// State machine getter for retrieving transaction state
	stateMachineGetter StateMachineGetter

	// Raft manager for executing SyncRead operations
	raftManager RaftManagerInterface

	// Persistent storage for half messages
	storage HalfMessageStorage

	// Metrics collector
	metrics *TransactionMetrics

	// Error handler
	errorHandler *TransactionErrorHandler

	logger *log.Logger

	stopCh chan struct{}
}

// RaftManagerInterface defines the Raft manager interface
type RaftManagerInterface interface {
	SyncRead(ctx context.Context, groupID uint64, query interface{}) (interface{}, error)
	GetLeaderID(groupID uint64) (uint64, bool, error)
	IsLeader(groupID uint64) bool
}

// NewPartitionTransactionManager creates a new partition transaction manager
func NewPartitionTransactionManager(
	topic string,
	partitionID int32,
	logger *log.Logger,
	maxCheckCount int,
	checkInterval time.Duration,
) *PartitionTransactionManager {
	config := DefaultPartitionTransactionManagerConfig(topic, partitionID)
	if logger != nil {
		config.Logger = logger
	}
	if maxCheckCount > 0 {
		config.MaxCheckCount = maxCheckCount
	}
	if checkInterval > 0 {
		config.CheckInterval = checkInterval
	}
	return NewPartitionTransactionManagerWithConfig(config)
}

func NewPartitionTransactionManagerWithConfig(config *PartitionTransactionManagerConfig) *PartitionTransactionManager {
	if err := config.Validate(); err != nil {
		panic(fmt.Sprintf("invalid config: %v", err))
	}

	// If no error handler is provided, create a default one
	errorHandler := config.ErrorHandler
	if errorHandler == nil {
		errorHandler = NewTransactionErrorHandler(config.Logger)
	}

	return &PartitionTransactionManager{
		defaultTimeout:        config.DefaultTimeout,
		maxCheckCount:         config.MaxCheckCount,
		checkInterval:         config.CheckInterval,
		maxCheckInterval:      config.MaxCheckInterval,
		producerGroupCheckers: make(map[string]*DefaultTransactionChecker),
		router:                NewTransactionRouter(),
		raftProposer:          config.RaftProposer,
		stateMachineGetter:    config.StateMachineGetter,
		raftManager:           config.RaftManager,
		storage:               config.Storage,
		metrics:               NewTransactionMetrics(),
		errorHandler:          errorHandler,
		logger:                config.Logger,
		stopCh:                make(chan struct{}),
	}
}



// CommitTransaction commits a transaction (public interface)
func (ptm *PartitionTransactionManager) CommitTransaction(transactionID TransactionID) (*TransactionCommitResponse, error) {
	ptm.logger.Printf("Public CommitTransaction called for: %s", transactionID)

	// Execute the actual transaction commit
	err := ptm.commitTransaction(transactionID)
	if err != nil {
		return &TransactionCommitResponse{
			TransactionID: transactionID,
			ErrorCode:     protocol.ErrorInternalError,
			Error:         fmt.Sprintf("failed to commit transaction: %v", err),
		}, nil
	}

	return &TransactionCommitResponse{
		TransactionID: transactionID,
		Offset:        -1, // At partition level, offset is handled by state machine
		Timestamp:     time.Now(),
		ErrorCode:     protocol.ErrorNone,
	}, nil
}

// RollbackTransaction rollback txn
func (ptm *PartitionTransactionManager) RollbackTransaction(transactionID TransactionID) (*TransactionRollbackResponse, error) {
	ptm.logger.Printf("Public RollbackTransaction called for: %s", transactionID)

	err := ptm.rollbackTransaction(transactionID)
	if err != nil {
		return &TransactionRollbackResponse{
			TransactionID: transactionID,
			ErrorCode:     protocol.ErrorInternalError,
			Error:         fmt.Sprintf("failed to rollback transaction: %v", err),
		}, nil
	}

	return &TransactionRollbackResponse{
		TransactionID: transactionID,
		ErrorCode:     protocol.ErrorNone,
	}, nil
}

// GetHalfMessage retrieves half message from persistent storage
func (ptm *PartitionTransactionManager) GetHalfMessage(transactionID TransactionID, topicName string) (*HalfMessage, bool) {
	ptm.mu.RLock()
	defer ptm.mu.RUnlock()

	if ptm.storage == nil {
		ptm.logger.Printf("Storage is not available")
		return nil, false
	}

	// Get half message from persistent storage
	storedMsg, err := ptm.storage.Get(string(transactionID))
	if err != nil {
		ptm.logger.Printf("Failed to get half message for transaction %s: %v", transactionID, err)
		return nil, false
	}

	if storedMsg == nil || storedMsg.HalfMessage == nil {
		return nil, false
	}

	halfMessage := storedMsg.HalfMessage

	// Validate topic match (if topicName is specified)
	if topicName != "" && halfMessage.Topic != topicName {
		return nil, false
	}

	return halfMessage, true
}

// GetExpiredHalfMessages retrieves expired half messages
// Uses Raft Lookup interface instead of type assertion
func (ptm *PartitionTransactionManager) GetExpiredHalfMessages() ([]ExpiredTransactionInfo, error) {
	ptm.mu.RLock()
	defer ptm.mu.RUnlock()

	if ptm.stateMachineGetter == nil {
		return nil, fmt.Errorf("state machine getter not available")
	}

	var allExpiredTransactions []ExpiredTransactionInfo

	// Get all relevant Raft group IDs
	raftGroupIDs := ptm.getAllRelevantRaftGroups()

	for _, groupID := range raftGroupIDs {
		// Add leader/follower check
		isLeader := ptm.raftManager.IsLeader(groupID)
		ptm.logger.Printf("Checking expired transactions for group %d, node role: %s", 
			groupID, map[bool]string{true: "leader", false: "follower"}[isLeader])

		// Use SyncRead to ensure linearizable read
		queryReq := map[string]interface{}{
			"type": "get_expired_transactions",
		}

		queryBytes, err := json.Marshal(queryReq)
		if err != nil {
			ptm.logger.Printf("Failed to marshal expired transactions query for group %d: %v", groupID, err)
			continue
		}

		result, err := ptm.raftManager.SyncRead(context.Background(), groupID, queryBytes)
		if err != nil {
			ptm.logger.Printf("Failed to query expired transactions for group %d: %v", groupID, err)
			continue
		}

		// Convert result to []byte
		var resultBytes []byte
		switch r := result.(type) {
		case []byte:
			resultBytes = r
		case string:
			resultBytes = []byte(r)
		default:
			ptm.logger.Printf("Unexpected result type for group %d: %T", groupID, result)
			continue
		}

		// Parse query result
		var response struct {
			Success             bool `json:"success"`
			ExpiredTransactions []struct {
				TransactionID string        `json:"transaction_id"`
				ProducerGroup string        `json:"producer_group"`
				Topic         string        `json:"topic"`
				Partition     int32         `json:"partition"`
				CreatedAt     time.Time     `json:"created_at"`
				Timeout       time.Duration `json:"timeout"`
			} `json:"expired_transactions"`
			Error string `json:"error"`
		}

		if err := json.Unmarshal(resultBytes, &response); err != nil {
			ptm.logger.Printf("Failed to unmarshal expired transactions response for group %d: %v", groupID, err)
			continue
		}

		if !response.Success {
			ptm.logger.Printf("Expired transactions query failed for group %d: %s", groupID, response.Error)
			continue
		}

		// Convert to ExpiredTransactionInfo
		for _, expiredTxn := range response.ExpiredTransactions {
			expiredInfo := ExpiredTransactionInfo{
				TransactionID: TransactionID(expiredTxn.TransactionID),
				TopicName:     expiredTxn.Topic,
				PartitionID:   expiredTxn.Partition,
				RaftGroupID:   groupID,
				ProducerGroup: expiredTxn.ProducerGroup,
				CreatedAt:     expiredTxn.CreatedAt,
				Timeout:       expiredTxn.Timeout,
				CheckCount:    0, // This field is not available in raft package, set to 0
			}
			allExpiredTransactions = append(allExpiredTransactions, expiredInfo)
		}
	}

	ptm.logger.Printf("Found %d expired transactions across all partitions using Raft Lookup", len(allExpiredTransactions))
	return allExpiredTransactions, nil
}

// getAllRelevantRaftGroups retrieves all relevant Raft group IDs
// This is a helper method, actual implementation needs to be determined based on partition allocation strategy
func (ptm *PartitionTransactionManager) getAllRelevantRaftGroups() []uint64 {
	if ptm.stateMachineGetter == nil {
		ptm.logger.Printf("StateMachineGetter is nil, cannot get Raft groups")
		return []uint64{}
	}

	allGroups := ptm.stateMachineGetter.GetAllRaftGroups()

	var relevantGroups []uint64
	for _, groupID := range allGroups {
		// Controller group
		if groupID == 1 {
			continue
		}

		// Try to get state machine, confirm it's a partition state machine
		if sm, err := ptm.stateMachineGetter.GetStateMachine(groupID); err == nil && sm != nil {
			relevantGroups = append(relevantGroups, groupID)
		}
	}

	ptm.logger.Printf("Found %d relevant partition Raft groups: %v", len(relevantGroups), relevantGroups)
	return relevantGroups
}

// ExpiredTransactionInfo contains expired transaction information
type ExpiredTransactionInfo struct {
	TransactionID TransactionID `json:"transaction_id"`
	TopicName     string        `json:"topic_name"`
	PartitionID   int32         `json:"partition_id"`
	RaftGroupID   uint64        `json:"raft_group_id"`
	ProducerGroup string        `json:"producer_group"`
	CreatedAt     time.Time     `json:"created_at"`
	Timeout       time.Duration `json:"timeout"`
	CheckCount    int           `json:"check_count"`
}

// RegisterProducerGroup registers a producer group (simplified version)
func (ptm *PartitionTransactionManager) RegisterProducerGroup(group, callbackAddr string) error {
	ptm.mu.Lock()
	defer ptm.mu.Unlock()

	// 创建事务检查器
	checker := NewDefaultTransactionChecker()
	checker.RegisterProducerGroup(group, callbackAddr)
	ptm.producerGroupCheckers[group] = checker

	ptm.logger.Printf("Registered producer group %s with callback address %s", group, callbackAddr)
	return nil
}

// UnregisterProducerGroup 注销生产者组
func (ptm *PartitionTransactionManager) UnregisterProducerGroup(group string) error {
	ptm.mu.Lock()
	defer ptm.mu.Unlock()

	checker, exists := ptm.producerGroupCheckers[group]
	if !exists {
		return fmt.Errorf("producer group %s not found", group)
	}

	// 清理资源 - 从 checker 中注销生产者组
	if checker != nil {
		checker.UnregisterProducerGroup(group)
	}

	delete(ptm.producerGroupCheckers, group)
	ptm.logger.Printf("Unregistered producer group %s", group)
	
	return nil
}

// GetRegisteredProducerGroups 获取已注册的生产者组
func (ptm *PartitionTransactionManager) GetRegisteredProducerGroups() []string {
	ptm.mu.RLock()
	defer ptm.mu.RUnlock()

	groups := make([]string, 0, len(ptm.producerGroupCheckers))
	for group := range ptm.producerGroupCheckers {
		groups = append(groups, group)
	}
	return groups
}



// commitTransaction 内部提交事务方法
func (ptm *PartitionTransactionManager) commitTransaction(transactionID TransactionID) error {
	ptm.logger.Printf("Committing transaction: %s", transactionID)

	// 创建错误上下文
	errorCtx := ErrorContext{
		TransactionID: string(transactionID),
		Operation:     "commit",
		Timestamp:     time.Now(),
	}

	// 检查持久化存储是否可用
	if ptm.storage == nil {
		err := fmt.Errorf("persistent storage not available for transaction %s", transactionID)
		ptm.errorHandler.HandleTransactionError(err, errorCtx)
		return err
	}

	// 从持久化存储获取半消息
	storedMsg, err := ptm.storage.Get(string(transactionID))
	if err != nil {
		err = fmt.Errorf("failed to get half message for transaction %s: %w", transactionID, err)
		ptm.errorHandler.HandleTransactionError(err, errorCtx)
		return err
	}

	halfMessage := storedMsg.HalfMessage
	if halfMessage.State != StatePrepared {
		err := fmt.Errorf("transaction %s not in prepared state: %v", transactionID, halfMessage.State)
		ptm.errorHandler.HandleTransactionError(err, errorCtx)
		return err
	}

	// 获取相关的Raft组ID
	raftGroupIDs := ptm.getAllRelevantRaftGroups()
	if len(raftGroupIDs) == 0 {
		err := fmt.Errorf("no relevant raft groups found for transaction %s", transactionID)
		ptm.errorHandler.HandleTransactionError(err, errorCtx)
		return err
	}

	// 构造消息生产命令
	produceCommand := map[string]interface{}{
		"type":      "produce_message",
		"topic":     halfMessage.Topic,
		"partition": halfMessage.Partition,
		"key":       string(halfMessage.Key),
		"value":     string(halfMessage.Value),
		"headers":   halfMessage.Headers,
		"timestamp": time.Now().Unix(),
	}

	// 跟踪每个组的执行结果
	type groupResult struct {
		groupID uint64
		success bool
		error   error
	}
	
	results := make([]groupResult, 0, len(raftGroupIDs))
	var lastErr error
	successCount := 0

	// 对每个相关的Raft组执行消息生产操作
	for _, groupID := range raftGroupIDs {
		// 通过Raft提议器执行消息生产
		ctx, cancel := context.WithTimeout(context.Background(), ptm.defaultTimeout)
		_, err := ptm.raftProposer.ProposeTransactionCommand(ctx, groupID, "produce_message", produceCommand)
		cancel()

		result := groupResult{
			groupID: groupID,
			success: err == nil,
			error:   err,
		}
		results = append(results, result)

		if err != nil {
			ptm.logger.Printf("Failed to produce message for transaction %s in group %d: %v", transactionID, groupID, err)
			lastErr = err
			continue
		}

		successCount++
		ptm.logger.Printf("Successfully produced message for transaction %s in group %d", transactionID, groupID)
	}

	// 记录详细的执行结果
	ptm.logger.Printf("Transaction %s commit results: %d/%d groups succeeded", 
		transactionID, successCount, len(raftGroupIDs))

	// 如果所有组都失败了，返回错误
	if successCount == 0 {
		err := fmt.Errorf("failed to commit transaction %s in all groups, last error: %v", transactionID, lastErr)
		ptm.errorHandler.HandleTransactionError(err, errorCtx)
		return err
	}

	// 从持久化存储中删除半消息
	if err := ptm.storage.Delete(string(transactionID)); err != nil {
		ptm.logger.Printf("Warning: failed to delete committed transaction %s from storage: %v", transactionID, err)
		// 不返回错误，因为消息已经成功提交
	}

	// 如果部分成功，记录详细信息
	if successCount < len(raftGroupIDs) {
		failedGroups := make([]uint64, 0)
		for _, result := range results {
			if !result.success {
				failedGroups = append(failedGroups, result.groupID)
				ptm.logger.Printf("Group %d failed with error: %v", result.groupID, result.error)
			}
		}
		
		ptm.logger.Printf("Transaction %s partially committed: succeeded in %d/%d groups, failed groups: %v", 
			transactionID, successCount, len(raftGroupIDs), failedGroups)
		
		// 记录部分成功的情况
		partialErr := fmt.Errorf("transaction %s partially committed in %d/%d groups", 
			transactionID, successCount, len(raftGroupIDs))
		
		errorCtx.RetryCount = 0
		errorCtx.MaxRetries = 1
		ptm.errorHandler.HandleTransactionError(partialErr, errorCtx)
	}

	ptm.logger.Printf("Transaction %s committed successfully", transactionID)
	return nil
}

func (ptm *PartitionTransactionManager) rollbackTransaction(transactionID TransactionID) error {
	ptm.logger.Printf("Rolling back transaction: %s", transactionID)

	errorCtx := ErrorContext{
		TransactionID: string(transactionID),
		Operation:     "rollback",
		Timestamp:     time.Now(),
	}

	if ptm.storage == nil {
		err := fmt.Errorf("persistent storage not available for transaction %s", transactionID)
		ptm.errorHandler.HandleTransactionError(err, errorCtx)
		return err
	}

	// 直接从持久化存储中删除半消息
	err := ptm.storage.Delete(string(transactionID))
	if err != nil {
		ptm.logger.Printf("Failed to delete transaction %s from storage: %v", transactionID, err)
		ptm.errorHandler.HandleTransactionError(err, errorCtx)
		return fmt.Errorf("failed to rollback transaction %s: %w", transactionID, err)
	}

	// 记录指标
	ptm.metrics.RecordTransactionRollback()

	ptm.logger.Printf("Transaction %s rollback completed", transactionID)
	return nil
}

// handleUnknownTransactionState 处理未知事务状态
func (ptm *PartitionTransactionManager) handleUnknownTransactionState(expiredTxn ExpiredTransactionInfo) error {
	// 检查是否达到最大重试次数
	if expiredTxn.CheckCount >= ptm.maxCheckCount {
		ptm.logger.Printf("Transaction %s reached max check count, rolling back", expiredTxn.TransactionID)
		ptm.metrics.RecordRetry()
		return ptm.rollbackTransaction(expiredTxn.TransactionID)
	}

	// 记录重试
	ptm.metrics.RecordRetry()
	ptm.logger.Printf("Transaction %s state unknown, will retry later (count: %d/%d)",
		expiredTxn.TransactionID, expiredTxn.CheckCount, ptm.maxCheckCount)

	return nil
}

// Stop 停止事务管理器
func (ptm *PartitionTransactionManager) Stop() {
	close(ptm.stopCh)
}

// Remove proposeToRaftGroup method since prepare operation is already handled at broker layer

// CheckTransactionState checks transaction state
// Uses Raft Lookup interface instead of type assertion
func (ptm *PartitionTransactionManager) CheckTransactionState(transactionID TransactionID) TransactionState {
	ptm.logger.Printf("Checking transaction state: %s", transactionID)

	// Get relevant Raft groups
	raftGroups := ptm.getAllRelevantRaftGroups()
	if len(raftGroups) == 0 {
		ptm.logger.Printf("No raft groups available for transaction state check")
		return StateUnknown
	}

	// Check transaction state in each Raft group
	for _, raftGroupID := range raftGroups {
		// Add leader/follower check
		isLeader := ptm.raftManager.IsLeader(raftGroupID)
		ptm.logger.Printf("Checking transaction state for group %d, node role: %s", 
			raftGroupID, map[bool]string{true: "leader", false: "follower"}[isLeader])

		// Use SyncRead to query half message
		queryReq := map[string]interface{}{
			"type":           "get_half_message",
			"transaction_id": string(transactionID),
		}

		queryBytes, err := json.Marshal(queryReq)
		if err != nil {
			ptm.logger.Printf("Failed to marshal half message query for group %d: %v", raftGroupID, err)
			continue
		}

		result, err := ptm.raftManager.SyncRead(context.Background(), raftGroupID, queryBytes)
		if err != nil {
			ptm.logger.Printf("Failed to query half message for group %d: %v", raftGroupID, err)
			continue
		}

		// Convert result to []byte
		var resultBytes []byte
		switch r := result.(type) {
		case []byte:
			resultBytes = r
		case string:
			resultBytes = []byte(r)
		default:
			ptm.logger.Printf("Unexpected result type for group %d: %T", raftGroupID, result)
			continue
		}

		// Parse query result
		var response struct {
			Success     bool `json:"success"`
			HalfMessage struct {
				TransactionID string        `json:"transaction_id"`
				ProducerGroup string        `json:"producer_group"`
				Topic         string        `json:"topic"`
				Partition     int32         `json:"partition"`
				State         int           `json:"state"`
				CreatedAt     time.Time     `json:"created_at"`
				Timeout       time.Duration `json:"timeout"`
			} `json:"half_message"`
			Error string `json:"error"`
		}

		if err := json.Unmarshal(resultBytes, &response); err != nil {
			ptm.logger.Printf("Failed to unmarshal half message response for group %d: %v", raftGroupID, err)
			continue
		}

		if response.Success && response.HalfMessage.TransactionID != "" {
			// Check if half message has expired
			if time.Since(response.HalfMessage.CreatedAt) > ptm.defaultTimeout {
				ptm.logger.Printf("Transaction %s has expired half-message", transactionID)
				return StateChecking // Use StateChecking to indicate state that needs checking
			}
			
			// Convert state
			state := TransactionState(response.HalfMessage.State)
			ptm.logger.Printf("Transaction %s is in progress with state: %s", transactionID, state.String())
			return state
		}

		// If no half message found, check if it's in expired transaction list
		expiredQueryReq := map[string]interface{}{
			"type": "get_expired_transactions",
		}

		expiredQueryBytes, err := json.Marshal(expiredQueryReq)
		if err != nil {
			ptm.logger.Printf("Failed to marshal expired transactions query for group %d: %v", raftGroupID, err)
			continue
		}

		expiredResult, err := ptm.raftManager.SyncRead(context.Background(), raftGroupID, expiredQueryBytes)
		if err != nil {
			ptm.logger.Printf("Failed to query expired transactions for group %d: %v", raftGroupID, err)
			continue
		}

		// Convert expiredResult to []byte
		var expiredResultBytes []byte
		switch r := expiredResult.(type) {
		case []byte:
			expiredResultBytes = r
		case string:
			expiredResultBytes = []byte(r)
		default:
			ptm.logger.Printf("Unexpected expired result type for group %d: %T", raftGroupID, expiredResult)
			continue
		}

		// Parse expired transaction query result
		var expiredResponse struct {
			Success             bool `json:"success"`
			ExpiredTransactions []struct {
				TransactionID string `json:"transaction_id"`
			} `json:"expired_transactions"`
			Error string `json:"error"`
		}

		if err := json.Unmarshal(expiredResultBytes, &expiredResponse); err != nil {
			ptm.logger.Printf("Failed to unmarshal expired transactions response for group %d: %v", raftGroupID, err)
			continue
		}

		if expiredResponse.Success {
			for _, expiredTxn := range expiredResponse.ExpiredTransactions {
				if expiredTxn.TransactionID == string(transactionID) {
					ptm.logger.Printf("Transaction %s is in timeout list", transactionID)
					return StateChecking // Use StateChecking to indicate state that needs checking
				}
			}
		}
	}

	// If transaction record is not found in any state machine, it may have been committed or rolled back
	ptm.logger.Printf("Transaction %s not found in any state machine", transactionID)
	return StateUnknown
}

// PrepareTransaction prepares a transaction by storing the half message in persistent storage
func (ptm *PartitionTransactionManager) PrepareTransaction(transactionID TransactionID, halfMessage *HalfMessage) error {
	ptm.mu.Lock()
	defer ptm.mu.Unlock()

	if ptm.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	// Calculate expiration time
	expireTime := halfMessage.CreatedAt.Add(halfMessage.Timeout)

	// Store the half message in persistent storage
	err := ptm.storage.Store(string(transactionID), halfMessage, expireTime)
	if err != nil {
		ptm.logger.Printf("Failed to store half message for transaction %s: %v", transactionID, err)
		return fmt.Errorf("failed to store half message: %w", err)
	}

	ptm.logger.Printf("Transaction prepared and stored persistently: %s", transactionID)
	return nil
}

// PrepareBatchTransaction prepares multiple messages for a single transaction
func (ptm *PartitionTransactionManager) PrepareBatchTransaction(transactionID TransactionID, halfMessages []*HalfMessage) error {
	ptm.mu.Lock()
	defer ptm.mu.Unlock()

	if transactionID == "" {
		return fmt.Errorf("transaction ID is empty")
	}

	if len(halfMessages) == 0 {
		return fmt.Errorf("no half messages provided")
	}

	// Validate all half messages first
	for i, halfMessage := range halfMessages {
		if halfMessage == nil {
			return fmt.Errorf("half message at index %d is nil", i)
		}
	}

	// Store all half messages with composite keys
	for i, halfMessage := range halfMessages {
		// Create composite key: transactionID:messageIndex
		compositeKey := fmt.Sprintf("%s:%d", string(transactionID), i)
		
		// Calculate expiration time
		expireTime := halfMessage.CreatedAt.Add(halfMessage.Timeout)

		// Store to persistent storage
		if ptm.storage != nil {
			if err := ptm.storage.Store(compositeKey, halfMessage, expireTime); err != nil {
				ptm.logger.Printf("Failed to store half message %d for transaction %s: %v", i, transactionID, err)
				return fmt.Errorf("failed to store half message %d for transaction %s: %v", i, transactionID, err)
			}
		}

		ptm.logger.Printf("Prepared message %d for transaction %s with topic %s, partition %d", 
			i, transactionID, halfMessage.Topic, halfMessage.Partition)
	}

	return nil
}

// GetBatchHalfMessages retrieves all half messages for a single transaction
func (ptm *PartitionTransactionManager) GetBatchHalfMessages(transactionID TransactionID) ([]*HalfMessage, error) {
	ptm.mu.RLock()
	defer ptm.mu.RUnlock()

	if ptm.storage == nil {
		return nil, fmt.Errorf("storage is not available")
	}

	var halfMessages []*HalfMessage
	
	// Iterate through possible message indices until we don't find any more
	for i := 0; ; i++ {
		compositeKey := fmt.Sprintf("%s:%d", string(transactionID), i)
		
		// Get half message from persistent storage
		storedMsg, err := ptm.storage.Get(compositeKey)
		if err != nil {
			ptm.logger.Printf("Failed to get half message %d for transaction %s: %v", i, transactionID, err)
			break // No more messages
		}

		if storedMsg == nil || storedMsg.HalfMessage == nil {
			break // No more messages
		}

		halfMessages = append(halfMessages, storedMsg.HalfMessage)
	}

	if len(halfMessages) == 0 {
		return nil, fmt.Errorf("no half messages found for transaction %s", transactionID)
	}

	return halfMessages, nil
}

// CommitBatchTransaction commits all messages in a batch transaction
func (ptm *PartitionTransactionManager) CommitBatchTransaction(transactionID TransactionID) (*BatchTransactionCommitResponse, error) {
	ptm.mu.Lock()
	defer ptm.mu.Unlock()

	if ptm.storage == nil {
		return &BatchTransactionCommitResponse{
			Results: []CommitResult{{
				TransactionID: transactionID,
				Success:       false,
				ErrorCode:     protocol.ErrorInternalError,
				ErrorMessage:  "storage not available",
			}},
		}, nil
	}

	// Get all half messages for this transaction
	halfMessages, err := ptm.GetBatchHalfMessages(transactionID)
	if err != nil {
		return &BatchTransactionCommitResponse{
			Results: []CommitResult{{
				TransactionID: transactionID,
				Success:       false,
				ErrorCode:     protocol.ErrorTransactionNotFound,
				ErrorMessage:  err.Error(),
			}},
		}, nil
	}

	var results []CommitResult
	
	// Process each message in the batch
	for i, halfMessage := range halfMessages {
		compositeKey := fmt.Sprintf("%s:%d", string(transactionID), i)
		
		// Create commit result for each message
		result := CommitResult{
			TransactionID: transactionID,
			Success:       true,
			ErrorCode:     protocol.ErrorNone,
			ErrorMessage:  "",
		}
		
		// Remove the half message from storage after successful commit
		if err := ptm.storage.Delete(compositeKey); err != nil {
			ptm.logger.Printf("Warning: failed to delete committed message %d for transaction %s: %v", i, transactionID, err)
		}
		
		ptm.logger.Printf("Committed message %d for transaction %s (topic: %s, partition: %d)", 
			i, transactionID, halfMessage.Topic, halfMessage.Partition)
		
		results = append(results, result)
	}

	return &BatchTransactionCommitResponse{
		Results: results,
	}, nil
}

// RollbackBatchTransaction rolls back all messages in a batch transaction
func (ptm *PartitionTransactionManager) RollbackBatchTransaction(transactionID TransactionID) (*BatchTransactionRollbackResponse, error) {
	ptm.mu.Lock()
	defer ptm.mu.Unlock()

	if ptm.storage == nil {
		return &BatchTransactionRollbackResponse{
			TransactionID: transactionID,
			ErrorCode:     protocol.ErrorInternalError,
			Error:         "storage not available",
		}, nil
	}

	// Get all half messages for this transaction
	halfMessages, err := ptm.GetBatchHalfMessages(transactionID)
	if err != nil {
		return &BatchTransactionRollbackResponse{
			TransactionID: transactionID,
			ErrorCode:     protocol.ErrorTransactionNotFound,
			Error:         err.Error(),
		}, nil
	}

	// Process each message in the batch
	for i, halfMessage := range halfMessages {
		compositeKey := fmt.Sprintf("%s:%d", string(transactionID), i)
		
		// Remove the half message from storage after rollback
		if err := ptm.storage.Delete(compositeKey); err != nil {
			ptm.logger.Printf("Warning: failed to delete rolled back message %d for transaction %s: %v", i, transactionID, err)
		}
		
		ptm.logger.Printf("Rolled back message %d for transaction %s (topic: %s, partition: %d)", 
			i, transactionID, halfMessage.Topic, halfMessage.Partition)
	}

	return &BatchTransactionRollbackResponse{
		TransactionID: transactionID,
		ErrorCode:     protocol.ErrorNone,
	}, nil
}
