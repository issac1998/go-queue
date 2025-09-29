package transaction

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
	"github.com/issac1998/go-queue/internal/raft"
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

// GetHalfMessage retrieves half message - uses Raft Lookup interface for linearizable reads
func (ptm *PartitionTransactionManager) GetHalfMessage(transactionID TransactionID, topicName string) (*HalfMessage, bool) {
	ptm.mu.RLock()
	defer ptm.mu.RUnlock()

	if ptm.raftManager == nil {
		ptm.logger.Printf("RaftManager is not available")
		return nil, false
	}

	// Get all relevant Raft group IDs
	raftGroupIDs := ptm.getAllRelevantRaftGroups()

	for _, groupID := range raftGroupIDs {
		// Check if current node is leader, skip write-related reads if not leader
		// For read operations, we allow follower reads but need to ensure data consistency
		isLeader := ptm.raftManager.IsLeader(groupID)
		if !isLeader {
			// For follower reads, we still use SyncRead to ensure linearizable reads
			ptm.logger.Printf("Performing follower read for group %d", groupID)
		}

		// Construct half message query request
		queryReq := map[string]interface{}{
			"type":           "get_half_message",
			"transaction_id": string(transactionID),
		}

		queryBytes, err := json.Marshal(queryReq)
		if err != nil {
			ptm.logger.Printf("Failed to marshal query request: %v", err)
			continue
		}

		// Use Raft's SyncRead for linearizable reads
		// SyncRead ensures linearizable read consistency, whether leader or follower
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result, err := ptm.raftManager.SyncRead(ctx, groupID, queryBytes)
		cancel()

		if err != nil {
			ptm.logger.Printf("Failed to perform SyncRead for group %d: %v", groupID, err)
			continue
		}

		// Parse query result
		if queryResponse, ok := result.(*raft.HalfMessageQueryResponse); ok {
			if !queryResponse.Success {
				// If query is not successful, continue to try next group
				continue
			}

			raftHalfMessage := queryResponse.HalfMessage
			if raftHalfMessage == nil {
				continue
			}

			// Convert raft.HalfMessage to transaction.HalfMessage
			halfMessage := &HalfMessage{
				TransactionID: TransactionID(raftHalfMessage.TransactionID),
				Topic:         raftHalfMessage.Topic,
				Partition:     raftHalfMessage.Partition,
				Key:           raftHalfMessage.Key,
				Value:         raftHalfMessage.Value,
				Headers:       raftHalfMessage.Headers,
				ProducerGroup: raftHalfMessage.ProducerGroup,
				CreatedAt:     raftHalfMessage.CreatedAt,
				Timeout:       raftHalfMessage.Timeout,
				State:         TransactionState(raftHalfMessage.State),
			}

			// Validate topic match (if topicName is specified)
			if topicName != "" && halfMessage.Topic != topicName {
				continue
			}

			return halfMessage, true
		}

		ptm.logger.Printf("Unexpected query result type: %T", result)
	}

	return nil, false
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

// UnregisterProducerGroup 注销生产者组（简化版本）
func (ptm *PartitionTransactionManager) UnregisterProducerGroup(group string) error {
	ptm.mu.Lock()
	defer ptm.mu.Unlock()

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

// PerformTransactionCheck 执行事务检查（简化版本，只处理单个分区）
func (ptm *PartitionTransactionManager) PerformTransactionCheck() error {
	ptm.logger.Printf("Starting transaction check")

	// 获取过期的半消息
	expiredTransactions, err := ptm.GetExpiredHalfMessages()
	if err != nil {
		return fmt.Errorf("failed to get expired transactions: %w", err)
	}

	if len(expiredTransactions) == 0 {
		ptm.logger.Printf("No expired transactions found")
		return nil
	}

	ptm.logger.Printf("Found %d expired transactions", len(expiredTransactions))

	// 处理每个过期事务
	for _, expiredTxn := range expiredTransactions {
		if err := ptm.performTransactionCheck(expiredTxn); err != nil {
			ptm.logger.Printf("Failed to check transaction %s: %v", expiredTxn.TransactionID, err)
			continue
		}
	}

	return nil
}

// performTransactionCheck 检查单个事务
func (ptm *PartitionTransactionManager) performTransactionCheck(expiredTxn ExpiredTransactionInfo) error {
	ptm.mu.RLock()
	checker, exists := ptm.producerGroupCheckers[expiredTxn.ProducerGroup]
	ptm.mu.RUnlock()

	if !exists {
		ptm.logger.Printf("No checker found for producer group: %s", expiredTxn.ProducerGroup)
		return fmt.Errorf("no checker found for producer group: %s", expiredTxn.ProducerGroup)
	}

	// 记录检查开始
	startTime := time.Now()

	// 构造HalfMessage用于检查
	halfMessage := HalfMessage{
		TransactionID: expiredTxn.TransactionID,
		Topic:         expiredTxn.TopicName,
		Partition:     expiredTxn.PartitionID,
		ProducerGroup: expiredTxn.ProducerGroup,
		CreatedAt:     expiredTxn.CreatedAt,
	}

	// 执行事务状态检查
	state := checker.CheckTransactionState(expiredTxn.TransactionID, halfMessage)

	// 记录检查结果
	latency := time.Since(startTime)
	ptm.metrics.RecordTransactionCheck(state, latency, nil)

	// 处理检查结果
	return ptm.handleTransactionCheckResult(expiredTxn, state)
}

// handleTransactionCheckResult 处理事务检查结果
func (ptm *PartitionTransactionManager) handleTransactionCheckResult(expiredTxn ExpiredTransactionInfo, state TransactionState) error {
	switch state {
	case StateCommit:
		ptm.logger.Printf("Transaction %s should be committed", expiredTxn.TransactionID)
		return ptm.commitTransaction(expiredTxn.TransactionID)
	case StateRollback:
		ptm.logger.Printf("Transaction %s should be rolled back", expiredTxn.TransactionID)
		return ptm.rollbackTransaction(expiredTxn.TransactionID)
	case StateUnknown:
		ptm.logger.Printf("Transaction %s state is unknown, will retry later", expiredTxn.TransactionID)
		return ptm.handleUnknownTransactionState(expiredTxn)
	default:
		return fmt.Errorf("unknown transaction state: %v", state)
	}
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

	// 获取所有相关的Raft组ID
	raftGroupIDs := ptm.getAllRelevantRaftGroups()
	if len(raftGroupIDs) == 0 {
		err := fmt.Errorf("no relevant raft groups found for transaction %s", transactionID)
		ptm.errorHandler.HandleTransactionError(err, errorCtx)
		return err
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

	// 对每个相关的Raft组执行提交操作
	for _, groupID := range raftGroupIDs {
		// 构造事务提交命令
		command := map[string]interface{}{
			"type":           "transaction_commit",
			"transaction_id": string(transactionID),
		}

		// 通过Raft提议器执行提交
		ctx, cancel := context.WithTimeout(context.Background(), ptm.defaultTimeout)
		_, err := ptm.raftProposer.ProposeTransactionCommand(ctx, groupID, "transaction_commit", command)
		cancel()

		result := groupResult{
			groupID: groupID,
			success: err == nil,
			error:   err,
		}
		results = append(results, result)

		if err != nil {
			ptm.logger.Printf("Failed to commit transaction %s in group %d: %v", transactionID, groupID, err)
			lastErr = err
			continue
		}

		successCount++
		ptm.logger.Printf("Successfully committed transaction %s in group %d", transactionID, groupID)
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

	// 如果部分成功，记录详细信息并考虑重试失败的组
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
		
		// 对于部分成功的情况，我们可以选择：
		// 1. 记录警告但继续（当前实现）
		// 2. 尝试重试失败的组
		// 3. 回滚已成功的组（需要补偿事务逻辑）
		
		// 这里我们记录一个可恢复的错误，但不阻止事务完成
		partialErr := fmt.Errorf("transaction %s partially committed in %d/%d groups", 
			transactionID, successCount, len(raftGroupIDs))
		
		// 使用错误处理器记录部分成功的情况
		errorCtx.RetryCount = 0
		errorCtx.MaxRetries = 1 // 部分成功时允许一次重试
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

	raftGroups := ptm.getAllRelevantRaftGroups()
	if len(raftGroups) == 0 {
		err := fmt.Errorf("no raft groups available for transaction rollback")
		ptm.errorHandler.HandleTransactionError(err, errorCtx)
		return err
	}

	cmdData := map[string]interface{}{
		"transaction_id": string(transactionID),
	}

	
	type rollbackResult struct {
		groupID uint64
		success bool
		error   error
	}
	
	results := make([]rollbackResult, 0, len(raftGroups))
	var lastErr error
	successCount := 0
	
	// 向所有相关的Raft组提议事务回滚命令
	for _, raftGroupID := range raftGroups {
		ctx, cancel := context.WithTimeout(context.Background(), ptm.defaultTimeout)
		_, err := ptm.raftProposer.ProposeTransactionCommand(ctx, raftGroupID, "transaction_rollback", cmdData)
		cancel()
		
		result := rollbackResult{
			groupID: raftGroupID,
			success: err == nil,
			error:   err,
		}
		results = append(results, result)
		
		if err != nil {
			ptm.logger.Printf("Failed to rollback transaction %s in raft group %d: %v", transactionID, raftGroupID, err)
			lastErr = err
			continue
		}
		
		ptm.logger.Printf("Successfully rolled back transaction %s in raft group %d", transactionID, raftGroupID)
		successCount++
	}

	// 记录详细的执行结果
	ptm.logger.Printf("Transaction %s rollback results: %d/%d groups succeeded", 
		transactionID, successCount, len(raftGroups))

	// 记录指标
	if successCount > 0 {
		ptm.metrics.RecordTransactionRollback()
	}

	// 如果所有组都失败，返回错误
	if successCount == 0 && lastErr != nil {
		err := fmt.Errorf("failed to rollback transaction in all raft groups: %w", lastErr)
		ptm.errorHandler.HandleTransactionError(err, errorCtx)
		return err
	}

	// 如果部分成功，记录详细信息
	if successCount < len(raftGroups) {
		failedGroups := make([]uint64, 0)
		for _, result := range results {
			if !result.success {
				failedGroups = append(failedGroups, result.groupID)
				ptm.logger.Printf("Rollback failed in group %d with error: %v", result.groupID, result.error)
			}
		}
		
		ptm.logger.Printf("Transaction %s partially rolled back: succeeded in %d/%d groups, failed groups: %v", 
			transactionID, successCount, len(raftGroups), failedGroups)
		
		// 对于回滚操作的部分成功，记录警告但不阻止操作完成
		// 回滚失败通常意味着某些资源可能没有被正确清理
		partialErr := fmt.Errorf("transaction %s partially rolled back in %d/%d groups", 
			transactionID, successCount, len(raftGroups))
		
		// 使用错误处理器记录部分成功的情况
		errorCtx.RetryCount = 0
		errorCtx.MaxRetries = 2 // 回滚失败时允许更多重试
		ptm.errorHandler.HandleTransactionError(partialErr, errorCtx)
	}

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
