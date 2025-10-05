package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/compression"
	"github.com/issac1998/go-queue/internal/deduplicator"
	"github.com/issac1998/go-queue/internal/delayed"
	typederrors "github.com/issac1998/go-queue/internal/errors"
	"github.com/issac1998/go-queue/internal/ordering"
	"github.com/issac1998/go-queue/internal/storage"
	"github.com/issac1998/go-queue/internal/transaction"
	"github.com/lni/dragonboat/v3/statemachine"
)

// RaftManagerInterface defines the interface for Raft manager operations
type RaftManagerInterface interface {
	IsLeader(groupID uint64) bool
	GetLeaderID(groupID uint64) (uint64, bool, error)
}

// ProduceMessage represents a message to be produced
type ProduceMessage struct {
	ProducerID     string            `json:"producer_id,omitempty"`
	SequenceNumber int64             `json:"sequence_number,omitempty"`
	AsyncIO        bool              `json:"async_io,omitempty"`
	Topic          string            `json:"topic"`
	Partition      int32             `json:"partition"`
	Key            []byte            `json:"key,omitempty"`
	Value          []byte            `json:"value"`
	Headers        map[string]string `json:"headers,omitempty"`
	Timestamp      time.Time         `json:"timestamp"`
}

// WriteResult represents the result of a write operation
type WriteResult struct {
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Error     string    `json:"error,omitempty"`
}

// FetchRequest represents a request to read messages
type FetchRequest struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	MaxBytes  int32  `json:"max_bytes"`
}

// BatchFetchRequest represents a batch request to read multiple ranges of messages
type BatchFetchRequest struct {
	Topic     string              `json:"topic"`
	Partition int32               `json:"partition"`
	Requests  []FetchRangeRequest `json:"requests"`
}

// FetchRangeRequest represents a single range request within a batch
type FetchRangeRequest struct {
	Offset   int64 `json:"offset"`
	MaxBytes int32 `json:"max_bytes"`
	MaxCount int32 `json:"max_count,omitempty"`
}

// FetchResponse represents the response to a fetch request
type FetchResponse struct {
	Topic      string          `json:"topic"`
	Partition  int32           `json:"partition"`
	Messages   []StoredMessage `json:"messages"`
	NextOffset int64           `json:"next_offset"`
	ErrorCode  int16           `json:"error_code"`
}

// BatchFetchResponse represents the response to a batch fetch request
type BatchFetchResponse struct {
	Topic     string        `json:"topic"`
	Partition int32         `json:"partition"`
	Results   []FetchResult `json:"results"`
	ErrorCode int16         `json:"error_code"`
	Error     string        `json:"error,omitempty"`
}

// FetchResult represents the result of a single fetch range
type FetchResult struct {
	Messages   []StoredMessage `json:"messages"`
	NextOffset int64           `json:"next_offset"`
	Error      string          `json:"error,omitempty"`
}

// StoredMessage represents a message stored in the partition
type StoredMessage struct {
	Offset    int64             `json:"offset"`
	Key       []byte            `json:"key,omitempty"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// PartitionCommand represents different types of operations on a partition
type PartitionCommand struct {
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data"`
}

// Command types for partition operations
const (
	CmdProduceMessage             = "produce_message"
	CmdProduceBatch               = "produce_batch"
	CmdCleanup                    = "cleanup"
	CmdStoreDelayedMessage        = "store_delayed_message"
	CmdUpdateDelayedMessage       = "update_delayed_message"
	CmdDeleteDelayedMessage       = "delete_delayed_message"
	CmdBatchUpdateDelayedMessages = "batch_update_delayed_messages"
	CmdStoreHalfMessage           = "store_half_message"
	CmdCommitHalfMessage          = "commit_half_message"
	CmdRollbackHalfMessage        = "rollback_half_message"
)

// ProduceBatchCommand represents a batch of messages to be produced
type ProduceBatchCommand struct {
	Messages []ProduceMessage `json:"messages"`
}

// BatchWriteResult represents the result of a batch write operation
type BatchWriteResult struct {
	Results []WriteResult `json:"results"`
	Error   string        `json:"error,omitempty"`
}

// deduplicatorCommand represents producer state persistence operations
type deduplicatorCommand struct {
	ProducerID string          `json:"producer_id"`
	States     map[int32]int64 `json:"states,omitempty"` // partition -> last sequence number
	Timestamp  time.Time       `json:"timestamp"`
}

// deduplicatorResult represents the result of producer state operations
type deduplicatorResult struct {
	Success   bool      `json:"success"`
	Error     string    `json:"error,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// PartitionStateMachine implements statemachine.IStateMachine for partition data
type PartitionStateMachine struct {
	TopicName   string
	PartitionID int32
	DataDir     string

	// Storage
	partition *storage.Partition
	mu        sync.RWMutex

	// Compression
	compressor compression.Compressor

	// Deduplication
	DeduplicatorManager *deduplicator.DeduplicatorManager
	deduplicatorEnabled bool

	// Ordering
	orderedMessageManager *ordering.OrderedMessageManager

	// Delayed messages
	delayedMessageStorage delayed.DelayedMessageStorage

	// Transaction support
	halfMessageStorage HalfMessageStorage // 新增：存储 halfMessage

	// Raft integration for leader checking
	raftManager RaftManagerInterface // 新增：Raft管理器接口
	raftGroupID uint64               // 新增：当前分区对应的Raft组ID

	// Cleanup task management
	cleanupTicker   *time.Ticker  // 新增：定时清理任务
	cleanupStopChan chan struct{} // 新增：停止清理任务的通道
	cleanupInterval time.Duration // 新增：清理间隔

	// Transaction check configuration
	maxCheckCount       int           // 最大回查次数
	checkTimeout        time.Duration // 单次回查超时时间
	batchProcessSize    int           // 批处理大小
	enableMetrics       bool          // 是否启用指标收集

	// Metrics
	messageCount int64
	bytesStored  int64
	lastWrite    time.Time
	lastRead     time.Time

	// Transaction check metrics
	totalChecks     int64 // 总回查次数
	successfulChecks int64 // 成功回查次数
	failedChecks    int64 // 失败回查次数
	commitCount     int64 // 提交次数
	rollbackCount   int64 // 回滚次数

	// State
	isReady bool

	// Logging
	logger *log.Logger

	// Transaction callback handler
	onTransactionCheckResult func(transactionID string, state transaction.TransactionState, record *HalfMessageRecord)
}

// HalfMessageRecord 表示存储在 state machine 中的 halfMessage 记录
type HalfMessageRecord struct {
	TransactionID string            `json:"transaction_id"`
	Topic         string            `json:"topic"`
	Partition     int32             `json:"partition"`
	Key           []byte            `json:"key,omitempty"`
	Value         []byte            `json:"value"`
	Headers       map[string]string `json:"headers,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	Timeout       time.Duration     `json:"timeout"`
	State         string            `json:"state"` // "prepared", "committed", "rolled_back"
	ExpiresAt     time.Time         `json:"expires_at"`
	// 新增：回查机制所需字段
	ProducerGroup   string    `json:"producer_group"`   // 生产者组，用于找到对应的回查处理器
	CallbackAddress string    `json:"callback_address"` // 回调地址，用于回查本地事务状态
	CheckCount      int       `json:"check_count"`      // 回查次数计数
	LastCheck       time.Time `json:"last_check"`       // 最后一次回查时间
}

// PartitionStateMachineConfig 配置选项
type PartitionStateMachineConfig struct {
	CleanupInterval     time.Duration // 清理间隔
	MaxCheckCount       int           // 最大回查次数
	CheckTimeout        time.Duration // 单次回查超时时间
	BatchProcessSize    int           // 批处理大小
	EnableMetrics       bool          // 是否启用指标收集
}

// DefaultPartitionStateMachineConfig 返回默认配置
func DefaultPartitionStateMachineConfig() *PartitionStateMachineConfig {
	return &PartitionStateMachineConfig{
		CleanupInterval:  30 * time.Second,
		MaxCheckCount:    3,
		CheckTimeout:     5 * time.Second,
		BatchProcessSize: 10,
		EnableMetrics:    true,
	}
}

func NewPartitionStateMachine(topicName string, partitionID int32, dataDir string, compressor compression.Compressor) (*PartitionStateMachine, error) {
	return NewPartitionStateMachineWithConfig(topicName, partitionID, dataDir, compressor, DefaultPartitionStateMachineConfig())
}

func NewPartitionStateMachineWithConfig(topicName string, partitionID int32, dataDir string, compressor compression.Compressor, config *PartitionStateMachineConfig) (*PartitionStateMachine, error) {
	partitionDir := filepath.Join(dataDir, "partitions", fmt.Sprintf("%s-%d", topicName, partitionID))

	// Create storage partition
	partition, err := storage.NewPartition(partitionDir, &storage.PartitionConfig{
		MaxSegmentSize: 1024 * 1024 * 1024,
		MaxIndexSize:   1024 * 1024 * 10,
		RetentionTime:  7 * 24 * time.Hour,
		RetentionSize:  10 * 1024 * 1024 * 1024,
	})
	if err != nil {
		return nil, typederrors.NewTypedError(typederrors.StorageError, "failed to create partition storage", err)
	}

	// Create half message storage
	halfMessageStorage, err := NewPebbleHalfMessageStorage(partitionDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create half message storage: %w", err)
	}

	file, err := os.OpenFile(fmt.Sprintf("partition-%d", partitionID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %v", err)
	}

	psm := &PartitionStateMachine{
		TopicName:             topicName,
		PartitionID:           partitionID,
		DataDir:               dataDir,
		partition:             partition,
		compressor:            compressor,
		DeduplicatorManager:   deduplicator.NewDeduplicatorManager(),
		deduplicatorEnabled:   true,
		orderedMessageManager: ordering.NewOrderedMessageManager(100, 30*time.Second), // 100 message window, 30s timeout
		halfMessageStorage:    halfMessageStorage,                                     // 使用 PebbleDB 存储
		cleanupInterval:       config.CleanupInterval,                                 // 使用配置的清理间隔
		cleanupStopChan:       make(chan struct{}),
		// Transaction check configuration
		maxCheckCount:    config.MaxCheckCount,    // 使用配置的最大回查次数
		checkTimeout:     config.CheckTimeout,     // 使用配置的单次回查超时
		batchProcessSize: config.BatchProcessSize, // 使用配置的批处理大小
		enableMetrics:    config.EnableMetrics,    // 使用配置的指标收集设置
		isReady:          true,
		lastWrite:        time.Now(),
		lastRead:         time.Now(),

		logger: log.New(file, fmt.Sprintf("[partition-%d] ", partitionID), log.LstdFlags),
	}

	psm.logger.Printf("Created PartitionStateMachine for %s-%d", topicName, partitionID)
	return psm, nil
}

func (psm *PartitionStateMachine) startHalfMessageCleanup() {
	psm.cleanupTicker = time.NewTicker(psm.cleanupInterval)

	go func() {
		defer psm.cleanupTicker.Stop()

		for {
			select {
			case <-psm.cleanupTicker.C:
				if psm.raftManager != nil && psm.raftManager.IsLeader(psm.raftGroupID) {
					psm.cleanupExpiredHalfMessages()
				}
			case <-psm.cleanupStopChan:
				psm.logger.Printf("Half message cleanup task stopped for partition %s-%d",
					psm.TopicName, psm.PartitionID)
				return
			}
		}
	}()

	psm.logger.Printf("Started half message cleanup task for partition %s-%d with interval %v",
		psm.TopicName, psm.PartitionID, psm.cleanupInterval)
}

func (psm *PartitionStateMachine) GetHalfMessage(transactionID string) (*HalfMessageRecord, bool) {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	record, exists, err := psm.halfMessageStorage.Get(transactionID)
	if err != nil || !exists {
		return nil, false
	}
	return record, true
}


func (psm *PartitionStateMachine) commitHalfMessage(record *HalfMessageRecord) error {
	messageData := record.Value

	if psm.partition != nil {
		offset, err := psm.partition.Append(messageData, time.Now())
		if err != nil {
			return fmt.Errorf("failed to append message to partition: %w", err)
		}

		psm.logger.Printf("Successfully committed half message %s to partition at offset %d",
			record.TransactionID, offset)

		psm.messageCount++
		psm.bytesStored += int64(len(messageData))
		psm.lastWrite = time.Now()
	} else {
		return fmt.Errorf("partition storage not available")
	}

	return nil
}

func (psm *PartitionStateMachine) cleanupExpiredHalfMessages() {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	if psm.raftManager != nil && psm.raftGroupID != 0 {
		isLeader := psm.raftManager.IsLeader(psm.raftGroupID)
		if !isLeader {
			psm.logger.Printf("Skipping half message cleanup: current node is not leader for group %d",
				psm.raftGroupID)
			return
		}
		psm.logger.Printf("Performing half message cleanup as leader for group %d", psm.raftGroupID)
	} else {
		psm.logger.Printf("Warning: Raft manager or group ID not set, proceeding with cleanup")
	}

	expiredMessages, err := psm.halfMessageStorage.GetExpired()
	if err != nil {
		psm.logger.Printf("Failed to get expired half messages: %v", err)
		return
	}

	if len(expiredMessages) == 0 {
		return
	}

	psm.logger.Printf("Found %d expired half messages to process", len(expiredMessages))

	// 批量处理过期消息
	for i := 0; i < len(expiredMessages); i += psm.batchProcessSize {
		end := i + psm.batchProcessSize
		if end > len(expiredMessages) {
			end = len(expiredMessages)
		}
		batch := expiredMessages[i:end]
		psm.processBatchExpiredMessages(batch)
	}
}

// processBatchExpiredMessages 批量处理过期消息
func (psm *PartitionStateMachine) processBatchExpiredMessages(batch []*HalfMessageRecord) {
	for _, record := range batch {
		if psm.enableMetrics {
			psm.totalChecks++
		}

		if record.CheckCount >= psm.maxCheckCount {
			psm.logger.Printf("[TRANSACTION] Half message %s exceeded max check count (%d), deleting",
				record.TransactionID, psm.maxCheckCount)

			if err := psm.halfMessageStorage.Delete(record.TransactionID); err != nil {
				psm.logger.Printf("[ERROR] Failed to delete expired half message %s: %v",
					record.TransactionID, err)
				if psm.enableMetrics {
					psm.failedChecks++
				}
			} else {
				psm.logger.Printf("[TRANSACTION] Successfully deleted expired half message %s",
					record.TransactionID)
			}
			continue
		}

		if record.CallbackAddress == "" {
			psm.logger.Printf("[WARN] No callback address for transaction %s, skipping check", record.TransactionID)
			continue
		}

		record.CheckCount++
		record.LastCheck = time.Now()

		halfMessage := transaction.HalfMessage{
			TransactionID:   transaction.TransactionID(record.TransactionID),
			Topic:           record.Topic,
			Partition:       record.Partition,
			Key:             record.Key,
			Value:           record.Value,
			Headers:         record.Headers,
			ProducerGroup:   record.ProducerGroup,
			CreatedAt:       record.CreatedAt,
			Timeout:         record.Timeout,
			CheckCount:      record.CheckCount,
			LastCheck:       record.LastCheck,
			CallbackAddress: record.CallbackAddress,
		}

		state := psm.checkTransactionStateWithRetry(record.CallbackAddress, record.TransactionID, halfMessage)

		psm.logger.Printf("[TRANSACTION] Check result for %s (attempt %d): %v",
			record.TransactionID, record.CheckCount, state)

		// 根据回查结果处理事务
		// 注意：这里不能直接执行commit/rollback操作，因为这些操作需要通过Raft同步
		// 应该通过回调函数通知上层（Broker）来处理
		switch state {
		case transaction.StateCommit:
			// 需要提交事务：通知上层通过Raft发送commit命令
			psm.logger.Printf("[TRANSACTION] Transaction %s needs to be committed via Raft", record.TransactionID)
			if psm.onTransactionCheckResult != nil {
				psm.onTransactionCheckResult(record.TransactionID, state, record)
			}

		case transaction.StateRollback:
			// 需要回滚事务：通知上层通过Raft发送rollback命令
			psm.logger.Printf("[TRANSACTION] Transaction %s needs to be rolled back via Raft", record.TransactionID)
			if psm.onTransactionCheckResult != nil {
				psm.onTransactionCheckResult(record.TransactionID, state, record)
			}

		case transaction.StateUnknown:
			// 状态未知：更新回查计数，等待下次检查
			psm.logger.Printf("[TRANSACTION] Transaction %s state unknown, will retry later (attempt %d/%d)",
				record.TransactionID, record.CheckCount, psm.maxCheckCount)
			if updateErr := psm.halfMessageStorage.Update(record.TransactionID, record); updateErr != nil {
				psm.logger.Printf("[ERROR] Failed to update half message record %s: %v",
					record.TransactionID, updateErr)
				if psm.enableMetrics {
					psm.failedChecks++
				}
			}

		default:
			psm.logger.Printf("[WARN] Unknown transaction state %v for %s", state, record.TransactionID)
			// 对于未知状态，也更新记录等待下次检查
			if updateErr := psm.halfMessageStorage.Update(record.TransactionID, record); updateErr != nil {
				psm.logger.Printf("[ERROR] Failed to update half message record %s: %v",
					record.TransactionID, updateErr)
				if psm.enableMetrics {
					psm.failedChecks++
				}
			}
		}
	}
}

func (psm *PartitionStateMachine) checkTransactionStateWithRetry(callbackAddress, transactionID string, halfMessage transaction.HalfMessage) transaction.TransactionState {
	checker := transaction.NewDefaultTransactionChecker()
	
	halfMsg := transaction.HalfMessage{
		TransactionID:   transaction.TransactionID(transactionID),
		Topic:           halfMessage.Topic,
		Partition:       halfMessage.Partition,
		Key:             halfMessage.Key,
		Value:           halfMessage.Value,
		Headers:         halfMessage.Headers,
		ProducerGroup:   halfMessage.ProducerGroup,
		CallbackAddress: callbackAddress,
	}

	start := time.Now()
	state := checker.CheckTransactionStateNet(callbackAddress, transaction.TransactionID(transactionID), halfMsg)
	duration := time.Since(start)
	
	psm.logger.Printf("[TRANSACTION] Check for %s took %v, result: %v", transactionID, duration, state)
	
	if duration > psm.checkTimeout {
		psm.logger.Printf("[WARN] Transaction check for %s took longer than expected (%v > %v)",
			transactionID, duration, psm.checkTimeout)
	}
	
	return state
}

// Stop 停止PartitionStateMachine
func (psm *PartitionStateMachine) Stop() {
	if psm.cleanupStopChan != nil {
		close(psm.cleanupStopChan)
	}

	if psm.cleanupTicker != nil {
		psm.cleanupTicker.Stop()
	}

	psm.logger.Printf("PartitionStateMachine stopped for %s-%d", psm.TopicName, psm.PartitionID)
}

// Update implements the statemachine.IStateMachine interface
func (psm *PartitionStateMachine) Update(data []byte) (statemachine.Result, error) {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	var cmd PartitionCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return statemachine.Result{}, fmt.Errorf("failed to unmarshal command: %w", err)
	}

	switch cmd.Type {
	case CmdStoreHalfMessage:
		return psm.handleStoreHalfMessage(cmd.Data)
	case CmdCommitHalfMessage:
		return psm.handleCommitHalfMessage(cmd.Data)
	case CmdRollbackHalfMessage:
		return psm.handleRollbackHalfMessage(cmd.Data)
	default:
		return statemachine.Result{}, fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

// handleStoreHalfMessage 处理存储半消息命令
func (psm *PartitionStateMachine) handleStoreHalfMessage(data map[string]interface{}) (statemachine.Result, error) {
	// 解析数据
	transactionID, ok := data["transaction_id"].(string)
	if !ok {
		return statemachine.Result{}, fmt.Errorf("missing or invalid transaction_id")
	}

	topic, ok := data["topic"].(string)
	if !ok {
		return statemachine.Result{}, fmt.Errorf("missing or invalid topic")
	}

	partition, ok := data["partition"].(float64) // JSON numbers are float64
	if !ok {
		return statemachine.Result{}, fmt.Errorf("missing or invalid partition")
	}

	// 处理key和value，可能是字符串或字节数组
	var key, value []byte
	if keyData, exists := data["key"]; exists {
		switch k := keyData.(type) {
		case string:
			key = []byte(k)
		case []byte:
			key = k
		}
	}

	if valueData, exists := data["value"]; exists {
		switch v := valueData.(type) {
		case string:
			value = []byte(v)
		case []byte:
			value = v
		default:
			return statemachine.Result{}, fmt.Errorf("invalid value type")
		}
	}

	// 解析headers
	var headers map[string]string
	if headersData, exists := data["headers"]; exists {
		if h, ok := headersData.(map[string]interface{}); ok {
			headers = make(map[string]string)
			for k, v := range h {
				if str, ok := v.(string); ok {
					headers[k] = str
				}
			}
		}
	}

	// 解析timeout
	var timeout time.Duration = 30 * time.Second // 默认值
	if timeoutStr, exists := data["timeout"].(string); exists {
		if t, err := time.ParseDuration(timeoutStr); err == nil {
			timeout = t
		}
	}

	state, _ := data["state"].(string)
	if state == "" {
		state = "prepared"
	}

	producerGroup, _ := data["producer_group"].(string)
	callbackAddress, _ := data["callback_address"].(string)

	// 创建半消息记录
	now := time.Now()
	record := &HalfMessageRecord{
		TransactionID:   transactionID,
		Topic:           topic,
		Partition:       int32(partition),
		Key:             key,
		Value:           value,
		Headers:         headers,
		CreatedAt:       now,
		Timeout:         timeout,
		State:           state,
		ExpiresAt:       now.Add(timeout),
		ProducerGroup:   producerGroup,
		CallbackAddress: callbackAddress,
		CheckCount:      0,
		LastCheck:       time.Time{},
	}

	// 存储半消息
	if err := psm.halfMessageStorage.Store(transactionID, record); err != nil {
		return statemachine.Result{}, fmt.Errorf("failed to store half message: %w", err)
	}

	psm.logger.Printf("Stored half message for transaction %s", transactionID)
	return statemachine.Result{Value: 1}, nil
}

// handleCommitHalfMessage 处理提交半消息命令
func (psm *PartitionStateMachine) handleCommitHalfMessage(data map[string]interface{}) (statemachine.Result, error) {
	transactionID, ok := data["transaction_id"].(string)
	if !ok {
		return statemachine.Result{}, fmt.Errorf("missing or invalid transaction_id")
	}

	// 获取半消息记录
	record, exists, err := psm.halfMessageStorage.Get(transactionID)
	if err != nil {
		return statemachine.Result{}, fmt.Errorf("failed to get half message: %w", err)
	}
	if !exists {
		return statemachine.Result{}, fmt.Errorf("half message not found: %s", transactionID)
	}

	// 提交半消息
	if err := psm.commitHalfMessage(record); err != nil {
		return statemachine.Result{}, fmt.Errorf("failed to commit half message: %w", err)
	}

	// 删除半消息记录
	if err := psm.halfMessageStorage.Delete(transactionID); err != nil {
		return statemachine.Result{}, fmt.Errorf("failed to delete half message: %w", err)
	}

	psm.logger.Printf("Committed half message for transaction %s", transactionID)
	return statemachine.Result{Value: 1}, nil
}

// handleRollbackHalfMessage 处理回滚半消息命令
func (psm *PartitionStateMachine) handleRollbackHalfMessage(data map[string]interface{}) (statemachine.Result, error) {
	transactionID, ok := data["transaction_id"].(string)
	if !ok {
		return statemachine.Result{}, fmt.Errorf("missing or invalid transaction_id")
	}

	// 删除半消息记录
	if err := psm.halfMessageStorage.Delete(transactionID); err != nil {
		return statemachine.Result{}, fmt.Errorf("failed to delete half message: %w", err)
	}

	psm.logger.Printf("Rolled back half message for transaction %s", transactionID)
	return statemachine.Result{Value: 1}, nil
}

// Lookup implements the statemachine.IStateMachine interface
func (psm *PartitionStateMachine) Lookup(query interface{}) (interface{}, error) {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	// Convert query to []byte if needed
	var queryBytes []byte
	switch q := query.(type) {
	case []byte:
		queryBytes = q
	case string:
		queryBytes = []byte(q)
	default:
		return nil, fmt.Errorf("invalid query type: %T", query)
	}

	var queryObj map[string]interface{}
	if err := json.Unmarshal(queryBytes, &queryObj); err != nil {
		return nil, fmt.Errorf("failed to unmarshal query: %w", err)
	}

	queryType, ok := queryObj["type"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid query type")
	}

	switch queryType {
	case "get_half_message":
		return psm.handleGetHalfMessageQuery(queryObj)
	case "get_expired_transactions":
		return psm.handleGetExpiredTransactionsQuery(queryObj)
	default:
		return nil, fmt.Errorf("unsupported query type: %s", queryType)
	}
}

// handleGetHalfMessageQuery 处理获取半消息的查询
func (psm *PartitionStateMachine) handleGetHalfMessageQuery(queryObj map[string]interface{}) (interface{}, error) {
	transactionID, ok := queryObj["transaction_id"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid transaction_id")
	}

	if psm.halfMessageStorage == nil {
		return map[string]interface{}{
			"exists": false,
			"error":  "half message storage not available",
		}, nil
	}

	// 查询半消息
	record, exists, err := psm.halfMessageStorage.Get(transactionID)
	if err != nil {
		return map[string]interface{}{
			"exists": false,
			"error":  err.Error(),
		}, nil
	}
	if !exists {
		return map[string]interface{}{
			"exists": false,
		}, nil
	}

	return map[string]interface{}{
		"exists": true,
		"half_message": map[string]interface{}{
			"transaction_id":   record.TransactionID,
			"topic":           record.Topic,
			"partition":       record.Partition,
			"key":             record.Key,
			"value":           record.Value,
			"headers":         record.Headers,
			"created_at":      record.CreatedAt,
			"timeout":         record.Timeout,
			"state":           record.State,
			"expires_at":      record.ExpiresAt,
			"producer_group":  record.ProducerGroup,
			"callback_address": record.CallbackAddress,
			"check_count":     record.CheckCount,
			"last_check":      record.LastCheck,
		},
	}, nil
}

// handleGetExpiredTransactionsQuery 处理获取过期事务的查询
func (psm *PartitionStateMachine) handleGetExpiredTransactionsQuery(queryObj map[string]interface{}) (interface{}, error) {
	if psm.halfMessageStorage == nil {
		return []interface{}{}, nil
	}

	expiredMessages, err := psm.halfMessageStorage.GetExpired()
	if err != nil {
		return nil, fmt.Errorf("failed to get expired messages: %w", err)
	}

	var result []interface{}
	for _, record := range expiredMessages {
		result = append(result, map[string]interface{}{
			"transaction_id":   record.TransactionID,
			"topic":           record.Topic,
			"partition":       record.Partition,
			"created_at":      record.CreatedAt,
			"expires_at":      record.ExpiresAt,
			"producer_group":  record.ProducerGroup,
			"callback_address": record.CallbackAddress,
			"check_count":     record.CheckCount,
			"last_check":      record.LastCheck,
		})
	}

	return result, nil
}

// SaveSnapshot implements the statemachine.IStateMachine interface
func (psm *PartitionStateMachine) SaveSnapshot(w io.Writer, fc statemachine.ISnapshotFileCollection, done <-chan struct{}) error {
	// TODO: Implement partition state machine snapshot save logic
	return nil
}

// RecoverFromSnapshot implements the statemachine.IStateMachine interface
func (psm *PartitionStateMachine) RecoverFromSnapshot(r io.Reader, files []statemachine.SnapshotFile, done <-chan struct{}) error {
	// TODO: Implement partition state machine snapshot recovery logic
	return nil
}

// GetHash implements the statemachine.IStateMachine interface
func (psm *PartitionStateMachine) GetHash() uint64 {
	psm.mu.RLock()
	defer psm.mu.RUnlock()
	return uint64(psm.messageCount)
}

// Close implements the statemachine.IStateMachine interface
func (psm *PartitionStateMachine) Close() error {
	psm.logger.Printf("Closing partition state machine for %s-%d", psm.TopicName, psm.PartitionID)

	// Stop cleanup task
	psm.Stop()

	// Close storage components
	if psm.partition != nil {
		if err := psm.partition.Close(); err != nil {
			psm.logger.Printf("Error closing partition storage: %v", err)
		}
	}

	if psm.halfMessageStorage != nil {
		if err := psm.halfMessageStorage.Close(); err != nil {
			psm.logger.Printf("Error closing half message storage: %v", err)
		}
	}

	if psm.delayedMessageStorage != nil {
		if err := psm.delayedMessageStorage.Close(); err != nil {
			psm.logger.Printf("Error closing delayed message storage: %v", err)
		}
	}

	return nil
}

// SetRaftManager 设置Raft管理器和组ID，并启动清理任务
func (psm *PartitionStateMachine) SetRaftManager(raftManager RaftManagerInterface, raftGroupID uint64) {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	psm.raftManager = raftManager
	psm.raftGroupID = raftGroupID

	go psm.startHalfMessageCleanup()
}

// SetTransactionCheckResultHandler sets the callback function for transaction check results
func (psm *PartitionStateMachine) SetTransactionCheckResultHandler(handler func(transactionID string, state transaction.TransactionState, record *HalfMessageRecord)) {
	psm.onTransactionCheckResult = handler
}

// GetMetrics returns partition metrics
func (psm *PartitionStateMachine) GetMetrics() map[string]interface{} {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	metrics := map[string]interface{}{
		"message_count": psm.messageCount,
		"bytes_stored":  psm.bytesStored,
		"last_write":    psm.lastWrite,
		"last_read":     psm.lastRead,
		"is_ready":      psm.isReady,
	}

	// 添加事务检查相关指标
	if psm.enableMetrics {
		metrics["transaction_checks"] = map[string]interface{}{
			"total_checks":     psm.totalChecks,
			"successful_checks": psm.successfulChecks,
			"failed_checks":    psm.failedChecks,
			"commit_count":     psm.commitCount,
			"rollback_count":   psm.rollbackCount,
			"max_check_count":  psm.maxCheckCount,
			"check_timeout":    psm.checkTimeout.String(),
			"batch_size":       psm.batchProcessSize,
		}
		
		// 计算成功率
		if psm.totalChecks > 0 {
			successRate := float64(psm.successfulChecks) / float64(psm.totalChecks) * 100
			metrics["transaction_checks"].(map[string]interface{})["success_rate"] = fmt.Sprintf("%.2f%%", successRate)
		}
	}

	return metrics
}

// handleStoreDelayedMessage handles storing a delayed message
func (psm *PartitionStateMachine) handleStoreDelayedMessage(data map[string]interface{}) (statemachine.Result, error) {
	if psm.delayedMessageStorage == nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("delayed message storage not initialized")
	}

	// Parse delayed message data
	messageID, ok := data["message_id"].(string)
	if !ok {
		return statemachine.Result{Value: 0}, fmt.Errorf("invalid message_id")
	}

	messageBytes, err := json.Marshal(data["message"])
	if err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to marshal delayed message: %w", err)
	}

	var message delayed.DelayedMessage
	if err := json.Unmarshal(messageBytes, &message); err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to unmarshal delayed message: %w", err)
	}

	// Store the delayed message
	if err := psm.delayedMessageStorage.Store(messageID, &message); err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to store delayed message: %w", err)
	}

	result := map[string]interface{}{
		"success":    true,
		"message_id": messageID,
		"timestamp":  time.Now(),
	}

	resultData, _ := json.Marshal(result)
	return statemachine.Result{
		Value: uint64(len(resultData)),
		Data:  resultData,
	}, nil
}

// handleUpdateDelayedMessage handles updating a delayed message
func (psm *PartitionStateMachine) handleUpdateDelayedMessage(data map[string]interface{}) (statemachine.Result, error) {
	if psm.delayedMessageStorage == nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("delayed message storage not initialized")
	}

	// Parse delayed message data
	messageID, ok := data["message_id"].(string)
	if !ok {
		return statemachine.Result{Value: 0}, fmt.Errorf("invalid message_id")
	}

	messageBytes, err := json.Marshal(data["message"])
	if err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to marshal delayed message: %w", err)
	}

	var message delayed.DelayedMessage
	if err := json.Unmarshal(messageBytes, &message); err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to unmarshal delayed message: %w", err)
	}

	// 如果消息已正常处理（Delivered），则删除消息而不是更新
	if message.Status == delayed.StatusDelivered {
		if err := psm.delayedMessageStorage.Delete(messageID); err != nil {
			return statemachine.Result{Value: 0}, fmt.Errorf("failed to delete delivered delayed message: %w", err)
		}
		result := map[string]interface{}{
			"success":    true,
			"message_id": messageID,
			"deleted":    true,
			"timestamp":  time.Now(),
		}
		resultData, _ := json.Marshal(result)
		return statemachine.Result{
			Value: uint64(len(resultData)),
			Data:  resultData,
		}, nil
	}

	// Update the delayed message
	if err := psm.delayedMessageStorage.Update(messageID, &message); err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to update delayed message: %w", err)
	}

	result := map[string]interface{}{
		"success":    true,
		"message_id": messageID,
		"timestamp":  time.Now(),
	}

	resultData, _ := json.Marshal(result)
	return statemachine.Result{
		Value: uint64(len(resultData)),
		Data:  resultData,
	}, nil
}

// handleDeleteDelayedMessage handles deleting a delayed message
func (psm *PartitionStateMachine) handleDeleteDelayedMessage(data map[string]interface{}) (statemachine.Result, error) {
	if psm.delayedMessageStorage == nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("delayed message storage not initialized")
	}

	// Parse message ID
	messageID, ok := data["message_id"].(string)
	if !ok {
		return statemachine.Result{Value: 0}, fmt.Errorf("invalid message_id")
	}

	// Delete the delayed message
	if err := psm.delayedMessageStorage.Delete(messageID); err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to delete delayed message: %w", err)
	}

	result := map[string]interface{}{
		"success":    true,
		"message_id": messageID,
		"timestamp":  time.Now(),
	}

	resultData, _ := json.Marshal(result)
	return statemachine.Result{
		Value: uint64(len(resultData)),
		Data:  resultData,
	}, nil
}

func (psm *PartitionStateMachine) handleBatchUpdateDelayedMessages(data map[string]interface{}) (statemachine.Result, error) {
	if psm.delayedMessageStorage == nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("delayed message storage not initialized")
	}

	updatesRaw, ok := data["updates"]
	if !ok {
		return statemachine.Result{Value: 0}, fmt.Errorf("missing updates field")
	}

	updatesBytes, err := json.Marshal(updatesRaw)
	if err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to marshal updates: %w", err)
	}

	type updateItem struct {
		MessageID string                 `json:"message_id"`
		Message   delayed.DelayedMessage `json:"message"`
	}
	var updates []updateItem
	if err := json.Unmarshal(updatesBytes, &updates); err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to unmarshal updates: %w", err)
	}

	toDelete := make([]string, 0, len(updates))
	toUpdate := make(map[string]*delayed.DelayedMessage, len(updates))
	for _, upd := range updates {
		if upd.Message.Status == delayed.StatusDelivered {
			toDelete = append(toDelete, upd.MessageID)
		} else {
			msg := upd.Message // create copy to take address
			toUpdate[upd.MessageID] = &msg
		}
	}

	// 批量删除
	if len(toDelete) > 0 {
		if err := psm.delayedMessageStorage.DeleteBatch(toDelete); err != nil {
			return statemachine.Result{Value: 0}, fmt.Errorf("batch delete failed: %w", err)
		}
	}
	// 批量更新
	if len(toUpdate) > 0 {
		if err := psm.delayedMessageStorage.UpdateBatch(toUpdate); err != nil {
			return statemachine.Result{Value: 0}, fmt.Errorf("batch update failed: %w", err)
		}
	}

	// 构造逐条结果（全部成功）
	results := make([]map[string]interface{}, len(updates))
	deletedSet := make(map[string]struct{}, len(toDelete))
	for _, id := range toDelete {
		deletedSet[id] = struct{}{}
	}
	for i, upd := range updates {
		res := map[string]interface{}{"success": true, "message_id": upd.MessageID}
		if _, ok := deletedSet[upd.MessageID]; ok {
			res["deleted"] = true
		}
		results[i] = res
	}

	resp := map[string]interface{}{
		"success":   true,
		"results":   results,
		"timestamp": time.Now(),
	}
	respBytes, _ := json.Marshal(resp)
	return statemachine.Result{Value: uint64(len(respBytes)), Data: respBytes}, nil
}

// GetTotalChecks 获取总回查次数
func (psm *PartitionStateMachine) GetTotalChecks() int64 {
	psm.mu.RLock()
	defer psm.mu.RUnlock()
	return psm.totalChecks
}

// GetFailedChecks 获取失败回查次数
func (psm *PartitionStateMachine) GetFailedChecks() int64 {
	psm.mu.RLock()
	defer psm.mu.RUnlock()
	return psm.failedChecks
}

// StoreHalfMessage 存储半消息
func (psm *PartitionStateMachine) StoreHalfMessage(storeData HalfMessageStoreData) error {
	record := &HalfMessageRecord{
		TransactionID:   storeData.ID,
		Topic:           storeData.Topic,
		Partition:       storeData.Partition,
		Value:           storeData.Data,
		CallbackAddress: storeData.CallbackAddress,
		CreatedAt:       storeData.Timestamp,
		ExpiresAt:       storeData.Timestamp.Add(30 * time.Second), // 默认30秒过期
		CheckCount:      storeData.CheckCount,
		State:           "prepared",
	}

	return psm.halfMessageStorage.Store(storeData.ID, record)
}

// HalfMessageStoreData 存储半消息的数据结构
type HalfMessageStoreData struct {
	ID              string
	Topic           string
	Partition       int32
	Data            []byte
	CallbackAddress string
	Timestamp       time.Time
	CheckCount      int
}
