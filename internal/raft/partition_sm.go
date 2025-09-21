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
	halfMessageStorage HalfMessageStorage             // 新增：存储 halfMessage
	transactionChecker transaction.TransactionChecker // 新增：事务回查器

	// Raft integration for leader checking
	raftManager RaftManagerInterface // 新增：Raft管理器接口
	raftGroupID uint64               // 新增：当前分区对应的Raft组ID

	// Cleanup task management
	cleanupTicker   *time.Ticker  // 新增：定时清理任务
	cleanupStopChan chan struct{} // 新增：停止清理任务的通道
	cleanupInterval time.Duration // 新增：清理间隔

	// Metrics
	messageCount int64
	bytesStored  int64
	lastWrite    time.Time
	lastRead     time.Time

	// State
	isReady bool

	// Logging
	logger *log.Logger
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

// NewPartitionStateMachine creates a state machine for a partition
func NewPartitionStateMachine(topicName string, partitionID int32, dataDir string, compressor compression.Compressor) (*PartitionStateMachine, error) {
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

	// Create transaction checker
	transactionChecker := transaction.NewDefaultTransactionChecker()

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
		transactionChecker:    transactionChecker,                                     // 事务回查器
		cleanupInterval:       30 * time.Second,                                       // 默认30秒清理间隔
		cleanupStopChan:       make(chan struct{}),
		isReady:               true,
		lastWrite:             time.Now(),
		lastRead:              time.Now(),

		logger: log.New(file, fmt.Sprintf("[partition-%d] ", partitionID), log.LstdFlags),
	}

	psm.logger.Printf("Created PartitionStateMachine for %s-%d", topicName, partitionID)
	return psm, nil
}

// startHalfMessageCleanup 启动定时清理任务
func (psm *PartitionStateMachine) startHalfMessageCleanup() {
	psm.cleanupTicker = time.NewTicker(psm.cleanupInterval)

	go func() {
		defer psm.cleanupTicker.Stop()

		for {
			select {
			case <-psm.cleanupTicker.C:
				// 只有Leader才执行清理任务
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

// GetHalfMessage 获取半消息记录（用于测试）
func (psm *PartitionStateMachine) GetHalfMessage(transactionID string) (*HalfMessageRecord, bool) {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	record, exists, err := psm.halfMessageStorage.Get(transactionID)
	if err != nil || !exists {
		return nil, false
	}
	return record, true
}

// CleanupExpiredHalfMessages 清理过期的半消息（公共方法，用于测试）
func (psm *PartitionStateMachine) CleanupExpiredHalfMessages() int {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	// 获取过期的半消息
	expiredMessages, err := psm.halfMessageStorage.GetExpired()
	if err != nil {
		psm.logger.Printf("Failed to get expired half messages: %v", err)
		return 0
	}

	cleanedCount := 0
	for _, record := range expiredMessages {
		// 删除过期的半消息
		err := psm.halfMessageStorage.Delete(record.TransactionID)
		if err != nil {
			psm.logger.Printf("Failed to delete expired half message %s: %v",
				record.TransactionID, err)
			continue
		}
		cleanedCount++
	}

	return cleanedCount
}

// commitHalfMessage 提交半消息为正式消息
func (psm *PartitionStateMachine) commitHalfMessage(record *HalfMessageRecord) error {
	// 构造消息数据（将key和value合并为一个数据包）
	messageData := record.Value

	// 将消息写入分区存储
	if psm.partition != nil {
		offset, err := psm.partition.Append(messageData, time.Now())
		if err != nil {
			return fmt.Errorf("failed to append message to partition: %w", err)
		}

		psm.logger.Printf("Successfully committed half message %s to partition at offset %d",
			record.TransactionID, offset)

		// 更新统计信息
		psm.messageCount++
		psm.bytesStored += int64(len(messageData))
		psm.lastWrite = time.Now()
	} else {
		return fmt.Errorf("partition storage not available")
	}

	return nil
}

// cleanupExpiredHalfMessages 清理过期的半消息（私有方法，只由定时任务调用）
func (psm *PartitionStateMachine) cleanupExpiredHalfMessages() {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	// 检查当前节点是否为Leader
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

	// 获取过期的半消息
	expiredMessages, err := psm.halfMessageStorage.GetExpired()
	if err != nil {
		psm.logger.Printf("Failed to get expired half messages: %v", err)
		return
	}

	if len(expiredMessages) == 0 {
		return
	}

	psm.logger.Printf("Found %d expired half messages to process", len(expiredMessages))

	for _, record := range expiredMessages {
		// 检查是否超过最大回查次数
		maxCheckCount := 3 // 可以配置化
		if record.CheckCount >= maxCheckCount {
			psm.logger.Printf("Half message %s exceeded max check count (%d), deleting",
				record.TransactionID, maxCheckCount)

			if err := psm.halfMessageStorage.Delete(record.TransactionID); err != nil {
				psm.logger.Printf("Failed to delete expired half message %s: %v",
					record.TransactionID, err)
			}
			continue
		}

		// 使用现有的TransactionChecker进行回查
		if psm.transactionChecker != nil {
			// 更新回查计数和时间
			record.CheckCount++
			record.LastCheck = time.Now()

			// 构造HalfMessage用于回查
			halfMessage := transaction.HalfMessage{
				TransactionID: transaction.TransactionID(record.TransactionID),
				Topic:         record.Topic,
				Partition:     record.Partition,
				Key:           record.Key,
				Value:         record.Value,
				Headers:       record.Headers,
				ProducerGroup: record.ProducerGroup,
				CreatedAt:     record.CreatedAt,
				Timeout:       record.Timeout,
				CheckCount:    record.CheckCount,
				LastCheck:     record.LastCheck,
			}

			// 执行事务状态回查
			state := psm.transactionChecker.CheckTransactionState(
				transaction.TransactionID(record.TransactionID),
				halfMessage,
			)

			psm.logger.Printf("Transaction check result for %s: %v",
				record.TransactionID, state)

			// 根据回查结果处理事务
			switch state {
			case transaction.StateCommit:
				// 提交事务：将半消息转换为正式消息
				psm.logger.Printf("Committing transaction %s", record.TransactionID)
				if err := psm.commitHalfMessage(record); err != nil {
					psm.logger.Printf("Failed to commit half message %s: %v",
						record.TransactionID, err)
				} else {
					// 提交成功，删除半消息记录
					if err := psm.halfMessageStorage.Delete(record.TransactionID); err != nil {
						psm.logger.Printf("Failed to delete committed half message %s: %v",
							record.TransactionID, err)
					}
				}

			case transaction.StateRollback:
				// 回滚事务：直接删除半消息
				psm.logger.Printf("Rolling back transaction %s", record.TransactionID)
				if err := psm.halfMessageStorage.Delete(record.TransactionID); err != nil {
					psm.logger.Printf("Failed to delete rolled back half message %s: %v",
						record.TransactionID, err)
				}

			case transaction.StateUnknown:
				// 状态未知：更新回查计数，等待下次检查
				psm.logger.Printf("Transaction %s state unknown, will retry later", record.TransactionID)
				if updateErr := psm.halfMessageStorage.Update(record.TransactionID, record); updateErr != nil {
					psm.logger.Printf("Failed to update half message record %s: %v",
						record.TransactionID, updateErr)
				}

			default:
				psm.logger.Printf("Unknown transaction state %v for %s", state, record.TransactionID)
				// 对于未知状态，也更新记录等待下次检查
				if updateErr := psm.halfMessageStorage.Update(record.TransactionID, record); updateErr != nil {
					psm.logger.Printf("Failed to update half message record %s: %v",
						record.TransactionID, updateErr)
				}
			}
		}
	}
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
	// TODO: Implement partition state machine lookup logic
	return nil, nil
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

	// 启动半消息清理任务
	go psm.startHalfMessageCleanup()
}

// GetMetrics returns partition metrics
func (psm *PartitionStateMachine) GetMetrics() map[string]interface{} {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	return map[string]interface{}{
		"message_count": psm.messageCount,
		"bytes_stored":  psm.bytesStored,
		"last_write":    psm.lastWrite,
		"last_read":     psm.lastRead,
		"is_ready":      psm.isReady,
	}
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
