package transaction

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/issac1998/go-queue/internal/protocol"
)

// PebbleTransactionManager 基于PebbleDB的事务管理器
type PebbleTransactionManager struct {
	db             *pebble.DB
	dbPath         string
	defaultTimeout time.Duration
	stopChan       chan struct{}
	mu             sync.RWMutex
}

// PebbleHalfMessage PebbleDB存储的半消息结构
type PebbleHalfMessage struct {
	TransactionID string           `json:"transaction_id"`
	Topic         string           `json:"topic"`
	Partition     int32            `json:"partition"`
	Data          []byte           `json:"data"`
	CreatedAt     time.Time        `json:"created_at"`
	ExpiresAt     time.Time        `json:"expires_at"`
	State         TransactionState `json:"state"`
}

// NewPebbleTransactionManager 创建基于PebbleDB的事务管理器
func NewPebbleTransactionManager(dbPath string) (*PebbleTransactionManager, error) {
	// 配置PebbleDB选项
	opts := &pebble.Options{
		Cache:                       pebble.NewCache(64 << 20), // 64MB cache
		MemTableSize:                16 << 20,                  // 16MB memtable
		MemTableStopWritesThreshold: 2,
		LBaseMaxBytes:               64 << 20, // 64MB
		Levels:                      make([]pebble.LevelOptions, 7),
	}

	// 打开数据库
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open PebbleDB: %w", err)
	}

	ptm := &PebbleTransactionManager{
		db:             db,
		dbPath:         dbPath,
		defaultTimeout: 30 * time.Second,
		stopChan:       make(chan struct{}),
	}

	// 启动清理过期事务的goroutine
	go ptm.cleanupExpiredTransactions()

	return ptm, nil
}

// PrepareTransaction 准备事务
func (ptm *PebbleTransactionManager) PrepareTransaction(req *TransactionPrepareRequest) (*TransactionPrepareResponse, error) {
	ptm.mu.Lock()
	defer ptm.mu.Unlock()

	txnID := string(req.TransactionID)
	key := []byte("txn:" + txnID)

	// 检查事务是否已存在
	_, closer, err := ptm.db.Get(key)
	if err == nil {
		closer.Close()
		return &TransactionPrepareResponse{
			TransactionID: req.TransactionID,
			ErrorCode:     protocol.ErrorInvalidRequest,
			Error:         "transaction already exists",
		}, nil
	}
	if err != pebble.ErrNotFound {
		return nil, fmt.Errorf("failed to check transaction existence: %w", err)
	}

	// 计算过期时间
	timeout := time.Duration(req.Timeout) * time.Millisecond
	if timeout <= 0 {
		timeout = ptm.defaultTimeout
	}

	now := time.Now()
	halfMsg := &PebbleHalfMessage{
		TransactionID: txnID,
		Topic:         req.Topic,
		Partition:     req.Partition,
		Data:          req.Value,
		CreatedAt:     now,
		ExpiresAt:     now.Add(timeout),
		State:         StatePrepared,
	}

	// 序列化半消息
	data, err := json.Marshal(halfMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal half message: %w", err)
	}

	// 存储到PebbleDB
	if err := ptm.db.Set(key, data, pebble.Sync); err != nil {
		return nil, fmt.Errorf("failed to store half message: %w", err)
	}

	return &TransactionPrepareResponse{
		TransactionID: req.TransactionID,
		ErrorCode:     protocol.ErrorNone,
	}, nil
}

// CommitTransaction 提交事务
func (ptm *PebbleTransactionManager) CommitTransaction(transactionID TransactionID) (*TransactionCommitResponse, error) {
	ptm.mu.Lock()
	defer ptm.mu.Unlock()

	txnID := string(transactionID)
	key := []byte("txn:" + txnID)

	// 获取半消息
	value, closer, err := ptm.db.Get(key)
	if err == pebble.ErrNotFound {
		return &TransactionCommitResponse{
			TransactionID: transactionID,
			ErrorCode:     protocol.ErrorInvalidRequest,
			Error:         "transaction not found",
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get half message: %w", err)
	}

	var halfMsg PebbleHalfMessage
	if err := json.Unmarshal(value, &halfMsg); err != nil {
		closer.Close()
		return nil, fmt.Errorf("failed to unmarshal half message: %w", err)
	}
	closer.Close()

	// 检查事务是否已过期
	if time.Now().After(halfMsg.ExpiresAt) {
		// 删除过期事务
		if err := ptm.db.Delete(key, pebble.Sync); err != nil {
			return nil, fmt.Errorf("failed to delete expired transaction: %w", err)
		}
		return &TransactionCommitResponse{
			TransactionID: transactionID,
			ErrorCode:     protocol.ErrorInvalidRequest,
			Error:         "transaction expired",
		}, nil
	}

	// 更新状态为已提交并删除事务
	if err := ptm.db.Delete(key, pebble.Sync); err != nil {
		return nil, fmt.Errorf("failed to delete committed transaction: %w", err)
	}

	return &TransactionCommitResponse{
		TransactionID: transactionID,
		ErrorCode:     protocol.ErrorNone,
	}, nil
}

// RollbackTransaction 回滚事务
func (ptm *PebbleTransactionManager) RollbackTransaction(transactionID TransactionID) (*TransactionRollbackResponse, error) {
	ptm.mu.Lock()
	defer ptm.mu.Unlock()

	txnID := string(transactionID)
	key := []byte("txn:" + txnID)

	// 检查事务是否存在
	_, closer, err := ptm.db.Get(key)
	if err == pebble.ErrNotFound {
		return &TransactionRollbackResponse{
			TransactionID: transactionID,
			ErrorCode:     protocol.ErrorInvalidRequest,
			Error:         "transaction not found",
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to check transaction existence: %w", err)
	}
	closer.Close()

	// 直接删除事务
	if err := ptm.db.Delete(key, pebble.Sync); err != nil {
		return nil, fmt.Errorf("failed to delete transaction: %w", err)
	}

	return &TransactionRollbackResponse{
		TransactionID: transactionID,
		ErrorCode:     protocol.ErrorNone,
	}, nil
}

// GetHalfMessage 获取半消息
func (ptm *PebbleTransactionManager) GetHalfMessage(transactionID TransactionID) (*HalfMessage, bool) {
	ptm.mu.RLock()
	defer ptm.mu.RUnlock()

	txnID := string(transactionID)
	key := []byte("txn:" + txnID)

	value, closer, err := ptm.db.Get(key)
	if err != nil {
		return nil, false
	}
	defer closer.Close()

	var pebbleMsg PebbleHalfMessage
	if err := json.Unmarshal(value, &pebbleMsg); err != nil {
		return nil, false
	}

	// 转换为标准的HalfMessage格式
	halfMsg := &HalfMessage{
		TransactionID: TransactionID(pebbleMsg.TransactionID),
		Topic:         pebbleMsg.Topic,
		Partition:     pebbleMsg.Partition,
		Value:         pebbleMsg.Data,
	}

	return halfMsg, true
}

// GetTransactionStatus 获取事务状态
func (ptm *PebbleTransactionManager) GetTransactionStatus(transactionID TransactionID) (map[string]interface{}, error) {
	ptm.mu.RLock()
	defer ptm.mu.RUnlock()

	txnID := string(transactionID)
	key := []byte("txn:" + txnID)

	value, closer, err := ptm.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, fmt.Errorf("transaction not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}
	defer closer.Close()

	var halfMsg PebbleHalfMessage
	if err := json.Unmarshal(value, &halfMsg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal half message: %w", err)
	}

	return map[string]interface{}{
		"transaction_id": halfMsg.TransactionID,
		"state":          halfMsg.State,
		"topic":          halfMsg.Topic,
		"partition":      halfMsg.Partition,
		"created_at":     halfMsg.CreatedAt,
		"expires_at":     halfMsg.ExpiresAt,
	}, nil
}

// cleanupExpiredTransactions 清理过期事务
func (ptm *PebbleTransactionManager) cleanupExpiredTransactions() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ptm.cleanupExpired()
		case <-ptm.stopChan:
			return
		}
	}
}

// cleanupExpired 清理过期事务的具体实现
func (ptm *PebbleTransactionManager) cleanupExpired() {
	ptm.mu.Lock()
	defer ptm.mu.Unlock()

	now := time.Now()
	iter := ptm.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("txn:"),
		UpperBound: []byte("txn;"), // 使用分号作为上界，确保包含所有以"txn:"开头的键
	})
	defer iter.Close()

	var expiredKeys [][]byte

	for iter.First(); iter.Valid(); iter.Next() {
		var halfMsg PebbleHalfMessage
		if err := json.Unmarshal(iter.Value(), &halfMsg); err != nil {
			continue // 跳过无法解析的记录
		}

		if now.After(halfMsg.ExpiresAt) {
			// 复制键，因为iter.Key()返回的切片在迭代器移动后可能会被修改
			key := make([]byte, len(iter.Key()))
			copy(key, iter.Key())
			expiredKeys = append(expiredKeys, key)
		}
	}

	// 删除过期的事务
	for _, key := range expiredKeys {
		if err := ptm.db.Delete(key, pebble.NoSync); err != nil {
			// 记录错误但继续清理其他过期事务
			continue
		}
	}

	// 如果有删除操作，执行一次同步
	if len(expiredKeys) > 0 {
		ptm.db.Flush()
	}
}

// Stop 停止事务管理器
func (ptm *PebbleTransactionManager) Stop() error {
	close(ptm.stopChan)
	return ptm.db.Close()
}

// GetActiveTransactionCount 获取活跃事务数量
func (ptm *PebbleTransactionManager) GetActiveTransactionCount() int {
	ptm.mu.RLock()
	defer ptm.mu.RUnlock()

	count := 0
	iter := ptm.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("txn:"),
		UpperBound: []byte("txn;"),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	return count
}

// GetStats 获取数据库统计信息
func (ptm *PebbleTransactionManager) GetStats() (map[string]interface{}, error) {
	metrics := ptm.db.Metrics()

	stats := map[string]interface{}{
		"db_path":           ptm.dbPath,
		"memtable_size":     metrics.MemTable.Size,
		"cache_size":        metrics.BlockCache.Size,
		"cache_hit_rate":    float64(metrics.BlockCache.Hits) / float64(metrics.BlockCache.Hits+metrics.BlockCache.Misses),
		"compaction_count":  metrics.Compact.Count,
		"flush_count":       metrics.Flush.Count,
		"active_txn_count":  ptm.GetActiveTransactionCount(),
	}

	return stats, nil
}

// Compact 触发手动压缩
func (ptm *PebbleTransactionManager) Compact() error {
	return ptm.db.Compact(nil, nil)
}