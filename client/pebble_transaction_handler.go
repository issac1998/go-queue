package client

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/issac1998/go-queue/internal/transaction"
)

// PebbleTransactionHandler 基于PebbleDB的事务处理器
type PebbleTransactionHandler struct {
	db        *pebble.DB
	dbPath    string
	listeners map[string]transaction.TransactionListener
	mu        sync.RWMutex
	stopChan  chan struct{}
}

// PebbleTransactionState 存储在PebbleDB中的事务状态
type PebbleTransactionState struct {
	TransactionID string                     `json:"transaction_id"`
	ProducerGroup string                     `json:"producer_group"`
	MessageKey    []byte                     `json:"message_key"`
	MessageValue  []byte                     `json:"message_value"`
	State         transaction.TransactionState `json:"state"`
	CreatedAt     time.Time                  `json:"created_at"`
	UpdatedAt     time.Time                  `json:"updated_at"`
	ExpiresAt     time.Time                  `json:"expires_at"`
}

// NewPebbleTransactionHandler 创建基于PebbleDB的事务处理器
func NewPebbleTransactionHandler(dbPath string) (*PebbleTransactionHandler, error) {
	// 配置PebbleDB选项
	opts := &pebble.Options{
		Cache:                       pebble.NewCache(32 << 20), // 32MB cache
		MemTableSize:                8 << 20,                   // 8MB memtable
		MemTableStopWritesThreshold: 2,
		LBaseMaxBytes:               32 << 20, // 32MB
		Levels:                      make([]pebble.LevelOptions, 7),
	}

	// 打开数据库
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open PebbleDB: %w", err)
	}

	pth := &PebbleTransactionHandler{
		db:        db,
		dbPath:    dbPath,
		listeners: make(map[string]transaction.TransactionListener),
		stopChan:  make(chan struct{}),
	}

	// 启动清理过期事务状态的goroutine
	go pth.cleanupExpiredStates()

	return pth, nil
}

// RegisterListener 注册事务监听器
func (pth *PebbleTransactionHandler) RegisterListener(producerGroup string, listener transaction.TransactionListener) {
	pth.mu.Lock()
	defer pth.mu.Unlock()

	pth.listeners[producerGroup] = listener
	log.Printf("Registered PebbleDB transaction listener for producer group: %s", producerGroup)
}

// UnregisterListener 注销事务监听器
func (pth *PebbleTransactionHandler) UnregisterListener(producerGroup string) {
	pth.mu.Lock()
	defer pth.mu.Unlock()

	delete(pth.listeners, producerGroup)
	log.Printf("Unregistered PebbleDB transaction listener for producer group: %s", producerGroup)
}

// CheckTransaction 检查事务状态
func (pth *PebbleTransactionHandler) CheckTransaction(producerGroup, transactionID string, messageKey, messageValue []byte) transaction.TransactionState {
	// 首先尝试从PebbleDB获取缓存的状态
	if state := pth.getCachedTransactionState(producerGroup, transactionID); state != transaction.StateUnknown {
		return state
	}

	// 如果没有缓存状态，使用监听器检查
	pth.mu.RLock()
	listener, exists := pth.listeners[producerGroup]
	pth.mu.RUnlock()

	if !exists {
		log.Printf("No transaction listener found for producer group: %s", producerGroup)
		return transaction.StateUnknown
	}

	// 构造HalfMessage
	halfMessage := transaction.HalfMessage{
		Key:   messageKey,
		Value: messageValue,
	}

	// 使用标准TransactionListener接口
	state := listener.CheckLocalTransaction(transaction.TransactionID(transactionID), halfMessage)

	// 缓存状态到PebbleDB
	pth.cacheTransactionState(producerGroup, transactionID, messageKey, messageValue, state)

	return state
}

// getCachedTransactionState 从PebbleDB获取缓存的事务状态
func (pth *PebbleTransactionHandler) getCachedTransactionState(producerGroup, transactionID string) transaction.TransactionState {
	key := pth.makeStateKey(producerGroup, transactionID)
	
	value, closer, err := pth.db.Get(key)
	if err != nil {
		return transaction.StateUnknown
	}
	defer closer.Close()

	var state PebbleTransactionState
	if err := json.Unmarshal(value, &state); err != nil {
		return transaction.StateUnknown
	}

	// 检查是否过期
	if time.Now().After(state.ExpiresAt) {
		// 异步删除过期状态
		go func() {
			pth.db.Delete(key, pebble.NoSync)
		}()
		return transaction.StateUnknown
	}

	return state.State
}

// cacheTransactionState 缓存事务状态到PebbleDB
func (pth *PebbleTransactionHandler) cacheTransactionState(producerGroup, transactionID string, messageKey, messageValue []byte, state transaction.TransactionState) {
	key := pth.makeStateKey(producerGroup, transactionID)
	
	now := time.Now()
	pebbleState := PebbleTransactionState{
		TransactionID: transactionID,
		ProducerGroup: producerGroup,
		MessageKey:    messageKey,
		MessageValue:  messageValue,
		State:         state,
		CreatedAt:     now,
		UpdatedAt:     now,
		ExpiresAt:     now.Add(1 * time.Hour), // 缓存1小时
	}

	data, err := json.Marshal(pebbleState)
	if err != nil {
		log.Printf("Failed to marshal transaction state: %v", err)
		return
	}

	if err := pth.db.Set(key, data, pebble.NoSync); err != nil {
		log.Printf("Failed to cache transaction state: %v", err)
	}
}

// makeStateKey 生成状态存储的键
func (pth *PebbleTransactionHandler) makeStateKey(producerGroup, transactionID string) []byte {
	return []byte(fmt.Sprintf("txn_state:%s:%s", producerGroup, transactionID))
}

// GetRegisteredGroups 获取所有注册的生产者组
func (pth *PebbleTransactionHandler) GetRegisteredGroups() []string {
	pth.mu.RLock()
	defer pth.mu.RUnlock()

	groups := make([]string, 0, len(pth.listeners))
	for group := range pth.listeners {
		groups = append(groups, group)
	}
	return groups
}

// GetCachedTransactionStates 获取所有缓存的事务状态
func (pth *PebbleTransactionHandler) GetCachedTransactionStates(producerGroup string) ([]PebbleTransactionState, error) {
	prefix := []byte(fmt.Sprintf("txn_state:%s:", producerGroup))
	upperBound := []byte(fmt.Sprintf("txn_state:%s;", producerGroup))

	iter := pth.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	defer iter.Close()

	var states []PebbleTransactionState
	now := time.Now()

	for iter.First(); iter.Valid(); iter.Next() {
		var state PebbleTransactionState
		if err := json.Unmarshal(iter.Value(), &state); err != nil {
			continue // 跳过无法解析的记录
		}

		// 跳过过期的状态
		if now.After(state.ExpiresAt) {
			continue
		}

		states = append(states, state)
	}

	return states, iter.Error()
}

// ClearCachedStates 清除指定生产者组的所有缓存状态
func (pth *PebbleTransactionHandler) ClearCachedStates(producerGroup string) error {
	prefix := []byte(fmt.Sprintf("txn_state:%s:", producerGroup))
	upperBound := []byte(fmt.Sprintf("txn_state:%s;", producerGroup))

	iter := pth.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	defer iter.Close()

	batch := pth.db.NewBatch()
	defer batch.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key(), nil); err != nil {
			return fmt.Errorf("failed to delete key in batch: %w", err)
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	return batch.Commit(pebble.Sync)
}

// cleanupExpiredStates 清理过期的事务状态
func (pth *PebbleTransactionHandler) cleanupExpiredStates() {
	ticker := time.NewTicker(5 * time.Minute) // 每5分钟清理一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pth.performCleanup()
		case <-pth.stopChan:
			return
		}
	}
}

// performCleanup 执行清理操作
func (pth *PebbleTransactionHandler) performCleanup() {
	iter := pth.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("txn_state:"),
		UpperBound: []byte("txn_state;"),
	})
	defer iter.Close()

	now := time.Now()
	batch := pth.db.NewBatch()
	defer batch.Close()

	expiredCount := 0
	for iter.First(); iter.Valid(); iter.Next() {
		var state PebbleTransactionState
		if err := json.Unmarshal(iter.Value(), &state); err != nil {
			continue // 跳过无法解析的记录
		}

		if now.After(state.ExpiresAt) {
			if err := batch.Delete(iter.Key(), nil); err != nil {
				log.Printf("Failed to delete expired state: %v", err)
				continue
			}
			expiredCount++
		}
	}

	if expiredCount > 0 {
		if err := batch.Commit(pebble.NoSync); err != nil {
			log.Printf("Failed to commit cleanup batch: %v", err)
		} else {
			log.Printf("Cleaned up %d expired transaction states", expiredCount)
		}
	}
}

// GetStats 获取统计信息
func (pth *PebbleTransactionHandler) GetStats() (map[string]interface{}, error) {
	metrics := pth.db.Metrics()

	// 统计缓存的事务状态数量
	iter := pth.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("txn_state:"),
		UpperBound: []byte("txn_state;"),
	})
	defer iter.Close()

	cachedStatesCount := 0
	for iter.First(); iter.Valid(); iter.Next() {
		cachedStatesCount++
	}

	stats := map[string]interface{}{
		"db_path":             pth.dbPath,
		"memtable_size":       metrics.MemTable.Size,
		"cache_size":          metrics.BlockCache.Size,
		"cache_hit_rate":      float64(metrics.BlockCache.Hits) / float64(metrics.BlockCache.Hits+metrics.BlockCache.Misses),
		"compaction_count":    metrics.Compact.Count,
		"flush_count":         metrics.Flush.Count,
		"registered_groups":   len(pth.listeners),
		"cached_states_count": cachedStatesCount,
	}

	return stats, nil
}

// Close 关闭事务处理器
func (pth *PebbleTransactionHandler) Close() error {
	close(pth.stopChan)
	return pth.db.Close()
}

// Compact 触发手动压缩
func (pth *PebbleTransactionHandler) Compact() error {
	return pth.db.Compact(nil, nil)
}

// PebbleTransactionClient 基于PebbleDB的事务客户端
type PebbleTransactionClient struct {
	*Client
	handler       *PebbleTransactionHandler
	producerGroup string
}

// NewPebbleTransactionClient 创建基于PebbleDB的事务客户端
func NewPebbleTransactionClient(config ClientConfig, producerGroup, dbPath string) (*PebbleTransactionClient, error) {
	client := NewClient(config)

	handler, err := NewPebbleTransactionHandler(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create PebbleDB transaction handler: %w", err)
	}

	ptc := &PebbleTransactionClient{
		Client:        client,
		handler:       handler,
		producerGroup: producerGroup,
	}

	return ptc, nil
}

// RegisterTransactionListener 注册事务监听器
func (ptc *PebbleTransactionClient) RegisterTransactionListener(listener transaction.TransactionListener) {
	ptc.handler.RegisterListener(ptc.producerGroup, listener)
}

// CheckTransactionState 检查事务状态
func (ptc *PebbleTransactionClient) CheckTransactionState(transactionID string, messageKey, messageValue []byte) transaction.TransactionState {
	return ptc.handler.CheckTransaction(ptc.producerGroup, transactionID, messageKey, messageValue)
}

// GetProducerGroup 获取生产者组
func (ptc *PebbleTransactionClient) GetProducerGroup() string {
	return ptc.producerGroup
}

// GetCachedStates 获取缓存的事务状态
func (ptc *PebbleTransactionClient) GetCachedStates() ([]PebbleTransactionState, error) {
	return ptc.handler.GetCachedTransactionStates(ptc.producerGroup)
}

// ClearCachedStates 清除缓存的事务状态
func (ptc *PebbleTransactionClient) ClearCachedStates() error {
	return ptc.handler.ClearCachedStates(ptc.producerGroup)
}

// GetStats 获取统计信息
func (ptc *PebbleTransactionClient) GetStats() (map[string]interface{}, error) {
	return ptc.handler.GetStats()
}

// Close 关闭客户端
func (ptc *PebbleTransactionClient) Close() error {
	return ptc.handler.Close()
}