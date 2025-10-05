package transaction

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/issac1998/go-queue/internal/utils"
)

// HalfMessageStorage interface for half message storage operations
type HalfMessageStorage interface {
	Store(txnID string, halfMsg *HalfMessage, expireTime time.Time) error
	Get(txnID string) (*StoredHalfMessage, error)
	Delete(txnID string) error
	GetExpiredMessages(before time.Time) ([]*StoredHalfMessage, error)
	CleanupExpired(before time.Time) error
	Close() error
}

// StoredHalfMessage represents a stored half message structure
type StoredHalfMessage struct {
	TransactionID string       `json:"transaction_id"`
	ExpireTime    int64        `json:"expire_time"`
	CreatedTime   int64        `json:"created_time"`
	Status        string       `json:"status"`
	HalfMessage   *HalfMessage `json:"half_message"`
}

// PebbleHalfMessageStorage PebbleDB-based half message storage implementation
type PebbleHalfMessageStorage struct {
	db    *pebble.DB
	cache *pebble.Cache
	path  string
}

const (
	HalfMessagePrefix = "half_msg:"
	ExpireIndexPrefix = "expire_idx:"
)

// NewPebbleHalfMessageStorage creates a new PebbleDB half message storage
func NewPebbleHalfMessageStorage(dataDir string) (*PebbleHalfMessageStorage, error) {
	dbPath := filepath.Join(dataDir, "half_messages")
	
	cache := pebble.NewCache(16 << 20)
	opts := &pebble.Options{
		Cache:        cache,
		MemTableSize: 4 << 20,
		Levels: []pebble.LevelOptions{
			{Compression: pebble.SnappyCompression},
		},
		MaxOpenFiles: 1000,
	}
	
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		cache.Unref()
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}
	
	return &PebbleHalfMessageStorage{
		db:    db,
		cache: cache,
		path:  dbPath,
	}, nil
}

func (s *PebbleHalfMessageStorage) Store(txnID string, halfMsg *HalfMessage, expireTime time.Time) error {
	storedMsg := &StoredHalfMessage{
		TransactionID: txnID,
		ExpireTime:    expireTime.UnixMilli(),
		CreatedTime:   time.Now().UnixMilli(),
		Status:        "PREPARED",
		HalfMessage:   halfMsg,
	}
	
	data, err := json.Marshal(storedMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal half message: %w", err)
	}
	
	// 使用批量写入保证原子性
	batch := s.db.NewBatch()
	defer batch.Close()
	
	// 1. 存储半消息数据
	dataKey := HalfMessagePrefix + txnID
	if err := batch.Set([]byte(dataKey), data, pebble.Sync); err != nil {
		return fmt.Errorf("failed to set half message data: %w", err)
	}
	
	// 2. 创建过期时间索引
	expireKey := s.buildExpireIndexKey(expireTime.UnixMilli(), txnID)
	if err := batch.Set([]byte(expireKey), []byte(txnID), pebble.Sync); err != nil {
		return fmt.Errorf("failed to set expire index: %w", err)
	}
	
	// 提交批量操作
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}
	
	return nil
}

func (s *PebbleHalfMessageStorage) Get(txnID string) (*StoredHalfMessage, error) {
	key := HalfMessagePrefix + txnID
	
	data, closer, err := s.db.Get([]byte(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("half message not found: %s", txnID)
		}
		return nil, fmt.Errorf("failed to get half message: %w", err)
	}
	defer closer.Close()
	
	var storedMsg StoredHalfMessage
	if err := json.Unmarshal(data, &storedMsg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal half message: %w", err)
	}
	
	return &storedMsg, nil
}

func (s *PebbleHalfMessageStorage) Delete(txnID string) error {
	storedMsg, err := s.Get(txnID)
	if err != nil {
		return err
	}
	if storedMsg == nil {
		return nil
	}
	
	batch := s.db.NewBatch()
	defer batch.Close()
	
	dataKey := HalfMessagePrefix + txnID
	if err := batch.Delete([]byte(dataKey), pebble.Sync); err != nil {
		return fmt.Errorf("failed to delete half message data: %w", err)
	}
	
	expireKey := s.buildExpireIndexKey(storedMsg.ExpireTime, txnID)
	if err := batch.Delete([]byte(expireKey), pebble.Sync); err != nil {
		return fmt.Errorf("failed to delete expire index: %w", err)
	}
	
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit delete batch: %w", err)
	}
	
	return nil
}

func (s *PebbleHalfMessageStorage) GetExpiredMessages(before time.Time) ([]*StoredHalfMessage, error) {
	beforeMilli := before.UnixMilli()
	
	startKey := []byte(ExpireIndexPrefix)
	endKey := []byte(s.buildExpireIndexKey(beforeMilli, ""))
	
	iter := s.db.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	defer iter.Close()
	
	var expiredMessages []*StoredHalfMessage
	
	for iter.First(); iter.Valid(); iter.Next() {
		txnID := string(iter.Value())
		
		storedMsg, err := s.Get(txnID)
		if err != nil {
			continue
		}
		if storedMsg != nil {
			expiredMessages = append(expiredMessages, storedMsg)
		}
	}
	
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("failed to iterate expired messages: %w", err)
	}
	
	return expiredMessages, nil
}

func (s *PebbleHalfMessageStorage) CleanupExpired(before time.Time) error {
	expiredMessages, err := s.GetExpiredMessages(before)
	if err != nil {
		return fmt.Errorf("failed to get expired messages: %w", err)
	}
	
	if len(expiredMessages) == 0 {
		return nil
	}
	
	// 使用新的清理工具函数，提供重试机制和更好的错误处理
	config := &utils.CleanupConfig{
		MaxRetries:      3,
		InitialDelay:    100 * time.Millisecond,
		MaxDelay:        2 * time.Second,
		BackoffFactor:   2.0,
		UseSync:         false, // 使用更宽松的提交选项
		BatchSize:       1000,
		LogErrors:       true,
	}

	cleanupManager := utils.NewCleanupManager(config)
	
	// 准备要删除的键
	var allKeys [][]byte
	for _, msg := range expiredMessages {
		dataKey := HalfMessagePrefix + msg.TransactionID
		allKeys = append(allKeys, []byte(dataKey))
		
		expireKey := s.buildExpireIndexKey(msg.ExpireTime, msg.TransactionID)
		allKeys = append(allKeys, []byte(expireKey))
	}

	err = cleanupManager.BatchCleanupWithRetry("transaction_cleanup", s.db, allKeys)
	if err != nil {
		return fmt.Errorf("failed to cleanup expired transactions: %w", err)
	}
	
	return nil
}

func (s *PebbleHalfMessageStorage) Close() error {
	var err error
	if s.db != nil {
		err = s.db.Close()
		s.db = nil
	}
	if s.cache != nil {
		s.cache.Unref()
		s.cache = nil
	}
	return err
}

func (s *PebbleHalfMessageStorage) buildExpireIndexKey(expireTimeMilli int64, txnID string) string {
	timeStr := fmt.Sprintf("%016d", expireTimeMilli)
	return ExpireIndexPrefix + timeStr + ":" + txnID
}

func (s *PebbleHalfMessageStorage) GetStats() map[string]interface{} {
	if s.db == nil {
		return nil
	}
	
	metrics := s.db.Metrics()
	return map[string]interface{}{
		"compactions":    metrics.Compact.Count,
		"flush_count":    metrics.Flush.Count,
		"memtable_size":  metrics.MemTable.Size,
		"wal_size":       metrics.WAL.Size,
	}
}