package raft

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/issac1998/go-queue/internal/utils"
)

// HalfMessageStorage 定义 HalfMessage 存储接口
type HalfMessageStorage interface {
	// Store 存储 HalfMessage
	Store(transactionID string, halfMessage *HalfMessageRecord) error

	// Get 获取 HalfMessage
	Get(transactionID string) (*HalfMessageRecord, bool, error)

	// Delete 删除 HalfMessage
	Delete(transactionID string) error

	// GetExpired 获取过期的 HalfMessage，返回过期消息列表
	GetExpired() ([]*HalfMessageRecord, error)

	// CleanupExpired 清理过期的 HalfMessage，返回清理数量
	CleanupExpired() (int, error)

	// Update 更新 HalfMessage（用于更新回查次数等）
	Update(transactionID string, halfMessage *HalfMessageRecord) error

	// Close 关闭存储
	Close() error
}

// PebbleHalfMessageStorage PebbleDB 实现的 HalfMessage 存储
type PebbleHalfMessageStorage struct {
	db    *pebble.DB
	cache *pebble.Cache
	path  string
}

const (
	HalfMessagePrefix = "half_msg:"
	ExpireIndexPrefix = "expire_idx:"
)

// NewPebbleHalfMessageStorage 创建新的 PebbleDB HalfMessage 存储
func NewPebbleHalfMessageStorage(dataDir string) (*PebbleHalfMessageStorage, error) {
	dbPath := filepath.Join(dataDir, "half_messages")

	cache := pebble.NewCache(16 << 20) // 16MB cache
	opts := &pebble.Options{
		Cache:        cache,
		MemTableSize: 4 << 20, // 4MB memtable
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

// Store store HalfMessage and
func (s *PebbleHalfMessageStorage) Store(transactionID string, halfMessage *HalfMessageRecord) error {
	key := HalfMessagePrefix + transactionID

	data, err := json.Marshal(halfMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal half message: %w", err)
	}

	if err := s.db.Set([]byte(key), data, pebble.Sync); err != nil {
		return fmt.Errorf("failed to store half message: %w", err)
	}

	expireKey := fmt.Sprintf("%s%d_%s", ExpireIndexPrefix, halfMessage.ExpiresAt.Unix(), transactionID)
	if err := s.db.Set([]byte(expireKey), []byte(transactionID), pebble.Sync); err != nil {
		s.db.Delete([]byte(key), pebble.Sync)
		return fmt.Errorf("failed to create expire index: %w", err)
	}

	return nil
}

// Get get HalfMessage
func (s *PebbleHalfMessageStorage) Get(transactionID string) (*HalfMessageRecord, bool, error) {
	key := HalfMessagePrefix + transactionID

	data, closer, err := s.db.Get([]byte(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to get half message: %w", err)
	}
	defer closer.Close()

	var halfMessage HalfMessageRecord
	if err := json.Unmarshal(data, &halfMessage); err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal half message: %w", err)
	}

	return &halfMessage, true, nil
}

// Delete delte HalfMessage
func (s *PebbleHalfMessageStorage) Delete(transactionID string) error {
	key := HalfMessagePrefix + transactionID

	halfMessage, exists, err := s.Get(transactionID)
	if err != nil {
		return err
	}

	if !exists {
		return nil 
	}

	if err := s.db.Delete([]byte(key), pebble.Sync); err != nil {
		return fmt.Errorf("failed to delete half message: %w", err)
	}

	expireKey := fmt.Sprintf("%s%d_%s", ExpireIndexPrefix, halfMessage.ExpiresAt.Unix(), transactionID)
	if err := s.db.Delete([]byte(expireKey), pebble.Sync); err != nil {
		
	}

	return nil
}

// GetExpired get expired HalfMessage
func (s *PebbleHalfMessageStorage) GetExpired() ([]*HalfMessageRecord, error) {
	now := time.Now()
	var expiredMessages []*HalfMessageRecord

	iter := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(ExpireIndexPrefix),
		UpperBound: []byte(ExpireIndexPrefix + "~"), 
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()

		keyStr := string(key)
		if len(keyStr) <= len(ExpireIndexPrefix) {
			continue
		}

		timeStr := keyStr[len(ExpireIndexPrefix):]
		var expireTime int64
		var transactionID string

		if n, err := fmt.Sscanf(timeStr, "%d_%s", &expireTime, &transactionID); n != 2 || err != nil {
			continue
		}

		if time.Unix(expireTime, 0).Before(now) {
			if halfMessage, exists, err := s.Get(transactionID); err == nil && exists {
				expiredMessages = append(expiredMessages, halfMessage)
			}
		} else {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("failed to iterate expire index: %w", err)
	}

	return expiredMessages, nil
}

// Update update HalfMessage
func (s *PebbleHalfMessageStorage) Update(transactionID string, halfMessage *HalfMessageRecord) error {
	return s.Store(transactionID, halfMessage)
}

// CleanupExpired clean expired HalfMessage
func (s *PebbleHalfMessageStorage) CleanupExpired() (int, error) {
	now := time.Now()
	expiredCount := 0

	iter := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(ExpireIndexPrefix),
		UpperBound: []byte(ExpireIndexPrefix + "~"), 
	})
	defer iter.Close()

	var expiredKeys [][]byte
	var expiredIndexKeys [][]byte

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()

		keyStr := string(key)
		if len(keyStr) <= len(ExpireIndexPrefix) {
			continue
		}

		timeStr := keyStr[len(ExpireIndexPrefix):]
		var expireTime int64
		var transactionID string

		if n, err := fmt.Sscanf(timeStr, "%d_%s", &expireTime, &transactionID); n != 2 || err != nil {
			continue
		}

		if time.Unix(expireTime, 0).Before(now) {
			expiredIndexKeys = append(expiredIndexKeys, append([]byte(nil), key...))

			mainKey := HalfMessagePrefix + transactionID
			expiredKeys = append(expiredKeys, []byte(mainKey))
			expiredCount++
		} else {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("failed to iterate expire index: %w", err)
	}

	if len(expiredKeys) > 0 {
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
		allKeys := make([][]byte, 0, len(expiredKeys)+len(expiredIndexKeys))
		allKeys = append(allKeys, expiredKeys...)
		allKeys = append(allKeys, expiredIndexKeys...)

		err := cleanupManager.BatchCleanupWithRetry("half_message_cleanup", s.db, allKeys)
		if err != nil {
			return 0, fmt.Errorf("failed to cleanup expired half messages: %w", err)
		}
	}

	return expiredCount, nil
}

// Close close
func (s *PebbleHalfMessageStorage) Close() error {
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return err
		}
	}
	if s.cache != nil {
		s.cache.Unref()
	}
	return nil
}
