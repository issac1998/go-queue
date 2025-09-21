package delayed

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
)

// DelayedMessageStorage interface for delayed message storage operations
type DelayedMessageStorage interface {
	Store(messageID string, message *DelayedMessage) error
	Get(messageID string) (*DelayedMessage, error)
	Update(messageID string, message *DelayedMessage) error
	Delete(messageID string) error
	// 按时间范围和groupID扫描消息，利用PebbleDB有序性
	ScanPendingMessages(groupID uint64, fromTime, toTime time.Time, limit int) ([]*DelayedMessage, error)
	CleanupDelivered(beforeTime time.Time) error
	GetStats() map[string]interface{}
	Close() error
	// 批量操作：使用单个 Pebble 批处理以提升吞吐
	UpdateBatch(updates map[string]*DelayedMessage) error
	DeleteBatch(messageIDs []string) error
}

// PebbleDelayedMessageStorage PebbleDB-based delayed message storage implementation
type PebbleDelayedMessageStorage struct {
	db   *pebble.DB
	path string
}

const (
	DelayedMessagePrefix = "delayed_msg_" // 新格式：delayed_msg_{group_id:016d}_{deliver_time:016d}_{message_id}
	DeliverIndexPrefix   = "deliver_idx:"
	StatusIndexPrefix    = "status_idx:"
)

// NewPebbleDelayedMessageStorage creates a new PebbleDB delayed message storage
func NewPebbleDelayedMessageStorage(dataDir string) (*PebbleDelayedMessageStorage, error) {
	dbPath := filepath.Join(dataDir, "delayed_messages")

	opts := &pebble.Options{
		Cache:        pebble.NewCache(16 << 20),
		MemTableSize: 4 << 20,
		Levels: []pebble.LevelOptions{
			{Compression: pebble.SnappyCompression},
		},
		MaxOpenFiles: 1000,
	}

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}

	return &PebbleDelayedMessageStorage{
		db:   db,
		path: dbPath,
	}, nil
}

// buildDelayedMessageKey builds the key for delayed message storage with groupID
func (s *PebbleDelayedMessageStorage) buildDelayedMessageKey(groupID uint64, deliverTimeMilli int64, messageID string) string {
	return fmt.Sprintf("%s%016d_%016d_%s", DelayedMessagePrefix, groupID, deliverTimeMilli, messageID)
}

func (s *PebbleDelayedMessageStorage) Store(messageID string, message *DelayedMessage) error {
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal delayed message: %w", err)
	}

	// 使用新的有序Key格式，包含groupID
	dataKey := s.buildDelayedMessageKey(message.GroupID, message.DeliverTime, messageID)

	// 使用批量写入保证原子性
	batch := s.db.NewBatch()
	defer batch.Close()

	// 1. 存储消息数据（使用新的有序Key）
	if err := batch.Set([]byte(dataKey), data, pebble.Sync); err != nil {
		return fmt.Errorf("failed to set delayed message data: %w", err)
	}

	// 2. 创建投递时间索引（保留用于兼容性）
	deliverKey := s.buildDeliverIndexKey(message.DeliverTime, messageID)
	if err := batch.Set([]byte(deliverKey), []byte(messageID), pebble.Sync); err != nil {
		return fmt.Errorf("failed to set deliver index: %w", err)
	}

	// 3. 创建状态索引
	statusKey := s.buildStatusIndexKey(message.Status, messageID)
	if err := batch.Set([]byte(statusKey), []byte(messageID), pebble.Sync); err != nil {
		return fmt.Errorf("failed to set status index: %w", err)
	}

	// 提交批量操作
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}

func (s *PebbleDelayedMessageStorage) Get(messageID string) (*DelayedMessage, error) {
	dataKey := DelayedMessagePrefix + messageID
	value, closer, err := s.db.Get([]byte(dataKey))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get delayed message: %w", err)
	}
	defer closer.Close()

	var message DelayedMessage
	if err := json.Unmarshal(value, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal delayed message: %w", err)
	}

	return &message, nil
}

func (s *PebbleDelayedMessageStorage) Update(messageID string, message *DelayedMessage) error {
	// 先获取旧消息以删除旧索引
	oldMessage, err := s.Get(messageID)
	if err != nil {
		return fmt.Errorf("failed to get old message: %w", err)
	}
	if oldMessage == nil {
		return fmt.Errorf("message not found: %s", messageID)
	}

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal delayed message: %w", err)
	}

	// 使用批量写入保证原子性
	batch := s.db.NewBatch()
	defer batch.Close()

	// 1. 更新消息数据
	dataKey := DelayedMessagePrefix + messageID
	if err := batch.Set([]byte(dataKey), data, pebble.Sync); err != nil {
		return fmt.Errorf("failed to update delayed message data: %w", err)
	}

	// 2. 删除旧的投递时间索引（如果时间发生变化）
	if oldMessage.DeliverTime != message.DeliverTime {
		oldDeliverKey := s.buildDeliverIndexKey(oldMessage.DeliverTime, messageID)
		if err := batch.Delete([]byte(oldDeliverKey), pebble.Sync); err != nil {
			return fmt.Errorf("failed to delete old deliver index: %w", err)
		}

		// 创建新的投递时间索引
		newDeliverKey := s.buildDeliverIndexKey(message.DeliverTime, messageID)
		if err := batch.Set([]byte(newDeliverKey), []byte(messageID), pebble.Sync); err != nil {
			return fmt.Errorf("failed to set new deliver index: %w", err)
		}
	}

	// 3. 删除旧的状态索引（如果状态发生变化）
	if oldMessage.Status != message.Status {
		oldStatusKey := s.buildStatusIndexKey(oldMessage.Status, messageID)
		if err := batch.Delete([]byte(oldStatusKey), pebble.Sync); err != nil {
			return fmt.Errorf("failed to delete old status index: %w", err)
		}

		// 创建新的状态索引
		newStatusKey := s.buildStatusIndexKey(message.Status, messageID)
		if err := batch.Set([]byte(newStatusKey), []byte(messageID), pebble.Sync); err != nil {
			return fmt.Errorf("failed to set new status index: %w", err)
		}
	}

	// 提交批量操作
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}

func (s *PebbleDelayedMessageStorage) Delete(messageID string) error {
	// 先获取消息以删除索引
	message, err := s.Get(messageID)
	if err != nil {
		return fmt.Errorf("failed to get message for deletion: %w", err)
	}
	if message == nil {
		return nil // 消息不存在，认为删除成功
	}

	// 使用批量写入保证原子性
	batch := s.db.NewBatch()
	defer batch.Close()

	// 1. 删除消息数据
	dataKey := DelayedMessagePrefix + messageID
	if err := batch.Delete([]byte(dataKey), pebble.Sync); err != nil {
		return fmt.Errorf("failed to delete delayed message data: %w", err)
	}

	// 2. 删除投递时间索引
	deliverKey := s.buildDeliverIndexKey(message.DeliverTime, messageID)
	if err := batch.Delete([]byte(deliverKey), pebble.Sync); err != nil {
		return fmt.Errorf("failed to delete deliver index: %w", err)
	}

	// 3. 删除状态索引
	statusKey := s.buildStatusIndexKey(message.Status, messageID)
	if err := batch.Delete([]byte(statusKey), pebble.Sync); err != nil {
		return fmt.Errorf("failed to delete status index: %w", err)
	}

	// 提交批量操作
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}

// ScanPendingMessages scan pending messages in a time range
func (s *PebbleDelayedMessageStorage) ScanPendingMessages(groupID uint64, fromTime, toTime time.Time, limit int) ([]*DelayedMessage, error) {
	var messages []*DelayedMessage

	startKey := fmt.Sprintf("%s%016d_%016d_", DelayedMessagePrefix, groupID, fromTime.UnixMilli())
	endKey := fmt.Sprintf("%s%016d_%016d_", DelayedMessagePrefix, groupID, toTime.UnixMilli())

	iter := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(startKey),
		UpperBound: []byte(endKey + "~"),
	})
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid() && count < limit; iter.Next() {
		var message DelayedMessage
		if err := json.Unmarshal(iter.Value(), &message); err != nil {
			continue
		}

		if message.Status == StatusPending && message.GroupID == groupID {
			messages = append(messages, &message)
			count++
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("failed to scan pending messages for group %d: %w", groupID, err)
	}

	return messages, nil
}

func (s *PebbleDelayedMessageStorage) getMessagesByDeliverTime(startTime, endTime int64, status DelayedMessageStatus) ([]*DelayedMessage, error) {
	var messages []*DelayedMessage

	// 构建时间范围的前缀
	startKey := DeliverIndexPrefix + fmt.Sprintf("%020d:", startTime)
	endKey := DeliverIndexPrefix + fmt.Sprintf("%020d:", endTime+1)

	iter := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(startKey),
		UpperBound: []byte(endKey),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		messageID := string(iter.Value())

		message, err := s.Get(messageID)
		if err != nil {
			continue // 跳过错误的消息
		}
		if message == nil {
			continue // 消息不存在
		}

		// 过滤状态
		if status != -1 && message.Status != status {
			continue
		}

		messages = append(messages, message)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("failed to iterate deliver index: %w", err)
	}

	return messages, nil
}

func (s *PebbleDelayedMessageStorage) CleanupDelivered(beforeTime time.Time) error {
	// 获取已投递的消息
	deliveredMessages, err := s.getMessagesByStatus(StatusDelivered)
	if err != nil {
		return fmt.Errorf("failed to get delivered messages: %w", err)
	}

	// 删除创建时间在beforeTime之前的已投递消息
	batch := s.db.NewBatch()
	defer batch.Close()

	deletedCount := 0
	for _, message := range deliveredMessages {
		if message.CreateTime < beforeTime.UnixMilli() {
			// 删除消息数据
			dataKey := DelayedMessagePrefix + message.ID
			if err := batch.Delete([]byte(dataKey), pebble.Sync); err != nil {
				continue
			}

			// 删除投递时间索引
			deliverKey := s.buildDeliverIndexKey(message.DeliverTime, message.ID)
			if err := batch.Delete([]byte(deliverKey), pebble.Sync); err != nil {
				continue
			}

			// 删除状态索引
			statusKey := s.buildStatusIndexKey(message.Status, message.ID)
			if err := batch.Delete([]byte(statusKey), pebble.Sync); err != nil {
				continue
			}

			deletedCount++
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit cleanup batch: %w", err)
	}

	return nil
}

func (s *PebbleDelayedMessageStorage) getMessagesByStatus(status DelayedMessageStatus) ([]*DelayedMessage, error) {
	var messages []*DelayedMessage

	// 构建状态索引前缀
	prefix := StatusIndexPrefix + strconv.Itoa(int(status)) + ":"

	iter := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		messageID := string(iter.Value())

		message, err := s.Get(messageID)
		if err != nil {
			continue // 跳过错误的消息
		}
		if message == nil {
			continue // 消息不存在
		}

		messages = append(messages, message)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("failed to iterate status index: %w", err)
	}

	return messages, nil
}

func (s *PebbleDelayedMessageStorage) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	// 统计各状态的消息数量
	for status := StatusPending; status <= StatusCanceled; status++ {
		messages, err := s.getMessagesByStatus(status)
		if err != nil {
			continue
		}

		statusName := map[DelayedMessageStatus]string{
			StatusPending:   "pending",
			StatusDelivered: "delivered",
			StatusFailed:    "failed",
			StatusCanceled:  "canceled",
		}[status]

		stats[statusName+"_count"] = len(messages)
	}

	return stats
}

func (s *PebbleDelayedMessageStorage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *PebbleDelayedMessageStorage) buildDeliverIndexKey(deliverTimeMilli int64, messageID string) string {
	return fmt.Sprintf("%s%020d:%s", DeliverIndexPrefix, deliverTimeMilli, messageID)
}

func (s *PebbleDelayedMessageStorage) buildStatusIndexKey(status DelayedMessageStatus, messageID string) string {
	return fmt.Sprintf("%s%d:%s", StatusIndexPrefix, int(status), messageID)
}

func (s *PebbleDelayedMessageStorage) UpdateBatch(updates map[string]*DelayedMessage) error {
	if len(updates) == 0 {
		return nil
	}
	batch := s.db.NewBatch()
	defer batch.Close()

	for messageID, message := range updates {
		// 获取旧消息以处理索引更新
		oldMessage, err := s.Get(messageID)
		if err != nil {
			return fmt.Errorf("failed to get old message for batch update: %w", err)
		}
		if oldMessage == nil {
			return fmt.Errorf("message not found: %s", messageID)
		}

		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("failed to marshal delayed message: %w", err)
		}

		// 与现有 Update 保持一致的数据键策略
		dataKey := DelayedMessagePrefix + messageID
		if err := batch.Set([]byte(dataKey), data, pebble.Sync); err != nil {
			return fmt.Errorf("failed to update delayed message data: %w", err)
		}

		// 投递时间索引变更
		if oldMessage.DeliverTime != message.DeliverTime {
			oldDeliverKey := s.buildDeliverIndexKey(oldMessage.DeliverTime, messageID)
			if err := batch.Delete([]byte(oldDeliverKey), pebble.Sync); err != nil {
				return fmt.Errorf("failed to delete old deliver index: %w", err)
			}
			newDeliverKey := s.buildDeliverIndexKey(message.DeliverTime, messageID)
			if err := batch.Set([]byte(newDeliverKey), []byte(messageID), pebble.Sync); err != nil {
				return fmt.Errorf("failed to set new deliver index: %w", err)
			}
		}

		// 状态索引变更
		if oldMessage.Status != message.Status {
			oldStatusKey := s.buildStatusIndexKey(oldMessage.Status, messageID)
			if err := batch.Delete([]byte(oldStatusKey), pebble.Sync); err != nil {
				return fmt.Errorf("failed to delete old status index: %w", err)
			}
			newStatusKey := s.buildStatusIndexKey(message.Status, messageID)
			if err := batch.Set([]byte(newStatusKey), []byte(messageID), pebble.Sync); err != nil {
				return fmt.Errorf("failed to set new status index: %w", err)
			}
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}
	return nil
}

func (s *PebbleDelayedMessageStorage) DeleteBatch(messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, messageID := range messageIDs {
		message, err := s.Get(messageID)
		if err != nil {
			return fmt.Errorf("failed to get message for batch deletion: %w", err)
		}
		if message == nil {
			// 不存在则忽略
			continue
		}
		// 与现有 Delete 保持一致的数据键策略
		dataKey := DelayedMessagePrefix + messageID
		if err := batch.Delete([]byte(dataKey), pebble.Sync); err != nil {
			return fmt.Errorf("failed to delete delayed message data: %w", err)
		}
		deliverKey := s.buildDeliverIndexKey(message.DeliverTime, messageID)
		if err := batch.Delete([]byte(deliverKey), pebble.Sync); err != nil {
			return fmt.Errorf("failed to delete deliver index: %w", err)
		}
		statusKey := s.buildStatusIndexKey(message.Status, messageID)
		if err := batch.Delete([]byte(statusKey), pebble.Sync); err != nil {
			return fmt.Errorf("failed to delete status index: %w", err)
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}
	return nil
}
