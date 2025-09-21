package delayed

import (
	"context"
	"sync"
	"time"
)

// DelayedMessageStatus delayed message status
type DelayedMessageStatus int32

const (
	StatusPending   DelayedMessageStatus = 0
	StatusDelivered DelayedMessageStatus = 1
	StatusFailed    DelayedMessageStatus = 2
	StatusCanceled  DelayedMessageStatus = 3
)

// MessagePriority 消息优先级定义
type MessagePriority int32

const (
	PriorityLow    MessagePriority = 0 // 低优先级，可丢弃
	PriorityNormal MessagePriority = 1 // 普通优先级
	PriorityHigh   MessagePriority = 2 // 高优先级，不可丢弃
	PriorityCritical MessagePriority = 3 // 关键优先级，绝不丢弃
)

// DelayLevel delay level definition
type DelayLevel int32

const (
	DelayLevel1s  DelayLevel = 1
	DelayLevel5s  DelayLevel = 2
	DelayLevel10s DelayLevel = 3
	DelayLevel30s DelayLevel = 4
	DelayLevel1m  DelayLevel = 5
	DelayLevel2m  DelayLevel = 6
	DelayLevel3m  DelayLevel = 7
	DelayLevel4m  DelayLevel = 8
	DelayLevel5m  DelayLevel = 9
	DelayLevel6m  DelayLevel = 10
	DelayLevel7m  DelayLevel = 11
	DelayLevel8m  DelayLevel = 12
	DelayLevel9m  DelayLevel = 13
	DelayLevel10m DelayLevel = 14
	DelayLevel20m DelayLevel = 15
	DelayLevel30m DelayLevel = 16
	DelayLevel1h  DelayLevel = 17
	DelayLevel2h  DelayLevel = 18
)

// DelayLevelDurations delay level duration mappings
var DelayLevelDurations = map[DelayLevel]time.Duration{
	DelayLevel1s:  1 * time.Second,
	DelayLevel5s:  5 * time.Second,
	DelayLevel10s: 10 * time.Second,
	DelayLevel30s: 30 * time.Second,
	DelayLevel1m:  1 * time.Minute,
	DelayLevel2m:  2 * time.Minute,
	DelayLevel3m:  3 * time.Minute,
	DelayLevel4m:  4 * time.Minute,
	DelayLevel5m:  5 * time.Minute,
	DelayLevel6m:  6 * time.Minute,
	DelayLevel7m:  7 * time.Minute,
	DelayLevel8m:  8 * time.Minute,
	DelayLevel9m:  9 * time.Minute,
	DelayLevel10m: 10 * time.Minute,
	DelayLevel20m: 20 * time.Minute,
	DelayLevel30m: 30 * time.Minute,
	DelayLevel1h:  1 * time.Hour,
	DelayLevel2h:  2 * time.Hour,
}

func GetDelayDuration(level DelayLevel) time.Duration {
	if duration, exists := DelayLevelDurations[level]; exists {
		return duration
	}
	return 0
}

// DelayedMessage represents a delayed message
type DelayedMessage struct {
	ID          string               `json:"id"`
	GroupID     uint64               `json:"group_id"`     // Raft组ID，用于分区隔离
	Topic       string               `json:"topic"`
	Partition   int32                `json:"partition"`
	Key         []byte               `json:"key,omitempty"`
	Value       []byte               `json:"value"`
	Headers     map[string]string    `json:"headers,omitempty"`
	CreateTime  int64                `json:"create_time"`
	DeliverTime int64                `json:"deliver_time"`
	Status      DelayedMessageStatus `json:"status"`
	RetryCount  int                  `json:"retry_count"`
	MaxRetries  int                  `json:"max_retries"`
	LastError   string               `json:"last_error,omitempty"`
	UpdateTime  int64                `json:"update_time"`
	Priority    MessagePriority      `json:"priority"`     // 消息优先级
	Droppable   bool                 `json:"droppable"`    // 是否可丢弃
}

// DelayedProduceRequest delayed message produce request
type DelayedProduceRequest struct {
	Topic       string            `json:"topic"`
	Partition   int32             `json:"partition"`
	Key         []byte            `json:"key,omitempty"`
	Value       []byte            `json:"value"`
	Headers     map[string]string `json:"headers,omitempty"`
	DelayLevel  int32             `json:"delay_level,omitempty"`
	DelayTime   int64             `json:"delay_time,omitempty"`
	DeliverTime int64             `json:"deliver_time,omitempty"`
}

// DelayedProduceResponse delayed message produce response
type DelayedProduceResponse struct {
	MessageID   string `json:"message_id"`
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	DeliverTime int64  `json:"deliver_time"`
	ErrorCode   int16  `json:"error_code"`
	Error       string `json:"error,omitempty"`
}

func (dm *DelayedMessage) IsExpired() bool {
	return time.Now().UnixMilli() >= dm.DeliverTime
}

func (dm *DelayedMessage) CanRetry() bool {
	return dm.RetryCount < dm.MaxRetries
}

func (dm *DelayedMessage) MarkDelivered() {
	dm.Status = StatusDelivered
	dm.UpdateTime = time.Now().UnixMilli()
}

func (dm *DelayedMessage) MarkFailed(err string) {
	dm.Status = StatusFailed
	dm.LastError = err
	dm.RetryCount++
	dm.UpdateTime = time.Now().UnixMilli()
}

func (dm *DelayedMessage) MarkCanceled() {
	dm.Status = StatusCanceled
	dm.UpdateTime = time.Now().UnixMilli()
}

func CalculateDeliverTime(delayLevel DelayLevel, delayTime int64, deliverTime int64) int64 {
	now := time.Now().UnixMilli()

	if deliverTime > 0 {
		return deliverTime
	}

	if delayTime > 0 {
		return now + delayTime
	}

	if duration := GetDelayDuration(delayLevel); duration > 0 {
		return now + duration.Milliseconds()
	}

	return now + 1000
}

const (
	// TimeWheel 配置常量 - 60个槽，每秒一个tick，支持60秒内的任务
	TimeWheelTickMs = 1000 // 1秒精度
	TimeWheelSize   = 60   // 60个槽位，支持60秒内的任务
	MaxDelayTime    = 40 * 24 * time.Hour
)

// MessageDeliveryCallback message delivery callback function type
type MessageDeliveryCallback func(topic string, partition int32, key, value []byte) error

// RaftProposer 接口用于向Raft组提议延迟消息命令
type RaftProposer interface {
	SyncPropose(ctx context.Context, groupID uint64, data []byte) (interface{}, error)
}

// DelayedMessageManager delayed message manager
type DelayedMessageManager struct {
	storage          DelayedMessageStorage
	timeWheel        *OptimizedTimeWheel
	deliveryCallback MessageDeliveryCallback
	raftProposer     RaftProposer // 添加 Raft 提议器字段

	maxRetries      int32
	cleanupInterval time.Duration

	running       bool
	stopCh        chan struct{}
	cleanupTicker *time.Ticker
	
	// 内存态管理：标记消息是否已在时间轮中
	inTimeWheelMap  map[string]bool
	timeWheelMutex  sync.RWMutex
}

// DelayedMessageStats delayed message statistics
type DelayedMessageStats struct {
	TotalMessages     int64 `json:"total_messages"`
	PendingMessages   int64 `json:"pending_messages"`
	DeliveredMessages int64 `json:"delivered_messages"`
	FailedMessages    int64 `json:"failed_messages"`
	CancelledMessages int64 `json:"cancelled_messages"`
}

// DelayedMessageManagerConfig delayed message manager configuration
type DelayedMessageManagerConfig struct {
	Storage         DelayedMessageStorage
	MaxRetries      int32
	CleanupInterval time.Duration
}

const (
	CmdUpdateDelayedMessage = "update_delayed_message"
)

type PartitionCommand struct {
	Type      string      `json:"type"`
	MessageID string      `json:"message_id,omitempty"`
	Data      interface{} `json:"data,omitempty"`
}
