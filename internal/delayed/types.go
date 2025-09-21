package delayed

import (
	"time"
)

// DelayedMessageStatus 延迟消息状态
type DelayedMessageStatus int32

const (
	StatusPending   DelayedMessageStatus = 0 // 等待投递
	StatusDelivered DelayedMessageStatus = 1 // 已投递
	StatusFailed    DelayedMessageStatus = 2 // 投递失败
	StatusCanceled  DelayedMessageStatus = 3 // 已取消
)

// DelayLevel 延迟级别定义
type DelayLevel int32

const (
	DelayLevel1s  DelayLevel = 1  // 1秒
	DelayLevel5s  DelayLevel = 2  // 5秒
	DelayLevel10s DelayLevel = 3  // 10秒
	DelayLevel30s DelayLevel = 4  // 30秒
	DelayLevel1m  DelayLevel = 5  // 1分钟
	DelayLevel2m  DelayLevel = 6  // 2分钟
	DelayLevel3m  DelayLevel = 7  // 3分钟
	DelayLevel4m  DelayLevel = 8  // 4分钟
	DelayLevel5m  DelayLevel = 9  // 5分钟
	DelayLevel6m  DelayLevel = 10 // 6分钟
	DelayLevel7m  DelayLevel = 11 // 7分钟
	DelayLevel8m  DelayLevel = 12 // 8分钟
	DelayLevel9m  DelayLevel = 13 // 9分钟
	DelayLevel10m DelayLevel = 14 // 10分钟
	DelayLevel20m DelayLevel = 15 // 20分钟
	DelayLevel30m DelayLevel = 16 // 30分钟
	DelayLevel1h  DelayLevel = 17 // 1小时
	DelayLevel2h  DelayLevel = 18 // 2小时
)

// DelayLevelDurations 延迟级别对应的时间
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

// GetDelayDuration 获取延迟级别对应的时间
func GetDelayDuration(level DelayLevel) time.Duration {
	if duration, exists := DelayLevelDurations[level]; exists {
		return duration
	}
	return 0
}

// DelayedMessage 延迟消息结构
type DelayedMessage struct {
	ID          string               `json:"id"`
	Topic       string               `json:"topic"`
	Partition   int32                `json:"partition"`
	Key         []byte               `json:"key,omitempty"`
	Value       []byte               `json:"value"`
	Headers     map[string]string    `json:"headers,omitempty"`
	CreateTime  int64                `json:"create_time"`  // 创建时间戳(毫秒)
	DeliverTime int64                `json:"deliver_time"` // 投递时间戳(毫秒)
	Status      DelayedMessageStatus `json:"status"`
	RetryCount  int                  `json:"retry_count"`
	MaxRetries  int                  `json:"max_retries"`
	LastError   string               `json:"last_error,omitempty"`
	UpdateTime  int64                `json:"update_time"` // 最后更新时间
}

// DelayedProduceRequest 延迟消息生产请求
type DelayedProduceRequest struct {
	Topic       string            `json:"topic"`
	Partition   int32             `json:"partition"`
	Key         []byte            `json:"key,omitempty"`
	Value       []byte            `json:"value"`
	Headers     map[string]string `json:"headers,omitempty"`
	DelayLevel  int32             `json:"delay_level,omitempty"`  // 延迟级别
	DelayTime   int64             `json:"delay_time,omitempty"`   // 延迟时长(毫秒)
	DeliverTime int64             `json:"deliver_time,omitempty"` // 投递时间戳(毫秒)
}

// DelayedProduceResponse 延迟消息生产响应
type DelayedProduceResponse struct {
	MessageID   string `json:"message_id"`
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	DeliverTime int64  `json:"deliver_time"`
	ErrorCode   int16  `json:"error_code"`
	Error       string `json:"error,omitempty"`
}

// IsExpired 检查消息是否已到期
func (dm *DelayedMessage) IsExpired() bool {
	return time.Now().UnixMilli() >= dm.DeliverTime
}

// CanRetry 检查消息是否可以重试
func (dm *DelayedMessage) CanRetry() bool {
	return dm.RetryCount < dm.MaxRetries
}

// MarkDelivered 标记消息为已投递
func (dm *DelayedMessage) MarkDelivered() {
	dm.Status = StatusDelivered
	dm.UpdateTime = time.Now().UnixMilli()
}

// MarkFailed 标记消息投递失败
func (dm *DelayedMessage) MarkFailed(err string) {
	dm.Status = StatusFailed
	dm.LastError = err
	dm.RetryCount++
	dm.UpdateTime = time.Now().UnixMilli()
}

// CalculateDeliverTime 计算投递时间
func CalculateDeliverTime(delayLevel DelayLevel, delayTime int64, deliverTime int64) int64 {
	now := time.Now().UnixMilli()

	// 优先使用明确指定的投递时间
	if deliverTime > 0 {
		return deliverTime
	}

	// 其次使用延迟时长
	if delayTime > 0 {
		return now + delayTime
	}

	// 最后使用延迟级别
	if duration := GetDelayDuration(delayLevel); duration > 0 {
		return now + duration.Milliseconds()
	}

	// 默认延迟1秒
	return now + 1000
}

// Constants for time wheel
const (
	// TimeWheelTickMs 时间轮刻度（毫秒）
	TimeWheelTickMs = 1000 // 1秒

	// TimeWheelSize 时间轮大小
	TimeWheelSize = 3600 // 1小时，3600秒

	// MaxDelayTime 最大延迟时间（40天）
	MaxDelayTime = 40 * 24 * time.Hour
)
