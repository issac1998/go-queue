package delayed

import (
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

// DelayedMessage delayed message structure
type DelayedMessage struct {
	ID          string               `json:"id"`
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
	TimeWheelTickMs = 1000
	TimeWheelSize   = 3600
	MaxDelayTime    = 40 * 24 * time.Hour
)
