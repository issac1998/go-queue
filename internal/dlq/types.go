package dlq

import (
	"time"
)

// DeadLetterMessage represents a message that failed to be processed
type DeadLetterMessage struct {
	OriginalTopic     string            `json:"original_topic"`
	OriginalPartition int32             `json:"original_partition"`
	OriginalOffset    int64             `json:"original_offset"`
	OriginalKey       []byte            `json:"original_key,omitempty"`
	OriginalValue     []byte            `json:"original_value"`
	OriginalTimestamp time.Time         `json:"original_timestamp"`
	
	// Dead letter queue info
	DLQTopic          string            `json:"dlq_topic"`
	DLQPartition      int32             `json:"dlq_partition"`
	DLQOffset         int64             `json:"dlq_offset"`
	DLQTimestamp      time.Time         `json:"dlq_timestamp"`
	
	// Failure info
	ConsumerGroup     string            `json:"consumer_group"`
	ConsumerID        string            `json:"consumer_id"`
	FailureReason     string            `json:"failure_reason"`
	RetryCount        int               `json:"retry_count"`
	MaxRetries        int               `json:"max_retries"`
	FirstFailureTime  time.Time         `json:"first_failure_time"`
	LastFailureTime   time.Time         `json:"last_failure_time"`
	
	// Additional metadata
	Headers           map[string]string `json:"headers,omitempty"`
}

// RetryPolicy defines retry behavior for failed messages
type RetryPolicy struct {
	MaxRetries      int           `json:"max_retries"`
	InitialDelay    time.Duration `json:"initial_delay"`
	MaxDelay        time.Duration `json:"max_delay"`
	BackoffFactor   float64       `json:"backoff_factor"`
	RetryableErrors []string      `json:"retryable_errors,omitempty"`
}

// DefaultRetryPolicy returns a sensible default retry policy
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxRetries:    3,
		InitialDelay:  1 * time.Second,
		MaxDelay:      30 * time.Second,
		BackoffFactor: 2.0,
	}
}

// DLQConfig configuration for dead letter queue
type DLQConfig struct {
	Enabled         bool          `json:"enabled"`
	TopicSuffix     string        `json:"topic_suffix"`
	RetryPolicy     *RetryPolicy  `json:"retry_policy"`
	RetentionPeriod time.Duration `json:"retention_period"`
	CleanupInterval time.Duration `json:"cleanup_interval"`
}

// DefaultDLQConfig returns default DLQ configuration
func DefaultDLQConfig() *DLQConfig {
	return &DLQConfig{
		Enabled:         true,
		TopicSuffix:     ".dlq",
		RetryPolicy:     DefaultRetryPolicy(),
		RetentionPeriod: 7 * 24 * time.Hour, // 7 days
		CleanupInterval: 1 * time.Hour,
	}
}

// FailureInfo contains information about a message processing failure
type FailureInfo struct {
	Error         error     `json:"-"`
	ErrorMessage  string    `json:"error_message"`
	FailureTime   time.Time `json:"failure_time"`
	ConsumerGroup string    `json:"consumer_group"`
	ConsumerID    string    `json:"consumer_id"`
}

// RetryAttempt represents a single retry attempt
type RetryAttempt struct {
	AttemptNumber int       `json:"attempt_number"`
	AttemptTime   time.Time `json:"attempt_time"`
	Error         string    `json:"error,omitempty"`
	NextRetryTime time.Time `json:"next_retry_time,omitempty"`
}

// MessageRetryState tracks retry state for a message
type MessageRetryState struct {
	OriginalTopic     string          `json:"original_topic"`
	OriginalPartition int32           `json:"original_partition"`
	OriginalOffset    int64           `json:"original_offset"`
	ConsumerGroup     string          `json:"consumer_group"`
	RetryCount        int             `json:"retry_count"`
	MaxRetries        int             `json:"max_retries"`
	FirstFailureTime  time.Time       `json:"first_failure_time"`
	LastFailureTime   time.Time       `json:"last_failure_time"`
	NextRetryTime     time.Time       `json:"next_retry_time"`
	RetryAttempts     []RetryAttempt  `json:"retry_attempts"`
	RetryPolicy       *RetryPolicy    `json:"retry_policy"`
}

// ShouldRetry determines if a message should be retried
func (mrs *MessageRetryState) ShouldRetry() bool {
	if mrs.RetryCount >= mrs.MaxRetries {
		return false
	}
	
	// Always allow retry if we haven't exceeded max retries
	// The timing check should be done separately when actually scheduling the retry
	return true
}

// CalculateNextRetryTime calculates when the next retry should happen
func (mrs *MessageRetryState) CalculateNextRetryTime() time.Time {
	if mrs.RetryPolicy == nil {
		mrs.RetryPolicy = DefaultRetryPolicy()
	}
	
	delay := mrs.RetryPolicy.InitialDelay
	for i := 0; i < mrs.RetryCount; i++ {
		delay = time.Duration(float64(delay) * mrs.RetryPolicy.BackoffFactor)
		if delay > mrs.RetryPolicy.MaxDelay {
			delay = mrs.RetryPolicy.MaxDelay
			break
		}
	}
	
	return time.Now().Add(delay)
}

// DLQStats provides statistics about dead letter queue
type DLQStats struct {
	TotalMessages     int64     `json:"total_messages"`
	MessagesLastHour  int64     `json:"messages_last_hour"`
	MessagesLastDay   int64     `json:"messages_last_day"`
	TopFailureReasons []string  `json:"top_failure_reasons"`
	OldestMessage     time.Time `json:"oldest_message"`
	NewestMessage     time.Time `json:"newest_message"`
}