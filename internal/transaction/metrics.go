package transaction

import (
	"sync"
	"sync/atomic"
	"time"
)

// TransactionMetrics transaction monitoring metrics
type TransactionMetrics struct {
	totalChecks        int64
	successfulChecks   int64
	failedChecks       int64
	unknownChecks      int64
	
	commitTransactions   int64
	rollbackTransactions int64
	timeoutTransactions  int64
	
	totalRetries       int64
	maxRetryReached    int64
	
	mu                 sync.RWMutex
	checkLatencies     []time.Duration
	avgCheckLatency    time.Duration
	maxCheckLatency    time.Duration
	minCheckLatency    time.Duration
	
	errorCounts        map[string]int64
	lastError          string
	lastErrorTime      time.Time
	
	checksLastHour     int64
	checksLastDay      int64
	lastHourReset      time.Time
	lastDayReset       time.Time
}

// NewTransactionMetrics creates a new transaction metrics collector
func NewTransactionMetrics() *TransactionMetrics {
	now := time.Now()
	return &TransactionMetrics{
		errorCounts:     make(map[string]int64),
		checkLatencies:  make([]time.Duration, 0, 1000),
		minCheckLatency: time.Hour,
		lastHourReset:   now,
		lastDayReset:    now,
	}
}

func (tm *TransactionMetrics) RecordTransactionCheck(state TransactionState, latency time.Duration, err error) {
	atomic.AddInt64(&tm.totalChecks, 1)
	tm.updateTimeWindowCounters()
	
	switch state {
	case StateCommit:
		atomic.AddInt64(&tm.successfulChecks, 1)
	case StateRollback:
		atomic.AddInt64(&tm.successfulChecks, 1)
	case StateUnknown:
		atomic.AddInt64(&tm.unknownChecks, 1)
	}
	
	if err != nil {
		atomic.AddInt64(&tm.failedChecks, 1)
		tm.recordError(err.Error())
	}
	
	tm.recordLatency(latency)
}

func (tm *TransactionMetrics) RecordTransactionCommit() {
	atomic.AddInt64(&tm.commitTransactions, 1)
}

func (tm *TransactionMetrics) RecordTransactionRollback() {
	atomic.AddInt64(&tm.rollbackTransactions, 1)
}

func (tm *TransactionMetrics) RecordTransactionTimeout() {
	atomic.AddInt64(&tm.timeoutTransactions, 1)
}

func (tm *TransactionMetrics) RecordRetry() {
	atomic.AddInt64(&tm.totalRetries, 1)
}

// RecordMaxRetryReached 记录达到最大重试次数
func (tm *TransactionMetrics) RecordMaxRetryReached() {
	atomic.AddInt64(&tm.maxRetryReached, 1)
}

// recordError 记录错误信息
func (tm *TransactionMetrics) recordError(errMsg string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	
	tm.errorCounts[errMsg]++
	tm.lastError = errMsg
	tm.lastErrorTime = time.Now()
}

// recordLatency 记录延迟信息
func (tm *TransactionMetrics) recordLatency(latency time.Duration) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	
	// 更新最大最小延迟
	if latency > tm.maxCheckLatency {
		tm.maxCheckLatency = latency
	}
	if latency < tm.minCheckLatency {
		tm.minCheckLatency = latency
	}
	
	// 记录延迟样本（保持最近1000个样本）
	if len(tm.checkLatencies) >= 1000 {
		// 移除最老的样本
		tm.checkLatencies = tm.checkLatencies[1:]
	}
	tm.checkLatencies = append(tm.checkLatencies, latency)
	
	// 计算平均延迟
	if len(tm.checkLatencies) > 0 {
		var total time.Duration
		for _, lat := range tm.checkLatencies {
			total += lat
		}
		tm.avgCheckLatency = total / time.Duration(len(tm.checkLatencies))
	}
}

// updateTimeWindowCounters 更新时间窗口计数器
func (tm *TransactionMetrics) updateTimeWindowCounters() {
	now := time.Now()
	
	// 重置小时计数器
	if now.Sub(tm.lastHourReset) >= time.Hour {
		atomic.StoreInt64(&tm.checksLastHour, 0)
		tm.lastHourReset = now
	}
	
	// 重置天计数器
	if now.Sub(tm.lastDayReset) >= 24*time.Hour {
		atomic.StoreInt64(&tm.checksLastDay, 0)
		tm.lastDayReset = now
	}
	
	atomic.AddInt64(&tm.checksLastHour, 1)
	atomic.AddInt64(&tm.checksLastDay, 1)
}

// GetMetrics 获取所有指标
func (tm *TransactionMetrics) GetMetrics() TransactionMetricsSnapshot {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	
	// 复制错误计数
	errorCounts := make(map[string]int64)
	for k, v := range tm.errorCounts {
		errorCounts[k] = v
	}
	
	return TransactionMetricsSnapshot{
		// 基础计数器
		TotalChecks:      atomic.LoadInt64(&tm.totalChecks),
		SuccessfulChecks: atomic.LoadInt64(&tm.successfulChecks),
		FailedChecks:     atomic.LoadInt64(&tm.failedChecks),
		UnknownChecks:    atomic.LoadInt64(&tm.unknownChecks),
		
		// 事务状态计数器
		CommitTransactions:   atomic.LoadInt64(&tm.commitTransactions),
		RollbackTransactions: atomic.LoadInt64(&tm.rollbackTransactions),
		TimeoutTransactions:  atomic.LoadInt64(&tm.timeoutTransactions),
		
		// 重试相关指标
		TotalRetries:    atomic.LoadInt64(&tm.totalRetries),
		MaxRetryReached: atomic.LoadInt64(&tm.maxRetryReached),
		
		// 性能指标
		AvgCheckLatency: tm.avgCheckLatency,
		MaxCheckLatency: tm.maxCheckLatency,
		MinCheckLatency: tm.minCheckLatency,
		
		// 错误统计
		ErrorCounts:   errorCounts,
		LastError:     tm.lastError,
		LastErrorTime: tm.lastErrorTime,
		
		// 时间窗口统计
		ChecksLastHour: atomic.LoadInt64(&tm.checksLastHour),
		ChecksLastDay:  atomic.LoadInt64(&tm.checksLastDay),
		
		// 计算成功率
		SuccessRate: tm.calculateSuccessRate(),
		
		// 计算重试率
		RetryRate: tm.calculateRetryRate(),
	}
}

// calculateSuccessRate 计算成功率
func (tm *TransactionMetrics) calculateSuccessRate() float64 {
	total := atomic.LoadInt64(&tm.totalChecks)
	if total == 0 {
		return 0.0
	}
	
	successful := atomic.LoadInt64(&tm.successfulChecks)
	return float64(successful) / float64(total) * 100.0
}

// calculateRetryRate 计算重试率
func (tm *TransactionMetrics) calculateRetryRate() float64 {
	total := atomic.LoadInt64(&tm.totalChecks)
	if total == 0 {
		return 0.0
	}
	
	retries := atomic.LoadInt64(&tm.totalRetries)
	return float64(retries) / float64(total) * 100.0
}

// TransactionMetricsSnapshot 事务指标快照
type TransactionMetricsSnapshot struct {
	// 基础计数器
	TotalChecks      int64 `json:"total_checks"`
	SuccessfulChecks int64 `json:"successful_checks"`
	FailedChecks     int64 `json:"failed_checks"`
	UnknownChecks    int64 `json:"unknown_checks"`
	
	// 事务状态计数器
	CommitTransactions   int64 `json:"commit_transactions"`
	RollbackTransactions int64 `json:"rollback_transactions"`
	TimeoutTransactions  int64 `json:"timeout_transactions"`
	
	// 重试相关指标
	TotalRetries    int64 `json:"total_retries"`
	MaxRetryReached int64 `json:"max_retry_reached"`
	
	// 性能指标
	AvgCheckLatency time.Duration `json:"avg_check_latency"`
	MaxCheckLatency time.Duration `json:"max_check_latency"`
	MinCheckLatency time.Duration `json:"min_check_latency"`
	
	// 错误统计
	ErrorCounts   map[string]int64 `json:"error_counts"`
	LastError     string           `json:"last_error"`
	LastErrorTime time.Time        `json:"last_error_time"`
	
	// 时间窗口统计
	ChecksLastHour int64 `json:"checks_last_hour"`
	ChecksLastDay  int64 `json:"checks_last_day"`
	
	// 计算指标
	SuccessRate float64 `json:"success_rate"`
	RetryRate   float64 `json:"retry_rate"`
}

func (tm *TransactionMetrics) GetTopErrors(limit int) []ErrorCount {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	
	type errorItem struct {
		message string
		count   int64
	}
	
	var errors []errorItem
	for msg, count := range tm.errorCounts {
		errors = append(errors, errorItem{message: msg, count: count})
	}
	
	for i := 0; i < len(errors)-1; i++ {
		for j := i + 1; j < len(errors); j++ {
			if errors[i].count < errors[j].count {
				errors[i], errors[j] = errors[j], errors[i]
			}
		}
	}
	
	if limit > 0 && len(errors) > limit {
		errors = errors[:limit]
	}
	
	result := make([]ErrorCount, len(errors))
	for i, err := range errors {
		result[i] = ErrorCount{
			Message: err.message,
			Count:   err.count,
		}
	}
	
	return result
}

type ErrorCount struct {
	Message string `json:"message"`
	Count   int64  `json:"count"`
}

func (tm *TransactionMetrics) Reset() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	
	atomic.StoreInt64(&tm.totalChecks, 0)
	atomic.StoreInt64(&tm.successfulChecks, 0)
	atomic.StoreInt64(&tm.failedChecks, 0)
	atomic.StoreInt64(&tm.unknownChecks, 0)
	atomic.StoreInt64(&tm.commitTransactions, 0)
	atomic.StoreInt64(&tm.rollbackTransactions, 0)
	atomic.StoreInt64(&tm.timeoutTransactions, 0)
	atomic.StoreInt64(&tm.totalRetries, 0)
	atomic.StoreInt64(&tm.maxRetryReached, 0)
	atomic.StoreInt64(&tm.checksLastHour, 0)
	atomic.StoreInt64(&tm.checksLastDay, 0)
	
	tm.checkLatencies = tm.checkLatencies[:0]
	tm.avgCheckLatency = 0
	tm.maxCheckLatency = 0
	tm.minCheckLatency = time.Hour
	tm.errorCounts = make(map[string]int64)
	tm.lastError = ""
	tm.lastErrorTime = time.Time{}
	tm.lastHourReset = time.Now()
	tm.lastDayReset = time.Now()
}