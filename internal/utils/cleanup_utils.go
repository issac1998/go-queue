package utils

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

// CleanupConfig 清理配置
type CleanupConfig struct {
	MaxRetries      int           // 最大重试次数
	InitialDelay    time.Duration // 初始延迟
	MaxDelay        time.Duration // 最大延迟
	BackoffFactor   float64       // 退避因子
	BatchSize       int           // 批处理大小
	UseSync         bool          // 是否使用同步提交
	LogErrors       bool          // 是否记录错误日志
}

// DefaultCleanupConfig 返回默认的清理配置
func DefaultCleanupConfig() *CleanupConfig {
	return &CleanupConfig{
		MaxRetries:    3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
		BatchSize:     1000,
		UseSync:       false, // 默认使用异步提交以提高性能
		LogErrors:     true,
	}
}

// CleanupManager 清理管理器，提供并发控制
type CleanupManager struct {
	mu       sync.Mutex
	running  map[string]bool // 跟踪正在运行的清理任务
	config   *CleanupConfig
}

// NewCleanupManager 创建新的清理管理器
func NewCleanupManager(config *CleanupConfig) *CleanupManager {
	if config == nil {
		config = DefaultCleanupConfig()
	}
	return &CleanupManager{
		running: make(map[string]bool),
		config:  config,
	}
}

// BatchCleanupWithRetry 执行带重试的批量清理操作
func (cm *CleanupManager) BatchCleanupWithRetry(taskName string, db *pebble.DB, keysToDelete [][]byte) error {
	// 检查是否已有同名任务在运行
	cm.mu.Lock()
	if cm.running[taskName] {
		cm.mu.Unlock()
		if cm.config.LogErrors {
			log.Printf("[CLEANUP] Task %s is already running, skipping", taskName)
		}
		return nil
	}
	cm.running[taskName] = true
	cm.mu.Unlock()

	defer func() {
		cm.mu.Lock()
		delete(cm.running, taskName)
		cm.mu.Unlock()
	}()

	if len(keysToDelete) == 0 {
		return nil
	}

	totalKeys := len(keysToDelete)
	deletedCount := 0

	// 分批处理
	for i := 0; i < totalKeys; i += cm.config.BatchSize {
		end := i + cm.config.BatchSize
		if end > totalKeys {
			end = totalKeys
		}
		
		batch := keysToDelete[i:end]
		batchSize := len(batch)
		
		err := cm.executeBatchWithRetry(taskName, db, batch)
		if err != nil {
			if cm.config.LogErrors {
				log.Printf("[CLEANUP] Failed to cleanup batch %d-%d for task %s: %v", i, end-1, taskName, err)
			}
			return fmt.Errorf("failed to cleanup batch %d-%d: %w", i, end-1, err)
		}
		
		deletedCount += batchSize
		
		if cm.config.LogErrors && batchSize > 0 {
			log.Printf("[CLEANUP] Successfully cleaned up batch %d-%d (%d keys) for task %s", i, end-1, batchSize, taskName)
		}
	}

	if cm.config.LogErrors && deletedCount > 0 {
		log.Printf("[CLEANUP] Task %s completed: cleaned up %d keys in total", taskName, deletedCount)
	}

	return nil
}

// executeBatchWithRetry 执行单个批次的清理操作，带重试机制
func (cm *CleanupManager) executeBatchWithRetry(taskName string, db *pebble.DB, keys [][]byte) error {
	var lastErr error
	delay := cm.config.InitialDelay

	for attempt := 0; attempt <= cm.config.MaxRetries; attempt++ {
		if attempt > 0 {
			if cm.config.LogErrors {
				log.Printf("[CLEANUP] Retrying batch cleanup for task %s (attempt %d/%d) after %v", 
					taskName, attempt, cm.config.MaxRetries, delay)
			}
			time.Sleep(delay)
			
			// 计算下次延迟（指数退避）
			delay = time.Duration(float64(delay) * cm.config.BackoffFactor)
			if delay > cm.config.MaxDelay {
				delay = cm.config.MaxDelay
			}
		}

		err := cm.executeBatch(db, keys)
		if err == nil {
			return nil // 成功
		}

		lastErr = err
		
		if cm.config.LogErrors {
			log.Printf("[CLEANUP] Batch cleanup attempt %d failed for task %s: %v", attempt+1, taskName, err)
		}
	}

	return fmt.Errorf("batch cleanup failed after %d attempts: %w", cm.config.MaxRetries+1, lastErr)
}

// executeBatch 执行单个批次的删除操作
func (cm *CleanupManager) executeBatch(db *pebble.DB, keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}

	batch := db.NewBatch()
	defer batch.Close()

	// 添加删除操作到批次
	for _, key := range keys {
		if err := batch.Delete(key, nil); err != nil {
			return fmt.Errorf("failed to add delete operation to batch: %w", err)
		}
	}

	// 提交批次
	var commitOption *pebble.WriteOptions
	if cm.config.UseSync {
		commitOption = pebble.Sync
	} else {
		commitOption = pebble.NoSync
	}

	if err := batch.Commit(commitOption); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}

// SimpleCleanupWithRetry 简单的清理操作，适用于不需要复杂管理的场景
func SimpleCleanupWithRetry(taskName string, db *pebble.DB, keysToDelete [][]byte, config *CleanupConfig) error {
	if config == nil {
		config = DefaultCleanupConfig()
	}
	
	manager := NewCleanupManager(config)
	return manager.BatchCleanupWithRetry(taskName, db, keysToDelete)
}

// CalculateBackoffDelay 计算退避延迟
func CalculateBackoffDelay(attempt int, initialDelay time.Duration, backoffFactor float64, maxDelay time.Duration) time.Duration {
	delay := time.Duration(float64(initialDelay) * math.Pow(backoffFactor, float64(attempt)))
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}