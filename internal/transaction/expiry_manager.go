package transaction

import (
	"context"
	"log"
	"sync"
	"time"
)

// ExpiryManager 过期检测管理器
type ExpiryManager struct {
	storage       HalfMessageStorage
	checkInterval time.Duration
	logger        *log.Logger
	
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	
	// 回调函数
	onExpired func(txnID string, halfMsg *HalfMessage)
}

// ExpiryManagerConfig 过期管理器配置
type ExpiryManagerConfig struct {
	CheckInterval time.Duration // 检查间隔，默认30秒
	Logger        *log.Logger   // 日志记录器
}

// NewExpiryManager 创建新的过期检测管理器
func NewExpiryManager(storage HalfMessageStorage, config *ExpiryManagerConfig) *ExpiryManager {
	if config == nil {
		config = &ExpiryManagerConfig{}
	}
	
	// 设置默认值
	if config.CheckInterval == 0 {
		config.CheckInterval = 30 * time.Second
	}
	if config.Logger == nil {
		config.Logger = log.Default()
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	return &ExpiryManager{
		storage:       storage,
		checkInterval: config.CheckInterval,
		logger:        config.Logger,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// SetExpiredCallback 设置过期回调函数
func (em *ExpiryManager) SetExpiredCallback(callback func(txnID string, halfMsg *HalfMessage)) {
	em.onExpired = callback
}

// Start 启动过期检测
func (em *ExpiryManager) Start() {
	em.wg.Add(1)
	go em.runExpiryCheck()
	em.logger.Printf("ExpiryManager started with check interval: %v", em.checkInterval)
}

// Stop 停止过期检测
func (em *ExpiryManager) Stop() {
	em.cancel()
	em.wg.Wait()
	em.logger.Println("ExpiryManager stopped")
}

// runExpiryCheck 运行过期检测循环
func (em *ExpiryManager) runExpiryCheck() {
	defer em.wg.Done()
	
	ticker := time.NewTicker(em.checkInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-em.ctx.Done():
			return
		case <-ticker.C:
			em.checkExpiredMessages()
		}
	}
}

// checkExpiredMessages 检查过期消息
func (em *ExpiryManager) checkExpiredMessages() {
	now := time.Now()
	
	// 获取过期的半消息
	expiredMessages, err := em.storage.GetExpiredMessages(now)
	if err != nil {
		em.logger.Printf("Failed to get expired messages: %v", err)
		return
	}
	
	if len(expiredMessages) == 0 {
		return // 没有过期消息
	}
	
	em.logger.Printf("Found %d expired half messages", len(expiredMessages))
	
	// 处理每个过期的半消息
	for _, storedMsg := range expiredMessages {
		em.handleExpiredMessage(storedMsg)
	}
	
	// 清理过期数据
	if err := em.storage.CleanupExpired(now); err != nil {
		em.logger.Printf("Failed to cleanup expired messages: %v", err)
	}
}

// handleExpiredMessage 处理单个过期消息
func (em *ExpiryManager) handleExpiredMessage(storedMsg *StoredHalfMessage) {
	em.logger.Printf("Processing expired half message: txnID=%s, expireTime=%d", 
		storedMsg.TransactionID, storedMsg.ExpireTime)
	
	// 调用过期回调函数
	if em.onExpired != nil {
		em.onExpired(storedMsg.TransactionID, storedMsg.HalfMessage)
	}
	
	// 标记为已过期（可选，也可以直接删除）
	storedMsg.Status = "EXPIRED"
}

// ForceCheck 强制执行一次过期检查
func (em *ExpiryManager) ForceCheck() {
	em.checkExpiredMessages()
}

// GetStats 获取过期管理器统计信息
func (em *ExpiryManager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"check_interval": em.checkInterval.String(),
		"running":        em.ctx.Err() == nil,
	}
}