package delayed

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// MessageDeliveryCallback 消息投递回调函数类型
type MessageDeliveryCallback func(topic string, partition int32, key, value []byte) error

// DelayedMessageManager 延迟消息管理器
type DelayedMessageManager struct {
	dataDir          string
	timeWheel        *TimeWheel
	messages         map[string]*DelayedMessage
	messagesMutex    sync.RWMutex
	deliveryCallback MessageDeliveryCallback

	// 配置参数
	maxRetries      int32
	cleanupInterval time.Duration

	// 运行状态
	running       bool
	stopCh        chan struct{}
	cleanupTicker *time.Ticker
}

// DelayedMessageStats 延迟消息统计
type DelayedMessageStats struct {
	TotalMessages     int64 `json:"total_messages"`
	PendingMessages   int64 `json:"pending_messages"`
	DeliveredMessages int64 `json:"delivered_messages"`
	FailedMessages    int64 `json:"failed_messages"`
	CancelledMessages int64 `json:"cancelled_messages"`
}

// DelayedMessageManagerConfig 延迟消息管理器配置
type DelayedMessageManagerConfig struct {
	DataDir         string
	MaxRetries      int32
	CleanupInterval time.Duration
}

// NewDelayedMessageManager 创建延迟消息管理器
func NewDelayedMessageManager(config DelayedMessageManagerConfig, deliveryCallback MessageDeliveryCallback) *DelayedMessageManager {
	dmm := &DelayedMessageManager{
		dataDir:          config.DataDir,
		messages:         make(map[string]*DelayedMessage),
		deliveryCallback: deliveryCallback,
		maxRetries:       config.MaxRetries,
		cleanupInterval:  config.CleanupInterval,
		stopCh:           make(chan struct{}),
	}

	// 创建时间轮，传入消息投递回调
	dmm.timeWheel = NewTimeWheel(TimeWheelSize, TimeWheelTickMs, dmm.handleExpiredTask)

	return dmm
}

// Start 启动延迟消息管理器
func (dmm *DelayedMessageManager) Start() error {
	if dmm.running {
		return fmt.Errorf("delayed message manager already running")
	}

	// 确保数据目录存在
	if err := os.MkdirAll(dmm.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}

	// 加载已有的延迟消息
	if err := dmm.loadMessages(); err != nil {
		log.Printf("Warning: Failed to load delayed messages: %v", err)
	}

	// 启动时间轮
	dmm.timeWheel.Start()

	// 重新调度已有的消息
	dmm.scheduleExistingMessages()

	// 启动清理协程
	dmm.cleanupTicker = time.NewTicker(dmm.cleanupInterval)
	go dmm.cleanupLoop()

	dmm.running = true
	log.Printf("Delayed message manager started with data directory: %s", dmm.dataDir)

	return nil
}

// Stop 停止延迟消息管理器
func (dmm *DelayedMessageManager) Stop() error {
	if !dmm.running {
		return nil
	}

	dmm.running = false
	close(dmm.stopCh)

	// 停止时间轮
	dmm.timeWheel.Stop()

	// 停止清理协程
	if dmm.cleanupTicker != nil {
		dmm.cleanupTicker.Stop()
	}

	// 保存所有消息
	if err := dmm.saveMessages(); err != nil {
		log.Printf("Warning: Failed to save messages during shutdown: %v", err)
	}

	log.Printf("Delayed message manager stopped")
	return nil
}

// ScheduleMessage 调度延迟消息
func (dmm *DelayedMessageManager) ScheduleMessage(req *DelayedProduceRequest) (*DelayedProduceResponse, error) {
	if !dmm.running {
		return nil, fmt.Errorf("delayed message manager not running")
	}

	// 生成消息ID
	messageID := dmm.generateMessageID()

	// 计算投递时间
	deliverTime := CalculateDeliverTime(DelayLevel(req.DelayLevel), req.DelayTime, req.DeliverTime)

	// 创建延迟消息
	message := &DelayedMessage{
		ID:          messageID,
		Topic:       req.Topic,
		Partition:   req.Partition,
		Key:         req.Key,
		Value:       req.Value,
		CreateTime:  time.Now().UnixMilli(),
		DeliverTime: deliverTime,
		Status:      StatusPending,
		RetryCount:  0,
		MaxRetries:  int(dmm.maxRetries),
		UpdateTime:  time.Now().UnixMilli(),
	}

	// 保存消息
	dmm.messagesMutex.Lock()
	dmm.messages[messageID] = message
	dmm.messagesMutex.Unlock()

	// 持久化消息
	if err := dmm.saveMessage(message); err != nil {
		log.Printf("Warning: Failed to save delayed message: %v", err)
	}

	// 添加到时间轮
	task := &TimeWheelTask{
		ID:          messageID,
		ExecuteTime: deliverTime,
		Data:        message,
	}
	dmm.timeWheel.AddTask(task)

	log.Printf("Scheduled delayed message %s for delivery at %s",
		messageID, time.Unix(deliverTime/1000, 0).Format("2006-01-02 15:04:05"))

	return &DelayedProduceResponse{
		MessageID:   messageID,
		DeliverTime: deliverTime,
		ErrorCode:   0,
		Error:       "",
	}, nil
}

// handleExpiredTask 处理过期任务
func (dmm *DelayedMessageManager) handleExpiredTask(task *TimeWheelTask) {
	message, ok := task.Data.(*DelayedMessage)
	if !ok {
		log.Printf("Invalid task data type for task %s", task.ID)
		return
	}

	dmm.deliverMessage(message)
}

// deliverMessage 投递延迟消息
func (dmm *DelayedMessageManager) deliverMessage(message *DelayedMessage) {
	log.Printf("Delivering delayed message %s to topic %s", message.ID, message.Topic)

	// 投递消息
	if dmm.deliveryCallback == nil {
		log.Printf("Warning: Delivery callback is nil, marking message %s as delivered", message.ID)
		message.MarkDelivered()
		dmm.saveMessage(message)
		return
	}

	err := dmm.deliveryCallback(message.Topic, message.Partition, message.Key, message.Value)
	if err != nil {
		// 投递失败，检查是否可以重试
		if message.CanRetry() {
			message.MarkFailed(err.Error())
			log.Printf("Failed to deliver delayed message %s, retry count: %d, error: %v",
				message.ID, message.RetryCount, err)

			// 重新调度重试
			retryTime := time.Now().Add(time.Duration(message.RetryCount) * time.Minute).UnixMilli()
			task := &TimeWheelTask{
				ID:          message.ID,
				ExecuteTime: retryTime,
				Data:        message,
			}
			dmm.timeWheel.AddTask(task)
		} else {
			log.Printf("Delayed message %s failed permanently after %d retries", message.ID, message.RetryCount)
		}
	} else {
		// 投递成功
		message.MarkDelivered()
		log.Printf("Successfully delivered delayed message %s", message.ID)
	}

	// 保存消息状态
	dmm.saveMessage(message)
}

// generateMessageID 生成消息ID
func (dmm *DelayedMessageManager) generateMessageID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// loadMessages 从磁盘加载消息
func (dmm *DelayedMessageManager) loadMessages() error {
	files, err := filepath.Glob(filepath.Join(dmm.dataDir, "*.json"))
	if err != nil {
		return err
	}

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			log.Printf("Failed to read message file %s: %v", file, err)
			continue
		}

		var message DelayedMessage
		if err := json.Unmarshal(data, &message); err != nil {
			log.Printf("Failed to parse message file %s: %v", file, err)
			continue
		}

		dmm.messages[message.ID] = &message
	}

	log.Printf("Loaded %d delayed messages from disk", len(dmm.messages))
	return nil
}

// saveMessages 保存所有消息到磁盘
func (dmm *DelayedMessageManager) saveMessages() error {
	dmm.messagesMutex.RLock()
	defer dmm.messagesMutex.RUnlock()

	for _, message := range dmm.messages {
		if err := dmm.saveMessage(message); err != nil {
			log.Printf("Failed to save message %s: %v", message.ID, err)
		}
	}

	return nil
}

// saveMessage 保存单个消息到磁盘
func (dmm *DelayedMessageManager) saveMessage(message *DelayedMessage) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	filename := filepath.Join(dmm.dataDir, message.ID+".json")
	return os.WriteFile(filename, data, 0644)
}

// scheduleExistingMessages 重新调度已有消息
func (dmm *DelayedMessageManager) scheduleExistingMessages() {
	dmm.messagesMutex.RLock()
	defer dmm.messagesMutex.RUnlock()

	now := time.Now().UnixMilli()
	scheduled := 0

	for _, message := range dmm.messages {
		if message.Status == StatusPending {
			if message.DeliverTime <= now {
				// 已过期，立即投递
				go dmm.deliverMessage(message)
			} else {
				// 重新调度
				task := &TimeWheelTask{
					ID:          message.ID,
					ExecuteTime: message.DeliverTime,
					Data:        message,
				}
				dmm.timeWheel.AddTask(task)
				scheduled++
			}
		}
	}

	log.Printf("Rescheduled %d pending delayed messages", scheduled)
}

// cleanupLoop 清理循环
func (dmm *DelayedMessageManager) cleanupLoop() {
	for {
		select {
		case <-dmm.stopCh:
			return
		case <-dmm.cleanupTicker.C:
			dmm.cleanup()
		}
	}
}

// cleanup 清理已完成的消息
func (dmm *DelayedMessageManager) cleanup() {
	dmm.messagesMutex.Lock()
	defer dmm.messagesMutex.Unlock()

	cutoffTime := time.Now().Add(-24 * time.Hour).UnixMilli() // 保留24小时
	cleaned := 0

	for id, message := range dmm.messages {
		if message.Status != StatusPending && message.CreateTime < cutoffTime {
			// 删除消息文件
			filename := filepath.Join(dmm.dataDir, message.ID+".json")
			os.Remove(filename)

			// 从内存中删除
			delete(dmm.messages, id)
			cleaned++
		}
	}

	if cleaned > 0 {
		log.Printf("Cleaned up %d old delayed messages", cleaned)
	}
}

// GetStats 获取统计信息
func (dmm *DelayedMessageManager) GetStats() DelayedMessageStats {
	dmm.messagesMutex.RLock()
	defer dmm.messagesMutex.RUnlock()

	stats := DelayedMessageStats{}

	for _, message := range dmm.messages {
		stats.TotalMessages++
		switch message.Status {
		case StatusPending:
			stats.PendingMessages++
		case StatusDelivered:
			stats.DeliveredMessages++
		case StatusFailed:
			stats.FailedMessages++
		case StatusCanceled:
			stats.CancelledMessages++
		}
	}

	return stats
}

// GetMessage 获取消息
func (dmm *DelayedMessageManager) GetMessage(messageID string) (*DelayedMessage, error) {
	dmm.messagesMutex.RLock()
	defer dmm.messagesMutex.RUnlock()

	message, exists := dmm.messages[messageID]
	if !exists {
		return nil, fmt.Errorf("message not found: %s", messageID)
	}

	return message, nil
}

// CancelMessage 取消消息
func (dmm *DelayedMessageManager) CancelMessage(messageID string) error {
	dmm.messagesMutex.Lock()
	defer dmm.messagesMutex.Unlock()

	message, exists := dmm.messages[messageID]
	if !exists {
		return fmt.Errorf("message not found: %s", messageID)
	}

	if message.Status != StatusPending {
		return fmt.Errorf("message %s is not in pending status", messageID)
	}

	message.Status = StatusCanceled
	dmm.saveMessage(message)

	log.Printf("Cancelled delayed message %s", messageID)
	return nil
}
