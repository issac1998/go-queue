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

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/errors"
)

// DelayedMessageManager 延迟消息管理器
type DelayedMessageManager struct {
	dataDir   string
	timeWheel *TimeWheel
	messages  map[string]*DelayedMessage
	producer  *client.Producer
	mutex     sync.RWMutex
	running   bool
	stopChan  chan struct{}

	// 配置
	maxRetries      int
	cleanupInterval time.Duration

	// 统计信息
	stats DelayedMessageStats
}

// DelayedMessageStats 延迟消息统计信息
type DelayedMessageStats struct {
	TotalMessages     int64 `json:"total_messages"`
	PendingMessages   int64 `json:"pending_messages"`
	DeliveredMessages int64 `json:"delivered_messages"`
	FailedMessages    int64 `json:"failed_messages"`
	CanceledMessages  int64 `json:"canceled_messages"`
}

// DelayedMessageManagerConfig 延迟消息管理器配置
type DelayedMessageManagerConfig struct {
	DataDir         string
	MaxRetries      int
	CleanupInterval time.Duration
}

// NewDelayedMessageManager 创建延迟消息管理器
func NewDelayedMessageManager(config DelayedMessageManagerConfig, producer *client.Producer) *DelayedMessageManager {
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.CleanupInterval == 0 {
		config.CleanupInterval = 1 * time.Hour
	}

	manager := &DelayedMessageManager{
		dataDir:         config.DataDir,
		messages:        make(map[string]*DelayedMessage),
		producer:        producer,
		running:         false,
		stopChan:        make(chan struct{}),
		maxRetries:      config.MaxRetries,
		cleanupInterval: config.CleanupInterval,
	}

	// 创建时间轮，1秒精度，3600个槽位(1小时)
	manager.timeWheel = NewTimeWheel(TimeWheelSize, TimeWheelTickMs, manager.handleExpiredTask)

	return manager
}

// Start 启动延迟消息管理器
func (dmm *DelayedMessageManager) Start() error {
	dmm.mutex.Lock()
	defer dmm.mutex.Unlock()

	if dmm.running {
		return nil
	}

	// 创建数据目录
	if err := os.MkdirAll(dmm.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// 加载已存在的延迟消息
	if err := dmm.loadMessages(); err != nil {
		return fmt.Errorf("failed to load messages: %w", err)
	}

	// 启动时间轮
	dmm.timeWheel.Start()

	// 将加载的消息添加到时间轮
	dmm.scheduleExistingMessages()

	// 启动清理任务
	go dmm.cleanupLoop()

	dmm.running = true
	log.Printf("Delayed message manager started with %d pending messages", len(dmm.messages))

	return nil
}

// Stop 停止延迟消息管理器
func (dmm *DelayedMessageManager) Stop() error {
	dmm.mutex.Lock()
	defer dmm.mutex.Unlock()

	if !dmm.running {
		return nil
	}

	dmm.running = false
	close(dmm.stopChan)

	// 停止时间轮
	dmm.timeWheel.Stop()

	// 保存所有消息状态
	if err := dmm.saveMessages(); err != nil {
		log.Printf("Failed to save messages during shutdown: %v", err)
	}

	log.Printf("Delayed message manager stopped")
	return nil
}

// ScheduleMessage 调度延迟消息
func (dmm *DelayedMessageManager) ScheduleMessage(req *DelayedProduceRequest) (*DelayedProduceResponse, error) {
	dmm.mutex.Lock()
	defer dmm.mutex.Unlock()

	if !dmm.running {
		return nil, fmt.Errorf("delayed message manager is not running")
	}

	// 计算投递时间
	deliverTime := CalculateDeliverTime(
		DelayLevel(req.DelayLevel),
		req.DelayTime,
		req.DeliverTime,
	)

	// 检查延迟时间是否超过最大限制
	maxDeliverTime := time.Now().Add(MaxDelayTime).UnixMilli()
	if deliverTime > maxDeliverTime {
		return nil, fmt.Errorf("delay time exceeds maximum limit of %v", MaxDelayTime)
	}

	// 生成消息ID
	messageID := dmm.generateMessageID()

	// 创建延迟消息
	message := &DelayedMessage{
		ID:          messageID,
		Topic:       req.Topic,
		Partition:   req.Partition,
		Key:         req.Key,
		Value:       req.Value,
		Headers:     req.Headers,
		CreateTime:  time.Now().UnixMilli(),
		DeliverTime: deliverTime,
		Status:      StatusPending,
		RetryCount:  0,
		MaxRetries:  dmm.maxRetries,
		UpdateTime:  time.Now().UnixMilli(),
	}

	// 存储消息
	dmm.messages[messageID] = message

	// 添加到时间轮
	task := &TimeWheelTask{
		ID:          messageID,
		ExecuteTime: deliverTime,
		Data:        message,
	}
	dmm.timeWheel.AddTask(task)

	// 持久化消息
	if err := dmm.saveMessage(message); err != nil {
		log.Printf("Failed to save delayed message %s: %v", messageID, err)
	}

	// 更新统计信息
	dmm.stats.TotalMessages++
	dmm.stats.PendingMessages++

	return &DelayedProduceResponse{
		MessageID:   messageID,
		Topic:       req.Topic,
		Partition:   req.Partition,
		DeliverTime: deliverTime,
		ErrorCode:   0,
	}, nil
}

// handleExpiredTask 处理到期的任务
func (dmm *DelayedMessageManager) handleExpiredTask(task *TimeWheelTask) {
	message, ok := task.Data.(*DelayedMessage)
	if !ok {
		log.Printf("Invalid task data type for task %s", task.ID)
		return
	}

	// 投递消息
	dmm.deliverMessage(message)
}

// deliverMessage 投递延迟消息
func (dmm *DelayedMessageManager) deliverMessage(message *DelayedMessage) {
	dmm.mutex.Lock()
	defer dmm.mutex.Unlock()

	// 检查消息是否仍然有效
	if message.Status != StatusPending {
		return
	}

	// 创建普通消息进行投递
	produceMsg := client.ProduceMessage{
		Topic:     message.Topic,
		Partition: message.Partition,
		Key:       message.Key,
		Value:     message.Value,
	}

	// 投递消息
	_, err := dmm.producer.Send(produceMsg)
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
			// 超过最大重试次数，标记为失败
			message.MarkFailed(err.Error())
			dmm.stats.PendingMessages--
			dmm.stats.FailedMessages++
			log.Printf("Delayed message %s failed permanently after %d retries: %v",
				message.ID, message.RetryCount, err)
		}

		// 保存消息状态
		if saveErr := dmm.saveMessage(message); saveErr != nil {
			log.Printf("Failed to save message state for %s: %v", message.ID, saveErr)
		}
	} else {
		// 投递成功
		message.MarkDelivered()
		dmm.stats.PendingMessages--
		dmm.stats.DeliveredMessages++
		log.Printf("Successfully delivered delayed message %s to %s-%d",
			message.ID, message.Topic, message.Partition)

		// 保存消息状态
		if saveErr := dmm.saveMessage(message); saveErr != nil {
			log.Printf("Failed to save message state for %s: %v", message.ID, saveErr)
		}
	}
}

// generateMessageID 生成消息ID
func (dmm *DelayedMessageManager) generateMessageID() string {
	timestamp := time.Now().UnixNano()
	randomBytes := make([]byte, 8)
	rand.Read(randomBytes)
	return fmt.Sprintf("delayed_%d_%s", timestamp, hex.EncodeToString(randomBytes))
}

// loadMessages 加载已存在的延迟消息
func (dmm *DelayedMessageManager) loadMessages() error {
	messagesFile := filepath.Join(dmm.dataDir, "delayed_messages.json")

	// 检查文件是否存在
	if _, err := os.Stat(messagesFile); os.IsNotExist(err) {
		return nil // 文件不存在，这是正常的
	}

	// 读取文件
	data, err := os.ReadFile(messagesFile)
	if err != nil {
		return fmt.Errorf("failed to read messages file: %w", err)
	}

	// 解析JSON
	var messages map[string]*DelayedMessage
	if err := json.Unmarshal(data, &messages); err != nil {
		return fmt.Errorf("failed to unmarshal messages: %w", err)
	}

	// 过滤出仍然有效的消息
	now := time.Now().UnixMilli()
	pendingCount := int64(0)
	deliveredCount := int64(0)
	failedCount := int64(0)
	canceledCount := int64(0)

	for id, message := range messages {
		switch message.Status {
		case StatusPending:
			if message.DeliverTime > now {
				dmm.messages[id] = message
				pendingCount++
			}
		case StatusDelivered:
			deliveredCount++
		case StatusFailed:
			failedCount++
		case StatusCanceled:
			canceledCount++
		}
	}

	// 更新统计信息
	dmm.stats.TotalMessages = int64(len(messages))
	dmm.stats.PendingMessages = pendingCount
	dmm.stats.DeliveredMessages = deliveredCount
	dmm.stats.FailedMessages = failedCount
	dmm.stats.CanceledMessages = canceledCount

	log.Printf("Loaded %d delayed messages (%d pending, %d delivered, %d failed, %d canceled)",
		len(messages), pendingCount, deliveredCount, failedCount, canceledCount)

	return nil
}

// saveMessages 保存所有延迟消息
func (dmm *DelayedMessageManager) saveMessages() error {
	messagesFile := filepath.Join(dmm.dataDir, "delayed_messages.json")

	// 序列化消息
	data, err := json.MarshalIndent(dmm.messages, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal messages: %w", err)
	}

	// 写入文件
	if err := os.WriteFile(messagesFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write messages file: %w", err)
	}

	return nil
}

// saveMessage 保存单个延迟消息
func (dmm *DelayedMessageManager) saveMessage(message *DelayedMessage) error {
	messageFile := filepath.Join(dmm.dataDir, fmt.Sprintf("msg_%s.json", message.ID))

	data, err := json.MarshalIndent(message, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	if err := os.WriteFile(messageFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write message file: %w", err)
	}

	return nil
}

// scheduleExistingMessages 将已存在的消息添加到时间轮
func (dmm *DelayedMessageManager) scheduleExistingMessages() {
	for _, message := range dmm.messages {
		if message.Status == StatusPending {
			task := &TimeWheelTask{
				ID:          message.ID,
				ExecuteTime: message.DeliverTime,
				Data:        message,
			}
			dmm.timeWheel.AddTask(task)
		}
	}
}

// cleanupLoop 清理循环
func (dmm *DelayedMessageManager) cleanupLoop() {
	ticker := time.NewTicker(dmm.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dmm.cleanup()
		case <-dmm.stopChan:
			return
		}
	}
}

// cleanup 清理已完成的消息
func (dmm *DelayedMessageManager) cleanup() {
	dmm.mutex.Lock()
	defer dmm.mutex.Unlock()

	// 清理已完成的消息（保留24小时）
	cutoffTime := time.Now().Add(-24 * time.Hour).UnixMilli()
	var toDelete []string

	for id, message := range dmm.messages {
		if (message.Status == StatusDelivered || message.Status == StatusFailed || message.Status == StatusCanceled) &&
			message.UpdateTime < cutoffTime {
			toDelete = append(toDelete, id)
		}
	}

	// 删除过期的消息
	for _, id := range toDelete {
		delete(dmm.messages, id)
		// 删除消息文件
		messageFile := filepath.Join(dmm.dataDir, fmt.Sprintf("msg_%s.json", id))
		os.Remove(messageFile)
	}

	if len(toDelete) > 0 {
		log.Printf("Cleaned up %d expired delayed messages", len(toDelete))
		// 保存更新后的消息列表
		dmm.saveMessages()
	}
}

// GetStats 获取统计信息
func (dmm *DelayedMessageManager) GetStats() DelayedMessageStats {
	dmm.mutex.RLock()
	defer dmm.mutex.RUnlock()
	return dmm.stats
}

// GetMessage 获取延迟消息
func (dmm *DelayedMessageManager) GetMessage(messageID string) (*DelayedMessage, error) {
	dmm.mutex.RLock()
	defer dmm.mutex.RUnlock()

	message, exists := dmm.messages[messageID]
	if !exists {
		return nil, &errors.TypedError{
			Type:    errors.GeneralError,
			Message: "delayed message not found",
		}
	}

	return message, nil
}

// CancelMessage 取消延迟消息
func (dmm *DelayedMessageManager) CancelMessage(messageID string) error {
	dmm.mutex.Lock()
	defer dmm.mutex.Unlock()

	message, exists := dmm.messages[messageID]
	if !exists {
		return &errors.TypedError{
			Type:    errors.GeneralError,
			Message: "delayed message not found",
		}
	}

	if message.Status != StatusPending {
		return &errors.TypedError{
			Type:    errors.GeneralError,
			Message: "message is not in pending status",
		}
	}

	// 标记为已取消
	message.Status = StatusCanceled
	message.UpdateTime = time.Now().UnixMilli()

	// 更新统计信息
	dmm.stats.PendingMessages--
	dmm.stats.CanceledMessages++

	// 保存消息状态
	if err := dmm.saveMessage(message); err != nil {
		log.Printf("Failed to save canceled message %s: %v", messageID, err)
	}

	log.Printf("Canceled delayed message %s", messageID)
	return nil
}
