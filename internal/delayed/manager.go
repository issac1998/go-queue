package delayed

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// NewDelayedMessageManager creates a delayed message manager with simplified architecture
func NewDelayedMessageManager(config DelayedMessageManagerConfig, deliveryCallback MessageDeliveryCallback) *DelayedMessageManager {
	dmm := &DelayedMessageManager{
		storage:          config.Storage,
		deliveryCallback: deliveryCallback,
		maxRetries:       config.MaxRetries,
		cleanupInterval:  config.CleanupInterval,
		stopCh:           make(chan struct{}),
		inTimeWheelMap:   make(map[string]bool),
	}

	// 使用优化的时间轮，启用智能丢弃和压力检测
	timeWheelConfig := OptimizedTimeWheelConfig{
		SlotCount:                 TimeWheelSize,
		TickMs:                    TimeWheelTickMs,
		MaxTasksPerTick:           100,
		EnablePressureDetection:   true,
		PressureThreshold:         0.8,
		CriticalPressureThreshold: 0.95,
	}

	workerConfig := WorkerPoolConfig{
		InitialWorkers:            10,
		MinWorkers:                2,
		MaxWorkers:                50,
		MaxQueueSize:              1000,
		DropPolicy:                DropPolicyBlock,
		ScaleUpThreshold:          0.8,
		ScaleDownThreshold:        0.3,
		EnableSmartDrop:           true,
		HighPressureThreshold:     0.7,
		CriticalPressureThreshold: 0.9,
	}

	dmm.timeWheel = NewOptimizedTimeWheel(timeWheelConfig, dmm.handleExpiredTask, workerConfig)

	return dmm
}

// Start start a manager
func (dmm *DelayedMessageManager) Start() error {
	if dmm.running {
		return fmt.Errorf("delayed message manager already running")
	}

	dmm.timeWheel.Start()

	if dmm.cleanupInterval > 0 {
		dmm.cleanupTicker = time.NewTicker(dmm.cleanupInterval)
		go dmm.cleanupLoop()
	}

	dmm.running = true
	log.Printf("Delayed Message Manager started")
	return nil
}

// Stop stop manager
func (dmm *DelayedMessageManager) Stop() error {
	if !dmm.running {
		return nil
	}

	close(dmm.stopCh)

	if dmm.timeWheel != nil {
		dmm.timeWheel.Stop()
	}

	if dmm.cleanupTicker != nil {
		dmm.cleanupTicker.Stop()
	}

	dmm.running = false
	log.Printf("DelayedMessageManager stopped")

	return nil
}

// 内存态管理方法
func (dmm *DelayedMessageManager) isInTimeWheel(messageID string) bool {
	dmm.timeWheelMutex.RLock()
	defer dmm.timeWheelMutex.RUnlock()
	return dmm.inTimeWheelMap[messageID]
}

func (dmm *DelayedMessageManager) setInTimeWheel(messageID string, inWheel bool) {
	dmm.timeWheelMutex.Lock()
	defer dmm.timeWheelMutex.Unlock()
	if inWheel {
		dmm.inTimeWheelMap[messageID] = true
	} else {
		delete(dmm.inTimeWheelMap, messageID)
	}
}

// ScanPendingMessagesForGroups scan leader group to send message to time wheel
func (dmm *DelayedMessageManager) ScanPendingMessagesForGroups(groupIDs []uint64) {
	now := time.Now()
	// only get 60s message
	timeWheelRange := time.Duration(TimeWheelSize) * time.Duration(TimeWheelTickMs) * time.Millisecond // 60秒
	endTime := now.Add(timeWheelRange)

	var allMessages []*DelayedMessage
	var addedCount int

	for _, groupID := range groupIDs {
		messages, err := dmm.storage.ScanPendingMessages(groupID, now, endTime, 1000)
		if err != nil {
			log.Printf("Failed to scan messages from Raft group %d: %v", groupID, err)
			continue
		}

		allMessages = append(allMessages, messages...)
	}

	for _, message := range allMessages {
		deliverTime := time.UnixMilli(message.DeliverTime)
		if deliverTime.After(endTime) {
			continue // 跳过超过60秒的任务
		}

		if !dmm.isInTimeWheel(message.ID) {
			task := &TimeWheelTask{
				ID:          message.ID,
				ExecuteTime: message.DeliverTime,
				Data: &DelayedTask{
					MessageID:   message.ID,
					DeliverTime: message.DeliverTime,
					Priority:    message.Priority,
					Droppable:   message.Droppable,
				},
			}
			dmm.timeWheel.AddTask(task)

			// 标记消息已在时间轮中（内存态）
			dmm.setInTimeWheel(message.ID, true)
			addedCount++
		}
	}

	if addedCount > 0 {
		log.Printf("Scanned %d pending messages, added %d new messages to TimeWheel from %d partitions (within 60s window)", len(allMessages), addedCount, len(groupIDs))
	}
}

// ScheduleMessageWithRaftGroup 直接接受raft group ID参数，避免使用callback
func (dmm *DelayedMessageManager) ScheduleMessageWithRaftGroup(req *DelayedProduceRequest, raftGroupID uint64) (*DelayedProduceResponse, []byte, error) {
	messageID := dmm.generateMessageID()

	// 计算投递时间
	var deliverTime int64
	if req.DeliverTime > 0 {
		deliverTime = req.DeliverTime
	} else if req.DelayTime > 0 {
		deliverTime = time.Now().UnixMilli() + req.DelayTime
	} else if req.DelayLevel > 0 {
		duration := GetDelayDuration(DelayLevel(req.DelayLevel))
		deliverTime = time.Now().Add(duration).UnixMilli()
	} else {
		return nil, nil, fmt.Errorf("no delay time specified")
	}

	message := &DelayedMessage{
		ID:          messageID,
		GroupID:     raftGroupID, // 设置正确的Raft组ID
		Topic:       req.Topic,
		Partition:   req.Partition,
		Key:         req.Key,
		Value:       req.Value,
		Headers:     req.Headers,
		CreateTime:  time.Now().UnixMilli(),
		DeliverTime: deliverTime,
		Status:      StatusPending,
		RetryCount:  0,
		MaxRetries:  3,
		UpdateTime:  time.Now().UnixMilli(),
	}

	cmdData, err := dmm.storeMessageViaRaftWithGroupID(messageID, message, raftGroupID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare delayed message command: %v", err)
	}

	// add to timewheel if it is close to expiry
	if dmm.shouldAddToTimeWheel(time.UnixMilli(deliverTime)) {
		task := &TimeWheelTask{
			ID:          messageID,
			ExecuteTime: deliverTime,
			Data: &DelayedTask{
				MessageID:   messageID,
				DeliverTime: deliverTime,
				Priority:    message.Priority,
				Droppable:   message.Droppable,
			},
		}
		dmm.timeWheel.AddTask(task)
	}

	return &DelayedProduceResponse{
		MessageID:   messageID,
		Topic:       req.Topic,
		Partition:   req.Partition,
		DeliverTime: deliverTime,
		ErrorCode:   0,
	}, cmdData, nil
}

// shouldAddToTimeWheel 判断消息是否应该立即添加到TimeWheel
func (dmm *DelayedMessageManager) shouldAddToTimeWheel(deliverTime time.Time) bool {
	timeWheelRange := time.Duration(TimeWheelSize) * time.Duration(TimeWheelTickMs) * time.Millisecond
	return deliverTime.Before(time.Now().Add(timeWheelRange))
}

// storeMessageViaRaftWithGroupID stores a delayed message via Raft to the specified raft group
func (dmm *DelayedMessageManager) storeMessageViaRaftWithGroupID(messageID string, message *DelayedMessage, raftGroupID uint64) ([]byte, error) {
	if raftGroupID == 0 {
		return nil, fmt.Errorf("invalid raft group ID (0) for topic %s partition %d", message.Topic, message.Partition)
	}

	// 构造延迟消息存储命令
	cmd := map[string]interface{}{
		"type": "store_delayed_message",
		"data": map[string]interface{}{
			"message_id": messageID,
			"message":    message,
		},
	}

	// 序列化命令
	cmdData, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal delayed message command: %w", err)
	}

	return cmdData, nil
}

func (dmm *DelayedMessageManager) handleExpiredTask(delayedTask *DelayedTask) {
	message, err := dmm.storage.Get(delayedTask.MessageID)
	if err != nil {
		log.Printf("Failed to get message %s from storage: %v", delayedTask.MessageID, err)
		return
	}
	// might execute by other broker or might duplicate due to restart
	if message == nil || message.Status != StatusPending {
		return
	}

	dmm.deliverMessage(message)
}

func (dmm *DelayedMessageManager) deliverMessage(message *DelayedMessage) {
	if dmm.deliveryCallback == nil {
		log.Printf("Warning: Delivery callback is nil, marking message %s as delivered", message.ID)
		return
	}

	err := dmm.deliveryCallback(message.Topic, message.Partition, message.Key, message.Value)
	if err != nil {
		log.Printf("Failed to deliver message %s: %v", message.ID, err)
		message.RetryCount++

		if int32(message.RetryCount) >= dmm.maxRetries {
			message.MarkFailed(err.Error())
			log.Printf("Message %s failed after %d retries", message.ID, message.RetryCount)
		} else {
			message.DeliverTime = time.Now().Add(time.Minute * time.Duration(message.RetryCount)).UnixMilli()
			log.Printf("Rescheduling message %s for retry %d", message.ID, message.RetryCount)
		}
	} else {
		message.MarkDelivered()
		log.Printf("Message %s delivered successfully", message.ID)
	}

	dmm.setInTimeWheel(message.ID, false)
	dmm.updateMessageStatus(message.ID, message)
}

func (dmm *DelayedMessageManager) updateMessageStatus(messageID string, message *DelayedMessage) {
	if dmm.raftProposer != nil {
		if err := dmm.submitUpdateToRaft(messageID, message); err != nil {
			log.Printf("Failed to update message %s status via Raft: %v", messageID, err)
			if err := dmm.storage.Update(messageID, message); err != nil {
				log.Printf("Failed to update message %s status directly: %v", messageID, err)
			}
		}
	}
}

func (dmm *DelayedMessageManager) submitUpdateToRaft(messageID string, message *DelayedMessage) error {
	cmd := map[string]interface{}{
		"type": "update_delayed_message",
		"data": map[string]interface{}{
			"message_id": messageID,
			"message":    message,
		},
	}

	cmdData, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal update command: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = dmm.raftProposer.SyncPropose(ctx, message.GroupID, cmdData)
	if err != nil {
		return fmt.Errorf("failed to submit update to Raft: %w", err)
	}

	return nil
}

// submitBatchUpdatesToRaft 将同一Group的延时消息更新批量提交到Raft
// updates参数为同一GroupID的多条更新（Delivered将被状态机删除，其他将更新）
func (dmm *DelayedMessageManager) submitBatchUpdatesToRaft(groupID uint64, updates map[string]*DelayedMessage) error {
	if dmm.raftProposer == nil {
		return fmt.Errorf("raft proposer not set")
	}
	// 构建批量命令
	items := make([]map[string]interface{}, 0, len(updates))
	for mid, msg := range updates {
		items = append(items, map[string]interface{}{
			"message_id": mid,
			"message":    msg,
		})
	}
	cmd := map[string]interface{}{
		"type": "batch_update_delayed_messages",
		"data": map[string]interface{}{
			"updates": items,
		},
	}
	cmdData, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal batch update command: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = dmm.raftProposer.SyncPropose(ctx, groupID, cmdData)
	if err != nil {
		return fmt.Errorf("failed to submit batch updates to Raft: %w", err)
	}
	return nil
}

// SetRaftProposer 设置 Raft 提议器（用于依赖注入）
func (dmm *DelayedMessageManager) SetRaftProposer(proposer RaftProposer) {
	dmm.raftProposer = proposer
}

func (dmm *DelayedMessageManager) generateMessageID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

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

func (dmm *DelayedMessageManager) cleanup() {
	beforeTime := time.Now().Add(-24 * time.Hour) // 清理24小时前的已投递消息
	if err := dmm.storage.CleanupDelivered(beforeTime); err != nil {
		log.Printf("Failed to cleanup delivered messages: %v", err)
	}
}

func (dmm *DelayedMessageManager) GetStats() DelayedMessageStats {
	stats := dmm.storage.GetStats()

	return DelayedMessageStats{
		TotalMessages:     getInt64FromStats(stats, "total_messages"),
		PendingMessages:   getInt64FromStats(stats, "pending_messages"),
		DeliveredMessages: getInt64FromStats(stats, "delivered_messages"),
		FailedMessages:    getInt64FromStats(stats, "failed_messages"),
		CancelledMessages: getInt64FromStats(stats, "cancelled_messages"),
	}
}

// GetOptimizedStats 获取优化时间轮的详细统计信息
func (dmm *DelayedMessageManager) GetOptimizedStats() OptimizedStats {
	if dmm.timeWheel != nil {
		return dmm.timeWheel.GetOptimizedStats()
	}
	return OptimizedStats{}
}

func getInt64FromStats(stats map[string]interface{}, key string) int64 {
	if val, ok := stats[key]; ok {
		if intVal, ok := val.(int64); ok {
			return intVal
		}
	}
	return 0
}

func (dmm *DelayedMessageManager) GetMessage(messageID string) (*DelayedMessage, error) {
	return dmm.storage.Get(messageID)
}

func (dmm *DelayedMessageManager) CancelMessage(messageID string) error {
	message, err := dmm.storage.Get(messageID)
	if err != nil {
		return fmt.Errorf("failed to get message: %v", err)
	}

	if message == nil {
		return fmt.Errorf("message not found")
	}

	if message.Status != StatusPending {
		return fmt.Errorf("message is not in pending status")
	}

	message.MarkCanceled()
	return dmm.storage.Update(messageID, message)
}
