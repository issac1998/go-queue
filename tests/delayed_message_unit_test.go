package tests

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/issac1998/go-queue/internal/delayed"
)

// TestDelayedMessageManagerUnit 延迟消息管理器单元测试
func TestDelayedMessageManagerUnit(t *testing.T) {
	log.Println("=== 开始延迟消息管理器单元测试 ===")

	// 创建临时目录
	tempDir := "./test_delayed_data"
	defer os.RemoveAll(tempDir)

	// 记录投递的消息
	deliveredMessages := make([]string, 0)
	deliveryCallback := func(topic string, partition int32, key, value []byte) error {
		message := string(value)
		deliveredMessages = append(deliveredMessages, message)
		log.Printf("Delivered message: %s to topic: %s, partition: %d", message, topic, partition)
		return nil
	}

	// 创建延迟消息管理器
	config := delayed.DelayedMessageManagerConfig{
		DataDir:         tempDir,
		MaxRetries:      3,
		CleanupInterval: 1 * time.Minute,
	}

	manager := delayed.NewDelayedMessageManager(config, deliveryCallback)

	// 启动管理器
	if err := manager.Start(); err != nil {
		t.Fatalf("启动延迟消息管理器失败: %v", err)
	}
	defer manager.Stop()

	// 测试1: 调度一个1秒延迟的消息
	log.Println("测试1: 调度1秒延迟消息")
	req1 := &delayed.DelayedProduceRequest{
		Topic:      "test-topic",
		Partition:  0,
		Key:        []byte("test-key-1"),
		Value:      []byte("test message 1"),
		DelayLevel: int32(delayed.DelayLevel1s),
	}

	resp1, err := manager.ScheduleMessage(req1)
	if err != nil {
		t.Fatalf("调度消息失败: %v", err)
	}

	log.Printf("消息已调度 - ID: %s, 投递时间: %s",
		resp1.MessageID, time.Unix(resp1.DeliverTime/1000, 0).Format("15:04:05"))

	// 验证消息ID不为空
	if resp1.MessageID == "" {
		t.Fatal("消息ID为空")
	}

	// 测试2: 查询消息状态
	log.Println("测试2: 查询消息状态")
	message, err := manager.GetMessage(resp1.MessageID)
	if err != nil {
		t.Fatalf("查询消息失败: %v", err)
	}

	if message.ID != resp1.MessageID {
		t.Fatal("消息ID不匹配")
	}

	if message.Status != delayed.StatusPending {
		t.Fatal("消息状态不正确")
	}

	log.Printf("消息状态: %d, 主题: %s", message.Status, message.Topic)

	// 测试3: 等待消息投递
	log.Println("测试3: 等待消息投递")

	// 轮询等待消息投递，最多等待5秒
	delivered := false
	for i := 0; i < 50; i++ {
		if len(deliveredMessages) > 0 {
			delivered = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !delivered {
		t.Fatal("消息未被投递")
	}

	if deliveredMessages[0] != "test message 1" {
		t.Fatalf("投递的消息内容不正确: %s", deliveredMessages[0])
	}

	log.Printf("成功投递消息: %s", deliveredMessages[0])

	// 测试4: 调度另一个消息
	log.Println("测试4: 调度另一个消息")

	req2 := &delayed.DelayedProduceRequest{
		Topic:     "test-topic",
		Partition: 1,
		Key:       []byte("test-key-2"),
		Value:     []byte("test message 2"),
		DelayTime: 1000, // 1秒
	}

	resp2, err := manager.ScheduleMessage(req2)
	if err != nil {
		t.Fatalf("调度第二个消息失败: %v", err)
	}
	log.Printf("第二个消息已调度 - ID: %s", resp2.MessageID)

	// 等待第二个消息投递
	delivered = false
	for i := 0; i < 30; i++ {
		if len(deliveredMessages) >= 2 {
			delivered = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !delivered {
		t.Fatalf("第二个消息未被投递，当前消息数: %d", len(deliveredMessages))
	}

	log.Printf("成功投递了%d个消息", len(deliveredMessages))

	// 测试5: 获取统计信息
	log.Println("测试5: 获取统计信息")
	stats := manager.GetStats()
	log.Printf("统计信息 - 总消息: %d, 待投递: %d, 已投递: %d",
		stats.TotalMessages, stats.PendingMessages, stats.DeliveredMessages)

	if stats.TotalMessages < 2 {
		t.Fatalf("统计信息中总消息数不正确: %d", stats.TotalMessages)
	}

	// 测试6: 取消消息
	log.Println("测试6: 取消消息")

	// 调度一个较长延迟的消息
	cancelReq := &delayed.DelayedProduceRequest{
		Topic:      "test-topic",
		Partition:  0,
		Key:        []byte("cancel-key"),
		Value:      []byte("message to cancel"),
		DelayLevel: int32(delayed.DelayLevel30s), // 30秒延迟
	}

	cancelResp, err := manager.ScheduleMessage(cancelReq)
	if err != nil {
		t.Fatalf("调度待取消消息失败: %v", err)
	}

	// 立即取消消息
	if err := manager.CancelMessage(cancelResp.MessageID); err != nil {
		t.Fatalf("取消消息失败: %v", err)
	}

	// 验证消息已被取消
	cancelledMessage, err := manager.GetMessage(cancelResp.MessageID)
	if err != nil {
		t.Fatalf("查询已取消消息失败: %v", err)
	}

	if cancelledMessage.Status != delayed.StatusCanceled {
		t.Fatalf("消息状态不正确: 期望%d, 实际%d", delayed.StatusCanceled, cancelledMessage.Status)
	}

	log.Printf("消息已成功取消: %s", cancelResp.MessageID)

	log.Println("=== 延迟消息管理器单元测试完成 ===")
}

// TestTimeWheelUnit 时间轮单元测试
func TestTimeWheelUnit(t *testing.T) {
	log.Println("=== 开始时间轮单元测试 ===")

	executedTasks := make([]string, 0)
	callback := func(task *delayed.TimeWheelTask) {
		executedTasks = append(executedTasks, task.ID)
		log.Printf("执行任务: %s", task.ID)
	}

	// 创建时间轮 (10个槽位，100ms精度)
	timeWheel := delayed.NewTimeWheel(10, 100, callback)
	timeWheel.Start()
	defer timeWheel.Stop()

	// 添加一个200ms后执行的任务
	task1 := &delayed.TimeWheelTask{
		ID:          "task-1",
		ExecuteTime: time.Now().Add(200 * time.Millisecond).UnixMilli(),
		Data:        "test data 1",
	}
	timeWheel.AddTask(task1)

	// 添加一个500ms后执行的任务
	task2 := &delayed.TimeWheelTask{
		ID:          "task-2",
		ExecuteTime: time.Now().Add(500 * time.Millisecond).UnixMilli(),
		Data:        "test data 2",
	}
	timeWheel.AddTask(task2)

	// 等待任务执行
	time.Sleep(1 * time.Second)

	// 验证任务是否被执行
	if len(executedTasks) != 2 {
		t.Fatalf("执行任务数量不正确: 期望2个, 实际%d个", len(executedTasks))
	}

	// 验证任务执行顺序
	if executedTasks[0] != "task-1" || executedTasks[1] != "task-2" {
		t.Fatalf("任务执行顺序不正确: %v", executedTasks)
	}

	log.Printf("时间轮成功执行了%d个任务", len(executedTasks))

	// 获取统计信息
	stats := timeWheel.GetStats()
	log.Printf("时间轮统计 - 槽位数: %d, 精度: %dms", stats.SlotCount, stats.TickMs)

	log.Println("=== 时间轮单元测试完成 ===")
}
