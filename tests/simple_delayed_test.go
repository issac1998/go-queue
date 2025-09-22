package tests

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/issac1998/go-queue/internal/delayed"
)

// TestSimpleDelayedMessage 简单的延迟消息测试
func TestSimpleDelayedMessage(t *testing.T) {
	log.Println("=== 开始简单延迟消息测试 ===")

	// 记录投递的消息
	delivered := make(chan string, 10)
	deliveryCallback := func(topic string, partition int32, key, value []byte) error {
		message := string(value)
		log.Printf("投递消息: %s", message)
		delivered <- message
		return nil
	}

	// 创建延迟消息管理器
	config := delayed.DelayedMessageManagerConfig{
		DataDir:         "./simple_test_data",
		MaxRetries:      3,
		CleanupInterval: 1 * time.Minute,
	}

	manager := delayed.NewDelayedMessageManager(config, deliveryCallback)

	// 启动管理器
	if err := manager.Start(); err != nil {
		t.Fatalf("启动延迟消息管理器失败: %v", err)
	}
	defer func() {
		manager.Stop()
		// 清理测试数据
		os.RemoveAll("./simple_test_data")
	}()

	// 调度一个500毫秒延迟的消息
	req := &delayed.DelayedProduceRequest{
		Topic:     "test-topic",
		Partition: 0,
		Key:       []byte("test-key"),
		Value:     []byte("hello delayed message"),
		DelayTime: 500, // 500毫秒延迟
	}

	startTime := time.Now()
	resp, err := manager.ScheduleMessage(req)
	if err != nil {
		t.Fatalf("调度消息失败: %v", err)
	}

	log.Printf("消息已调度 - ID: %s", resp.MessageID)

	// 等待消息投递
	select {
	case msg := <-delivered:
		endTime := time.Now()
		delay := endTime.Sub(startTime)
		log.Printf("收到消息: %s, 实际延迟: %v", msg, delay)

		if msg != "hello delayed message" {
			t.Fatalf("消息内容不正确: %s", msg)
		}

		// 验证延迟时间在合理范围内 (500ms - 1500ms)，考虑到时间轮1秒精度
		if delay < 500*time.Millisecond || delay > 1500*time.Millisecond {
			t.Fatalf("延迟时间不在预期范围内: %v", delay)
		}

	case <-time.After(3 * time.Second):
		t.Fatal("超时未收到消息")
	}

	log.Println("=== 简单延迟消息测试完成 ===")
}
