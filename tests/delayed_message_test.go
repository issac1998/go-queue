package tests

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/issac1998/go-queue/client"
)

// TestDelayedMessage 测试延迟消息功能
func TestDelayedMessage(t *testing.T) {
	log.Println("=== 开始延迟消息测试 ===")

	// 创建客户端配置
	config := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		Timeout:     10 * time.Second,
	}

	// 创建客户端
	c := client.NewClient(config)
	defer c.Close()

	// 创建延迟消息生产者
	delayedProducer := client.NewDelayedProducer(c)

	// 创建管理客户端和主题
	admin := client.NewAdmin(c)
	createReq := client.CreateTopicRequest{
		Name:       "test-delayed-topic",
		Partitions: 3,
		Replicas:   3,
	}
	if _, err := admin.CreateTopic(createReq); err != nil {
		log.Printf("创建主题失败 (可能已存在): %v", err)
	}

	// 等待主题创建完成
	time.Sleep(2 * time.Second)

	// 测试用例1: 使用延迟级别发送消息
	log.Println("测试1: 使用延迟级别发送消息")
	testDelayLevelMessage(t, delayedProducer)

	// 测试用例2: 使用指定时间发送消息
	log.Println("测试2: 使用指定时间发送消息")
	testDelayAtMessage(t, delayedProducer)

	// 测试用例3: 使用延迟时间发送消息
	log.Println("测试3: 使用延迟时间发送消息")
	testDelayAfterMessage(t, delayedProducer)

	// 测试用例4: 查询延迟消息状态
	log.Println("测试4: 查询延迟消息状态")
	testQueryDelayedMessage(t, delayedProducer)

	// 测试用例5: 取消延迟消息
	log.Println("测试5: 取消延迟消息")
	testCancelDelayedMessage(t, delayedProducer)

	log.Println("=== 延迟消息测试完成 ===")
}

// testDelayLevelMessage 测试使用延迟级别发送消息
func testDelayLevelMessage(t *testing.T, producer *client.DelayedProducer) {
	key := []byte("test-key-level")
	value := []byte(fmt.Sprintf("test message with delay level at %s", time.Now().Format("15:04:05")))

	// 发送5秒延迟的消息
	resp, err := producer.ProduceDelayed("test-delayed-topic", 0, key, value, client.DelayLevel5s)
	if err != nil {
		t.Fatalf("发送延迟消息失败: %v", err)
	}

	log.Printf("延迟消息已发送 - MessageID: %s, DeliverTime: %s",
		resp.MessageID, time.Unix(resp.DeliverTime/1000, 0).Format("15:04:05"))

	// 验证消息ID不为空
	if resp.MessageID == "" {
		t.Fatal("消息ID为空")
	}

	// 验证投递时间大于当前时间
	if resp.DeliverTime <= time.Now().UnixMilli() {
		t.Fatal("投递时间不正确")
	}
}

// testDelayAtMessage 测试使用指定时间发送消息
func testDelayAtMessage(t *testing.T, producer *client.DelayedProducer) {
	key := []byte("test-key-at")
	value := []byte(fmt.Sprintf("test message with delay at time %s", time.Now().Format("15:04:05")))

	// 设置10秒后投递
	deliverTime := time.Now().Add(10 * time.Second).UnixMilli()

	resp, err := producer.ProduceDelayedAt("test-delayed-topic", 1, key, value, deliverTime)
	if err != nil {
		t.Fatalf("发送定时消息失败: %v", err)
	}

	log.Printf("定时消息已发送 - MessageID: %s, DeliverTime: %s",
		resp.MessageID, time.Unix(resp.DeliverTime/1000, 0).Format("15:04:05"))

	// 验证投递时间
	if abs(resp.DeliverTime-deliverTime) > 1000 { // 允许1秒误差
		t.Fatal("投递时间不匹配")
	}
}

// testDelayAfterMessage 测试使用延迟时间发送消息
func testDelayAfterMessage(t *testing.T, producer *client.DelayedProducer) {
	key := []byte("test-key-after")
	value := []byte(fmt.Sprintf("test message with delay after duration %s", time.Now().Format("15:04:05")))

	// 设置15秒后投递
	delayDuration := 15 * time.Second

	resp, err := producer.ProduceDelayedAfter("test-delayed-topic", 2, key, value, delayDuration)
	if err != nil {
		t.Fatalf("发送延迟消息失败: %v", err)
	}

	log.Printf("延迟消息已发送 - MessageID: %s, DeliverTime: %s",
		resp.MessageID, time.Unix(resp.DeliverTime/1000, 0).Format("15:04:05"))

	// 验证投递时间在合理范围内
	expectedTime := time.Now().Add(delayDuration).UnixMilli()
	if abs(resp.DeliverTime-expectedTime) > 2000 { // 允许2秒误差
		t.Fatal("投递时间不在预期范围内")
	}
}

// testQueryDelayedMessage 测试查询延迟消息状态
func testQueryDelayedMessage(t *testing.T, producer *client.DelayedProducer) {
	key := []byte("test-key-query")
	value := []byte("test message for query")

	// 发送一个延迟消息
	resp, err := producer.ProduceDelayed("test-delayed-topic", 0, key, value, client.DelayLevel30s)
	if err != nil {
		t.Fatalf("发送延迟消息失败: %v", err)
	}

	messageID := resp.MessageID
	log.Printf("准备查询消息: %s", messageID)

	// 查询消息状态
	message, err := producer.QueryDelayedMessage(messageID)
	if err != nil {
		t.Fatalf("查询延迟消息失败: %v", err)
	}

	// 验证查询结果
	if message == nil {
		t.Fatal("查询结果为空")
	}

	if message.ID != messageID {
		t.Fatal("消息ID不匹配")
	}

	if message.Topic != "test-delayed-topic" {
		t.Fatal("主题不匹配")
	}

	log.Printf("查询成功 - Status: %d, DeliverTime: %s",
		message.Status, time.Unix(message.DeliverTime/1000, 0).Format("15:04:05"))
}

// testCancelDelayedMessage 测试取消延迟消息
func testCancelDelayedMessage(t *testing.T, producer *client.DelayedProducer) {
	key := []byte("test-key-cancel")
	value := []byte("test message for cancel")

	// 发送一个延迟消息
	resp, err := producer.ProduceDelayed("test-delayed-topic", 0, key, value, client.DelayLevel1h)
	if err != nil {
		t.Fatalf("发送延迟消息失败: %v", err)
	}

	messageID := resp.MessageID
	log.Printf("准备取消消息: %s", messageID)

	// 取消消息
	err = producer.CancelDelayedMessage(messageID)
	if err != nil {
		t.Fatalf("取消延迟消息失败: %v", err)
	}

	log.Printf("消息已取消: %s", messageID)

	// 再次查询确认已取消
	message, err := producer.QueryDelayedMessage(messageID)
	if err != nil {
		log.Printf("查询已取消的消息失败 (预期行为): %v", err)
	} else if message != nil && message.Status == 3 { // 假设3表示已取消状态
		log.Printf("消息状态已更新为已取消")
	}
}

// abs 返回整数的绝对值
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// TestDelayedMessageIntegration 集成测试：验证延迟消息是否按时投递
func TestDelayedMessageIntegration(t *testing.T) {
	log.Println("=== 开始延迟消息集成测试 ===")

	// 创建客户端配置
	config := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		Timeout:     10 * time.Second,
	}

	// 创建客户端
	c := client.NewClient(config)
	defer c.Close()

	// 创建延迟消息生产者
	delayedProducer := client.NewDelayedProducer(c)

	// 创建普通消费者验证消息投递
	consumer := client.NewConsumer(c)

	// 创建主题
	admin := client.NewAdmin(c)
	createReq := client.CreateTopicRequest{
		Name:       "test-delayed-integration",
		Partitions: 1,
		Replicas:   1,
	}
	if _, err := admin.CreateTopic(createReq); err != nil {
		log.Printf("创建主题失败 (可能已存在): %v", err)
	}

	time.Sleep(2 * time.Second)

	// 发送一个3秒延迟的消息
	key := []byte("integration-test-key")
	value := []byte("integration test message")
	sendTime := time.Now()

	resp, err := delayedProducer.ProduceDelayed("test-delayed-integration", 0, key, value, client.DelayLevel1s)
	if err != nil {
		t.Fatalf("发送延迟消息失败: %v", err)
	}

	log.Printf("延迟消息已发送，等待投递...")

	// 订阅主题并等待消息
	if err := consumer.Subscribe([]string{"test-delayed-integration"}); err != nil {
		t.Fatalf("订阅主题失败: %v", err)
	}

	// 等待消息到达
	timeout := time.After(10 * time.Second)
	messageReceived := false

	go func() {
		for {
			messages, err := consumer.Poll(1 * time.Second)
			if err != nil {
				continue
			}

			if len(messages) > 0 {
				receiveTime := time.Now()
				delay := receiveTime.Sub(sendTime)

				log.Printf("收到延迟消息! 实际延迟: %v", delay)

				// 验证延迟时间在合理范围内 (1-3秒)
				if delay >= 1*time.Second && delay <= 3*time.Second {
					messageReceived = true
					log.Printf("延迟时间符合预期")
				} else {
					log.Printf("警告: 延迟时间不在预期范围内: %v", delay)
				}
				return
			}
		}
	}()

	select {
	case <-timeout:
		t.Fatal("超时未收到延迟消息")
	case <-time.After(5 * time.Second):
		if !messageReceived {
			t.Fatal("未收到预期的延迟消息")
		}
	}

	log.Printf("集成测试完成 - MessageID: %s", resp.MessageID)
	log.Println("=== 延迟消息集成测试完成 ===")
}
