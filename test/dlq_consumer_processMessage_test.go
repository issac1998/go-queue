package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/dlq"
)

// Message alias for client.Message
type Message = client.Message

// TestProcessMessage_Success 测试成功处理消息
func TestProcessMessage_Success(t *testing.T) {
	// 创建客户端
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     10 * time.Second,
	}
	client := client.NewClient(clientConfig)
	defer client.Close()
	
	// 创建DLQ配置
	dlqConfig := dlq.DefaultDLQConfig()
	dlqConfig.RetryPolicy.MaxRetries = 3
	dlqConfig.RetryPolicy.InitialDelay = time.Second
	dlqConfig.TopicSuffix = ".dlq"
	
	// 创建DLQ消费者
	consumer, err := dlq.NewDlqConsumerWithConfig(client, "test-group", "consumer-1", dlqConfig)
	if err != nil {
		t.Fatalf("Failed to create DLQ consumer: %v", err)
	}
	defer consumer.Stop()
	
	// 创建测试消息
	message := &Message{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    1,
		Value:     []byte("test message"),
	}
	
	// 定义成功处理器
	processor := func(msg *Message) error {
		t.Logf("Processing message: %s", string(msg.Value))
		return nil
	}
	
	// 处理消息
	err = consumer.ProcessMessage(message, processor)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
}

// TestProcessMessage_WithRetry 测试带重试的消息处理
func TestProcessMessage_WithRetry(t *testing.T) {
	// 创建客户端
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     10 * time.Second,
	}
	client := client.NewClient(clientConfig)
	defer client.Close()
	
	// 创建DLQ配置
	dlqConfig := dlq.DefaultDLQConfig()
	dlqConfig.RetryPolicy.MaxRetries = 3
	dlqConfig.RetryPolicy.InitialDelay = time.Millisecond * 100
	dlqConfig.TopicSuffix = ".dlq"
	
	// 创建DLQ消费者
	consumer, err := dlq.NewDlqConsumerWithConfig(client, "test-group", "consumer-1", dlqConfig)
	if err != nil {
		t.Fatalf("Failed to create DLQ consumer: %v", err)
	}
	defer consumer.Stop()
	
	// 创建测试消息
	message := &Message{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    2,
		Value:     []byte("retry test message"),
	}
	
	// 定义带重试逻辑的处理器
	attemptCount := 0
	processor := func(msg *Message) error {
		attemptCount++
		t.Logf("Processing attempt %d: %s", attemptCount, string(msg.Value))
		
		// 前2次失败，第3次成功
		if attemptCount <= 2 {
			return fmt.Errorf("temporary failure on attempt %d", attemptCount)
		}
		
		return nil
	}
	
	// 处理消息
	err = consumer.ProcessMessage(message, processor)
	if err != nil {
		t.Errorf("Expected no error after retries, got: %v", err)
	}
	
	// 验证重试次数
	if attemptCount != 3 {
		t.Errorf("Expected 3 attempts, got: %d", attemptCount)
	}
}

// TestProcessMessage_MaxRetriesExceeded 测试超过最大重试次数
func TestProcessMessage_MaxRetriesExceeded(t *testing.T) {
	// 创建客户端
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     10 * time.Second,
	}
	client := client.NewClient(clientConfig)
	defer client.Close()
	
	// 创建DLQ配置
	dlqConfig := dlq.DefaultDLQConfig()
	dlqConfig.RetryPolicy.MaxRetries = 2
	dlqConfig.RetryPolicy.InitialDelay = time.Millisecond * 50
	dlqConfig.TopicSuffix = ".dlq"
	
	// 创建DLQ消费者
	consumer, err := dlq.NewDlqConsumerWithConfig(client, "test-group", "consumer-1", dlqConfig)
	if err != nil {
		t.Fatalf("Failed to create DLQ consumer: %v", err)
	}
	defer consumer.Stop()
	
	// 创建测试消息
	message := &Message{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    3,
		Value:     []byte("failed message"),
	}
	
	// 定义总是失败的处理器
	attemptCount := 0
	processor := func(msg *Message) error {
		attemptCount++
		t.Logf("Processing attempt %d: %s", attemptCount, string(msg.Value))
		return fmt.Errorf("persistent failure on attempt %d", attemptCount)
	}
	
	// 处理消息
	err = consumer.ProcessMessage(message, processor)
	if err == nil {
		t.Error("Expected error after max retries exceeded, got nil")
	}
	
	// 验证重试次数（初始尝试 + 重试次数）
	expectedAttempts := 1 + dlqConfig.RetryPolicy.MaxRetries
	if attemptCount != expectedAttempts {
		t.Errorf("Expected %d attempts, got: %d", expectedAttempts, attemptCount)
	}
}

// TestProcessMessage_Concurrent 测试并发处理消息
func TestProcessMessage_Concurrent(t *testing.T) {
	// 创建客户端
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     10 * time.Second,
	}
	client := client.NewClient(clientConfig)
	defer client.Close()
	
	// 创建DLQ配置
	dlqConfig := dlq.DefaultDLQConfig()
	dlqConfig.RetryPolicy.MaxRetries = 1
	dlqConfig.RetryPolicy.InitialDelay = time.Millisecond * 10
	dlqConfig.TopicSuffix = ".dlq"
	
	// 创建DLQ消费者
	consumer, err := dlq.NewDlqConsumerWithConfig(client, "test-group", "consumer-1", dlqConfig)
	if err != nil {
		t.Fatalf("Failed to create DLQ consumer: %v", err)
	}
	defer consumer.Stop()
	
	// 定义处理器
	processor := func(msg *Message) error {
		t.Logf("Processing concurrent message: %s", string(msg.Value))
		return nil
	}
	
	// 并发处理多个消息
	const numMessages = 5
	done := make(chan bool, numMessages)
	
	for i := 0; i < numMessages; i++ {
		go func(id int) {
			message := &Message{
				Topic:     "test-topic",
				Partition: 0,
				Offset:    int64(id + 10),
				Value:     []byte(fmt.Sprintf("concurrent message %d", id)),
			}
			
			err := consumer.ProcessMessage(message, processor)
			if err != nil {
				t.Errorf("Concurrent processing failed for message %d: %v", id, err)
			}
			
			done <- true
		}(i)
	}
	
	// 等待所有goroutine完成
	for i := 0; i < numMessages; i++ {
		<-done
	}
}