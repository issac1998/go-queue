package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/transaction"
)

// MockTransactionListener 模拟事务监听器
type MockTransactionListener struct{}

func (m *MockTransactionListener) ExecuteLocalTransaction(transactionID transaction.TransactionID, messageID string) transaction.TransactionState {
	// 模拟本地事务执行成功
	return transaction.StateCommit
}

func (m *MockTransactionListener) CheckLocalTransaction(transactionID transaction.TransactionID, messageID string) transaction.TransactionState {
	// 模拟检查本地事务状态
	return transaction.StateCommit
}

// ExecuteBatchLocalTransaction 执行批量本地事务
func (m *MockTransactionListener) ExecuteBatchLocalTransaction(transactionID transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	// 模拟批量本地事务执行成功
	return transaction.StateCommit
}

// CheckBatchLocalTransaction 检查批量本地事务状态
func (m *MockTransactionListener) CheckBatchLocalTransaction(transactionID transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	// 模拟检查批量本地事务状态
	return transaction.StateCommit
}

// TestConfiguration 测试配置
var TestConfig = struct {
	BrokerAddrs []string
	Timeout     time.Duration
}{
	BrokerAddrs: []string{"127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"},
	Timeout:     10 * time.Second,
}

// TestBasicProduceConsume 测试基础的生产消费功能
func TestBasicProduceConsume(t *testing.T) {
	// 创建客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: TestConfig.BrokerAddrs,
		Timeout:     TestConfig.Timeout,
	})

	// 发现Controller
	err := c.DiscoverController()
	if err != nil {
		t.Fatalf("Failed to discover controller: %v", err)
	}

	// 创建管理员客户端
	admin := client.NewAdmin(c)

	// 创建测试Topic
	topicName := fmt.Sprintf("test-topic-%d", time.Now().Unix())
	result, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 3,
		Replicas:   1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	t.Logf("Created topic: %s", result.Name)

	// 创建生产者
	producer := client.NewProducer(c)

	// 发送测试消息
	testMessage := []byte("Hello, Go-Queue Integration Test!")
	sendResult, err := producer.Send(client.ProduceMessage{
		Topic:     topicName,
		Partition: 0,
		Value:     testMessage,
	})
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}
	t.Logf("Message sent with offset: %d", sendResult.Offset)

	// 创建消费者
	consumer := client.NewConsumer(c)

	// 消费消息
	fetchResult, err := consumer.FetchFrom(topicName, 0, 0)
	if err != nil {
		t.Fatalf("Failed to fetch message: %v", err)
	}

	if len(fetchResult.Messages) == 0 {
		t.Fatal("No messages received")
	}

	receivedMessage := fetchResult.Messages[0]
	if string(receivedMessage.Value) != string(testMessage) {
		t.Fatalf("Message mismatch. Expected: %s, Got: %s",
			string(testMessage), string(receivedMessage.Value))
	}

	t.Logf("Successfully received message: %s", string(receivedMessage.Value))
}

// TestBatchProduceConsume 测试批量生产消费功能
func TestBatchProduceConsume(t *testing.T) {
	// 创建客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: TestConfig.BrokerAddrs,
		Timeout:     TestConfig.Timeout,
	})

	err := c.DiscoverController()
	if err != nil {
		t.Fatalf("Failed to discover controller: %v", err)
	}

	admin := client.NewAdmin(c)

	// 创建测试Topic
	topicName := fmt.Sprintf("batch-test-topic-%d", time.Now().Unix())
	_, err = admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
		Replicas:   1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	producer := client.NewProducer(c)

	// 批量发送消息
	messages := []client.ProduceMessage{
		{Topic: topicName, Partition: 0, Value: []byte("Message 1")},
		{Topic: topicName, Partition: 0, Value: []byte("Message 2")},
		{Topic: topicName, Partition: 0, Value: []byte("Message 3")},
		{Topic: topicName, Partition: 0, Value: []byte("Message 4")},
		{Topic: topicName, Partition: 0, Value: []byte("Message 5")},
	}

	batchResult, err := producer.SendBatch(messages)
	if err != nil {
		t.Fatalf("Failed to send batch messages: %v", err)
	}
	t.Logf("Batch sent starting from offset: %d", batchResult.Offset)

	// 消费所有消息
	consumer := client.NewConsumer(c)
	fetchResult, err := consumer.FetchFrom(topicName, 0, 0)
	if err != nil {
		t.Fatalf("Failed to fetch messages: %v", err)
	}

	if len(fetchResult.Messages) != len(messages) {
		t.Fatalf("Expected %d messages, got %d", len(messages), len(fetchResult.Messages))
	}

	// 验证消息内容
	for i, msg := range fetchResult.Messages {
		expectedContent := fmt.Sprintf("Message %d", i+1)
		if string(msg.Value) != expectedContent {
			t.Fatalf("Message %d mismatch. Expected: %s, Got: %s",
				i, expectedContent, string(msg.Value))
		}
	}

	t.Logf("Successfully sent and received %d batch messages", len(messages))
}

// TestConsumerGroup 测试消费者组功能
func TestConsumerGroup(t *testing.T) {
	// 创建客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: TestConfig.BrokerAddrs,
		Timeout:     TestConfig.Timeout,
	})

	err := c.DiscoverController()
	if err != nil {
		t.Fatalf("Failed to discover controller: %v", err)
	}

	admin := client.NewAdmin(c)

	// 创建测试Topic
	topicName := fmt.Sprintf("group-test-topic-%d", time.Now().Unix())
	_, err = admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 2,
		Replicas:   1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 先发送一些消息
	producer := client.NewProducer(c)
	for i := 0; i < 10; i++ {
		_, err := producer.Send(client.ProduceMessage{
			Topic:     topicName,
			Partition: int32(i % 2), // 轮询发送到两个分区
			Value:     []byte(fmt.Sprintf("Group test message %d", i)),
		})
		if err != nil {
			t.Fatalf("Failed to send message %d: %v", i, err)
		}
	}

	// 创建消费者组消费者
	groupID := fmt.Sprintf("test-group-%d", time.Now().Unix())
	groupConsumer := client.NewGroupConsumer(c, client.GroupConsumerConfig{
		GroupID:        groupID,
		ConsumerID:     "consumer-1",
		Topics:         []string{topicName},
		SessionTimeout: 30 * time.Second,
	})

	// 加入消费者组
	err = groupConsumer.JoinGroup()
	if err != nil {
		t.Fatalf("Failed to join group: %v", err)
	}
	defer groupConsumer.LeaveGroup()

	// 获取分区分配
	assignment := groupConsumer.GetAssignment()
	t.Logf("Consumer assigned to partitions: %v", assignment)

	// 验证分区分配
	if len(assignment[topicName]) == 0 {
		t.Fatal("No partitions assigned to consumer")
	}

	t.Logf("Consumer group test completed successfully")
}

// TestTopicManagement 测试Topic管理功能
func TestTopicManagement(t *testing.T) {
	// 创建客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: TestConfig.BrokerAddrs,
		Timeout:     TestConfig.Timeout,
	})

	err := c.DiscoverController()
	if err != nil {
		t.Fatalf("Failed to discover controller: %v", err)
	}

	admin := client.NewAdmin(c)

	// 创建测试Topic
	topicName := fmt.Sprintf("mgmt-test-topic-%d", time.Now().Unix())
	createResult, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 4,
		Replicas:   1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	t.Logf("Created topic: %s", createResult.Name)

	// 列出所有Topic
	topics, err := admin.ListTopics()
	if err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	}

	// 验证创建的Topic在列表中
	found := false
	for _, topic := range topics {
		if topic.Name == topicName {
			found = true
			if topic.Partitions != 4 {
				t.Fatalf("Expected 4 partitions, got %d", topic.Partitions)
			}
			break
		}
	}

	if !found {
		t.Fatalf("Created topic %s not found in topic list", topicName)
	}

	t.Logf("Topic management test completed successfully")
}

// TestHighLoadScenario 测试高负载场景
func TestHighLoadScenario(t *testing.T) {
	// 创建客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: TestConfig.BrokerAddrs,
		Timeout:     TestConfig.Timeout,
	})

	err := c.DiscoverController()
	if err != nil {
		t.Fatalf("Failed to discover controller: %v", err)
	}

	admin := client.NewAdmin(c)

	// 创建测试Topic
	topicName := fmt.Sprintf("load-test-topic-%d", time.Now().Unix())
	_, err = admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 3,
		Replicas:   1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	producer := client.NewProducer(c)

	// 高负载发送消息
	messageCount := 100
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	for i := 0; i < messageCount; i++ {
		select {
		case <-ctx.Done():
			t.Fatal("Test timeout during message sending")
		default:
		}

		_, err := producer.Send(client.ProduceMessage{
			Topic:     topicName,
			Partition: int32(i % 3),
			Value:     []byte(fmt.Sprintf("Load test message %d", i)),
		})
		if err != nil {
			t.Fatalf("Failed to send message %d: %v", i, err)
		}
	}
	sendDuration := time.Since(start)

	t.Logf("Sent %d messages in %v (%.2f msg/s)",
		messageCount, sendDuration, float64(messageCount)/sendDuration.Seconds())

	// 验证消息数量
	consumer := client.NewConsumer(c)
	totalReceived := 0

	for partition := int32(0); partition < 3; partition++ {
		fetchResult, err := consumer.FetchFrom(topicName, partition, 0)
		if err != nil {
			t.Fatalf("Failed to fetch from partition %d: %v", partition, err)
		}
		totalReceived += len(fetchResult.Messages)
	}

	if totalReceived != messageCount {
		t.Fatalf("Expected %d messages, received %d", messageCount, totalReceived)
	}

	t.Logf("High load test completed successfully: %d messages processed", totalReceived)
}

// TestErrorHandling 测试错误处理
func TestErrorHandling(t *testing.T) {
	// 创建客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: TestConfig.BrokerAddrs,
		Timeout:     TestConfig.Timeout,
	})

	err := c.DiscoverController()
	if err != nil {
		t.Fatalf("Failed to discover controller: %v", err)
	}

	admin := client.NewAdmin(c)
	producer := client.NewProducer(c)
	consumer := client.NewConsumer(c)

	// 测试发送到不存在的Topic
	nonExistentTopic := "non-existent-topic-12345"
	_, err = producer.Send(client.ProduceMessage{
		Topic:     nonExistentTopic,
		Partition: 0,
		Value:     []byte("test message"),
	})
	if err == nil {
		t.Fatal("Expected error when sending to non-existent topic")
	}
	t.Logf("Correctly handled non-existent topic error: %v", err)

	// 测试从不存在的Topic消费
	_, err = consumer.FetchFrom(nonExistentTopic, 0, 0)
	if err == nil {
		t.Fatal("Expected error when fetching from non-existent topic")
	}
	t.Logf("Correctly handled non-existent topic fetch error: %v", err)

	// 测试创建重复的Topic
	topicName := fmt.Sprintf("duplicate-test-%d", time.Now().Unix())
	_, err = admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
		Replicas:   1,
	})
	if err != nil {
		t.Fatalf("Failed to create initial topic: %v", err)
	}

	// 尝试创建同名Topic
	_, err = admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
		Replicas:   1,
	})
	if err == nil {
		t.Fatal("Expected error when creating duplicate topic")
	}
	t.Logf("Correctly handled duplicate topic error: %v", err)

	t.Logf("Error handling test completed successfully")
}

// TestClusterResilience 测试集群弹性
func TestClusterResilience(t *testing.T) {
	// 创建客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: TestConfig.BrokerAddrs,
		Timeout:     TestConfig.Timeout,
	})

	err := c.DiscoverController()
	if err != nil {
		t.Fatalf("Failed to discover controller: %v", err)
	}

	admin := client.NewAdmin(c)

	// 创建测试Topic
	topicName := fmt.Sprintf("resilience-test-%d", time.Now().Unix())
	_, err = admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 3,
		Replicas:   1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	producer := client.NewProducer(c)

	// 测试在多个broker间发送消息
	for i := 0; i < 10; i++ {
		_, err := producer.Send(client.ProduceMessage{
			Topic:     topicName,
			Partition: int32(i % 3),
			Value:     []byte(fmt.Sprintf("Resilience test message %d", i)),
		})
		if err != nil {
			t.Fatalf("Failed to send message %d: %v", i, err)
		}
	}

	// 验证消息在所有分区中
	consumer := client.NewConsumer(c)
	totalMessages := 0

	for partition := int32(0); partition < 3; partition++ {
		fetchResult, err := consumer.FetchFrom(topicName, partition, 0)
		if err != nil {
			t.Fatalf("Failed to fetch from partition %d: %v", partition, err)
		}
		totalMessages += len(fetchResult.Messages)
		t.Logf("Partition %d has %d messages", partition, len(fetchResult.Messages))
	}

	if totalMessages != 10 {
		t.Fatalf("Expected 10 messages total, got %d", totalMessages)
	}

	t.Logf("Cluster resilience test completed successfully")
}

// Helper function to cleanup test resources
func cleanupTestResources(t *testing.T, admin *client.Admin, topicNames []string) {
	for _, topicName := range topicNames {
		err := admin.DeleteTopic(topicName)
		if err != nil {
			t.Logf("Warning: Failed to cleanup topic %s: %v", topicName, err)
		}
	}
}
