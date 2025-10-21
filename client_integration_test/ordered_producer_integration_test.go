package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/testutil"
)

// TestOrderedProducerIntegration 测试 OrderedProducer 的完整功能
func TestOrderedProducerIntegration(t *testing.T) {
	// 创建测试集群
	cluster := testutil.NewTestCluster()

	// 启动集群
	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start test cluster: %v", err)
	}
	defer cluster.Stop()

	// 创建测试 topic
	topicName := "test-ordered-topic"
	partitions := int32(3)
	replicas := int32(1)

	if err := cluster.CreateTopic(topicName, partitions, replicas); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 创建 OrderedProducer
	producer := client.NewOrderedProducer(cluster.Client)
	if producer == nil {
		t.Fatal("Failed to create OrderedProducer")
	}

	// 测试发送消息
	testMessages := []struct {
		messageGroup string
		content      string
	}{
		{"user-1", "message-1-1"},
		{"user-1", "message-1-2"},
		{"user-2", "message-2-1"},
		{"user-1", "message-1-3"},
		{"user-3", "message-3-1"},
		{"user-2", "message-2-2"},
	}

	// 发送消息并记录分区
	partitionMap := make(map[string]int32)
	for _, msg := range testMessages {
		result, err := producer.SendSingleOrderedMessage(topicName, msg.messageGroup, nil, []byte(msg.content))
		if err != nil {
			t.Fatalf("Failed to send message: %v", err)
		}

		var partition int32 = -1
		if len(result.PartitionResponses) > 0 {
			for _, resp := range result.PartitionResponses {
				partition = resp.Partition
				break
			}
		}

		t.Logf("Sent message '%s' from group '%s' to partition %d",
			msg.content, msg.messageGroup, partition)

		// 验证同一个 MessageGroup 的消息都发送到同一个分区
		if existingPartition, exists := partitionMap[msg.messageGroup]; exists {
			if existingPartition != partition {
				t.Errorf("Messages from group '%s' sent to different partitions: %d vs %d",
					msg.messageGroup, existingPartition, partition)
			}
		} else {
			partitionMap[msg.messageGroup] = partition
		}

		// 验证分区在有效范围内
		if partition < 0 || partition >= partitions {
			t.Errorf("Partition %d is out of range [0, %d)", partition, partitions)
		}
	}

	// 验证不同的 MessageGroup 可能分布在不同分区
	t.Logf("Partition distribution: %v", partitionMap)
	if len(partitionMap) > 1 {
		uniquePartitions := make(map[int32]bool)
		for _, partition := range partitionMap {
			uniquePartitions[partition] = true
		}
		t.Logf("Used %d different partitions out of %d total", len(uniquePartitions), partitions)
	}
}

// TestOrderedProducerBatch 测试批量发送有序消息
func TestOrderedProducerBatch(t *testing.T) {
	// 创建测试集群
	cluster := testutil.NewTestCluster()

	// 启动集群
	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start test cluster: %v", err)
	}
	defer cluster.Stop()

	// 创建测试 topic
	topicName := "test-batch-ordered-topic"
	partitions := int32(2)
	replicas := int32(1)

	if err := cluster.CreateTopic(topicName, partitions, replicas); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 创建 OrderedProducer
	producer := client.NewOrderedProducer(cluster.Client)
	if producer == nil {
		t.Fatal("Failed to create OrderedProducer")
	}

	// 准备批量消息
	messageGroup := "batch-group-1"
	messages := []client.OrderedMessage{
		{
			MessageGroup: messageGroup,
			Value:        []byte("batch-message-1"),
		},
		{
			MessageGroup: messageGroup,
			Value:        []byte("batch-message-2"),
		},
		{
			MessageGroup: messageGroup,
			Value:        []byte("batch-message-3"),
		},
	}

	// 发送批量消息
	result, err := producer.SendMessageGroupBatch(topicName, messageGroup, messages)
	if err != nil {
		t.Fatalf("Failed to send batch messages: %v", err)
	}

	// 验证结果
	if result.TotalMessages != len(messages) {
		t.Errorf("Expected %d total messages, got %d", len(messages), result.TotalMessages)
	}

	if result.SuccessfulMessages != len(messages) {
		t.Errorf("Expected %d successful messages, got %d", len(messages), result.SuccessfulMessages)
	}

	// 验证所有消息都发送到同一个分区
	var partition int32 = -1
	for _, resp := range result.PartitionResponses {
		if partition == -1 {
			partition = resp.Partition
		} else if partition != resp.Partition {
			t.Errorf("Batch messages sent to different partitions: %d vs %d", partition, resp.Partition)
		}
	}

	t.Logf("Batch messages sent to partition %d", partition)
}

// TestOrderedProducerErrorHandling 测试错误处理
func TestOrderedProducerErrorHandling(t *testing.T) {
	// 创建测试集群
	cluster := testutil.NewTestCluster()

	// 启动集群
	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start test cluster: %v", err)
	}
	defer cluster.Stop()

	// 创建 OrderedProducer
	producer := client.NewOrderedProducer(cluster.Client)
	if producer == nil {
		t.Fatal("Failed to create OrderedProducer")
	}

	// 测试发送到不存在的 topic
	nonExistentTopic := "non-existent-topic"
	_, err := producer.SendSingleOrderedMessage(nonExistentTopic, "test-group", nil, []byte("test-message"))
	if err == nil {
		t.Error("Expected error when sending to non-existent topic, but got nil")
	} else {
		t.Logf("Got expected error for non-existent topic: %v", err)
	}
}

// TestOrderedProducerConcurrency 测试并发安全性
func TestOrderedProducerConcurrency(t *testing.T) {
	// 创建测试集群
	cluster := testutil.NewTestCluster()

	// 启动集群
	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start test cluster: %v", err)
	}
	defer cluster.Stop()

	// 创建测试 topic
	topicName := "test-concurrent-topic"
	partitions := int32(4)
	replicas := int32(1)

	if err := cluster.CreateTopic(topicName, partitions, replicas); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 创建 OrderedProducer
	producer := client.NewOrderedProducer(cluster.Client)
	if producer == nil {
		t.Fatal("Failed to create OrderedProducer")
	}

	// 并发发送消息
	numGoroutines := 10
	messagesPerGoroutine := 5
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer func() { done <- true }()

			messageGroup := fmt.Sprintf("group-%d", goroutineID)
			for j := 0; j < messagesPerGoroutine; j++ {
				message := fmt.Sprintf("message-%d-%d", goroutineID, j)
				_, err := producer.SendSingleOrderedMessage(topicName, messageGroup, nil, []byte(message))
				if err != nil {
					t.Errorf("Goroutine %d failed to send message %d: %v", goroutineID, j, err)
					return
				}
			}
		}(i)
	}

	// 等待所有 goroutine 完成
	timeout := time.After(30 * time.Second)
	completed := 0
	for completed < numGoroutines {
		select {
		case <-done:
			completed++
		case <-timeout:
			t.Fatal("Test timed out waiting for goroutines to complete")
		}
	}

	t.Logf("Successfully completed concurrent test with %d goroutines", numGoroutines)
}
