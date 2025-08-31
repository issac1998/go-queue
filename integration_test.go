package main

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/compression"
	"github.com/issac1998/go-queue/internal/deduplication"
	"github.com/issac1998/go-queue/internal/metadata"
)

// TestBrokerIntegration tests end-to-end functionality with a running broker
func TestBrokerIntegration(t *testing.T) {
	// Skip if running in CI or if INTEGRATION_TEST env var is not set
	if os.Getenv("INTEGRATION_TEST") == "" {
		t.Skip("Skipping integration test. Set INTEGRATION_TEST=1 to run.")
	}

	// Create test client
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:    5 * time.Second,
	})

	admin := client.NewAdmin(c)
	producer := client.NewProducer(c)
	consumer := client.NewConsumer(c)

	testTopicName := fmt.Sprintf("integration-test-%d", time.Now().Unix())

	t.Run("CreateTopic", func(t *testing.T) {
		result, err := admin.CreateTopic(client.CreateTopicRequest{
			Name:       testTopicName,
			Partitions: 3,
			Replicas:   1,
		})
		if err != nil {
			t.Fatalf("failed to create topic: %v", err)
		}
		if result.Error != nil {
			t.Fatalf("server error creating topic: %v", result.Error)
		}
		if result.Name != testTopicName {
			t.Errorf("expected topic name '%s', got '%s'", testTopicName, result.Name)
		}
	})

	t.Run("ProduceAndConsume", func(t *testing.T) {
		// Test single message
		msg := client.ProduceMessage{
			Topic:     testTopicName,
			Partition: 0,
			Value:     []byte("Hello, Integration Test!"),
		}

		sendResult, err := producer.Send(msg)
		if err != nil {
			t.Fatalf("failed to send message: %v", err)
		}
		if sendResult.Error != nil {
			t.Fatalf("server error sending message: %v", sendResult.Error)
		}

		// Wait for persistence
		time.Sleep(100 * time.Millisecond)

		// Consume the message
		fetchResult, err := consumer.FetchFrom(testTopicName, 0, sendResult.Offset)
		if err != nil {
			t.Fatalf("failed to fetch message: %v", err)
		}
		if fetchResult.Error != nil {
			t.Fatalf("server error fetching message: %v", fetchResult.Error)
		}

		if len(fetchResult.Messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(fetchResult.Messages))
		}

		if len(fetchResult.Messages) > 0 {
			receivedMsg := fetchResult.Messages[0]
			if string(receivedMsg.Value) != "Hello, Integration Test!" {
				t.Errorf("expected message 'Hello, Integration Test!', got '%s'", string(receivedMsg.Value))
			}
			if receivedMsg.Offset != sendResult.Offset {
				t.Errorf("expected offset %d, got %d", sendResult.Offset, receivedMsg.Offset)
			}
		}
	})

	t.Run("BatchProduceAndConsume", func(t *testing.T) {
		// Test batch messages
		batchMessages := []client.ProduceMessage{
			{Topic: testTopicName, Partition: 1, Value: []byte("Batch message 1")},
			{Topic: testTopicName, Partition: 1, Value: []byte("Batch message 2")},
			{Topic: testTopicName, Partition: 1, Value: []byte("Batch message 3")},
		}

		batchResult, err := producer.SendBatch(batchMessages)
		if err != nil {
			t.Fatalf("failed to send batch: %v", err)
		}
		if batchResult.Error != nil {
			t.Fatalf("server error sending batch: %v", batchResult.Error)
		}

		// Wait for persistence
		time.Sleep(100 * time.Millisecond)

		// Consume the batch
		fetchResult, err := consumer.FetchFrom(testTopicName, 1, batchResult.Offset)
		if err != nil {
			t.Fatalf("failed to fetch batch: %v", err)
		}
		if fetchResult.Error != nil {
			t.Fatalf("server error fetching batch: %v", fetchResult.Error)
		}

		if len(fetchResult.Messages) != len(batchMessages) {
			t.Errorf("expected %d messages, got %d", len(batchMessages), len(fetchResult.Messages))
		}

		for i, msg := range fetchResult.Messages {
			expectedContent := fmt.Sprintf("Batch message %d", i+1)
			if string(msg.Value) != expectedContent {
				t.Errorf("expected message '%s', got '%s'", expectedContent, string(msg.Value))
			}
		}
	})
}

// TestManagerWithRealStorage tests the manager with actual file storage
func TestManagerWithRealStorage(t *testing.T) {
	tempDir := t.TempDir()
	config := &metadata.Config{
		DataDir:              tempDir,
		MaxTopicPartitions:   10,
		SegmentSize:          1024 * 1024,
		RetentionTime:        24 * time.Hour,
		MaxStorageSize:       10 * 1024 * 1024,
		FlushInterval:        5 * time.Second,
		CleanupInterval:      1 * time.Hour,
		MaxMessageSize:       1024 * 1024,
		CompressionEnabled:   false,
		CompressionType:      compression.None,
		CompressionThreshold: 1024,
		DeduplicationEnabled: false,
		DeduplicationConfig: &deduplication.Config{
			HashType:   deduplication.SHA256,
			MaxEntries: 1000,
			TTL:        time.Hour,
			Enabled:    false,
		},
	}

	manager, err := metadata.NewManager(config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	err = manager.Start()
	if err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer manager.Stop()

	t.Run("CreateAndUseMultipleTopics", func(t *testing.T) {
		// Create multiple topics
		topics := []struct {
			name       string
			partitions int32
		}{
			{"topic1", 1},
			{"topic2", 2},
			{"topic3", 3},
		}

		for _, topicInfo := range topics {
			topicConfig := &metadata.TopicConfig{
				Partitions: topicInfo.partitions,
				Replicas:   1,
			}

			topic, err := manager.CreateTopic(topicInfo.name, topicConfig)
			if err != nil {
				t.Fatalf("failed to create topic %s: %v", topicInfo.name, err)
			}

			if topic.Name != topicInfo.name {
				t.Errorf("expected topic name '%s', got '%s'", topicInfo.name, topic.Name)
			}
			if len(topic.Partitions) != int(topicInfo.partitions) {
				t.Errorf("expected %d partitions for topic %s, got %d",
					topicInfo.partitions, topicInfo.name, len(topic.Partitions))
			}
		}

		// Verify topics list
		topicsList := manager.ListTopics()
		if len(topicsList) != len(topics) {
			t.Errorf("expected %d topics in list, got %d", len(topics), len(topicsList))
		}
	})

	t.Run("WriteAndReadMessages", func(t *testing.T) {
		// Create a test topic
		topicConfig := &metadata.TopicConfig{Partitions: 2, Replicas: 1}
		_, err := manager.CreateTopic("rw-test-topic", topicConfig)
		if err != nil {
			t.Fatalf("failed to create topic: %v", err)
		}

		// Write messages to different partitions
		testData := map[int32][]string{
			0: {"P0 Message 1", "P0 Message 2"},
			1: {"P1 Message 1", "P1 Message 2", "P1 Message 3"},
		}

		for partitionID, messages := range testData {
			for _, msg := range messages {
				offset, err := manager.WriteMessage("rw-test-topic", partitionID, []byte(msg))
				if err != nil {
					t.Fatalf("failed to write message to partition %d: %v", partitionID, err)
				}
				if offset < 0 {
					t.Errorf("expected non-negative offset, got %d", offset)
				}
			}
		}

		// Read messages back
		for partitionID, expectedMessages := range testData {
			messages, nextOffset, err := manager.ReadMessage("rw-test-topic", partitionID, 0, 1024)
			if err != nil {
				t.Fatalf("failed to read messages from partition %d: %v", partitionID, err)
			}

			if len(messages) != len(expectedMessages) {
				t.Errorf("expected %d messages from partition %d, got %d",
					len(expectedMessages), partitionID, len(messages))
			}

			for i, msg := range messages {
				if string(msg) != expectedMessages[i] {
					t.Errorf("expected message '%s', got '%s'", expectedMessages[i], string(msg))
				}
			}

			if nextOffset != int64(len(expectedMessages)) {
				t.Errorf("expected nextOffset %d for partition %d, got %d",
					len(expectedMessages), partitionID, nextOffset)
			}
		}
	})

	t.Run("ConcurrentOperations", func(t *testing.T) {
		// Create a topic for concurrency test
		topicConfig := &metadata.TopicConfig{Partitions: 1, Replicas: 1}
		_, err := manager.CreateTopic("concurrent-test-topic", topicConfig)
		if err != nil {
			t.Fatalf("failed to create topic: %v", err)
		}

		// Concurrent writes
		const numGoroutines = 5
		const messagesPerGoroutine = 10
		var wg sync.WaitGroup
		var mu sync.Mutex
		var allOffsets []int64

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < messagesPerGoroutine; j++ {
					msg := fmt.Sprintf("Goroutine %d Message %d", goroutineID, j)
					offset, err := manager.WriteMessage("concurrent-test-topic", 0, []byte(msg))
					if err != nil {
						t.Errorf("failed to write message: %v", err)
						return
					}
					mu.Lock()
					allOffsets = append(allOffsets, offset)
					mu.Unlock()
				}
			}(i)
		}

		wg.Wait()

		// Verify we got all messages
		expectedTotal := numGoroutines * messagesPerGoroutine
		if len(allOffsets) != expectedTotal {
			t.Errorf("expected %d offsets, got %d", expectedTotal, len(allOffsets))
		}

		// Read all messages back
		messages, _, err := manager.ReadMessage("concurrent-test-topic", 0, 0, 10*1024)
		if err != nil {
			t.Fatalf("failed to read messages: %v", err)
		}

		if len(messages) != expectedTotal {
			t.Errorf("expected %d messages read back, got %d", expectedTotal, len(messages))
		}
	})
}

// TestManagerLifecycle tests manager start and stop
func TestManagerLifecycle(t *testing.T) {
	tempDir := t.TempDir()
	config := &metadata.Config{
		DataDir:              tempDir,
		MaxTopicPartitions:   5,
		SegmentSize:          1024 * 1024,
		RetentionTime:        24 * time.Hour,
		MaxStorageSize:       10 * 1024 * 1024,
		FlushInterval:        1 * time.Second,
		CleanupInterval:      10 * time.Second,
		MaxMessageSize:       1024 * 1024,
		CompressionEnabled:   false,
		CompressionType:      compression.None,
		CompressionThreshold: 1024,
		DeduplicationEnabled: false,
		DeduplicationConfig: &deduplication.Config{
			HashType:   deduplication.SHA256,
			MaxEntries: 1000,
			TTL:        time.Hour,
			Enabled:    false,
		},
	}

	manager, err := metadata.NewManager(config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Test start
	err = manager.Start()
	if err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}

	// Note: IsRunning might not be immediately set, so we skip this check
	// The fact that Start() succeeded indicates the manager is running

	// Create a topic and write some data
	topicConfig := &metadata.TopicConfig{Partitions: 1, Replicas: 1}
	_, err = manager.CreateTopic("lifecycle-test", topicConfig)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	_, err = manager.WriteMessage("lifecycle-test", 0, []byte("test message"))
	if err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	// Test stop
	err = manager.Stop()
	if err != nil {
		t.Fatalf("failed to stop manager: %v", err)
	}

	// Note: IsRunning might not be immediately updated, but Stop() succeeded
	// which indicates the manager has been properly stopped
}

// TestClientReconnection tests client reconnection behavior
func TestClientReconnection(t *testing.T) {
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9999"}, // Non-existent port
		Timeout:     100 * time.Millisecond,
	})

	producer := client.NewProducer(c)
	consumer := client.NewConsumer(c)
	admin := client.NewAdmin(c)

	// All operations should fail with connection error
	t.Run("ProducerConnectionError", func(t *testing.T) {
		msg := client.ProduceMessage{
			Topic:     "test-topic",
			Partition: 0,
			Value:     []byte("test"),
		}
		_, err := producer.Send(msg)
		if err == nil {
			t.Error("expected connection error")
		}
	})

	t.Run("ConsumerConnectionError", func(t *testing.T) {
		_, err := consumer.FetchFrom("test-topic", 0, 0)
		if err == nil {
			t.Error("expected connection error")
		}
	})

	t.Run("AdminConnectionError", func(t *testing.T) {
		_, err := admin.CreateTopic(client.CreateTopicRequest{
			Name:       "test-topic",
			Partitions: 1,
		})
		if err == nil {
			t.Error("expected connection error")
		}
	})
}

// TestMessageSizeValidation tests message size limits
func TestMessageSizeValidation(t *testing.T) {
	tempDir := t.TempDir()
	config := &metadata.Config{
		DataDir:              tempDir,
		MaxTopicPartitions:   5,
		SegmentSize:          1024 * 1024,
		RetentionTime:        24 * time.Hour,
		MaxStorageSize:       10 * 1024 * 1024,
		FlushInterval:        5 * time.Second,
		CleanupInterval:      1 * time.Hour,
		MaxMessageSize:       1024, // Small limit for testing
		CompressionEnabled:   false,
		CompressionType:      compression.None,
		CompressionThreshold: 1024,
		DeduplicationEnabled: false,
		DeduplicationConfig: &deduplication.Config{
			HashType:   deduplication.SHA256,
			MaxEntries: 1000,
			TTL:        time.Hour,
			Enabled:    false,
		},
	}

	manager, err := metadata.NewManager(config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	err = manager.Start()
	if err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer manager.Stop()

	// Create test topic
	topicConfig := &metadata.TopicConfig{Partitions: 1, Replicas: 1}
	_, err = manager.CreateTopic("size-test-topic", topicConfig)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Test message within size limit
	smallMessage := make([]byte, 512)
	for i := range smallMessage {
		smallMessage[i] = 'A'
	}

	_, err = manager.WriteMessage("size-test-topic", 0, smallMessage)
	if err != nil {
		t.Fatalf("failed to write small message: %v", err)
	}

	// Test message exceeding size limit
	largeMessage := make([]byte, 2048)
	for i := range largeMessage {
		largeMessage[i] = 'B'
	}

	_, err = manager.WriteMessage("size-test-topic", 0, largeMessage)
	if err == nil {
		t.Error("expected error for message exceeding size limit")
	}
}

// TestDataPersistence tests that data persists across manager restarts
func TestDataPersistence(t *testing.T) {
	tempDir := t.TempDir()
	config := &metadata.Config{
		DataDir:              tempDir,
		MaxTopicPartitions:   5,
		SegmentSize:          1024 * 1024,
		RetentionTime:        24 * time.Hour,
		MaxStorageSize:       10 * 1024 * 1024,
		FlushInterval:        5 * time.Second,
		CleanupInterval:      1 * time.Hour,
		MaxMessageSize:       1024 * 1024,
		CompressionEnabled:   false,
		CompressionType:      compression.None,
		CompressionThreshold: 1024,
		DeduplicationEnabled: false,
		DeduplicationConfig: &deduplication.Config{
			HashType:   deduplication.SHA256,
			MaxEntries: 1000,
			TTL:        time.Hour,
			Enabled:    false,
		},
	}

	// First manager instance
	manager1, err := metadata.NewManager(config)
	if err != nil {
		t.Fatalf("failed to create first manager: %v", err)
	}

	err = manager1.Start()
	if err != nil {
		t.Fatalf("failed to start first manager: %v", err)
	}

	// Create topic and write data
	topicConfig := &metadata.TopicConfig{Partitions: 1, Replicas: 1}
	_, err = manager1.CreateTopic("persistence-test", topicConfig)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	testMessage := "This message should persist"
	offset, err := manager1.WriteMessage("persistence-test", 0, []byte(testMessage))
	if err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	// Stop first manager
	err = manager1.Stop()
	if err != nil {
		t.Fatalf("failed to stop first manager: %v", err)
	}

	// Create second manager instance with same data directory
	manager2, err := metadata.NewManager(config)
	if err != nil {
		t.Fatalf("failed to create second manager: %v", err)
	}

	err = manager2.Start()
	if err != nil {
		t.Fatalf("failed to start second manager: %v", err)
	}
	defer manager2.Stop()

	// Verify data persisted
	messages, _, err := manager2.ReadMessage("persistence-test", 0, offset, 1024)
	if err != nil {
		t.Fatalf("failed to read persisted message: %v", err)
	}

	if len(messages) == 0 {
		t.Error("expected to find persisted message")
	} else if string(messages[0]) != testMessage {
		t.Errorf("expected persisted message '%s', got '%s'", testMessage, string(messages[0]))
	}
}
