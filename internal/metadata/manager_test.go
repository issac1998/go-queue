package metadata

import (
	"testing"
	"time"

	"github.com/issac1998/go-queue/internal/compression"
	"github.com/issac1998/go-queue/internal/deduplication"
)

func createTestConfig(dataDir string) *Config {
	return &Config{
		DataDir:              dataDir,
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
}

func TestNewManager(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer manager.Stop()

	if manager.Config != config {
		t.Error("manager config not set correctly")
	}
	if manager.Topics == nil {
		t.Error("topics map not initialized")
	}
}

func TestManagerCreateTopic(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer manager.Stop()

	// Test creating a topic
	topicConfig := &TopicConfig{
		Partitions: 3,
		Replicas:   1,
	}

	topic, err := manager.CreateTopic("test-topic", topicConfig)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if topic.Name != "test-topic" {
		t.Errorf("expected topic name 'test-topic', got '%s'", topic.Name)
	}
	if len(topic.Partitions) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(topic.Partitions))
	}

	// Test creating duplicate topic
	_, err = manager.CreateTopic("test-topic", topicConfig)
	if err == nil {
		t.Error("expected error when creating duplicate topic")
	}

	// Test creating topic with too many partitions
	largeTopicConfig := &TopicConfig{
		Partitions: 100, // Exceeds MaxTopicPartitions
		Replicas:   1,
	}
	_, err = manager.CreateTopic("large-topic", largeTopicConfig)
	if err == nil {
		t.Error("expected error when creating topic with too many partitions")
	}
}

func TestManagerGetTopic(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer manager.Stop()

	// Test getting non-existent topic
	_, err = manager.GetTopic("non-existent")
	if err == nil {
		t.Error("expected error when getting non-existent topic")
	}

	// Create a topic
	topicConfig := &TopicConfig{Partitions: 2, Replicas: 1}
	_, err = manager.CreateTopic("test-topic", topicConfig)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Test getting existing topic
	topic, err := manager.GetTopic("test-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	if topic.Name != "test-topic" {
		t.Errorf("expected topic name 'test-topic', got '%s'", topic.Name)
	}
}

func TestManagerGetPartition(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer manager.Stop()

	// Create a topic with partitions
	topicConfig := &TopicConfig{Partitions: 3, Replicas: 1}
	_, err = manager.CreateTopic("test-topic", topicConfig)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Test getting existing partition
	partition, err := manager.GetPartition("test-topic", 1)
	if err != nil {
		t.Fatalf("failed to get partition: %v", err)
	}
	if partition.ID != 1 {
		t.Errorf("expected partition ID 1, got %d", partition.ID)
	}

	// Test getting non-existent partition
	_, err = manager.GetPartition("test-topic", 5)
	if err == nil {
		t.Error("expected error when getting non-existent partition")
	}

	// Test getting partition from non-existent topic
	_, err = manager.GetPartition("non-existent", 0)
	if err == nil {
		t.Error("expected error when getting partition from non-existent topic")
	}
}

func TestManagerWriteMessage(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer manager.Stop()

	// Create a topic
	topicConfig := &TopicConfig{Partitions: 1, Replicas: 1}
	_, err = manager.CreateTopic("test-topic", topicConfig)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Test writing message
	message := []byte("Hello, World!")
	offset, err := manager.WriteMessage("test-topic", 0, message)
	if err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	if offset != 0 {
		t.Errorf("expected offset 0 for first message, got %d", offset)
	}

	// Test writing to non-existent topic
	_, err = manager.WriteMessage("non-existent", 0, message)
	if err == nil {
		t.Error("expected error when writing to non-existent topic")
	}

	// Test writing message that's too large
	config.MaxMessageSize = 10
	largeMessage := make([]byte, 100)
	_, err = manager.WriteMessage("test-topic", 0, largeMessage)
	if err == nil {
		t.Error("expected error when writing message that's too large")
	}
}

func TestManagerReadMessage(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer manager.Stop()

	// Create a topic
	topicConfig := &TopicConfig{Partitions: 1, Replicas: 1}
	_, err = manager.CreateTopic("test-topic", topicConfig)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Write test messages
	testMessages := []string{
		"Message 1",
		"Message 2",
		"Message 3",
	}

	for _, msg := range testMessages {
		_, err := manager.WriteMessage("test-topic", 0, []byte(msg))
		if err != nil {
			t.Fatalf("failed to write message: %v", err)
		}
	}

	// Test reading messages
	messages, nextOffset, err := manager.ReadMessage("test-topic", 0, 0, 1024)
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}

	if len(messages) != len(testMessages) {
		t.Errorf("expected %d messages, got %d", len(testMessages), len(messages))
	}

	for i, msg := range messages {
		if string(msg) != testMessages[i] {
			t.Errorf("expected message '%s', got '%s'", testMessages[i], string(msg))
		}
	}

	if nextOffset != int64(len(testMessages)) {
		t.Errorf("expected nextOffset %d, got %d", len(testMessages), nextOffset)
	}

	// Test reading from non-existent topic
	_, _, err = manager.ReadMessage("non-existent", 0, 0, 1024)
	if err == nil {
		t.Error("expected error when reading from non-existent topic")
	}
}

func TestManagerListTopics(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer manager.Stop()

	// Initially should have no topics
	topics := manager.ListTopics()
	if len(topics) != 0 {
		t.Errorf("expected 0 topics initially, got %d", len(topics))
	}

	// Create some topics
	topicNames := []string{"topic1", "topic2", "topic3"}
	topicConfig := &TopicConfig{Partitions: 1, Replicas: 1}

	for _, name := range topicNames {
		_, err := manager.CreateTopic(name, topicConfig)
		if err != nil {
			t.Fatalf("failed to create topic %s: %v", name, err)
		}
	}

	// Test listing topics
	topics = manager.ListTopics()
	if len(topics) != len(topicNames) {
		t.Errorf("expected %d topics, got %d", len(topicNames), len(topics))
	}

	// Verify all topic names are present
	topicMap := make(map[string]bool)
	for _, topic := range topics {
		topicMap[topic] = true
	}

	for _, expectedName := range topicNames {
		if !topicMap[expectedName] {
			t.Errorf("expected topic '%s' not found in list", expectedName)
		}
	}
}

func TestManagerStats(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer manager.Stop()

	// Get initial stats
	stats := manager.GetStats()
	if stats == nil {
		t.Error("stats should not be nil")
	}
	if stats.TotalTopics != 0 {
		t.Errorf("expected 0 topics initially, got %d", stats.TotalTopics)
	}

	// Create a topic and verify stats update
	topicConfig := &TopicConfig{Partitions: 2, Replicas: 1}
	_, err = manager.CreateTopic("test-topic", topicConfig)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	stats = manager.GetStats()
	if stats.TotalTopics != 1 {
		t.Errorf("expected 1 topic after creation, got %d", stats.TotalTopics)
	}
}
