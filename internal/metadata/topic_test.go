package metadata

import (
	"testing"
)

func TestNewTopic(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	topicConfig := &TopicConfig{
		Partitions: 3,
		Replicas:   1,
	}

	topic, err := NewTopic("test-topic", topicConfig, config)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	defer topic.Close()

	if topic.Name != "test-topic" {
		t.Errorf("expected topic name 'test-topic', got '%s'", topic.Name)
	}
	if len(topic.Partitions) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(topic.Partitions))
	}
	if topic.Config.Partitions != 3 {
		t.Errorf("expected config partitions 3, got %d", topic.Config.Partitions)
	}

	// Verify all partitions are created correctly
	for i := int32(0); i < 3; i++ {
		partition, exists := topic.Partitions[i]
		if !exists {
			t.Errorf("partition %d not found", i)
		}
		if partition.ID != i {
			t.Errorf("expected partition ID %d, got %d", i, partition.ID)
		}
		if partition.Topic != "test-topic" {
			t.Errorf("expected partition topic 'test-topic', got '%s'", partition.Topic)
		}
	}
}

func TestTopicGetPartition(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	topicConfig := &TopicConfig{
		Partitions: 2,
		Replicas:   1,
	}

	topic, err := NewTopic("test-topic", topicConfig, config)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	defer topic.Close()

	// Test getting existing partition
	partition, err := topic.GetPartition(0)
	if err != nil {
		t.Fatalf("failed to get partition 0: %v", err)
	}
	if partition.ID != 0 {
		t.Errorf("expected partition ID 0, got %d", partition.ID)
	}

	// Test getting non-existent partition
	_, err = topic.GetPartition(5)
	if err == nil {
		t.Error("expected error when getting non-existent partition")
	}
}

func TestNewPartition(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	partition, err := NewPartition(0, "test-topic", config)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}
	defer partition.Close()

	if partition.ID != 0 {
		t.Errorf("expected partition ID 0, got %d", partition.ID)
	}
	if partition.Topic != "test-topic" {
		t.Errorf("expected topic 'test-topic', got '%s'", partition.Topic)
	}
	if partition.ActiveSeg == nil {
		t.Error("active segment should not be nil")
	}
	if len(partition.Segments) == 0 {
		t.Error("segments map should not be empty")
	}
}

func TestPartitionAppend(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	partition, err := NewPartition(0, "test-topic", config)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}
	defer partition.Close()

	// Test appending messages
	testMessages := []string{
		"Message 1",
		"Message 2",
		"Message 3",
	}

	var offsets []int64
	for i, msg := range testMessages {
		offset, err := partition.Append([]byte(msg))
		if err != nil {
			t.Fatalf("failed to append message %d: %v", i, err)
		}
		offsets = append(offsets, offset)
	}

	// Verify offsets are sequential
	for i, offset := range offsets {
		if offset != int64(i) {
			t.Errorf("expected offset %d, got %d", i, offset)
		}
	}
}

func TestPartitionRead(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	partition, err := NewPartition(0, "test-topic", config)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}
	defer partition.Close()

	// Append test messages
	testMessages := []string{
		"Message 1",
		"Message 2",
		"Message 3",
	}

	for _, msg := range testMessages {
		_, err := partition.Append([]byte(msg))
		if err != nil {
			t.Fatalf("failed to append message: %v", err)
		}
	}

	// Test reading all messages
	messages, nextOffset, err := partition.Read(0, 1024)
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

	// Test reading from specific offset
	messages, nextOffset, err = partition.Read(1, 1024)
	if err != nil {
		t.Fatalf("failed to read messages from offset 1: %v", err)
	}

	expectedMessages := testMessages[1:] // Should get messages 1 and 2
	if len(messages) != len(expectedMessages) {
		t.Errorf("expected %d messages from offset 1, got %d", len(expectedMessages), len(messages))
	}
}

func TestPartitionClose(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	partition, err := NewPartition(0, "test-topic", config)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}

	// Append a message
	_, err = partition.Append([]byte("test message"))
	if err != nil {
		t.Fatalf("failed to append message: %v", err)
	}

	// Test close
	err = partition.Close()
	if err != nil {
		t.Fatalf("failed to close partition: %v", err)
	}
}

func TestTopicClose(t *testing.T) {
	tempDir := t.TempDir()
	config := createTestConfig(tempDir)

	topicConfig := &TopicConfig{
		Partitions: 2,
		Replicas:   1,
	}

	topic, err := NewTopic("test-topic", topicConfig, config)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Append messages to partitions
	for i := int32(0); i < 2; i++ {
		partition := topic.Partitions[i]
		_, err := partition.Append([]byte("test message"))
		if err != nil {
			t.Fatalf("failed to append message to partition %d: %v", i, err)
		}
	}

	// Test close
	err = topic.Close()
	if err != nil {
		t.Fatalf("failed to close topic: %v", err)
	}
}
