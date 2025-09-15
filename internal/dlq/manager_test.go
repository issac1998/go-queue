package dlq

import (
	"testing"
	"time"

	"github.com/issac1998/go-queue/client"
)

func TestNewManager(t *testing.T) {
	// Create a real client for testing
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	}
	client := client.NewClient(clientConfig)

	config := &DLQConfig{}
	config.Enabled = true
	config.RetryPolicy = DefaultRetryPolicy()
	config.RetryPolicy.MaxRetries = 3
	config.TopicSuffix = ".dlq"

	manager, err := NewManager(client, config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	if manager == nil {
		t.Fatal("manager should not be nil")
	}

	if !manager.config.Enabled {
		t.Error("manager should be enabled")
	}
}

func TestShouldRetryMessage(t *testing.T) {
	// Create a real client for testing
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	}
	client := client.NewClient(clientConfig)

	config := &DLQConfig{}
	config.Enabled = true
	config.RetryPolicy = DefaultRetryPolicy()
	config.RetryPolicy.MaxRetries = 2

	manager, err := NewManager(client, config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	topic := "test-topic"
	partition := int32(0)
	offset := int64(100)
	consumerGroup := "test-group"

	// Should not retry non-existent message
	should := manager.ShouldRetryMessage(topic, partition, offset, consumerGroup)
	if should {
		t.Error("should not retry non-existent message")
	}
}

func TestGetRetryDelay(t *testing.T) {
	// Create a real client for testing
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	}
	client := client.NewClient(clientConfig)

	config := &DLQConfig{}
	config.Enabled = true
	config.RetryPolicy = DefaultRetryPolicy()
	config.RetryPolicy.InitialDelay = time.Second
	config.RetryPolicy.BackoffFactor = 2.0
	config.RetryPolicy.MaxDelay = time.Minute

	manager, err := NewManager(client, config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	topic := "test-topic"
	partition := int32(0)
	offset := int64(100)
	consumerGroup := "test-group"

	// Should return 0 for non-existent message
	delay := manager.GetRetryDelay(topic, partition, offset, consumerGroup)
	if delay != 0 {
		t.Errorf("expected 0 delay for non-existent message, got %v", delay)
	}
}

func TestGetDLQStats(t *testing.T) {
	// Create a real client for testing
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	}
	client := client.NewClient(clientConfig)

	config := &DLQConfig{}
	config.Enabled = true
	config.RetryPolicy = DefaultRetryPolicy()

	manager, err := NewManager(client, config)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	stats := manager.GetDLQStats()
	if stats == nil {
		t.Fatal("stats should not be nil")
	}

	if stats.TotalMessages < 0 {
		t.Error("total messages should not be negative")
	}
}

func TestRetryPolicyDefaults(t *testing.T) {
	policy := DefaultRetryPolicy()
	if policy == nil {
		t.Fatal("default retry policy should not be nil")
	}

	if policy.MaxRetries <= 0 {
		t.Error("max retries should be positive")
	}

	if policy.InitialDelay <= 0 {
		t.Error("initial delay should be positive")
	}

	if policy.BackoffFactor <= 1.0 {
		t.Error("backoff factor should be greater than 1.0")
	}
}

func TestDLQConfigDefaults(t *testing.T) {
	config := DefaultDLQConfig()
	if config == nil {
		t.Fatal("default DLQ config should not be nil")
	}

	if !config.Enabled {
		t.Error("DLQ should be enabled by default")
	}

	if config.TopicSuffix == "" {
		t.Error("topic suffix should not be empty")
	}

	if config.RetryPolicy == nil {
		t.Error("retry policy should not be nil")
	}
}