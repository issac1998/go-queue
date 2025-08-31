package broker

import (
	"testing"
	"time"

	"github.com/issac1998/go-queue/internal/discovery"
	"github.com/issac1998/go-queue/internal/raft"
)

func TestNewBroker(t *testing.T) {
	config := &BrokerConfig{
		NodeID:   "test-broker",
		BindAddr: "127.0.0.1",
		BindPort: 9092,
		DataDir:  "./test-data",
		RaftConfig: &raft.RaftConfig{
			RTTMillisecond:     200,
			HeartbeatRTT:       5,
			ElectionRTT:        10,
			CheckQuorum:        true,
			SnapshotEntries:    10000,
			CompactionOverhead: 5000,
			MaxInMemLogSize:    67108864,
			ControllerGroupID:  1,
		},
		Discovery: &discovery.DiscoveryConfig{
			Type:    "memory",
			Timeout: "5s",
		},
	}

	broker, err := NewBroker(config)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	if broker.ID != config.NodeID {
		t.Errorf("Expected broker ID %s, got %s", config.NodeID, broker.ID)
	}

	if broker.Address != config.BindAddr {
		t.Errorf("Expected broker address %s, got %s", config.BindAddr, broker.Address)
	}

	if broker.Port != config.BindPort {
		t.Errorf("Expected broker port %d, got %d", config.BindPort, broker.Port)
	}
}

func TestBrokerStartStop(t *testing.T) {
	config := &BrokerConfig{
		NodeID:   "test-broker",
		BindAddr: "127.0.0.1",
		BindPort: 19092, // Use different port to avoid conflicts
		DataDir:  "./test-data",
		RaftConfig: &raft.RaftConfig{
			RTTMillisecond:     200,
			HeartbeatRTT:       5,
			ElectionRTT:        10,
			CheckQuorum:        true,
			SnapshotEntries:    10000,
			CompactionOverhead: 5000,
			MaxInMemLogSize:    67108864,
			ControllerGroupID:  1,
		},
		Discovery: &discovery.DiscoveryConfig{
			Type:    "memory",
			Timeout: "5s",
		},
	}

	broker, err := NewBroker(config)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	// Note: We're not actually starting the broker because it requires
	// external dependencies like etcd and proper file system setup
	// For now, just test creation and basic validation

	// Test configuration validation
	if err := validateConfig(config); err != nil {
		t.Errorf("Configuration validation failed: %v", err)
	}

	// Test invalid configurations
	invalidConfig := &BrokerConfig{}
	if err := validateConfig(invalidConfig); err == nil {
		t.Error("Expected validation to fail for empty config")
	}

	// Clean up
	if broker.cancel != nil {
		broker.cancel()
	}

	// Wait a bit for cleanup
	time.Sleep(100 * time.Millisecond)
}
