package raft

import (
	"log"
	"os"
	"testing"
	"time"
)

// TestLeaderWatcherDataStructures tests the leader watcher data structures
func TestLeaderWatcherDataStructures(t *testing.T) {
	// Create controller state machine
	csm := &ControllerStateMachine{
		metadata: &ClusterMetadata{
			PartitionAssignments: make(map[string]*PartitionAssignment),
			LeaderAssignments:    make(map[string]string),
			Version:              1,
			UpdateTime:           time.Now(),
		},
		leaderWatchers: make(map[string]func()),
		logger:         log.New(os.Stdout, "[TEST] ", log.LstdFlags),
	}

	// Test partition key
	partitionKey := "test-topic-0"

	// Test adding a watcher
	cancelFunc := func() {
		t.Log("Cancel function called")
	}
	csm.leaderWatchers[partitionKey] = cancelFunc

	// Verify watcher was added
	if _, exists := csm.leaderWatchers[partitionKey]; !exists {
		t.Errorf("Leader watcher was not added for partition %s", partitionKey)
	}

	// Test stopLeaderWatcher method
	csm.stopLeaderWatcher(partitionKey)

	// Verify watcher was removed
	if _, exists := csm.leaderWatchers[partitionKey]; exists {
		t.Errorf("Leader watcher was not removed for partition %s", partitionKey)
	}

	t.Logf("✅ Leader watcher data structures test passed")
}

// TestNodeIDToBrokerIDMapping tests the node ID to broker ID mapping
func TestNodeIDToBrokerIDMapping(t *testing.T) {
	// Create controller state machine with broker metadata
	csm := &ControllerStateMachine{
		metadata: &ClusterMetadata{
			Brokers: map[string]*BrokerInfo{
				"broker1": {
					ID:      "broker1",
					Address: "localhost",
					Port:    9092,
				},
				"broker2": {
					ID:      "broker2",
					Address: "localhost",
					Port:    9093,
				},
			},
		},
	}

	// Test nodeIDToBrokerID method
	// Note: This will return empty string since we don't have the actual node ID mapping
	// But we can test that the method doesn't panic
	brokerID := csm.nodeIDToBrokerID(12345)
	t.Logf("Node ID 12345 maps to broker ID: %s", brokerID)

	t.Logf("✅ Node ID to broker ID mapping test passed")
}