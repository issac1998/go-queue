package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/issac1998/go-queue/internal/broker"
	"github.com/issac1998/go-queue/internal/discovery"
	"github.com/issac1998/go-queue/internal/raft"
)

func TestIndependentLoggers(t *testing.T) {
	// Create temporary directories for two broker instances
	tempDir1, err := os.MkdirTemp("", "broker1_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir for broker1: %v", err)
	}
	defer os.RemoveAll(tempDir1)

	tempDir2, err := os.MkdirTemp("", "broker2_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir for broker2: %v", err)
	}
	defer os.RemoveAll(tempDir2)

	// Create broker configurations
	config1 := &broker.BrokerConfig{
		NodeID:   "broker1",
		BindAddr: "localhost",
		BindPort: 9001,
		DataDir:  tempDir1,
		LogDir:   tempDir1,
		Discovery: &discovery.DiscoveryConfig{
			Type: "memory",
		},
		RaftConfig: &raft.RaftConfig{
			RTTMillisecond:         200,
			CheckQuorum:            true,
			MaxInMemLogSize:        67108864,
			ControllerSnapshotFreq: 1000,
			NodeID:                 1,
			RaftAddr:               "localhost:63001",
		},
	}

	config2 := &broker.BrokerConfig{
		NodeID:   "broker2",
		BindAddr: "localhost",
		BindPort: 9002,
		DataDir:  tempDir2,
		LogDir:   tempDir2,
		Discovery: &discovery.DiscoveryConfig{
			Type: "memory",
		},
		RaftConfig: &raft.RaftConfig{
			RTTMillisecond:         200,
			CheckQuorum:            true,
			MaxInMemLogSize:        67108864,
			ControllerSnapshotFreq: 1000,
			NodeID:                 2,
			RaftAddr:               "localhost:63002",
		},
	}

	// Create broker instances
	broker1, err := broker.NewBroker(config1)
	if err != nil {
		t.Fatalf("Failed to create broker1: %v", err)
	}

	broker2, err := broker.NewBroker(config2)
	if err != nil {
		t.Fatalf("Failed to create broker2: %v", err)
	}

	// Start brokers to initialize logging
	go broker1.Start()
	defer broker1.Stop()

	go broker2.Start()
	defer broker2.Stop()

	// Wait a moment for initialization
	time.Sleep(100 * time.Millisecond)

	// Test that each broker has its own logger
	logger1 := broker1.GetLogger()
	logger2 := broker2.GetLogger()

	if logger1 == nil {
		t.Fatal("Broker1 logger is nil")
	}

	if logger2 == nil {
		t.Fatal("Broker2 logger is nil")
	}

	// Log messages from each broker
	logger1.Printf("Test message from broker1")
	logger2.Printf("Test message from broker2")

	// Wait for logs to be written
	time.Sleep(100 * time.Millisecond)

	// Check that log files exist and contain the expected messages
	logFile1 := filepath.Join(tempDir1, "broker-"+config1.NodeID+".log")
	logFile2 := filepath.Join(tempDir2, "broker-"+config2.NodeID+".log")

	// Check broker1 log file
	if _, err := os.Stat(logFile1); os.IsNotExist(err) {
		t.Errorf("Broker1 log file does not exist: %s", logFile1)
	} else {
		content1, err := os.ReadFile(logFile1)
		if err != nil {
			t.Errorf("Failed to read broker1 log file: %v", err)
		} else if !strings.Contains(string(content1), "Test message from broker1") {
			t.Errorf("Broker1 log file does not contain expected message")
		}
	}

	// Check broker2 log file
	if _, err := os.Stat(logFile2); os.IsNotExist(err) {
		t.Errorf("Broker2 log file does not exist: %s", logFile2)
	} else {
		content2, err := os.ReadFile(logFile2)
		if err != nil {
			t.Errorf("Failed to read broker2 log file: %v", err)
		} else if !strings.Contains(string(content2), "Test message from broker2") {
			t.Errorf("Broker2 log file does not contain expected message")
		}
	}

	t.Log("Independent logger test completed successfully")
}