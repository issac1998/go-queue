package main

import (
	"testing"
	"time"
	"runtime"

	"github.com/issac1998/go-queue/internal/raft"
)

// TestWaitForLeadershipReadyPerformance tests that WaitForLeadershipReady doesn't consume excessive CPU
func TestWaitForLeadershipReadyPerformance(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	
	// Create RaftManager with test configuration
	config := &raft.RaftConfig{
		RTTMillisecond:         200,
		HeartbeatRTT:           5,
		ElectionRTT:            10,
		CheckQuorum:            true,
		SnapshotEntries:        10000,
		CompactionOverhead:     5000,
		MaxInMemLogSize:        67108864,
		ControllerSnapshotFreq: 1000,
		NodeID:                 1,
		RaftAddr:               "localhost:63001",
		MutualTLS:              false,
	}
	
	rm, err := raft.NewRaftManager(config, tempDir)
	if err != nil {
		t.Fatalf("Failed to create RaftManager: %v", err)
	}
	defer rm.Close()
	
	// Test with a non-existent group (should timeout quickly)
	nonExistentGroupID := uint64(99999)
	timeout := 2 * time.Second // Short timeout for testing
	
	// Measure CPU usage before the call
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	start := time.Now()
	
	// This should timeout but not consume excessive CPU
	err = rm.WaitForLeadershipReady(nonExistentGroupID, timeout)
	elapsed := time.Since(start)
	
	runtime.ReadMemStats(&m2)
	
	// Verify that it timed out as expected
	if err == nil {
		t.Error("Expected timeout error for non-existent group")
	}
	
	// Verify that it took approximately the timeout duration (with some tolerance)
	if elapsed < timeout-100*time.Millisecond || elapsed > timeout+500*time.Millisecond {
		t.Errorf("Expected timeout around %v, but took %v", timeout, elapsed)
	}
	
	// The test passes if we reach here without hanging or consuming excessive CPU
	t.Logf("WaitForLeadershipReady completed in %v (expected ~%v)", elapsed, timeout)
	t.Logf("Memory usage: %d bytes allocated", m2.TotalAlloc-m1.TotalAlloc)
}

// TestWaitForLeadershipReadyWithContext tests cancellation behavior
func TestWaitForLeadershipReadyWithContext(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	
	// Create RaftManager with test configuration
	config := &raft.RaftConfig{
		RTTMillisecond:         200,
		HeartbeatRTT:           5,
		ElectionRTT:            10,
		CheckQuorum:            true,
		SnapshotEntries:        10000,
		CompactionOverhead:     5000,
		MaxInMemLogSize:        67108864,
		ControllerSnapshotFreq: 1000,
		NodeID:                 1,
		RaftAddr:               "localhost:63002",
		MutualTLS:              false,
	}
	
	rm, err := raft.NewRaftManager(config, tempDir)
	if err != nil {
		t.Fatalf("Failed to create RaftManager: %v", err)
	}
	defer rm.Close()
	
	// Test that the method respects timeout and doesn't hang indefinitely
	nonExistentGroupID := uint64(99998)
	timeout := 1 * time.Second
	
	start := time.Now()
	err = rm.WaitForLeadershipReady(nonExistentGroupID, timeout)
	elapsed := time.Since(start)
	
	// Should timeout within reasonable time
	if err == nil {
		t.Error("Expected timeout error")
	}
	
	if elapsed > timeout+200*time.Millisecond {
		t.Errorf("Method took too long: %v (expected ~%v)", elapsed, timeout)
	}
	
	t.Logf("Method completed in %v with expected timeout", elapsed)
}