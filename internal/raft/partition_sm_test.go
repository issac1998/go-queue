package raft

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/issac1998/go-queue/internal/compression"
)

func TestPartitionStateMachineHalfMessageStorage(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "partition_sm_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create PartitionStateMachine
	compressor, _ := compression.GetCompressor(compression.None)
	psm, err := NewPartitionStateMachine("test-topic", 0, tempDir, compressor)
	if err != nil {
		t.Fatalf("Failed to create PartitionStateMachine: %v", err)
	}
	defer psm.Close()

	// Test storing a half message
	storeData := map[string]interface{}{
		"transaction_id":   "test-tx-001",
		"topic":            "test-topic",
		"partition":        int32(0),
		"key":              "test-key",    // 使用字符串而不是[]byte
		"value":            "test-value",  // 使用字符串而不是[]byte
		"headers":          map[string]string{"header1": "value1"},
		"timeout":          "30s",
		"state":            "prepared",
		"producer_group":   "test-producer-group",
		"callback_address": "http://localhost:8080/callback",
	}

	cmd := PartitionCommand{
		Type: CmdStoreHalfMessage,
		Data: storeData,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}

	// Execute store command
	result, err := psm.Update(cmdBytes)
	if err != nil {
		t.Fatalf("Failed to store half message: %v", err)
	}

	if result.Value == 0 {
		t.Errorf("Expected non-zero result value")
	}

	// Test getting the half message
	record, exists := psm.GetHalfMessage("test-tx-001")
	if !exists {
		t.Fatalf("Half message should exist")
	}

	if record.TransactionID != "test-tx-001" {
		t.Errorf("Expected transaction ID 'test-tx-001', got '%s'", record.TransactionID)
	}

	if record.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", record.Topic)
	}

	if string(record.Key) != "test-key" {
		t.Errorf("Expected key 'test-key', got '%s'", string(record.Key))
	}

	if string(record.Value) != "test-value" {
		t.Errorf("Expected value 'test-value', got '%s'", string(record.Value))
	}

	// Test committing the half message
	commitData := map[string]interface{}{
		"transaction_id": "test-tx-001",
	}

	commitCmd := PartitionCommand{
		Type: CmdCommitHalfMessage,
		Data: commitData,
	}

	commitCmdBytes, err := json.Marshal(commitCmd)
	if err != nil {
		t.Fatalf("Failed to marshal commit command: %v", err)
	}

	// Execute commit command
	_, err = psm.Update(commitCmdBytes)
	if err != nil {
		t.Fatalf("Failed to commit half message: %v", err)
	}

	// Verify the half message is deleted after commit
	_, exists = psm.GetHalfMessage("test-tx-001")
	if exists {
		t.Errorf("Half message should be deleted after commit")
	}

	t.Logf("✅ PartitionStateMachine half message storage test passed")
}

func TestPartitionStateMachineHalfMessageRollback(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "partition_sm_rollback_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create PartitionStateMachine
	compressor, _ := compression.GetCompressor(compression.None)
	psm, err := NewPartitionStateMachine("test-topic", 0, tempDir, compressor)
	if err != nil {
		t.Fatalf("Failed to create PartitionStateMachine: %v", err)
	}
	defer psm.Close()

	// Test storing a half message
	storeData := map[string]interface{}{
		"transaction_id": "test-tx-002",
		"topic":          "test-topic",
		"partition":      int32(0),
		"key":            []byte("test-key-2"),
		"value":          []byte("test-value-2"),
		"headers":        map[string]string{"header2": "value2"},
		"timeout":        "30s",
		"state":          "prepared",
	}

	cmd := PartitionCommand{
		Type: CmdStoreHalfMessage,
		Data: storeData,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}

	// Execute store command
	_, err = psm.Update(cmdBytes)
	if err != nil {
		t.Fatalf("Failed to store half message: %v", err)
	}

	// Verify the half message exists
	_, exists := psm.GetHalfMessage("test-tx-002")
	if !exists {
		t.Fatalf("Half message should exist")
	}

	// Test rolling back the half message
	rollbackData := map[string]interface{}{
		"transaction_id": "test-tx-002",
	}

	rollbackCmd := PartitionCommand{
		Type: CmdRollbackHalfMessage,
		Data: rollbackData,
	}

	rollbackCmdBytes, err := json.Marshal(rollbackCmd)
	if err != nil {
		t.Fatalf("Failed to marshal rollback command: %v", err)
	}

	// Execute rollback command
	_, err = psm.Update(rollbackCmdBytes)
	if err != nil {
		t.Fatalf("Failed to rollback half message: %v", err)
	}

	// Verify the half message is deleted after rollback
	_, exists = psm.GetHalfMessage("test-tx-002")
	if exists {
		t.Errorf("Half message should be deleted after rollback")
	}

	t.Logf("✅ PartitionStateMachine half message rollback test passed")
}

func TestPartitionStateMachineHalfMessageCleanup(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "partition_sm_cleanup_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create PartitionStateMachine
	compressor, _ := compression.GetCompressor(compression.None)
	psm, err := NewPartitionStateMachine("test-topic", 0, tempDir, compressor)
	if err != nil {
		t.Fatalf("Failed to create PartitionStateMachine: %v", err)
	}
	defer psm.Close()

	// Store an expired half message (timeout = 1ms)
	storeData := map[string]interface{}{
		"transaction_id": "test-tx-003",
		"topic":          "test-topic",
		"partition":      int32(0),
		"key":            []byte("test-key-3"),
		"value":          []byte("test-value-3"),
		"headers":        map[string]string{"header3": "value3"},
		"timeout":        "1ms", // Very short timeout
		"state":          "prepared",
	}

	cmd := PartitionCommand{
		Type: CmdStoreHalfMessage,
		Data: storeData,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}

	// Execute store command
	_, err = psm.Update(cmdBytes)
	if err != nil {
		t.Fatalf("Failed to store half message: %v", err)
	}

	// Wait for message to expire
	time.Sleep(10 * time.Millisecond)

	// Test cleanup
	cleanedCount := psm.CleanupExpiredHalfMessages()
	if cleanedCount != 1 {
		t.Errorf("Expected to clean up 1 message, got %d", cleanedCount)
	}

	// Verify the half message is deleted after cleanup
	_, exists := psm.GetHalfMessage("test-tx-003")
	if exists {
		t.Errorf("Half message should be deleted after cleanup")
	}

	t.Logf("✅ PartitionStateMachine half message cleanup test passed")
}