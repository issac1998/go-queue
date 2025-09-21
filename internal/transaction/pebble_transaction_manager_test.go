package transaction

import (
	"fmt"
	"testing"
)

func TestPebbleTransactionManager_Basic(t *testing.T) {
	tempDir := t.TempDir()
	
	manager, err := NewPebbleTransactionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create PebbleTransactionManager: %v", err)
	}
	defer manager.Stop()

	// Test GetActiveTransactionCount - should be 0 initially
	count := manager.GetActiveTransactionCount()
	if count != 0 {
		t.Errorf("Expected 0 active transactions initially, got %d", count)
	}
}

func TestPebbleTransactionManager_PrepareTransaction(t *testing.T) {
	tempDir := t.TempDir()
	
	manager, err := NewPebbleTransactionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create PebbleTransactionManager: %v", err)
	}
	defer manager.Stop()

	// Test PrepareTransaction
	req := &TransactionPrepareRequest{
		TransactionID: TransactionID("test-txn-1"),
		Topic:         "test-topic",
		Partition:     0,
		Value:         []byte("test message"),
		Timeout:       5000,
	}

	resp, err := manager.PrepareTransaction(req)
	if err != nil {
		t.Fatalf("PrepareTransaction failed: %v", err)
	}
	if resp == nil {
		t.Fatal("PrepareTransaction response is nil")
	}
	if resp.TransactionID != req.TransactionID {
		t.Errorf("Expected transaction ID %s, got %s", req.TransactionID, resp.TransactionID)
	}

	// Test GetActiveTransactionCount - should be 1 now
	count := manager.GetActiveTransactionCount()
	if count != 1 {
		t.Errorf("Expected 1 active transaction, got %d", count)
	}

	// Test GetHalfMessage
	halfMsg, found := manager.GetHalfMessage(req.TransactionID)
	if !found {
		t.Fatal("GetHalfMessage returned false, expected true")
	}
	if halfMsg.TransactionID != req.TransactionID {
		t.Errorf("Expected transaction ID %s, got %s", req.TransactionID, halfMsg.TransactionID)
	}
	if halfMsg.Topic != req.Topic {
		t.Errorf("Expected topic %s, got %s", req.Topic, halfMsg.Topic)
	}
}

func TestPebbleTransactionManager_CommitTransaction(t *testing.T) {
	tempDir := t.TempDir()
	
	manager, err := NewPebbleTransactionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create PebbleTransactionManager: %v", err)
	}
	defer manager.Stop()

	// Prepare a transaction first
	req := &TransactionPrepareRequest{
		TransactionID: TransactionID("commit-txn"),
		Topic:         "test-topic",
		Partition:     0,
		Value:         []byte("commit test"),
		Timeout:       5000,
	}

	_, err = manager.PrepareTransaction(req)
	if err != nil {
		t.Fatalf("PrepareTransaction failed: %v", err)
	}

	// Test CommitTransaction
	resp, err := manager.CommitTransaction(req.TransactionID)
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}
	if resp == nil {
		t.Fatal("CommitTransaction response is nil")
	}
	if resp.TransactionID != req.TransactionID {
		t.Errorf("Expected transaction ID %s, got %s", req.TransactionID, resp.TransactionID)
	}

	// Verify transaction is no longer active
	count := manager.GetActiveTransactionCount()
	if count != 0 {
		t.Errorf("Expected 0 active transactions after commit, got %d", count)
	}

	// Verify we can't get the half message anymore
	_, found := manager.GetHalfMessage(req.TransactionID)
	if found {
		t.Error("Expected GetHalfMessage to return false for committed transaction")
	}
}

func TestPebbleTransactionManager_RollbackTransaction(t *testing.T) {
	tempDir := t.TempDir()
	
	manager, err := NewPebbleTransactionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create PebbleTransactionManager: %v", err)
	}
	defer manager.Stop()

	// Prepare a transaction first
	req := &TransactionPrepareRequest{
		TransactionID: TransactionID("rollback-txn"),
		Topic:         "test-topic",
		Partition:     0,
		Value:         []byte("rollback test"),
		Timeout:       5000,
	}

	_, err = manager.PrepareTransaction(req)
	if err != nil {
		t.Fatalf("PrepareTransaction failed: %v", err)
	}

	// Test RollbackTransaction
	resp, err := manager.RollbackTransaction(req.TransactionID)
	if err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}
	if resp == nil {
		t.Fatal("RollbackTransaction response is nil")
	}
	if resp.TransactionID != req.TransactionID {
		t.Errorf("Expected transaction ID %s, got %s", req.TransactionID, resp.TransactionID)
	}

	// Verify transaction is no longer active
	count := manager.GetActiveTransactionCount()
	if count != 0 {
		t.Errorf("Expected 0 active transactions after rollback, got %d", count)
	}

	// Verify we can't get the half message anymore
	_, found := manager.GetHalfMessage(req.TransactionID)
	if found {
		t.Error("Expected GetHalfMessage to return false for rolled back transaction")
	}
}

func TestPebbleTransactionManager_GetTransactionStatus(t *testing.T) {
	tempDir := t.TempDir()
	
	manager, err := NewPebbleTransactionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create PebbleTransactionManager: %v", err)
	}
	defer manager.Stop()

	// Prepare a transaction
	req := &TransactionPrepareRequest{
		TransactionID: TransactionID("status-txn"),
		Topic:         "test-topic",
		Partition:     0,
		Value:         []byte("status test"),
		Timeout:       5000,
	}

	_, err = manager.PrepareTransaction(req)
	if err != nil {
		t.Fatalf("PrepareTransaction failed: %v", err)
	}

	// Test GetTransactionStatus
	status, err := manager.GetTransactionStatus(req.TransactionID)
	if err != nil {
		t.Fatalf("GetTransactionStatus failed: %v", err)
	}
	if status == nil {
		t.Fatal("GetTransactionStatus returned nil")
	}

	// Verify status contains expected fields
	if txnID, ok := status["transaction_id"]; !ok || txnID != string(req.TransactionID) {
		t.Errorf("Expected transaction_id %s, got %v", req.TransactionID, txnID)
	}
	if state, ok := status["state"]; !ok || state != StatePrepared {
		t.Errorf("Expected state %v, got %v", StatePrepared, state)
	}
	if topic, ok := status["topic"]; !ok || topic != req.Topic {
		t.Errorf("Expected topic %s, got %v", req.Topic, topic)
	}
}

func TestPebbleTransactionManager_GetStats(t *testing.T) {
	tempDir := t.TempDir()
	
	manager, err := NewPebbleTransactionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create PebbleTransactionManager: %v", err)
	}
	defer manager.Stop()

	// Get initial stats
	stats, err := manager.GetStats()
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if stats == nil {
		t.Error("Expected stats to be non-nil")
	}

	// Add some transactions and check stats again
	for i := 0; i < 5; i++ {
		req := &TransactionPrepareRequest{
			TransactionID: TransactionID(fmt.Sprintf("stats-txn-%d", i)),
			Topic:         "test-topic",
			Partition:     0,
			Value:         []byte("stats test"),
			Timeout:       5000,
		}
		_, err = manager.PrepareTransaction(req)
		if err != nil {
			t.Fatalf("PrepareTransaction failed: %v", err)
		}
	}

	stats, err = manager.GetStats()
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if stats == nil {
		t.Error("Expected stats to be non-nil after adding transactions")
	}
}

func TestPebbleTransactionManager_Compact(t *testing.T) {
	tempDir := t.TempDir()
	
	manager, err := NewPebbleTransactionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create PebbleTransactionManager: %v", err)
	}
	defer manager.Stop()

	// Add some transactions
	for i := 0; i < 10; i++ {
		req := &TransactionPrepareRequest{
			TransactionID: TransactionID(fmt.Sprintf("compact-txn-%d", i)),
			Topic:         "test-topic",
			Partition:     0,
			Value:         []byte("compact test"),
			Timeout:       5000,
		}
		_, err = manager.PrepareTransaction(req)
		if err != nil {
			t.Fatalf("PrepareTransaction failed: %v", err)
		}
	}

	// Test compaction
	err = manager.Compact()
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify transactions are still accessible after compaction
	count := manager.GetActiveTransactionCount()
	if count != 10 {
		t.Errorf("Expected 10 active transactions after compaction, got %d", count)
	}
}

func TestPebbleTransactionManager_Persistence(t *testing.T) {
	tempDir := t.TempDir()

	// Create manager and add transactions
	manager1, err := NewPebbleTransactionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create PebbleTransactionManager: %v", err)
	}

	req := &TransactionPrepareRequest{
		TransactionID: TransactionID("persistent-txn"),
		Topic:         "test-topic",
		Partition:     0,
		Value:         []byte("persistence test"),
		Timeout:       5000,
	}

	_, err = manager1.PrepareTransaction(req)
	if err != nil {
		t.Fatalf("PrepareTransaction failed: %v", err)
	}

	manager1.Stop()

	// Reopen manager and verify transaction persists
	manager2, err := NewPebbleTransactionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to reopen PebbleTransactionManager: %v", err)
	}
	defer manager2.Stop()

	// Verify transaction is still there
	halfMsg, found := manager2.GetHalfMessage(req.TransactionID)
	if !found {
		t.Fatal("GetHalfMessage returned false after reopening")
	}
	if halfMsg.TransactionID != req.TransactionID {
		t.Errorf("Expected transaction ID %s, got %s", req.TransactionID, halfMsg.TransactionID)
	}

	// Verify active count
	count := manager2.GetActiveTransactionCount()
	if count != 1 {
		t.Errorf("Expected 1 active transaction after reopening, got %d", count)
	}
}