package raft

import (
	"io"
	"testing"

	"github.com/lni/dragonboat/v3/statemachine"
)

// MockStateMachine for testing
type MockStateMachine struct{}

func (m *MockStateMachine) Update(data []byte) (statemachine.Result, error) {
	return statemachine.Result{}, nil
}

func (m *MockStateMachine) Lookup(query interface{}) (interface{}, error) {
	return []byte("test result"), nil
}

func (m *MockStateMachine) SaveSnapshot(w io.Writer, fc statemachine.ISnapshotFileCollection, done <-chan struct{}) error {
	return nil
}

func (m *MockStateMachine) RecoverFromSnapshot(r io.Reader, files []statemachine.SnapshotFile, done <-chan struct{}) error {
	return nil
}

func (m *MockStateMachine) Close() error {
	return nil
}

func TestGetStateMachine(t *testing.T) {
	// Create a RaftManager with mock data
	rm := &RaftManager{
		groups: make(map[uint64]*RaftGroup),
	}

	// Add a mock raft group
	mockSM := &MockStateMachine{}
	rm.groups[1] = &RaftGroup{
		GroupID:      1,
		StateMachine: mockSM,
		IsController: true,
	}

	// Test getting existing state machine
	sm, err := rm.GetStateMachine(1)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if sm == nil {
		t.Fatalf("Expected state machine, got nil")
	}

	// Test getting non-existent state machine
	_, err = rm.GetStateMachine(999)
	if err == nil {
		t.Fatalf("Expected error for non-existent group, got nil")
	}

	expectedError := "raft group 999 not found"
	if err.Error() != expectedError {
		t.Fatalf("Expected error '%s', got '%s'", expectedError, err.Error())
	}

	t.Log("✅ GetStateMachine test passed")
}