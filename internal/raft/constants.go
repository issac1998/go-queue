package raft

// Raft group ID constant definitions
// Use simple predefined values to avoid hash conflicts
const (
	ControllerGroupID uint64 = 1

	TransactionManagerGroupID uint64 = 1000000000

	ConsumerGroupManagerID uint64 = 1000000001
)
