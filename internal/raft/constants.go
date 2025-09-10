package raft

// Raft组ID常量定义
// 使用简单的预定义值避免哈希冲突
const (
	ControllerGroupID uint64 = 1 

	TransactionManagerGroupID uint64 = 1000000000

	ConsumerGroupManagerID uint64 = 1000000001
)
