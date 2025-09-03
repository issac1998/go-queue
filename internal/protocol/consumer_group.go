package protocol

import (
	"time"
)

// JoinGroupRequest represents a request to join a consumer group
type JoinGroupRequest struct {
	// GroupID is the ID of the consumer group to join
	GroupID string
	// ConsumerID is the unique ID of the consumer
	ConsumerID string
	// ClientID is the client application identifier
	ClientID string
	// Topics is the list of topics the consumer wants to subscribe to
	Topics []string
	// SessionTimeout is the maximum time the coordinator will wait for heartbeats
	SessionTimeout time.Duration
}

// JoinGroupResponse represents the response from a join group operation
type JoinGroupResponse struct {
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
	// Generation is the current generation ID of the consumer group
	Generation int32
	// GroupID is the ID of the consumer group
	GroupID string
	// ConsumerID is the assigned consumer ID
	ConsumerID string
	// Leader is the ID of the consumer that is the group leader
	Leader string
	// Members is the list of all members in the consumer group
	Members []GroupMember
	// Assignment maps topics to partition assignments for this consumer
	Assignment map[string][]int32 // Topic -> Partitions
}

// GroupMember represents a member of a consumer group
type GroupMember struct {
	// ID is the unique identifier of the group member
	ID string
	// ClientID is the client application identifier
	ClientID string
}

// LeaveGroupRequest represents a request to leave a consumer group
type LeaveGroupRequest struct {
	// GroupID is the ID of the consumer group to leave
	GroupID string
	// ConsumerID is the unique ID of the consumer
	ConsumerID string
}

// LeaveGroupResponse represents the response from a leave group operation
type LeaveGroupResponse struct {
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
}

// HeartbeatRequest represents a heartbeat request from a consumer
type HeartbeatRequest struct {
	// GroupID is the ID of the consumer group
	GroupID string
	// ConsumerID is the unique ID of the consumer
	ConsumerID string
	// Generation is the current generation ID of the consumer group
	Generation int32
}

// HeartbeatResponse represents the response from a heartbeat operation
type HeartbeatResponse struct {
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
}

// CommitOffsetRequest represents a request to commit an offset
type CommitOffsetRequest struct {
	// GroupID is the ID of the consumer group
	GroupID string
	// Topic is the topic name
	Topic string
	// Partition is the partition ID
	Partition int32
	// Offset is the offset to commit
	Offset int64
}

// CommitOffsetResponse represents the response from a commit offset operation
type CommitOffsetResponse struct {
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
}

// FetchOffsetRequest represents a request to fetch the committed offset
type FetchOffsetRequest struct {
	// GroupID is the ID of the consumer group
	GroupID string
	// Topic is the topic name
	Topic string
	// Partition is the partition ID
	Partition int32
}

// FetchOffsetResponse represents the response from a fetch offset operation
type FetchOffsetResponse struct {
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
	// Offset is the committed offset value
	Offset int64
}
