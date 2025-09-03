package protocol

import (
	"fmt"
	"net"
	"time"
)

// ProtocolVersion defines the current version of the communication protocol
const (
	ProtocolVersion = 1
)

// Request type constants define different types of requests that can be made to the queue system
const (
	// Client request types
	ProduceRequestType            int32 = 0
	FetchRequestType              int32 = 1
	CreateTopicRequestType        int32 = 2
	ListTopicsRequestType         int32 = 3
	DeleteTopicRequestType        int32 = 4
	JoinGroupRequestType          int32 = 5
	LeaveGroupRequestType         int32 = 6
	HeartbeatRequestType          int32 = 7
	CommitOffsetRequestType       int32 = 8
	FetchOffsetRequestType        int32 = 9
	DescribeTopicRequestType      int32 = 10
	DiscoverControllerRequestType int32 = 11
	GetTopicInfoRequestType       int32 = 12

	// Group management request types
	ListGroupsRequestType    int32 = 20
	DescribeGroupRequestType int32 = 21

	ControllerDiscoverRequestType = 1000
	ControllerVerifyRequestType   = 1001

	GetTopicMetadataRequestType        = 1002
	StartPartitionRaftGroupRequestType = 1003
	StopPartitionRaftGroupRequestType  = 1004
)

// Error code constants define different types of errors that can occur
const (
	ErrorNone = 0

	ErrorInvalidRequest   = 1
	ErrorInvalidTopic     = 2
	ErrorUnknownPartition = 3
	ErrorInvalidMessage   = 4
	ErrorMessageTooLarge  = 5
	ErrorOffsetOutOfRange = 6

	ErrorBrokerNotAvailable = 100
	ErrorFetchFailed        = 101
	ErrorProduceFailed      = 102

	ErrorUnauthorized  = 200
	ErrorQuotaExceeded = 201
)

const (
	MaxMessageSize       = 1 << 20
	DefaultMaxFetchBytes = 1 << 20
	MaxFetchBytesLimit   = 5 << 20
	CompressionNone      = 0x00
)

// RequestTypeNames maps request type constants to human-readable names
var RequestTypeNames = map[int32]string{
	ProduceRequestType:       "PRODUCE",
	FetchRequestType:         "FETCH",
	CreateTopicRequestType:   "CREATE_TOPIC",
	JoinGroupRequestType:     "JOIN_GROUP",
	LeaveGroupRequestType:    "LEAVE_GROUP",
	HeartbeatRequestType:     "HEARTBEAT",
	CommitOffsetRequestType:  "COMMIT_OFFSET",
	FetchOffsetRequestType:   "FETCH_OFFSET",
	ListGroupsRequestType:    "LIST_GROUPS",
	DescribeGroupRequestType: "DESCRIBE_GROUP",
	ListTopicsRequestType:    "LIST_TOPICS",
	DeleteTopicRequestType:   "DELETE_TOPIC",
	GetTopicInfoRequestType:  "GET_TOPIC_INFO",
}

// GetRequestTypeName returns the human-readable name for a request type
func GetRequestTypeName(requestType int32) string {
	if name, exists := RequestTypeNames[requestType]; exists {
		return name
	}
	return "UNKNOWN"
}

// ErrorCodeNames maps error code constants to human-readable names
var ErrorCodeNames = map[int16]string{
	ErrorNone:               "NONE",
	ErrorInvalidRequest:     "INVALID_REQUEST",
	ErrorInvalidTopic:       "INVALID_TOPIC",
	ErrorUnknownPartition:   "UNKNOWN_PARTITION",
	ErrorInvalidMessage:     "INVALID_MESSAGE",
	ErrorMessageTooLarge:    "MESSAGE_TOO_LARGE",
	ErrorOffsetOutOfRange:   "OFFSET_OUT_OF_RANGE",
	ErrorBrokerNotAvailable: "BROKER_NOT_AVAILABLE",
	ErrorFetchFailed:        "FETCH_FAILED",
	ErrorProduceFailed:      "PRODUCE_FAILED",
	ErrorUnauthorized:       "UNAUTHORIZED",
	ErrorQuotaExceeded:      "QUOTA_EXCEEDED",
}

// GetErrorCodeName returns the human-readable name for an error code
func GetErrorCodeName(errorCode int16) string {
	if name, exists := ErrorCodeNames[errorCode]; exists {
		return name
	}
	return "UNKNOWN_ERROR"
}

const (
	// Raft command types
	RaftCmdRegisterBroker            = "register_broker"
	RaftCmdUnregisterBroker          = "unregister_broker"
	RaftCmdCreateTopic               = "create_topic"
	RaftCmdDeleteTopic               = "delete_topic"
	RaftCmdJoinGroup                 = "join_group"
	RaftCmdLeaveGroup                = "leave_group"
	RaftCmdMigrateLeader             = "migrate_leader"
	RaftCmdUpdatePartitionAssignment = "update_partition_assignment"
	RaftCmdUpdateBrokerLoad          = "update_broker_load"
	RaftCmdMarkBrokerFailed          = "mark_broker_failed"
	RaftCmdRebalancePartitions       = "rebalance_partitions"
)

const (
	RaftQueryGetBrokers = "get_brokers"

	RaftQueryGetTopics = "get_topics"
	RaftQueryGetTopic  = "get_topic"

	RaftQueryGetGroups = "get_groups"
	RaftQueryGetGroup  = "get_group"

	RaftQueryGetPartitionAssignments = "get_partition_assignments"
	RaftQueryGetPartitionLeader      = "get_partition_leader"

	RaftQueryGetClusterMetadata = "get_cluster_metadata"
)

// ConnectToSpecificBroker connects to a specific broker address
// This replicates the simple and effective logic from client/client_connect.go
func ConnectToSpecificBroker(brokerAddr string, timeout time.Duration) (net.Conn, error) {
	if brokerAddr == "" {
		return nil, fmt.Errorf("failed to connect to broker: missing address")
	}

	conn, err := net.DialTimeout("tcp", brokerAddr, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker %s: %v", brokerAddr, err)
	}

	return conn, nil
}
