package protocol

// ProtocolVersion defines the current version of the communication protocol
const (
	ProtocolVersion = 1
)

// Request type constants define different types of requests that can be made to the queue system
const (
	ProduceRequestType = 0
	FetchRequestType   = 1

	CreateTopicRequestType  = 2
	ListTopicsRequestType   = 10
	DeleteTopicRequestType  = 11
	GetTopicInfoRequestType = 12

	JoinGroupRequestType     = 3
	LeaveGroupRequestType    = 4
	HeartbeatRequestType     = 5
	CommitOffsetRequestType  = 6
	FetchOffsetRequestType   = 7
	ListGroupsRequestType    = 8
	DescribeGroupRequestType = 9

	ControllerDiscoverRequestType = 1000
	ControllerVerifyRequestType   = 1001

	GetTopicMetadataRequestType = 1002
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
	RaftCmdRegisterBroker   = "register_broker"
	RaftCmdUnregisterBroker = "unregister_broker"

	RaftCmdCreateTopic = "create_topic"
	RaftCmdDeleteTopic = "delete_topic"

	RaftCmdJoinGroup  = "join_group"
	RaftCmdLeaveGroup = "leave_group"

	RaftCmdMigrateLeader             = "migrate_leader"
	RaftCmdUpdatePartitionAssignment = "update_partition_assignment"
	RaftCmdUpdateBrokerLoad          = "update_broker_load"
	RaftCmdMarkBrokerFailed          = "mark_broker_failed"
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
