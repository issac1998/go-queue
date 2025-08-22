package protocol

// ProtocolVersion defines the current version of the communication protocol
const (
	ProtocolVersion = 1
)

// Request type constants define different types of requests that can be made to the queue system
const (
	// ProduceRequestType is used for sending messages to a topic
	ProduceRequestType = 0
	// FetchRequestType is used for retrieving messages from a topic
	FetchRequestType = 1

	// CreateTopicRequestType is used for creating a new topic
	CreateTopicRequestType = 2
	// ListTopicsRequestType is used for listing all available topics
	ListTopicsRequestType = 10
	// DescribeTopicRequestType is used for getting detailed information about a topic
	DescribeTopicRequestType = 11
	// DeleteTopicRequestType is used for deleting an existing topic
	DeleteTopicRequestType = 12
	// GetTopicInfoRequestType is used for getting basic topic information
	GetTopicInfoRequestType = 13

	// JoinGroupRequestType is used for joining a consumer group
	JoinGroupRequestType = 3
	// LeaveGroupRequestType is used for leaving a consumer group
	LeaveGroupRequestType = 4
	// HeartbeatRequestType is used for sending heartbeats to maintain group membership
	HeartbeatRequestType = 5
	// CommitOffsetRequestType is used for committing message offsets
	CommitOffsetRequestType = 6
	// FetchOffsetRequestType is used for retrieving committed offsets
	FetchOffsetRequestType = 7
	// ListGroupsRequestType is used for listing all consumer groups
	ListGroupsRequestType = 8
	// DescribeGroupRequestType is used for getting detailed information about a consumer group
	DescribeGroupRequestType = 9
)

// Error code constants define different types of errors that can occur
const (
	// ErrorNone indicates no error occurred
	ErrorNone = 0

	// ErrorInvalidRequest indicates the request format is invalid
	ErrorInvalidRequest = 1
	// ErrorInvalidTopic indicates the topic name is invalid or not found
	ErrorInvalidTopic = 2
	// ErrorUnknownPartition indicates the specified partition does not exist
	ErrorUnknownPartition = 3
	// ErrorInvalidMessage indicates the message format is invalid
	ErrorInvalidMessage = 4
	// ErrorMessageTooLarge indicates the message exceeds the maximum allowed size
	ErrorMessageTooLarge = 5
	// ErrorOffsetOutOfRange indicates the requested offset is not available
	ErrorOffsetOutOfRange = 6

	// ErrorBrokerNotAvailable indicates the broker is not available
	ErrorBrokerNotAvailable = 100
	// ErrorFetchFailed indicates a fetch operation failed
	ErrorFetchFailed = 101
	// ErrorProduceFailed indicates a produce operation failed
	ErrorProduceFailed = 102

	// ErrorUnauthorized indicates the client is not authorized for the operation
	ErrorUnauthorized = 200
	// ErrorQuotaExceeded indicates the client has exceeded their quota
	ErrorQuotaExceeded = 201
)

// Size and limit constants define various system limits
const (
	// MaxMessageSize defines the maximum size of a single message in bytes (1MB)
	MaxMessageSize = 1 << 20
	// DefaultMaxFetchBytes defines the default maximum bytes to fetch in a single request (1MB)
	DefaultMaxFetchBytes = 1 << 20
	// MaxFetchBytesLimit defines the absolute maximum bytes that can be fetched (5MB)
	MaxFetchBytesLimit = 5 << 20
	// CompressionNone indicates no compression is applied
	CompressionNone = 0x00
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
	DescribeTopicRequestType: "DESCRIBE_TOPIC",
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
