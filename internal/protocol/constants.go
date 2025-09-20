package protocol

import (
	"fmt"
	"net"
	"time"

	typederrors "github.com/issac1998/go-queue/internal/errors"
)

// ProtocolVersion defines the current version of the communication protocol
const (
	ProtocolVersion = 1
)

// Request type constants define different types of operations in the system
const (
	ProduceRequestType     int32 = 1
	FetchRequestType       int32 = 2
	ListTopicsRequestType  int32 = 3
	CreateTopicRequestType int32 = 4
	DeleteTopicRequestType int32 = 5

	JoinGroupRequestType           int32 = 10
	LeaveGroupRequestType          int32 = 11
	HeartbeatRequestType           int32 = 12
	CommitOffsetRequestType        int32 = 13
	FetchOffsetRequestType         int32 = 14
	GetTopicInfoRequestType        int32 = 15
	FetchAssignmentRequestType     int32 = 16
	BatchFetchRequestType          int32 = 17
	TransactionPrepareRequestType  int32 = 18
	TransactionCommitRequestType   int32 = 19
	TransactionRollbackRequestType int32 = 20
	TransactionCheckRequestType    int32 = 21
	OrderedProduceRequestType      int32 = 22

	// Group management request types
	ListGroupsRequestType    int32 = 30
	DescribeGroupRequestType int32 = 31

	ControllerDiscoverRequestType int32 = 1000
	ControllerVerifyRequestType   int32 = 1001

	GetTopicMetadataRequestType        int32 = 1002
	StartPartitionRaftGroupRequestType int32 = 1003
	StopPartitionRaftGroupRequestType  int32 = 1004
)

// Error code constants define different types of errors that can occur
const (
	ErrorNone = 0

	ErrorInvalidRequest        = 1
	ErrorInvalidTopic          = 2
	ErrorUnknownPartition      = 3
	ErrorInvalidMessage        = 4
	ErrorMessageTooLarge       = 5
	ErrorOffsetOutOfRange      = 6
	ErrorInvalidSequenceNumber = 7

	// Broker and network errors
	ErrorBrokerNotAvailable = 10
	ErrorFetchFailed        = 11
	ErrorProduceFailed      = 12
	ErrorNotLeader          = 13
	ErrorInternalError      = 14

	// Authentication and authorization
	ErrorUnauthorized  = 20
	ErrorQuotaExceeded = 21

	// Consumer group and rebalancing errors
	ErrorUnknownGroup        = 30
	ErrorUnknownMember       = 31
	ErrorRebalanceInProgress = 32
	ErrorGenerationMismatch  = 33

	// Transaction states
	TransactionStateUnknown  int16 = 0
	TransactionStateCommit   int16 = 1
	TransactionStateRollback int16 = 2
	TransactionStatePrepared int16 = 3
	TransactionStateChecking int16 = 4
)

const (
	MaxMessageSize       = 1 << 20
	DefaultMaxFetchBytes = 1 << 20
	MaxFetchBytesLimit   = 5 << 20
	CompressionNone      = 0x00
)

// RequestTypeNames maps request types to human-readable names
var RequestTypeNames = map[int32]string{
	ProduceRequestType:             "PRODUCE",
	FetchRequestType:               "FETCH",
	ListTopicsRequestType:          "LIST_TOPICS",
	CreateTopicRequestType:         "CREATE_TOPIC",
	DeleteTopicRequestType:         "DELETE_TOPIC",
	JoinGroupRequestType:           "JOIN_GROUP",
	LeaveGroupRequestType:          "LEAVE_GROUP",
	HeartbeatRequestType:           "HEARTBEAT",
	CommitOffsetRequestType:        "COMMIT_OFFSET",
	FetchOffsetRequestType:         "FETCH_OFFSET",
	GetTopicInfoRequestType:        "GET_TOPIC_INFO",
	FetchAssignmentRequestType:     "FETCH_ASSIGNMENT",
	BatchFetchRequestType:          "BATCH_FETCH",
	TransactionPrepareRequestType:  "TRANSACTION_PREPARE",
	TransactionCommitRequestType:   "TRANSACTION_COMMIT",
	TransactionRollbackRequestType: "TRANSACTION_ROLLBACK",
	TransactionCheckRequestType:    "TRANSACTION_CHECK",
	OrderedProduceRequestType:      "ORDERED_PRODUCE",
	ListGroupsRequestType:          "LIST_GROUPS",
	DescribeGroupRequestType:       "DESCRIBE_GROUP",
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
	ErrorNone:                "NONE",
	ErrorInvalidRequest:      "INVALID_REQUEST",
	ErrorInvalidTopic:        "INVALID_TOPIC",
	ErrorUnknownPartition:    "UNKNOWN_PARTITION",
	ErrorInvalidMessage:      "INVALID_MESSAGE",
	ErrorMessageTooLarge:     "MESSAGE_TOO_LARGE",
	ErrorOffsetOutOfRange:    "OFFSET_OUT_OF_RANGE",
	ErrorBrokerNotAvailable:  "BROKER_NOT_AVAILABLE",
	ErrorFetchFailed:         "FETCH_FAILED",
	ErrorProduceFailed:       "PRODUCE_FAILED",
	ErrorUnauthorized:        "UNAUTHORIZED",
	ErrorQuotaExceeded:       "QUOTA_EXCEEDED",
	ErrorUnknownGroup:        "UNKNOWN_GROUP",
	ErrorUnknownMember:       "UNKNOWN_MEMBER",
	ErrorRebalanceInProgress: "REBALANCE_IN_PROGRESS",
	ErrorGenerationMismatch:  "GENERATION_MISMATCH",
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
	RaftCmdRegisterBroker             = "register_broker"
	RaftCmdUnregisterBroker           = "unregister_broker"
	RaftCmdCreateTopic                = "create_topic"
	RaftCmdDeleteTopic                = "delete_topic"
	RaftCmdJoinGroup                  = "join_group"
	RaftCmdLeaveGroup                 = "leave_group"
	RaftCmdMigrateLeader              = "migrate_leader"
	RaftCmdUpdatePartitionAssignments = "update_partition_assignments"
	RaftCmdMarkBrokerFailed           = "mark_broker_failed"
	RaftCmdRebalancePartitions        = "rebalance_partitions"
	RaftCmdCommitOffset               = "commit_offset"
	RaftCmdUpdateSubscription         = "update_subscription"
	RaftCmdUpdateTopicAssignment      = "update_topic_assignment"
	RaftCmdUpdateBrokerLoad           = "update_broker_load"
	RaftCmdStoreHalfMessage           = "store_half_message"
	RaftCmdUpdateTransactionState     = "update_transaction_state"
	RaftCmdDeleteHalfMessage          = "delete_half_message"
	RaftCmdRegisterProducerGroup      = "register_producer_group"
	RaftCmdUnregisterProducerGroup    = "unregister_producer_group"
)

const (
	RaftQueryGetBrokers = "get_brokers"

	RaftQueryGetTopics = "get_topics"
	RaftQueryGetTopic  = "get_topic"

	RaftQueryGetGroups = "get_groups"
	RaftQueryGetGroup  = "get_group"

	RaftQueryGetPartitionLeader      = "get_partition_leader"
	RaftQueryGetPartitionAssignments = "get_partition_assignments"
	RaftQueryGetCommittedOffset      = "get_committed_offset"

	RaftQueryGetClusterMetadata = "get_cluster_metadata"
)

// ConnectToSpecificBroker connects to a specific broker address
// This replicates the simple and effective logic from client/client_connect.go
func ConnectToSpecificBroker(brokerAddr string, timeout time.Duration) (net.Conn, error) {
	if brokerAddr == "" {
		return nil, typederrors.NewTypedError(typederrors.GeneralError, "failed to connect to broker: missing address", nil)
	}

	conn, err := net.DialTimeout("tcp", brokerAddr, timeout)
	if err != nil {
		return nil, typederrors.NewTypedError(typederrors.GeneralError, fmt.Sprintf("failed to connect to broker %s", brokerAddr), err)
	}

	return conn, nil
}
