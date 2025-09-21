package transaction

import (
	"context"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

type TransactionID string

type TransactionState int16

const (
	StateUnknown  TransactionState = TransactionState(protocol.TransactionStateUnknown)
	StateCommit   TransactionState = TransactionState(protocol.TransactionStateCommit)
	StateRollback TransactionState = TransactionState(protocol.TransactionStateRollback)
	StatePrepared TransactionState = TransactionState(protocol.TransactionStatePrepared)
	StateChecking TransactionState = TransactionState(protocol.TransactionStateChecking)
)

// String returns the string representation of TransactionState
func (s TransactionState) String() string {
	switch s {
	case StateUnknown:
		return "UNKNOWN"
	case StateCommit:
		return "COMMIT"
	case StateRollback:
		return "ROLLBACK"
	case StatePrepared:
		return "PREPARED"
	case StateChecking:
		return "CHECKING"
	default:
		return "INVALID"
	}
}

// HalfMessage half message
type HalfMessage struct {
	TransactionID TransactionID     `json:"transaction_id"`
	Topic         string            `json:"topic"`
	Partition     int32             `json:"partition"`
	Key           []byte            `json:"key,omitempty"`
	Value         []byte            `json:"value"`
	Headers       map[string]string `json:"headers,omitempty"`
	ProducerGroup string            `json:"producer_group"`
	CreatedAt     time.Time         `json:"created_at"`
	Timeout       time.Duration     `json:"timeout"`
	CheckCount    int               `json:"check_count"`
	LastCheck     time.Time         `json:"last_check"`
	State         TransactionState  `json:"state"`
}

type TransactionPrepareRequest struct {
	TransactionID TransactionID     `json:"transaction_id"`
	Topic         string            `json:"topic"`
	Partition     int32             `json:"partition"`
	Key           []byte            `json:"key,omitempty"`
	Value         []byte            `json:"value"`
	Headers       map[string]string `json:"headers,omitempty"`
	Timeout       int64             `json:"timeout_ms"`
	ProducerGroup string            `json:"producer_group"`
}

type TransactionPrepareResponse struct {
	TransactionID TransactionID `json:"transaction_id"`
	ErrorCode     int16         `json:"error_code"`
	Error         string        `json:"error,omitempty"`
}

type TransactionCommitRequest struct {
	TransactionID TransactionID `json:"transaction_id"`
}

type TransactionCommitResponse struct {
	TransactionID TransactionID `json:"transaction_id"`
	Offset        int64         `json:"offset"`
	Timestamp     time.Time     `json:"timestamp"`
	ErrorCode     int16         `json:"error_code"`
	Error         string        `json:"error,omitempty"`
}

type TransactionRollbackRequest struct {
	TransactionID TransactionID `json:"transaction_id"`
}

type TransactionRollbackResponse struct {
	TransactionID TransactionID `json:"transaction_id"`
	ErrorCode     int16         `json:"error_code"`
	Error         string        `json:"error,omitempty"`
}

type TransactionCheckRequest struct {
	TransactionID   TransactionID `json:"transaction_id"`
	Topic           string        `json:"topic"`
	Partition       int32         `json:"partition"`
	ProducerGroup   string        `json:"producer_group"`
	OriginalMessage HalfMessage   `json:"original_message"`
}

type TransactionCheckResponse struct {
	TransactionID TransactionID    `json:"transaction_id"`
	State         TransactionState `json:"state"`
	ErrorCode     int16            `json:"error_code"`
	Error         string           `json:"error,omitempty"`
}

// TransactionChecker check txn
type TransactionChecker interface {
	CheckTransactionState(transactionID TransactionID, originalMessage HalfMessage) TransactionState
}

// RaftProposer 接口用于向Raft组提议事务命令
type RaftProposer interface {
	// ProposeTransactionCommand 向指定的Raft组提议事务命令
	ProposeTransactionCommand(ctx context.Context, raftGroupID uint64, cmdType string, data map[string]interface{}) (interface{}, error)
}

// TransactionListener listen txn
type TransactionListener interface {
	ExecuteLocalTransaction(transactionID TransactionID, message HalfMessage) TransactionState

	CheckLocalTransaction(transactionID TransactionID, message HalfMessage) TransactionState
}

// StateMachineGetter 接口用于获取状态机实例
type StateMachineGetter interface {
	GetStateMachine(groupID uint64) (PartitionStateMachineInterface, error)
	GetAllRaftGroups() []uint64 // 新增方法获取所有活跃的Raft组ID
}

// PartitionStateMachineInterface 定义分区状态机的接口
type PartitionStateMachineInterface interface {
	GetTimeoutTransactions() []string // 使用string类型避免循环导入
	GetHalfMessage(txnID string) (interface{}, bool) // 使用interface{}避免循环导入
}
