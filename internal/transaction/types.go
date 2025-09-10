package transaction

import (
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

// TransactionListener listen txn
type TransactionListener interface {
	ExecuteLocalTransaction(transactionID TransactionID, message HalfMessage) TransactionState

	CheckLocalTransaction(transactionID TransactionID, message HalfMessage) TransactionState
}

type RaftProposer interface {
	ProposeCommand(groupID uint64, data []byte) error
	SyncRead(groupID uint64, query []byte) (interface{}, error)
}
