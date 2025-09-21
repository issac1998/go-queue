package client

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/issac1998/go-queue/internal/errors"
	"github.com/issac1998/go-queue/internal/protocol"
	"github.com/issac1998/go-queue/internal/transaction"
)

// TransactionProducer produce
type TransactionProducer struct {
	client   *Client
	listener transaction.TransactionListener
	group    string
}

// TransactionMessage massage
type TransactionMessage struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Key       []byte            `json:"key,omitempty"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timeout   time.Duration     `json:"timeout"`
}

// Transaction txn
type Transaction struct {
	ID            transaction.TransactionID
	producer      *TransactionProducer
	producerGroup string
	prepared      bool
	topic         string
	partition     int32
}

// TransactionResult txn result
type TransactionResult struct {
	TransactionID transaction.TransactionID
	Offset        int64
	Timestamp     time.Time
	Error         error
}

// NewTransactionProducer create producer
func NewTransactionProducer(client *Client, listener transaction.TransactionListener) *TransactionProducer {
	return NewTransactionProducerWithGroup(client, listener, "default-txn-group")
}

// NewTransactionProducerWithGroup creates producer with explicit producer group
func NewTransactionProducerWithGroup(client *Client, listener transaction.TransactionListener, group string) *TransactionProducer {
	return &TransactionProducer{client: client, listener: listener, group: group}
}

// BeginTransaction start new txn
func (tp *TransactionProducer) BeginTransaction() (*Transaction, error) {
	transactionID := tp.generateTransactionID()

	return &Transaction{
		ID:            transactionID,
		producer:      tp,
		prepared:      false,
		producerGroup: tp.group,
	}, nil
}

// SendHalfMessageAndDoLocal send half message, then do local txn
func (tp *TransactionProducer) SendHalfMessageAndDoLocal(msg *TransactionMessage) (*Transaction, *TransactionResult, error) {
	txn, err := tp.BeginTransaction()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	result, err := txn.SendHalfMessageAndDoLocal(msg)
	if err != nil {
		return txn, nil, fmt.Errorf("failed to send half message: %w", err)
	}

	return txn, result, nil
}

// SendHalfMessage send half message
func (t *Transaction) SendHalfMessageAndDoLocal(msg *TransactionMessage) (*TransactionResult, error) {
	halfMessage := transaction.HalfMessage{
		TransactionID: t.ID,
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Key:           msg.Key,
		Value:         msg.Value,
		Headers:       msg.Headers,
		CreatedAt:     time.Now(),
		Timeout:       msg.Timeout,
		State:         transaction.StatePrepared,
	}

	err := t.sendHalfMessage(&halfMessage, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to send half message: %w", err)
	}

	t.prepared = true
	t.topic = msg.Topic
	t.partition = msg.Partition

	// do local
	localTxnState := t.producer.listener.ExecuteLocalTransaction(t.ID, halfMessage)

	var finalResult *TransactionResult
	switch localTxnState {
	case transaction.StateCommit:
		finalResult, err := t.Commit()
		if err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
		return finalResult, nil
	case transaction.StateRollback:
		err := t.Rollback()
		if err != nil {
			return nil, fmt.Errorf("failed to rollback transaction: %w", err)
		}
		finalResult = &TransactionResult{
			TransactionID: t.ID,
			Error:         fmt.Errorf("transaction rolled back"),
		}
	case transaction.StateUnknown:
		finalResult = &TransactionResult{
			TransactionID: t.ID,
			Error:         fmt.Errorf("transaction state unknown, will be checked later"),
		}
	default:
		err := t.Rollback()
		if err != nil {
			return nil, fmt.Errorf("failed to rollback transaction: %w", err)
		}
		finalResult = &TransactionResult{
			TransactionID: t.ID,
			Error:         fmt.Errorf("unknown transaction state, rolled back"),
		}
	}

	return finalResult, nil
}

func (t *Transaction) sendHalfMessage(halfMessage *transaction.HalfMessage, msg *TransactionMessage) error {
	req := &transaction.TransactionPrepareRequest{
		TransactionID: t.ID,
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Key:           msg.Key,
		Value:         msg.Value,
		Headers:       msg.Headers,
		Timeout:       int64(msg.Timeout / time.Millisecond),
		ProducerGroup: t.producerGroup,
	}

	conn, err := t.producer.client.connectForDataOperation(msg.Topic, msg.Partition, false)
	if err != nil {
		return &errors.TypedError{
			Type:    errors.PartitionLeaderError,
			Message: errors.FailedToConnectToPartitionMsg,
			Cause:   err,
		}
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(t.producer.client.timeout))

	if err := t.sendTransactionPrepareRequest(conn, req); err != nil {
		return fmt.Errorf("failed to send prepare request: %w", err)
	}

	response, err := t.readTransactionPrepareResponse(conn)
	if err != nil {
		return fmt.Errorf("failed to read prepare response: %w", err)
	}

	if response.ErrorCode != protocol.ErrorNone {
		return fmt.Errorf("prepare failed: %s", response.Error)
	}

	return nil
}

// Commit commits
func (t *Transaction) Commit() (*TransactionResult, error) {
	if !t.prepared {
		return nil, fmt.Errorf("transaction not prepared")
	}

	req := &transaction.TransactionCommitRequest{
		TransactionID: t.ID,
	}

	requestData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal commit request: %w", err)
	}

	// Connect to partition leader for data operation
	conn, err := t.producer.client.connectForDataOperation(t.topic, t.partition, true)
	if err != nil {
		return nil, fmt.Errorf("failed to connect for transaction commit: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(t.producer.client.timeout))

	// Send request type
	if err := binary.Write(conn, binary.BigEndian, protocol.TransactionCommitRequestType); err != nil {
		return nil, fmt.Errorf("failed to write request type: %w", err)
	}

	// Send request length
	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return nil, fmt.Errorf("failed to write request length: %w", err)
	}

	// Send request data
	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to write request data: %w", err)
	}

	// Read response length
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}

	// Read response data
	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}

	var response transaction.TransactionCommitResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, fmt.Errorf("failed to parse commit response: %w", err)
	}

	if response.ErrorCode != protocol.ErrorNone {
		return nil, fmt.Errorf("commit failed: %s", response.Error)
	}

	return &TransactionResult{
		TransactionID: response.TransactionID,
		Offset:        response.Offset,
		Timestamp:     response.Timestamp,
	}, nil
}

// Rollback rollback transaction
func (t *Transaction) Rollback() error {
	if !t.prepared {
		return fmt.Errorf("transaction not prepared")
	}

	req := &transaction.TransactionRollbackRequest{
		TransactionID: t.ID,
	}

	requestData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal rollback request: %w", err)
	}

	// Connect to partition leader for data operation
	conn, err := t.producer.client.connectForDataOperation(t.topic, t.partition, true)
	if err != nil {
		return fmt.Errorf("failed to connect for transaction rollback: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(t.producer.client.timeout))

	// Send request type
	if err := binary.Write(conn, binary.BigEndian, protocol.TransactionRollbackRequestType); err != nil {
		return fmt.Errorf("failed to write request type: %w", err)
	}

	// Send request length
	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return fmt.Errorf("failed to write request length: %w", err)
	}

	// Send request data
	if _, err := conn.Write(requestData); err != nil {
		return fmt.Errorf("failed to write request data: %w", err)
	}

	// Read response length
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return fmt.Errorf("failed to read response length: %w", err)
	}

	// Read response data
	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return fmt.Errorf("failed to read response data: %w", err)
	}

	var response transaction.TransactionRollbackResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return fmt.Errorf("failed to parse rollback response: %w", err)
	}

	if response.ErrorCode != protocol.ErrorNone {
		return fmt.Errorf("rollback failed: %s", response.Error)
	}

	return nil
}

func (tp *TransactionProducer) generateTransactionID() transaction.TransactionID {
	timestamp := time.Now().UnixNano()
	randomBytes := make([]byte, 8)
	// TODO: How to generate a unique ID?
	rand.Read(randomBytes)

	id := fmt.Sprintf("txn_%d_%s", timestamp, hex.EncodeToString(randomBytes))
	return transaction.TransactionID(id)
}

func (t *Transaction) sendTransactionPrepareRequest(conn io.Writer, req *transaction.TransactionPrepareRequest) error {
	requestData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	if err := binary.Write(conn, binary.BigEndian, protocol.TransactionPrepareRequestType); err != nil {
		return fmt.Errorf("failed to send request type: %w", err)
	}

	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return fmt.Errorf("failed to send request length: %w", err)
	}

	if _, err := conn.Write(requestData); err != nil {
		return fmt.Errorf("failed to send request data: %w", err)
	}

	return nil
}

func (t *Transaction) readTransactionPrepareResponse(conn io.Reader) (*transaction.TransactionPrepareResponse, error) {
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}

	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}

	var response transaction.TransactionPrepareResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &response, nil
}
