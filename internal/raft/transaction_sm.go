package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
	"github.com/issac1998/go-queue/internal/transaction"
	typederrors "github.com/issac1998/go-queue/internal/errors"
	"github.com/lni/dragonboat/v3/statemachine"
)

// TransactionStateMachine implements statemachine.IStateMachine for transaction data
type TransactionStateMachine struct {
	halfMessages map[transaction.TransactionID]*transaction.HalfMessage

	producerGroupCallbacks map[string]string

	mu sync.RWMutex

	lastAppliedIndex uint64
	dataDir          string
}

// TransactionCommand represents a transaction command
type TransactionCommand struct {
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data"`
}

// NewTransactionStateMachine creates a new transaction state machine
func NewTransactionStateMachine(dataDir string) *TransactionStateMachine {
	return &TransactionStateMachine{
		halfMessages:           make(map[transaction.TransactionID]*transaction.HalfMessage),
		producerGroupCallbacks: make(map[string]string),
		dataDir:                dataDir,
	}
}

// Update implements statemachine.IStateMachine interface
func (tsm *TransactionStateMachine) Update(data []byte) (statemachine.Result, error) {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	var cmd TransactionCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		log.Printf("Failed to unmarshal transaction command: %v", err)
		return statemachine.Result{Value: 0}, err
	}

	result, err := tsm.executeCommand(&cmd)
	if err != nil {
		log.Printf("Failed to execute transaction command %s: %v", cmd.Type, err)
		return statemachine.Result{Value: 0}, err
	}

	resultData, _ := json.Marshal(result)
	return statemachine.Result{
		Value: uint64(len(resultData)),
		Data:  resultData,
	}, nil
}

// executeCommand executes a transaction command
func (tsm *TransactionStateMachine) executeCommand(cmd *TransactionCommand) (interface{}, error) {
	switch cmd.Type {
	case protocol.RaftCmdStoreHalfMessage:
		return tsm.storeHalfMessage(cmd.Data)
	case protocol.RaftCmdUpdateTransactionState:
		return tsm.updateTransactionState(cmd.Data)
	case protocol.RaftCmdDeleteHalfMessage:
		return tsm.deleteHalfMessage(cmd.Data)
	case protocol.RaftCmdRegisterProducerGroup:
		return tsm.registerProducerGroup(cmd.Data)
	case protocol.RaftCmdUnregisterProducerGroup:
		return tsm.unregisterProducerGroup(cmd.Data)
	default:
		return nil, fmt.Errorf("unknown transaction command type: %s", cmd.Type)
	}
}

// storeHalfMessage stores a half message
func (tsm *TransactionStateMachine) storeHalfMessage(data map[string]interface{}) (interface{}, error) {
	msgData, _ := json.Marshal(data["half_message"])
	var halfMessage transaction.HalfMessage
	if err := json.Unmarshal(msgData, &halfMessage); err != nil {
		return nil, typederrors.NewTypedError(typederrors.GeneralError, "failed to parse half message", err)
	}

	tsm.halfMessages[halfMessage.TransactionID] = &halfMessage

	log.Printf("Stored half message for transaction: %s", halfMessage.TransactionID)
	return map[string]interface{}{
		"transaction_id": halfMessage.TransactionID,
		"status":         "stored",
	}, nil
}

// updateTransactionState updates the state of a transaction
func (tsm *TransactionStateMachine) updateTransactionState(data map[string]interface{}) (interface{}, error) {
	transactionID := transaction.TransactionID(data["transaction_id"].(string))
	newState := transaction.TransactionState(data["state"].(float64))

	halfMessage, exists := tsm.halfMessages[transactionID]
	if !exists {
		return nil, fmt.Errorf("transaction not found: %s", transactionID)
	}

	oldState := halfMessage.State
	halfMessage.State = newState

	log.Printf("Updated transaction %s state from %d to %d", transactionID, oldState, newState)
	return map[string]interface{}{
		"transaction_id": transactionID,
		"old_state":      oldState,
		"new_state":      newState,
	}, nil
}

// deleteHalfMessage deletes a half message
func (tsm *TransactionStateMachine) deleteHalfMessage(data map[string]interface{}) (interface{}, error) {
	transactionID := transaction.TransactionID(data["transaction_id"].(string))

	_, exists := tsm.halfMessages[transactionID]
	if !exists {
		return nil, fmt.Errorf("transaction not found: %s", transactionID)
	}

	delete(tsm.halfMessages, transactionID)

	log.Printf("Deleted half message for transaction: %s", transactionID)
	return map[string]interface{}{
		"transaction_id": transactionID,
		"status":         "deleted",
	}, nil
}

// registerProducerGroup registers a producer group
func (tsm *TransactionStateMachine) registerProducerGroup(data map[string]interface{}) (interface{}, error) {
	producerGroup := data["producer_group"].(string)
	callbackAddr := data["callback_addr"].(string)

	tsm.producerGroupCallbacks[producerGroup] = callbackAddr

	log.Printf("Registered producer group: %s with callback: %s", producerGroup, callbackAddr)
	return map[string]interface{}{
		"producer_group": producerGroup,
		"callback_addr":  callbackAddr,
		"status":         "registered",
	}, nil
}

// unregisterProducerGroup unregisters a producer group
func (tsm *TransactionStateMachine) unregisterProducerGroup(data map[string]interface{}) (interface{}, error) {
	producerGroup := data["producer_group"].(string)

	delete(tsm.producerGroupCallbacks, producerGroup)

	log.Printf("Unregistered producer group: %s", producerGroup)
	return map[string]interface{}{
		"producer_group": producerGroup,
		"status":         "unregistered",
	}, nil
}

// Lookup implements statemachine.IStateMachine interface for read operations
func (tsm *TransactionStateMachine) Lookup(query interface{}) (interface{}, error) {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	queryBytes, ok := query.([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid query format")
	}

	var queryCmd TransactionQuery
	if err := json.Unmarshal(queryBytes, &queryCmd); err != nil {
		return nil, typederrors.NewTypedError(typederrors.GeneralError, "failed to unmarshal query", err)
	}

	switch queryCmd.Type {
	case "get_half_message":
		transactionID := transaction.TransactionID(queryCmd.Data["transaction_id"].(string))
		if halfMessage, exists := tsm.halfMessages[transactionID]; exists {
			return halfMessage, nil
		}
		return nil, fmt.Errorf("transaction not found: %s", transactionID)

	case "get_all_half_messages":
		result := make(map[transaction.TransactionID]*transaction.HalfMessage)
		for id, msg := range tsm.halfMessages {
			msgCopy := *msg
			result[id] = &msgCopy
		}
		return result, nil

	case "get_producer_groups":
		result := make(map[string]string)
		for group, callback := range tsm.producerGroupCallbacks {
			result[group] = callback
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unknown query type: %s", queryCmd.Type)
	}
}

// TransactionQuery represents a query for transaction data
type TransactionQuery struct {
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data"`
}

// SaveSnapshot implements statemachine.IStateMachine interface
func (tsm *TransactionStateMachine) SaveSnapshot(w io.Writer, fc statemachine.ISnapshotFileCollection, done <-chan struct{}) error {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	snapshot := TransactionSnapshot{
		HalfMessages:           tsm.halfMessages,
		ProducerGroupCallbacks: tsm.producerGroupCallbacks,
		LastAppliedIndex:       tsm.lastAppliedIndex,
		Timestamp:              time.Now(),
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return typederrors.NewTypedError(typederrors.GeneralError, "failed to marshal snapshot", err)
	}

	if _, err := w.Write(data); err != nil {
		return typederrors.NewTypedError(typederrors.GeneralError, "failed to write snapshot", err)
	}

	log.Printf("Saved transaction state machine snapshot")
	return nil
}

// RecoverFromSnapshot implements statemachine.IStateMachine interface
func (tsm *TransactionStateMachine) RecoverFromSnapshot(r io.Reader, files []statemachine.SnapshotFile, done <-chan struct{}) error {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	data, err := io.ReadAll(r)
	if err != nil {
		return typederrors.NewTypedError(typederrors.GeneralError, "failed to read snapshot", err)
	}

	var snapshot TransactionSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return typederrors.NewTypedError(typederrors.GeneralError, "failed to unmarshal snapshot", err)
	}

	tsm.halfMessages = snapshot.HalfMessages
	tsm.producerGroupCallbacks = snapshot.ProducerGroupCallbacks
	tsm.lastAppliedIndex = snapshot.LastAppliedIndex

	log.Printf("Recovered transaction state machine from snapshot at index %d", tsm.lastAppliedIndex)
	return nil
}

// Close implements statemachine.IStateMachine interface
func (tsm *TransactionStateMachine) Close() error {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	tsm.halfMessages = nil
	tsm.producerGroupCallbacks = nil

	log.Printf("Transaction state machine closed")
	return nil
}

// TransactionSnapshot represents a snapshot of transaction state
type TransactionSnapshot struct {
	HalfMessages           map[transaction.TransactionID]*transaction.HalfMessage `json:"half_messages"`
	ProducerGroupCallbacks map[string]string                                      `json:"producer_group_callbacks"`
	LastAppliedIndex       uint64                                                 `json:"last_applied_index"`
	Timestamp              time.Time                                              `json:"timestamp"`
}
