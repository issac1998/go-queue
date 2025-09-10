package transaction

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// TransactionManager
type TransactionManager struct {
	halfMessages map[TransactionID]*HalfMessage
	mu           sync.RWMutex

	defaultTimeout   time.Duration
	maxCheckCount    int
	checkInterval    time.Duration
	maxCheckInterval time.Duration

	producerGroupCallbacks map[string]string
	producerGroupCheckers  map[string]*DefaultTransactionChecker

	enableRaft   bool
	raftGroupID  uint64
	raftProposer RaftProposer

	stopChan chan struct{}
}

// NewTransactionManager create txn
func NewTransactionManager() *TransactionManager {
	tm := &TransactionManager{
		halfMessages:           make(map[TransactionID]*HalfMessage),
		defaultTimeout:         30 * time.Second,
		maxCheckCount:          5,
		checkInterval:          5 * time.Second,
		maxCheckInterval:       2 * time.Minute,
		producerGroupCallbacks: make(map[string]string),
		producerGroupCheckers:  make(map[string]*DefaultTransactionChecker),
		enableRaft:             false,
		stopChan:               make(chan struct{}),
	}

	go tm.startTransactionChecker()

	return tm
}

// EnableRaft enables Raft storage for transactions
func (tm *TransactionManager) EnableRaft(raftProposer RaftProposer, groupID uint64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.enableRaft = true
	tm.raftProposer = raftProposer
	tm.raftGroupID = groupID

	log.Printf("Enabled Raft storage for transactions with group ID: %d", groupID)
}

// PrepareTransaction prepare transaction
func (tm *TransactionManager) PrepareTransaction(req *TransactionPrepareRequest) (*TransactionPrepareResponse, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.halfMessages[req.TransactionID]; exists {
		return &TransactionPrepareResponse{
			TransactionID: req.TransactionID,
			ErrorCode:     protocol.ErrorInvalidRequest,
			Error:         "transaction already exists",
		}, nil
	}

	timeout := tm.defaultTimeout
	if req.Timeout > 0 {
		timeout = time.Duration(req.Timeout) * time.Millisecond
	}

	_, exists := tm.producerGroupCheckers[req.ProducerGroup]
	if !exists {
		return &TransactionPrepareResponse{
			TransactionID: req.TransactionID,
			ErrorCode:     protocol.ErrorInvalidRequest,
			Error:         fmt.Sprintf("producer group not registered: %s", req.ProducerGroup),
		}, nil
	}

	halfMessage := &HalfMessage{
		TransactionID: req.TransactionID,
		Topic:         req.Topic,
		Partition:     req.Partition,
		Key:           req.Key,
		Value:         req.Value,
		Headers:       req.Headers,
		ProducerGroup: req.ProducerGroup,
		CreatedAt:     time.Now(),
		Timeout:       timeout,
		State:         StatePrepared,
		CheckCount:    0,
		LastCheck:     time.Time{},
	}

	tm.halfMessages[req.TransactionID] = halfMessage

	if tm.enableRaft && tm.raftProposer != nil {
		if err := tm.storeHalfMessageToRaft(halfMessage); err != nil {
			delete(tm.halfMessages, req.TransactionID)
			log.Printf("Failed to store half message to Raft: %v", err)
			return &TransactionPrepareResponse{
				TransactionID: req.TransactionID,
				ErrorCode:     protocol.ErrorInternalError,
				Error:         "failed to persist transaction",
			}, nil
		}
	}

	return &TransactionPrepareResponse{
		TransactionID: req.TransactionID,
		ErrorCode:     protocol.ErrorNone,
	}, nil
}

// CommitTransaction commit txn
func (tm *TransactionManager) CommitTransaction(transactionID TransactionID) (*TransactionCommitResponse, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	halfMessage, exists := tm.halfMessages[transactionID]
	if !exists {
		return &TransactionCommitResponse{
			TransactionID: transactionID,
			ErrorCode:     protocol.ErrorInvalidRequest,
			Error:         "transaction not found",
		}, nil
	}

	halfMessage.State = StateCommit

	if tm.enableRaft {
		if err := tm.deleteHalfMessageFromRaft(transactionID); err != nil {
			log.Printf("Failed to delete half message from Raft: %v", err)
		}
	}

	delete(tm.halfMessages, transactionID)

	log.Printf("Transaction committed: %s", transactionID)

	return &TransactionCommitResponse{
		TransactionID: transactionID,
		Offset:        -1,
		Timestamp:     time.Now(),
		ErrorCode:     protocol.ErrorNone,
	}, nil
}

// RollbackTransaction rollback transaction
func (tm *TransactionManager) RollbackTransaction(transactionID TransactionID) (*TransactionRollbackResponse, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.enableRaft {
		if err := tm.deleteHalfMessageFromRaft(transactionID); err != nil {
			log.Printf("Failed to delete half message from Raft: %v", err)
		}
	}

	delete(tm.halfMessages, transactionID)
	return &TransactionRollbackResponse{
		TransactionID: transactionID,
		ErrorCode:     protocol.ErrorNone,
	}, nil
}

// GetHalfMessage get half message
func (tm *TransactionManager) GetHalfMessage(transactionID TransactionID) (*HalfMessage, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	message, exists := tm.halfMessages[transactionID]
	return message, exists
}

// GetAllHalfMessages get all half messages
func (tm *TransactionManager) GetAllHalfMessages() map[TransactionID]*HalfMessage {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	result := make(map[TransactionID]*HalfMessage)
	for id, message := range tm.halfMessages {
		messageCopy := *message
		result[id] = &messageCopy
	}
	return result
}

// RegisterProducerGroup 注册生产者组和对应的回调地址
func (tm *TransactionManager) RegisterProducerGroup(producerGroup, callbackAddr string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	checker := NewDefaultTransactionChecker()
	checker.RegisterProducerGroup(producerGroup, callbackAddr)

	tm.producerGroupCallbacks[producerGroup] = callbackAddr
	tm.producerGroupCheckers[producerGroup] = checker

	if tm.enableRaft {
		if err := tm.registerProducerGroupInRaft(producerGroup, callbackAddr); err != nil {
			delete(tm.producerGroupCallbacks, producerGroup)
			delete(tm.producerGroupCheckers, producerGroup)
			log.Printf("Failed to register producer group in Raft: %v", err)
			return fmt.Errorf("failed to persist producer group registration")
		}
	}

	log.Printf("Registered producer group: %s with callback: %s", producerGroup, callbackAddr)
	return nil
}

// UnregisterProducerGroup unregister producer group
func (tm *TransactionManager) UnregisterProducerGroup(producerGroup string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	delete(tm.producerGroupCallbacks, producerGroup)
	delete(tm.producerGroupCheckers, producerGroup)

	log.Printf("Unregistered producer group: %s", producerGroup)
	return nil
}

// GetRegisteredProducerGroups get registered producer groups
func (tm *TransactionManager) GetRegisteredProducerGroups() map[string]string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	result := make(map[string]string)
	for group, callback := range tm.producerGroupCallbacks {
		result[group] = callback
	}
	return result
}

func (tm *TransactionManager) startTransactionChecker() {
	ticker := time.NewTicker(tm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.checkTimeoutTransactions()
		case <-tm.stopChan:
			return
		}
	}
}

func (tm *TransactionManager) checkTimeoutTransactions() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	now := time.Now()
	var timeoutTransactions []TransactionID
	// do we need to sort to avoid for loop?
	for transactionID, halfMessage := range tm.halfMessages {
		if now.Sub(halfMessage.CreatedAt) > halfMessage.Timeout {
			// exceed retry time
			if halfMessage.CheckCount >= tm.maxCheckCount {
				timeoutTransactions = append(timeoutTransactions, transactionID)
				continue
			}

			checkInterval := tm.calculateCheckInterval(halfMessage.CheckCount)
			if halfMessage.LastCheck.IsZero() || now.Sub(halfMessage.LastCheck) >= checkInterval {
				if checker, exists := tm.producerGroupCheckers[halfMessage.ProducerGroup]; exists {
					halfMessage.State = StateChecking
					halfMessage.CheckCount++
					halfMessage.LastCheck = now

					go func(id TransactionID, msg *HalfMessage, ch *DefaultTransactionChecker) {
						state := ch.CheckTransactionState(id, *msg)
						tm.handleCheckResult(id, state)
					}(transactionID, halfMessage, checker)
				} else {
					log.Printf("No checker found for producer group: %s", halfMessage.ProducerGroup)
					halfMessage.State = StateRollback
					tm.handleCheckResult(transactionID, StateUnknown)
				}
			}
		}
	}

	for _, transactionID := range timeoutTransactions {
		tm.RollbackTransaction(transactionID)
		log.Printf("Transaction timeout, rolling back: %s", transactionID)
	}
}

func (tm *TransactionManager) calculateCheckInterval(checkCount int) time.Duration {
	interval := tm.checkInterval * time.Duration(1<<uint(checkCount))
	if interval > tm.maxCheckInterval {
		interval = tm.maxCheckInterval
	}
	return interval
}

func (tm *TransactionManager) handleCheckResult(transactionID TransactionID, state TransactionState) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	halfMessage, exists := tm.halfMessages[transactionID]
	if !exists {
		return
	}

	switch state {
	case StateCommit:
		log.Printf("Transaction check result: COMMIT %s", transactionID)
		tm.CommitTransaction(transactionID)

	case StateRollback:
		log.Printf("Transaction check result: ROLLBACK %s", transactionID)
		tm.RollbackTransaction(transactionID)
	default:
		log.Printf("Unknown transaction state from check: %d for %s", state, transactionID)
		halfMessage.State = StatePrepared
	}
}

// Stop stop
func (tm *TransactionManager) Stop() {
	close(tm.stopChan)
}

func (tm *TransactionManager) storeHalfMessageToRaft(halfMessage *HalfMessage) error {
	if !tm.enableRaft || tm.raftProposer == nil {
		return nil
	}

	cmd := map[string]interface{}{
		"type": protocol.RaftCmdStoreHalfMessage,
		"data": map[string]interface{}{
			"half_message": halfMessage,
		},
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal store command: %v", err)
	}

	return tm.raftProposer.ProposeCommand(tm.raftGroupID, data)
}

func (tm *TransactionManager) updateTransactionStateInRaft(transactionID TransactionID, state TransactionState) error {
	if !tm.enableRaft || tm.raftProposer == nil {
		return nil
	}

	cmd := map[string]interface{}{
		"type": protocol.RaftCmdUpdateTransactionState,
		"data": map[string]interface{}{
			"transaction_id": string(transactionID),
			"state":          int16(state),
		},
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal update command: %v", err)
	}

	return tm.raftProposer.ProposeCommand(tm.raftGroupID, data)
}

func (tm *TransactionManager) deleteHalfMessageFromRaft(transactionID TransactionID) error {
	if !tm.enableRaft || tm.raftProposer == nil {
		return nil
	}

	cmd := map[string]interface{}{
		"type": protocol.RaftCmdDeleteHalfMessage,
		"data": map[string]interface{}{
			"transaction_id": string(transactionID),
		},
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal delete command: %v", err)
	}

	return tm.raftProposer.ProposeCommand(tm.raftGroupID, data)
}

// registerProducerGroupInRaft registers a producer group in Raft
func (tm *TransactionManager) registerProducerGroupInRaft(producerGroup, callbackAddr string) error {
	if !tm.enableRaft || tm.raftProposer == nil {
		return nil
	}

	cmd := map[string]interface{}{
		"type": protocol.RaftCmdRegisterProducerGroup,
		"data": map[string]interface{}{
			"producer_group": producerGroup,
			"callback_addr":  callbackAddr,
		},
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal register command: %v", err)
	}

	return tm.raftProposer.ProposeCommand(tm.raftGroupID, data)
}
