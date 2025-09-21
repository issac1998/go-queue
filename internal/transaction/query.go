package transaction

import (
	"context"
	"fmt"
	"log"
	"time"
)

// TransactionQueryService provides query operations for transaction data
type TransactionQueryService struct {
	stateMachineGetter StateMachineGetter
	logger             *log.Logger
}

// NewTransactionQueryService creates a new transaction query service
func NewTransactionQueryService(stateMachineGetter StateMachineGetter, logger *log.Logger) *TransactionQueryService {
	return &TransactionQueryService{
		stateMachineGetter: stateMachineGetter,
		logger:             logger,
	}
}

// QueryRequest represents a transaction query request
type QueryRequest struct {
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data"`
}

// QueryResponse represents a transaction query response
type QueryResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// GetHalfMessage retrieves a specific half message by transaction ID
func (tqs *TransactionQueryService) GetHalfMessage(ctx context.Context, groupID uint64, transactionID string) (*QueryResponse, error) {
	stateMachine, err := tqs.stateMachineGetter.GetStateMachine(groupID)
	if err != nil {
		tqs.logger.Printf("Failed to get state machine for group %d: %v", groupID, err)
		return &QueryResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get state machine: %v", err),
		}, nil
	}

	halfMessage, exists := stateMachine.GetHalfMessage(transactionID)
	if !exists {
		return &QueryResponse{
			Success: false,
			Error:   fmt.Sprintf("transaction not found: %s", transactionID),
		}, nil
	}

	return &QueryResponse{
		Success: true,
		Data:    halfMessage,
	}, nil
}

// GetExpiredTransactions retrieves transactions that have exceeded their timeout
func (tqs *TransactionQueryService) GetExpiredTransactions(ctx context.Context, groupID uint64) (*QueryResponse, error) {
	stateMachine, err := tqs.stateMachineGetter.GetStateMachine(groupID)
	if err != nil {
		tqs.logger.Printf("Failed to get state machine for group %d: %v", groupID, err)
		return &QueryResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get state machine: %v", err),
		}, nil
	}

	expiredTransactions := stateMachine.GetTimeoutTransactions()

	return &QueryResponse{
		Success: true,
		Data: map[string]interface{}{
			"expired_transactions": expiredTransactions,
			"count":                len(expiredTransactions),
		},
	}, nil
}

// GetAllExpiredTransactions retrieves expired transactions from all Raft groups
func (tqs *TransactionQueryService) GetAllExpiredTransactions(ctx context.Context) (*QueryResponse, error) {
	allGroups := tqs.stateMachineGetter.GetAllRaftGroups()
	allExpiredTransactions := make([]string, 0)

	for _, groupID := range allGroups {
		stateMachine, err := tqs.stateMachineGetter.GetStateMachine(groupID)
		if err != nil {
			tqs.logger.Printf("Failed to get state machine for group %d: %v", groupID, err)
			continue
		}

		expiredTransactions := stateMachine.GetTimeoutTransactions()
		allExpiredTransactions = append(allExpiredTransactions, expiredTransactions...)
	}

	return &QueryResponse{
		Success: true,
		Data: map[string]interface{}{
			"expired_transactions": allExpiredTransactions,
			"count":                len(allExpiredTransactions),
			"groups_checked":       len(allGroups),
		},
	}, nil
}

// TransactionQueryHandler provides HTTP handlers for transaction queries
type TransactionQueryHandler struct {
	queryService *TransactionQueryService
	logger       *log.Logger
}

// NewTransactionQueryHandler creates a new transaction query handler
func NewTransactionQueryHandler(queryService *TransactionQueryService, logger *log.Logger) *TransactionQueryHandler {
	return &TransactionQueryHandler{
		queryService: queryService,
		logger:       logger,
	}
}

// HandleGetHalfMessage handles GET /api/transactions/{groupId}/{id}/half-message
func (tqh *TransactionQueryHandler) HandleGetHalfMessage(groupID uint64, transactionID string) (*QueryResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return tqh.queryService.GetHalfMessage(ctx, groupID, transactionID)
}

// HandleGetExpiredTransactions handles GET /api/transactions/{groupId}/expired
func (tqh *TransactionQueryHandler) HandleGetExpiredTransactions(groupID uint64) (*QueryResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return tqh.queryService.GetExpiredTransactions(ctx, groupID)
}

// HandleGetAllExpiredTransactions handles GET /api/transactions/expired
func (tqh *TransactionQueryHandler) HandleGetAllExpiredTransactions() (*QueryResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return tqh.queryService.GetAllExpiredTransactions(ctx)
}