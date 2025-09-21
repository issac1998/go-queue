package transaction

import (
	"fmt"
	"log"
	"time"

	typederrors "github.com/issac1998/go-queue/internal/errors"
)

// TransactionErrorHandler provides enhanced error handling for transaction operations
type TransactionErrorHandler struct {
	logger *log.Logger
}

// NewTransactionErrorHandler creates a new transaction error handler
func NewTransactionErrorHandler(logger *log.Logger) *TransactionErrorHandler {
	return &TransactionErrorHandler{
		logger: logger,
	}
}

// ErrorContext provides context information for error handling
type ErrorContext struct {
	TransactionID string
	Operation     string
	ProducerGroup string
	Topic         string
	Partition     int32
	Timestamp     time.Time
	RetryCount    int
	MaxRetries    int
}

// HandleTransactionError handles transaction-related errors with appropriate logging and recovery
func (teh *TransactionErrorHandler) HandleTransactionError(err error, context ErrorContext) error {
	if err == nil {
		return nil
	}

	// Log the error with context
	teh.logError(err, context)

	// Determine if the error is recoverable
	if teh.isRecoverableError(err) {
		return teh.handleRecoverableError(err, context)
	}

	// Handle non-recoverable errors
	return teh.handleNonRecoverableError(err, context)
}

// logError logs the error with detailed context information
func (teh *TransactionErrorHandler) logError(err error, context ErrorContext) {
	teh.logger.Printf(
		"[TRANSACTION_ERROR] Operation: %s, TransactionID: %s, ProducerGroup: %s, Topic: %s, Partition: %d, RetryCount: %d/%d, Error: %v",
		context.Operation,
		context.TransactionID,
		context.ProducerGroup,
		context.Topic,
		context.Partition,
		context.RetryCount,
		context.MaxRetries,
		err,
	)
}

// isRecoverableError determines if an error can be recovered from
func (teh *TransactionErrorHandler) isRecoverableError(err error) bool {
	// Check for connection errors
	if typederrors.IsConnectionError(err) {
		return true
	}

	// Check for typed errors
	if typedErr, ok := err.(*typederrors.TypedError); ok {
		switch typedErr.Type {
		case typederrors.ConnectionError, typederrors.TimeoutError:
			return true
		case typederrors.GeneralError:
			return false
		default:
			return false
		}
	}

	return false
}

// handleRecoverableError handles errors that can potentially be recovered from
func (teh *TransactionErrorHandler) handleRecoverableError(err error, context ErrorContext) error {
	if context.RetryCount >= context.MaxRetries {
		teh.logger.Printf(
			"[TRANSACTION_ERROR] Max retries exceeded for transaction %s, operation %s",
			context.TransactionID,
			context.Operation,
		)
		return typederrors.NewTypedError(
			typederrors.GeneralError,
			fmt.Sprintf("max retries exceeded for transaction %s", context.TransactionID),
			err,
		)
	}

	// Calculate retry delay with exponential backoff
	retryDelay := teh.calculateRetryDelay(context.RetryCount)
	
	teh.logger.Printf(
		"[TRANSACTION_ERROR] Recoverable error for transaction %s, will retry in %v",
		context.TransactionID,
		retryDelay,
	)

	// Return a wrapped error indicating retry is needed
	return &RetryableError{
		OriginalError: err,
		RetryDelay:    retryDelay,
		Context:       context,
	}
}

// handleNonRecoverableError handles errors that cannot be recovered from
func (teh *TransactionErrorHandler) handleNonRecoverableError(err error, context ErrorContext) error {
	teh.logger.Printf(
		"[TRANSACTION_ERROR] Non-recoverable error for transaction %s, operation %s: %v",
		context.TransactionID,
		context.Operation,
		err,
	)

	// Mark transaction for rollback if it's in a prepared state
	if context.Operation == "commit" || context.Operation == "prepare" {
		teh.logger.Printf(
			"[TRANSACTION_ERROR] Marking transaction %s for rollback due to non-recoverable error",
			context.TransactionID,
		)
	}

	return typederrors.NewTypedError(
		typederrors.GeneralError,
		fmt.Sprintf("non-recoverable error in transaction %s", context.TransactionID),
		err,
	)
}

// calculateRetryDelay calculates the delay before the next retry using exponential backoff
func (teh *TransactionErrorHandler) calculateRetryDelay(retryCount int) time.Duration {
	baseDelay := 100 * time.Millisecond
	maxDelay := 30 * time.Second

	delay := baseDelay * time.Duration(1<<uint(retryCount))
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

// RetryableError represents an error that can be retried
type RetryableError struct {
	OriginalError error
	RetryDelay    time.Duration
	Context       ErrorContext
}

func (re *RetryableError) Error() string {
	return fmt.Sprintf("retryable error: %v (retry in %v)", re.OriginalError, re.RetryDelay)
}

func (re *RetryableError) Unwrap() error {
	return re.OriginalError
}

// IsRetryableError checks if an error is retryable
func IsRetryableError(err error) bool {
	_, ok := err.(*RetryableError)
	return ok
}

// TransactionErrorMetrics provides metrics collection for transaction errors
type TransactionErrorMetrics struct {
	logger *log.Logger
}

// NewTransactionErrorMetrics creates a new transaction error metrics collector
func NewTransactionErrorMetrics(logger *log.Logger) *TransactionErrorMetrics {
	return &TransactionErrorMetrics{
		logger: logger,
	}
}

// RecordError records an error occurrence for metrics
func (tem *TransactionErrorMetrics) RecordError(errorType string, operation string, producerGroup string) {
	tem.logger.Printf(
		"[TRANSACTION_METRICS] ErrorType: %s, Operation: %s, ProducerGroup: %s, Timestamp: %v",
		errorType,
		operation,
		producerGroup,
		time.Now(),
	)
}

// RecordRetry records a retry attempt for metrics
func (tem *TransactionErrorMetrics) RecordRetry(transactionID string, operation string, retryCount int) {
	tem.logger.Printf(
		"[TRANSACTION_METRICS] Retry - TransactionID: %s, Operation: %s, RetryCount: %d, Timestamp: %v",
		transactionID,
		operation,
		retryCount,
		time.Now(),
	)
}

// RecordSuccess records a successful operation for metrics
func (tem *TransactionErrorMetrics) RecordSuccess(transactionID string, operation string, duration time.Duration) {
	tem.logger.Printf(
		"[TRANSACTION_METRICS] Success - TransactionID: %s, Operation: %s, Duration: %v, Timestamp: %v",
		transactionID,
		operation,
		duration,
		time.Now(),
	)
}