package errors

import (
	"fmt"
	"strings"
)

// ErrorType represents the type of error
type ErrorType int

const (
	// Connection related error types
	ConnectionError ErrorType = iota
	TimeoutError

	// Controller related error types
	ControllerError
	LeadershipError

	// Partition related error types
	PartitionError
	PartitionLeaderError

	// Storage related error types
	StorageError

	// General error types
	GeneralError
)

// TypedError represents an error with a specific type
type TypedError struct {
	Type    ErrorType
	Message string
	Cause   error
}

// Error implements the error interface
func (e *TypedError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

// NewTypedError creates a new typed error
func NewTypedError(errorType ErrorType, message string, cause error) *TypedError {
	return &TypedError{
		Type:    errorType,
		Message: message,
		Cause:   cause,
	}
}

// Error constants for common error types
const (
	ConnectionRefusedMsg          = "connection refused"
	ConnectionResetMsg            = "connection reset"
	NoRouteToHostMsg              = "no route to host"
	TimeoutMsg                    = "timeout"
	NotLeaderMsg                  = "not leader"
	NotControllerMsg              = "not controller"
	ControllerNotAvailableMsg     = "controller not available"
	ControllerErrorMsg            = "controller error"
	NotTheLeaderMsg               = "not the leader"
	PartitionLeaderNotFoundMsg    = "partition leader not found"
	FailedToConnectToPartitionMsg = "failed to connect to partition"
	LeaderIsMsg                   = "leader is"
)

// Error checking functions

// IsConnectionError checks if the error is a connection-related error
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}

	if typedErr, ok := err.(*TypedError); ok {
		return typedErr.Type == ConnectionError || typedErr.Type == TimeoutError
	}

	// Fallback to string matching for backward compatibility
	errorStr := err.Error()
	return contains(errorStr, "connection refused") ||
		contains(errorStr, "connection reset") ||
		contains(errorStr, "no route to host") ||
		contains(errorStr, "timeout")
}

// IsControllerError checks if the error is controller-related
func IsControllerError(err error) bool {
	if err == nil {
		return false
	}

	if typedErr, ok := err.(*TypedError); ok {
		return typedErr.Type == ControllerError ||
			typedErr.Type == LeadershipError ||
			typedErr.Type == ConnectionError
	}

	// Fallback to string matching for backward compatibility
	errorStr := err.Error()
	return contains(errorStr, "controller") ||
		contains(errorStr, "not leader") ||
		contains(errorStr, "not controller") ||
		contains(errorStr, "connection refused") ||
		contains(errorStr, "connection reset")
}

// IsPartitionLeaderError checks if the error is partition leader-related
func IsPartitionLeaderError(err error) bool {
	if err == nil {
		return false
	}

	// Check if it's a TypedError with partition-related type
	if typedErr, ok := err.(*TypedError); ok {
		return typedErr.Type == PartitionError || typedErr.Type == PartitionLeaderError
	}

	// Fallback to string matching for backward compatibility
	errorStr := err.Error()
	return contains(errorStr, "not the leader") ||
		contains(errorStr, "partition leader not found") ||
		contains(errorStr, "leader is") ||
		contains(errorStr, "failed to connect to partition") ||
		contains(errorStr, "connection refused") ||
		contains(errorStr, "no route to host")
}

// ShouldRetryWithMetadataRefresh determines if an error should trigger
// metadata refresh and retry
func ShouldRetryWithMetadataRefresh(err error) bool {
	return IsPartitionLeaderError(err) || IsControllerError(err)
}

// GetErrorType returns the error type if it's a TypedError, otherwise returns GeneralError
func GetErrorType(err error) ErrorType {
	if typedErr, ok := err.(*TypedError); ok {
		return typedErr.Type
	}
	return GeneralError
}

// contains is a helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
