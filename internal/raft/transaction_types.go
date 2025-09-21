package raft

import "time"

// Local transaction types to avoid circular imports
type TransactionID string
type TransactionState int

const (
	StatePrepared TransactionState = iota
	StateCommit
	StateRollback
)

type HalfMessage struct {
	TransactionID TransactionID     `json:"transaction_id"`
	ProducerGroup string            `json:"producer_group"`
	Topic         string            `json:"topic"`
	Partition     int32             `json:"partition"`
	Key           []byte            `json:"key,omitempty"`
	Value         []byte            `json:"value"`
	Headers       map[string]string `json:"headers,omitempty"`
	State         TransactionState  `json:"state"`
	CreatedAt     time.Time         `json:"created_at"`
	Timeout       time.Duration     `json:"timeout"`
	Messages      [][]byte          `json:"messages"`
	Timestamp     time.Time         `json:"timestamp"`
}