package transaction

import (
	"fmt"
	"log"
	"time"
)

// ManagerConfig transaction manager configuration
type ManagerConfig struct {
	StoragePath string `json:"storage_path" yaml:"storage_path"`

	DefaultTimeout   time.Duration `json:"default_timeout" yaml:"default_timeout"`
	CheckInterval    time.Duration `json:"check_interval" yaml:"check_interval"`
	MaxCheckInterval time.Duration `json:"max_check_interval" yaml:"max_check_interval"`

	MaxCheckCount int `json:"max_check_count" yaml:"max_check_count"`

	ExpiryCheckInterval time.Duration `json:"expiry_check_interval" yaml:"expiry_check_interval"`

	EnableRaft  bool   `json:"enable_raft" yaml:"enable_raft"`
	RaftGroupID uint64 `json:"raft_group_id" yaml:"raft_group_id"`

	Logger *log.Logger `json:"-" yaml:"-"`
}

// DefaultManagerConfig returns default configuration
func DefaultManagerConfig() *ManagerConfig {
	return &ManagerConfig{
		StoragePath:         "./data/half_messages",
		DefaultTimeout:      30 * time.Second,
		CheckInterval:       5 * time.Second,
		MaxCheckInterval:    2 * time.Minute,
		MaxCheckCount:       5,
		ExpiryCheckInterval: 30 * time.Second,
		EnableRaft:          false,
		RaftGroupID:         0,
		Logger:              log.New(log.Writer(), "[TransactionManager] ", log.LstdFlags|log.Lshortfile),
	}
}

func (c *ManagerConfig) Validate() error {
	if c.StoragePath == "" {
		return fmt.Errorf("storage path cannot be empty")
	}
	if c.DefaultTimeout <= 0 {
		return fmt.Errorf("default timeout must be positive")
	}
	if c.CheckInterval <= 0 {
		return fmt.Errorf("check interval must be positive")
	}
	if c.MaxCheckInterval <= 0 {
		return fmt.Errorf("max check interval must be positive")
	}
	if c.MaxCheckCount <= 0 {
		return fmt.Errorf("max check count must be positive")
	}
	if c.ExpiryCheckInterval <= 0 {
		return fmt.Errorf("expiry check interval must be positive")
	}
	if c.CheckInterval > c.MaxCheckInterval {
		return fmt.Errorf("check interval cannot be greater than max check interval")
	}
	return nil
}

// PartitionTransactionManagerConfig configuration for partition transaction manager
type PartitionTransactionManagerConfig struct {
	Topic                string
	PartitionID          int32
	StoragePath          string
	DefaultTimeout       time.Duration
	CheckInterval        time.Duration
	MaxCheckInterval     time.Duration
	MaxCheckCount        int
	ExpiryCheckInterval  time.Duration
	EnableRaft           bool
	RaftGroupID          uint64
	Logger               *log.Logger
	RaftProposer         RaftProposer
	StateMachineGetter   StateMachineGetter
	ErrorHandler         *TransactionErrorHandler
}

// DefaultPartitionTransactionManagerConfig returns default configuration for partition transaction manager
func DefaultPartitionTransactionManagerConfig(topic string, partitionID int32) *PartitionTransactionManagerConfig {
	return &PartitionTransactionManagerConfig{
		Topic:            topic,
		PartitionID:      partitionID,
		DefaultTimeout:   30 * time.Second,
		CheckInterval:    5 * time.Second,
		MaxCheckInterval: 2 * time.Minute,
		MaxCheckCount:    5,
		Logger:           log.New(log.Writer(), "[PartitionTransactionManager] ", log.LstdFlags|log.Lshortfile),
	}
}

func (c *PartitionTransactionManagerConfig) Validate() error {
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if c.PartitionID < 0 {
		return fmt.Errorf("partition ID must be non-negative")
	}
	if c.DefaultTimeout <= 0 {
		return fmt.Errorf("default timeout must be positive")
	}
	if c.CheckInterval <= 0 {
		return fmt.Errorf("check interval must be positive")
	}
	if c.MaxCheckInterval <= 0 {
		return fmt.Errorf("max check interval must be positive")
	}
	if c.MaxCheckCount <= 0 {
		return fmt.Errorf("max check count must be positive")
	}
	if c.CheckInterval > c.MaxCheckInterval {
		return fmt.Errorf("check interval cannot be greater than max check interval")
	}
	return nil
}