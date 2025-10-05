package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/compression"
	"github.com/issac1998/go-queue/internal/delayed"
	"github.com/issac1998/go-queue/internal/discovery"
	"github.com/issac1998/go-queue/internal/protocol"
	"github.com/issac1998/go-queue/internal/raft"
	"github.com/issac1998/go-queue/internal/transaction"
	"github.com/lni/dragonboat/v3"
)

// Broker is the core node of the system, integrating all functionality
type Broker struct {
	// Basic information
	ID      string
	Address string
	Port    int

	// NodeHost - Dragonboat NodeHost instance
	NodeHost *dragonboat.NodeHost

	// Controller functionality
	Controller *ControllerManager

	// Consumer Group management
	ConsumerGroupManager *ConsumerGroupManager

	// Delayed message management
	DelayedMessageManager *delayed.DelayedMessageManager
	delayedScanner        *DelayedMessageScanner

	// Client service
	ClientServer *ClientServer

	// Configuration
	Config *BrokerConfig

	// Lifecycle management
	Ctx    context.Context
	Cancel context.CancelFunc
	wg     sync.WaitGroup

	// Raft manager
	raftManager *raft.RaftManager

	// Service discovery
	discovery discovery.Discovery

	systemMetrics *SystemMetrics
	LoadMetrics   *LoadMetrics

	compressor compression.Compressor

	// Ordered message routing
	orderedRouter *OrderedMessageRouter

	// Independent logger for this broker instance
	logger *log.Logger

	mu sync.RWMutex
}

func (b *Broker) panicHandler(component string) {
	if r := recover(); r != nil {
		stack := debug.Stack()
		if b.logger != nil {
			b.logger.Printf("PANIC in %s: %v\nStack trace:\n%s", component, r, string(stack))
		} else {
			log.Printf("PANIC in %s: %v\nStack trace:\n%s", component, r, string(stack))
		}

		if logFile, err := os.OpenFile("broker-panic.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err == nil {
			fmt.Fprintf(logFile, "[%s] PANIC in %s (Broker %s): %v\nStack trace:\n%s\n\n",
				time.Now().Format("2006-01-02 15:04:05"), component, b.ID, r, string(stack))
			logFile.Close()
		}
	}
}

// BrokerConfig contains all broker configuration
type BrokerConfig struct {
	// Node configuration
	NodeID   string `yaml:"node_id"`
	BindAddr string `yaml:"bind_addr"`
	BindPort int    `yaml:"bind_port"`

	// Data directory
	DataDir string `yaml:"data_dir"`

	// Log configuration
	LogDir string `yaml:"log_dir"`

	// Raft configuration
	RaftConfig *raft.RaftConfig `yaml:"raft"`

	// Cluster discovery configuration
	Discovery *discovery.DiscoveryConfig `yaml:"discovery"`

	// Performance tuning
	Performance *PerformanceConfig `yaml:"performance"`

	// FollowerRead configuration
	EnableFollowerRead bool `yaml:"enable_follower_read"`

	// Compression configuration
	CompressionEnabled   bool   `yaml:"compression_enabled"`
	CompressionType      string `yaml:"compression_type"`
	CompressionThreshold int    `yaml:"compression_threshold"`

	DeduplicationEnabled bool `yaml:"deduplication_enabled"`
	DeduplicationTTL     int  `yaml:"deduplication_ttl"`
	DeduplicationMaxSize int  `yaml:"deduplication_max_size"`
}

// PerformanceConfig contains performance tuning configuration
type PerformanceConfig struct {
	// Batch configuration
	MaxBatchSize  int    `yaml:"max_batch_size"`
	MaxBatchBytes int64  `yaml:"max_batch_bytes"`
	BatchTimeout  string `yaml:"batch_timeout"`

	// Cache configuration
	WriteCacheSize int64 `yaml:"write_cache_size"`
	ReadCacheSize  int64 `yaml:"read_cache_size"`

	// Connection pool configuration
	ConnectionPoolSize    int    `yaml:"connection_pool_size"`
	ConnectionIdleTimeout string `yaml:"connection_idle_timeout"`

	// Compression configuration
	CompressionType      string `yaml:"compression_type"`
	CompressionThreshold int    `yaml:"compression_threshold"`
}

// NewBroker creates a new Broker instance
func NewBroker(config *BrokerConfig) (*Broker, error) {
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	broker := &Broker{
		ID:      config.NodeID,
		Address: config.BindAddr,
		Port:    config.BindPort,
		Config:  config,
		Ctx:     ctx,
		Cancel:  cancel,
	}

	return broker, nil
}

// Start initializes and starts the broker
func (b *Broker) Start() error {
	defer b.panicHandler("Broker.Start")

	// 0. Initialize logging
	if err := b.initLogging(); err != nil {
		return fmt.Errorf("logging init failed: %v", err)
	}

	b.logger.Printf("Starting Broker %s...", b.ID)

	// 1. Load and validate configuration
	if err := b.loadAndValidateConfig(); err != nil {
		return fmt.Errorf("config validation failed: %v", err)
	}

	// 2. Initialize Raft NodeHost
	if err := b.initRaft(); err != nil {
		return fmt.Errorf("raft init failed: %v", err)
	}

	// 3. Initialize service discovery
	if err := b.initServiceDiscovery(); err != nil {
		return fmt.Errorf("service discovery init failed: %v", err)
	}

	// 4. Register to service discovery
	if err := b.registerBroker(); err != nil {
		return fmt.Errorf("broker registration failed: %v", err)
	}

	// 5. Initialize message processing components
	if err := b.initMessageProcessing(); err != nil {
		return fmt.Errorf("message processing init failed: %v", err)
	}

	// 6. Initialize Controller
	if err := b.initController(); err != nil {
		return fmt.Errorf("controller init failed: %v", err)
	}

	// 7. Initialize Consumer Group Manager
	if err := b.initConsumerGroupManager(); err != nil {
		return fmt.Errorf("consumer group manager init failed: %v", err)
	}

	// 8.5. Initialize Delayed Message Manager
	if err := b.initDelayedMessageManager(); err != nil {
		return fmt.Errorf("delayed message manager init failed: %v", err)
	}

	// 9. Start client server
	if err := b.startClientServer(); err != nil {
		return fmt.Errorf("client server start failed: %v", err)
	}

	// 9.5. Setup transaction callback handlers for all partition state machines
	if err := b.setupTransactionCallbackHandlers(); err != nil {
		return fmt.Errorf("transaction callback handlers setup failed: %v", err)
	}

	// 9. Start system metrics collection
	if err := b.startSystemMetrics(); err != nil {
		return fmt.Errorf("system metrics start failed: %v", err)
	}

	// 10. Register broker to controller Raft state machine
	if err := b.Controller.RegisterBroker(); err != nil {
		b.logger.Printf("Warning: Failed to register broker to controller: %v", err)
	}

	// 10.5. Transaction management is now handled at partition level
	// No need to start a global transaction Raft group

	// 11. Update broker status to active after successful startup
	if err := b.discovery.UpdateBrokerStatus(b.ID, "active"); err != nil {
		b.logger.Printf("Warning: Failed to update broker status to active: %v", err)
	}

	// 12. Start heartbeat to controller
	b.startHeartbeat()

	b.logger.Printf("Broker %s started successfully", b.ID)
	// 13. Block and wait for shutdown signal
	<-b.Ctx.Done()
	b.logger.Printf("Broker %s shutdown signal received", b.ID)
	return nil
}

func (b *Broker) initLogging() error {
	if b.Config.LogDir == "" {
		// Use default logger that outputs to stdout/stderr
		b.logger = log.New(os.Stdout, fmt.Sprintf("[Broker-%s] ", b.ID), log.LstdFlags)
		return nil
	}

	if err := os.MkdirAll(b.Config.LogDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %v", err)
	}

	logFile := filepath.Join(b.Config.LogDir, fmt.Sprintf("broker-%s.log", b.ID))

	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}

	// Create independent logger for this broker instance
	b.logger = log.New(file, fmt.Sprintf("[Broker-%s] ", b.ID), log.LstdFlags)
	b.logger.Printf("Logging initialized, output to: %s", logFile)

	return nil
}

// GetCompressor returns the broker's compressor (implements BrokerInterface)
func (b *Broker) GetCompressor() compression.Compressor {
	return b.compressor
}

// Stop gracefully shuts down the broker
func (b *Broker) Stop() error {
	b.logger.Printf("Stopping Broker %s...", b.ID)

	// Unregister broker from controller before shutdown
	if b.Controller != nil {
		if err := b.unregisterBroker(); err != nil {
			b.logger.Printf("Warning: Failed to unregister broker: %v", err)
		}
	}

	b.Cancel()

	if b.ClientServer != nil {
		b.ClientServer.Stop()
	}

	if b.Controller != nil {
		b.Controller.Stop()
	}

	if b.ConsumerGroupManager != nil {
		b.ConsumerGroupManager.Stop()
	}

	if b.DelayedMessageManager != nil {
		if err := b.DelayedMessageManager.Stop(); err != nil {
			b.logger.Printf("Warning: Failed to stop delayed message manager: %v", err)
		}
	}

	if b.delayedScanner != nil {
		b.delayedScanner.Stop()
	}

	if b.raftManager != nil {
		b.raftManager.Close()
	}

	if zstdCompressor, ok := b.compressor.(*compression.ZstdCompression); ok {
		zstdCompressor.Close()
	}

	b.wg.Wait()

	b.logger.Printf("Broker %s stopped successfully", b.ID)
	return nil
}

// loadAndValidateConfig loads and validates the configuration
func (b *Broker) loadAndValidateConfig() error {
	if b.Config.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}
	if b.Config.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}
	if b.Config.BindAddr == "" {
		b.Config.BindAddr = "0.0.0.0"
	}
	if b.Config.BindPort == 0 {
		b.Config.BindPort = 9092
	}

	return nil
}

// initRaft initializes the Raft NodeHost
func (b *Broker) initRaft() error {
	b.logger.Printf("Initializing Raft NodeHost...")

	// Create Raft manager
	raftManager, err := raft.NewRaftManager(b.Config.RaftConfig, b.Config.DataDir, b.logger)
	if err != nil {
		return fmt.Errorf("failed to create raft manager: %v", err)
	}

	b.raftManager = raftManager
	b.NodeHost = raftManager.GetNodeHost()

	b.logger.Printf("Raft NodeHost initialized successfully")
	return nil
}

// initServiceDiscovery initializes service discovery
func (b *Broker) initServiceDiscovery() error {
	b.logger.Printf("Initializing service discovery...")

	disc, err := discovery.NewDiscovery(b.Config.Discovery)
	if err != nil {
		return fmt.Errorf("failed to create discovery: %v", err)
	}

	b.discovery = disc
	b.logger.Printf("Service discovery initialized successfully")
	return nil
}

// registerBroker registers this broker to service discovery
func (b *Broker) registerBroker() error {
	b.logger.Printf("Registering broker to service discovery...")

	brokerInfo := &discovery.BrokerInfo{
		ID:          b.ID,
		Address:     b.Address,
		Port:        b.Port,
		RaftAddress: b.Config.RaftConfig.RaftAddr,
		Status:      "starting",
	}

	if err := b.discovery.RegisterBroker(brokerInfo); err != nil {
		return fmt.Errorf("failed to register broker: %v", err)
	}

	b.logger.Printf("Broker registered successfully")
	return nil
}

// initController initializes the Controller
func (b *Broker) initController() error {
	b.logger.Printf("Initializing Controller...")

	controller, err := NewControllerManager(b)
	if err != nil {
		return fmt.Errorf("failed to create controller: %v", err)
	}

	b.Controller = controller

	if err := b.Controller.Start(); err != nil {
		return fmt.Errorf("failed to start controller: %v", err)
	}

	if b.Controller.stateMachine != nil {
		if partitionAssigner := b.Controller.stateMachine.GetPartitionAssigner(); partitionAssigner != nil {
			partitionAssigner.SetBroker(b, b.ID)
		}
	}

	b.logger.Printf("Controller initialized successfully")
	return nil
}

// initConsumerGroupManager initializes the consumer group manager
func (b *Broker) initConsumerGroupManager() error {
	b.ConsumerGroupManager = NewConsumerGroupManager(b)
	b.logger.Printf("Consumer Group Manager initialized")
	return nil
}

// initDelayedMessageManager initializes the delayed message manager
func (b *Broker) initDelayedMessageManager() error {
	// Create delayed message storage
	storage, err := delayed.NewPebbleDelayedMessageStorage(filepath.Join(b.Config.DataDir, "delayed"))
	if err != nil {
		return fmt.Errorf("failed to create delayed message storage: %v", err)
	}

	// Create delayed message manager configuration (simplified without callbacks)
	config := delayed.DelayedMessageManagerConfig{
		Storage:         storage,
		MaxRetries:      3,
		CleanupInterval: 1 * time.Hour,
	}

	// Create message delivery callback function
	deliveryCallback := func(topic string, partition int32, key, value []byte) error {
		b.logger.Printf("Delivering delayed message to topic %s, partition %d", topic, partition)

		// 构造ProduceRequest
		produceReq := &ProduceRequest{
			Topic:     topic,
			Partition: partition,
			Messages: []ProduceMessage{
				{
					Key:       key,
					Value:     value,
					Timestamp: time.Now(), 
				},
			},
		}

		if b.ClientServer == nil {
			return fmt.Errorf("client server not initialized")
		}

		response, err := b.ClientServer.handleProduceToRaft(produceReq)
		if err != nil {
			b.logger.Printf("❌ Failed to deliver delayed message: %v", err)
			return err
		}

		if len(response.Results) > 0 && response.Results[0].Error != "" {
			b.logger.Printf("❌ Failed to deliver delayed message: %s", response.Results[0].Error)
			return fmt.Errorf("delivery failed: %s", response.Results[0].Error)
		}

		b.logger.Printf("✅ Successfully delivered delayed message to %s-%d", topic, partition)
		return nil
	}

	// Create delayed message manager
	delayedMessageManager := delayed.NewDelayedMessageManager(config, deliveryCallback)
	b.DelayedMessageManager = delayedMessageManager

	// Start delayed message manager
	if err := delayedMessageManager.Start(); err != nil {
		return fmt.Errorf("failed to start delayed message manager: %v", err)
	}

	delayedScanner := NewDelayedMessageScanner(b, delayedMessageManager, 60*time.Second)
	if err := delayedScanner.Start(); err != nil {
		return fmt.Errorf("failed to start delayed message scanner: %v", err)
	}

	// Store scanner reference for cleanup
	b.delayedScanner = delayedScanner

	b.logger.Printf("Delayed Message Manager and Scanner initialized and started")
	return nil
}

// startClientServer starts the client server
func (b *Broker) startClientServer() error {
	b.logger.Printf("Starting client server...")

	clientServer, err := NewClientServer(b)
	if err != nil {
		return fmt.Errorf("failed to create client server: %v", err)
	}

	b.ClientServer = clientServer

	if err := b.ClientServer.Start(); err != nil {
		return fmt.Errorf("failed to start client server: %v", err)
	}

	b.logger.Printf("Client server started successfully")
	return nil
}

// validateConfig validates the broker configuration
func validateConfig(config *BrokerConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	if config.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}

	if config.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}

	if config.RaftConfig == nil {
		return fmt.Errorf("raft config is required")
	}

	if config.Discovery == nil {
		return fmt.Errorf("discovery config is required")
	}

	return nil
}

// startHeartbeat starts periodic heartbeat to controller to update LastSeen time
func (b *Broker) startHeartbeat() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer b.panicHandler("heartbeat-goroutine")

		ticker := time.NewTicker(30 * time.Second) // Send heartbeat every 30 seconds
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := b.sendHeartbeat(); err != nil {
					b.logger.Printf("Failed to send heartbeat: %v", err)
				}
			case <-b.Ctx.Done():
				return
			}
		}
	}()
}

// sendHeartbeat sends heartbeat to controller to update LastSeen time
func (b *Broker) sendHeartbeat() error {
	if b.Controller == nil {
		return fmt.Errorf("controller not initialized")
	}

	if b.raftManager == nil {
		return fmt.Errorf("raft manager not initialized")
	}

	cmd := &raft.ControllerCommand{
		Type:      protocol.RaftCmdUpdateBrokerLoad,
		ID:        fmt.Sprintf("heartbeat-%s-%d", b.ID, time.Now().UnixNano()),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"broker_id": b.ID,
		},
	}

	return b.Controller.ExecuteCommand(cmd)
}

// unregisterBroker unregisters this broker from controller
func (b *Broker) unregisterBroker() error {
	if b.Controller == nil {
		return fmt.Errorf("controller not initialized")
	}

	cmd := &raft.ControllerCommand{
		Type:      protocol.RaftCmdUnregisterBroker,
		ID:        fmt.Sprintf("unregister-%s-%d", b.ID, time.Now().UnixNano()),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"broker_id": b.ID,
		},
	}

	return b.Controller.ExecuteCommand(cmd)
}

func (b *Broker) startSystemMetrics() error {
	b.logger.Printf("Starting system metrics collection for broker %s", b.ID)

	b.systemMetrics = NewSystemMetrics(b)
	return b.systemMetrics.Start()
}

// initMessageProcessing initializes compression and deduplication components
func (b *Broker) initMessageProcessing() error {
	b.logger.Printf("Initializing message processing components for broker %s", b.ID)

	if b.Config.CompressionEnabled {
		var compressionType compression.CompressionType
		switch b.Config.CompressionType {
		case "gzip":
			compressionType = compression.Gzip
		case "zlib":
			compressionType = compression.Zlib
		case "snappy":
			compressionType = compression.Snappy
		case "zstd":
			compressionType = compression.Zstd
		default:
			compressionType = compression.Snappy
		}

		compressor, err := compression.GetCompressor(compressionType)
		if err != nil {
			return fmt.Errorf("failed to initialize compressor: %v", err)
		}
		b.compressor = compressor
		b.logger.Printf("Compression enabled with type: %s", compressionType.String())
	} else {
		b.compressor, _ = compression.GetCompressor(compression.None)
		b.logger.Printf("Compression disabled")
	}

	// Initialize ordered message router
	b.orderedRouter = NewOrderedMessageRouter()
	b.logger.Printf("Ordered message router initialized")

	return nil
}

// GetDelayedMessageManager gets the delayed message manager
func (b *Broker) GetDelayedMessageManager() *delayed.DelayedMessageManager {
	return b.DelayedMessageManager
}

// GetLogger returns the broker's independent logger instance
func (b *Broker) GetLogger() *log.Logger {
	return b.logger
}

// setupTransactionCallbackHandlers sets up transaction callback handlers for all partition state machines
func (b *Broker) setupTransactionCallbackHandlers() error {
	b.logger.Printf("Setting up transaction callback handlers...")

	// Get all Raft groups
	allGroups := b.raftManager.GetGroups()

	partitionCount := 0
	for groupID := range allGroups {
		// Skip Controller group (groupID = 1)
		if groupID == 1 {
			continue
		}

		// Get state machine and convert to PartitionStateMachine
		sm, err := b.raftManager.GetStateMachine(groupID)
		if err != nil {
			b.logger.Printf("Failed to get state machine for group %d: %v", groupID, err)
			continue
		}

		// Type assert to PartitionStateMachine
		if psm, ok := sm.(*raft.PartitionStateMachine); ok {
			// Set callback handler for each partition state machine
			psm.SetTransactionCheckResultHandler(b.handleTransactionCheckResult)
			partitionCount++
		} else {
			b.logger.Printf("State machine for group %d is not a PartitionStateMachine", groupID)
		}
	}

	b.logger.Printf("Transaction callback handlers setup completed for %d partitions", partitionCount)
	return nil
}

// handleTransactionCheckResult handles transaction check results from partition state machines
func (b *Broker) handleTransactionCheckResult(transactionID string, state transaction.TransactionState, record *raft.HalfMessageRecord) {
	b.logger.Printf("Handling transaction check result: ID=%s, state=%s", transactionID, state)

	switch state {
	case transaction.StateCommit:
		// 提交事务
		if record != nil {
			err := b.commitHalfMessageToRaft(record.Topic, record.Partition, transactionID)
			if err != nil {
				b.logger.Printf("Failed to commit transaction %s: %v", transactionID, err)
			} else {
				b.logger.Printf("Successfully committed transaction %s", transactionID)
			}
		}
	case transaction.StateRollback:
		// 回滚事务
		if record != nil {
			err := b.rollbackHalfMessageToRaft(record.Topic, record.Partition, transactionID)
			if err != nil {
				b.logger.Printf("Failed to rollback transaction %s: %v", transactionID, err)
			} else {
				b.logger.Printf("Successfully rolled back transaction %s", transactionID)
			}
		}
	default:
		b.logger.Printf("Unknown transaction state: %s for transaction %s", state, transactionID)
	}
}

// commitHalfMessageToRaft sends a commit command via Raft
func (b *Broker) commitHalfMessageToRaft(topic string, partition int32, transactionID string) error {
	// 通过Controller查询分区对应的Raft组ID
	raftGroupID, err := b.getRaftGroupIDForPartition(topic, partition)
	if err != nil {
		return fmt.Errorf("failed to get raft group ID for partition %s-%d: %w", topic, partition, err)
	}

	cmd := &raft.PartitionCommand{
		Type: raft.CmdCommitHalfMessage,
		Data: map[string]interface{}{
			"transaction_id": transactionID,
			"topic":          topic,
			"partition":      partition,
			"timestamp":      time.Now(),
		},
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal commit command: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = b.raftManager.SyncPropose(ctx, raftGroupID, cmdBytes)
	return err
}

// rollbackHalfMessageToRaft sends a rollback command via Raft
func (b *Broker) rollbackHalfMessageToRaft(topic string, partition int32, transactionID string) error {
	// 通过Controller查询分区对应的Raft组ID
	raftGroupID, err := b.getRaftGroupIDForPartition(topic, partition)
	if err != nil {
		return fmt.Errorf("failed to get raft group ID for partition %s-%d: %w", topic, partition, err)
	}

	cmd := &raft.PartitionCommand{
		Type: raft.CmdRollbackHalfMessage,
		Data: map[string]interface{}{
			"transaction_id": transactionID,
			"topic":          topic,
			"partition":      partition,
			"timestamp":      time.Now(),
		},
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal rollback command: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = b.raftManager.SyncPropose(ctx, raftGroupID, cmdBytes)
	return err
}

// getRaftGroupIDForPartition 通过Controller查询分区对应的Raft组ID
func (b *Broker) getRaftGroupIDForPartition(topic string, partition int32) (uint64, error) {
	// 通过Controller的状态机直接查询分区分配信息
	assignments, err := b.Controller.getTopicAssignments(topic)
	if err != nil {
		return 0, fmt.Errorf("failed to get topic assignments: %w", err)
	}

	// 查找对应分区的Raft组ID
	for _, assignment := range assignments {
		if assignment.PartitionID == partition {
			return assignment.RaftGroupID, nil
		}
	}

	return 0, fmt.Errorf("partition %d not found in topic %s", partition, topic)
}
