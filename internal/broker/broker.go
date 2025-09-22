package broker

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
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

	// Transaction management
	TransactionManager *transaction.TransactionManager
	TransactionChecker *TransactionChecker

	// Delayed message management
	DelayedMessageManager *delayed.DelayedMessageManager

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

	mu sync.RWMutex
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
	// 0. Initialize logging
	if err := b.initLogging(); err != nil {
		return fmt.Errorf("logging init failed: %v", err)
	}

	log.Printf("Starting Broker %s...", b.ID)

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

	// 8. Initialize Transaction Manager
	if err := b.initTransactionManager(); err != nil {
		return fmt.Errorf("transaction manager init failed: %v", err)
	}

	// 8.5. Initialize Delayed Message Manager
	if err := b.initDelayedMessageManager(); err != nil {
		return fmt.Errorf("delayed message manager init failed: %v", err)
	}

	// 9. Start client server
	if err := b.startClientServer(); err != nil {
		return fmt.Errorf("client server start failed: %v", err)
	}

	// 9. Start system metrics collection
	if err := b.startSystemMetrics(); err != nil {
		return fmt.Errorf("system metrics start failed: %v", err)
	}

	// 10. Register broker to controller Raft state machine
	if err := b.Controller.RegisterBroker(); err != nil {
		log.Printf("Warning: Failed to register broker to controller: %v", err)
	}

	// 11. Update broker status to active after successful startup
	if err := b.discovery.UpdateBrokerStatus(b.ID, "active"); err != nil {
		log.Printf("Warning: Failed to update broker status to active: %v", err)
	}

	// 12. Start heartbeat to controller
	b.startHeartbeat()

	log.Printf("Broker %s started successfully", b.ID)
	return nil
}

func (b *Broker) initLogging() error {
	if b.Config.LogDir == "" {
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

	log.SetOutput(file)
	log.Printf("Logging initialized, output to: %s", logFile)

	return nil
}

// GetCompressor returns the broker's compressor (implements BrokerInterface)
func (b *Broker) GetCompressor() compression.Compressor {
	return b.compressor
}

// Stop gracefully shuts down the broker
func (b *Broker) Stop() error {
	log.Printf("Stopping Broker %s...", b.ID)

	// Unregister broker from controller before shutdown
	if b.Controller != nil {
		if err := b.unregisterBroker(); err != nil {
			log.Printf("Warning: Failed to unregister broker: %v", err)
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
			log.Printf("Warning: Failed to stop delayed message manager: %v", err)
		}
	}

	if b.raftManager != nil {
		b.raftManager.Close()
	}

	if zstdCompressor, ok := b.compressor.(*compression.ZstdCompression); ok {
		zstdCompressor.Close()
	}

	b.wg.Wait()

	log.Printf("Broker %s stopped successfully", b.ID)
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
	log.Printf("Initializing Raft NodeHost...")

	// Create Raft manager
	raftManager, err := raft.NewRaftManager(b.Config.RaftConfig, b.Config.DataDir)
	if err != nil {
		return fmt.Errorf("failed to create raft manager: %v", err)
	}

	b.raftManager = raftManager
	b.NodeHost = raftManager.GetNodeHost()

	log.Printf("Raft NodeHost initialized successfully")
	return nil
}

// initServiceDiscovery initializes service discovery
func (b *Broker) initServiceDiscovery() error {
	log.Printf("Initializing service discovery...")

	disc, err := discovery.NewDiscovery(b.Config.Discovery)
	if err != nil {
		return fmt.Errorf("failed to create discovery: %v", err)
	}

	b.discovery = disc
	log.Printf("Service discovery initialized successfully")
	return nil
}

// registerBroker registers this broker to service discovery
func (b *Broker) registerBroker() error {
	log.Printf("Registering broker to service discovery...")

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

	log.Printf("Broker registered successfully")
	return nil
}

// initController initializes the Controller
func (b *Broker) initController() error {
	log.Printf("Initializing Controller...")

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

	log.Printf("Controller initialized successfully")
	return nil
}

// initConsumerGroupManager initializes the consumer group manager
func (b *Broker) initConsumerGroupManager() error {
	b.ConsumerGroupManager = NewConsumerGroupManager(b)
	log.Printf("Consumer Group Manager initialized")
	return nil
}

// initTransactionManager initializes the transaction manager
func (b *Broker) initTransactionManager() error {
	b.TransactionChecker = NewTransactionChecker(b)
	b.TransactionManager = transaction.NewTransactionManager()

	if b.raftManager != nil {
		groupID := raft.TransactionManagerGroupID

		raftProposer := NewBrokerRaftProposer(b.raftManager)

		b.TransactionManager.EnableRaft(raftProposer, groupID)

		if err := b.startTransactionRaftGroup(groupID); err != nil {
			log.Printf("Warning: Failed to start transaction Raft group: %v", err)
		}
	}

	log.Printf("Transaction Manager and Checker initialized")
	return nil
}

// initDelayedMessageManager initializes the delayed message manager
func (b *Broker) initDelayedMessageManager() error {
	// 创建延迟消息管理器配置
	config := delayed.DelayedMessageManagerConfig{
		DataDir:         filepath.Join(b.Config.DataDir, "delayed"),
		MaxRetries:      3,
		CleanupInterval: 1 * time.Hour,
	}

	// 创建延迟消息管理器 (暂时不传producer，后续可以优化)
	delayedMessageManager := delayed.NewDelayedMessageManager(config, nil)
	b.DelayedMessageManager = delayedMessageManager

	// 启动延迟消息管理器
	if err := delayedMessageManager.Start(); err != nil {
		return fmt.Errorf("failed to start delayed message manager: %v", err)
	}

	log.Printf("Delayed Message Manager initialized and started")
	return nil
}

// startClientServer starts the client server
func (b *Broker) startClientServer() error {
	log.Printf("Starting client server...")

	clientServer, err := NewClientServer(b)
	if err != nil {
		return fmt.Errorf("failed to create client server: %v", err)
	}

	b.ClientServer = clientServer

	if err := b.ClientServer.Start(); err != nil {
		return fmt.Errorf("failed to start client server: %v", err)
	}

	log.Printf("Client server started successfully")
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

		// Wait for controller to be ready before starting heartbeat
		for i := 0; i < 10; i++ {
			time.Sleep(1 * time.Second)
			if b.Controller != nil {
				break
			}
		}

		if b.Controller == nil {
			log.Printf("Controller not initialized after 10 seconds, heartbeat disabled")
			return
		}

		ticker := time.NewTicker(30 * time.Second) // Send heartbeat every 30 seconds
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := b.sendHeartbeat(); err != nil {
					log.Printf("Failed to send heartbeat: %v", err)
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
	log.Printf("Starting system metrics collection for broker %s", b.ID)

	b.systemMetrics = NewSystemMetrics(b)
	return b.systemMetrics.Start()
}

// initMessageProcessing initializes compression and deduplication components
func (b *Broker) initMessageProcessing() error {
	log.Printf("Initializing message processing components for broker %s", b.ID)

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
		log.Printf("Compression enabled with type: %s", compressionType.String())
	} else {
		b.compressor, _ = compression.GetCompressor(compression.None)
		log.Printf("Compression disabled")
	}

	// Initialize ordered message router
	b.orderedRouter = NewOrderedMessageRouter()
	log.Printf("Ordered message router initialized")

	return nil
}

// GetDelayedMessageManager 获取延迟消息管理器
func (b *Broker) GetDelayedMessageManager() *delayed.DelayedMessageManager {
	return b.DelayedMessageManager
}
