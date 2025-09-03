package broker

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/issac1998/go-queue/internal/discovery"
	"github.com/issac1998/go-queue/internal/raft"
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

	// Partition management
	PartitionManager *PartitionManager

	// Client service
	ClientServer *ClientServer

	// Configuration
	Config *BrokerConfig

	// Lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Raft manager
	raftManager *raft.RaftManager

	// Service discovery
	discovery discovery.Discovery
}

// BrokerConfig contains all broker configuration
type BrokerConfig struct {
	// Node configuration
	NodeID   string `yaml:"node_id"`
	BindAddr string `yaml:"bind_addr"`
	BindPort int    `yaml:"bind_port"`

	// Data directory
	DataDir string `yaml:"data_dir"`

	// Raft configuration
	RaftConfig *raft.RaftConfig `yaml:"raft"`

	// Cluster discovery configuration
	Discovery *discovery.DiscoveryConfig `yaml:"discovery"`

	// Performance tuning
	Performance *PerformanceConfig `yaml:"performance"`

	// FollowerRead configuration
	EnableFollowerRead bool `yaml:"enable_follower_read"` // Enable follower read for this broker
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
		ctx:     ctx,
		cancel:  cancel,
	}

	return broker, nil
}

// Start initializes and starts the broker
func (b *Broker) Start() error {
	log.Printf("Starting Broker %s...", b.ID)

	// 1. Load and validate configuration
	if err := b.loadAndValidateConfig(); err != nil {
		return fmt.Errorf("config validation failed: %v", err)
	}

	// 2. Create data directories
	if err := b.createDataDirectories(); err != nil {
		return fmt.Errorf("data directory creation failed: %v", err)
	}

	// 3. Initialize Raft NodeHost
	if err := b.initRaft(); err != nil {
		return fmt.Errorf("raft init failed: %v", err)
	}

	// 4. Initialize service discovery
	if err := b.initServiceDiscovery(); err != nil {
		return fmt.Errorf("service discovery init failed: %v", err)
	}

	// 5. Register to service discovery
	if err := b.registerBroker(); err != nil {
		return fmt.Errorf("broker registration failed: %v", err)
	}

	// 6. Initialize Controller
	if err := b.initController(); err != nil {
		return fmt.Errorf("controller init failed: %v", err)
	}

	// 7. Start client server
	if err := b.startClientServer(); err != nil {
		return fmt.Errorf("client server start failed: %v", err)
	}

	log.Printf("Broker %s started successfully", b.ID)
	return nil
}

// Stop gracefully shuts down the broker
func (b *Broker) Stop() error {
	log.Printf("Stopping Broker %s...", b.ID)

	b.cancel()

	if b.ClientServer != nil {
		b.ClientServer.Stop()
	}

	if b.Controller != nil {
		b.Controller.Stop()
	}

	if b.PartitionManager != nil {
		b.PartitionManager.Stop()
	}

	if b.raftManager != nil {
		b.raftManager.Close()
	}

	// Wait for all goroutines to finish
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

// initLogging initializes the logging system
func (b *Broker) initLogging() error {
	// TODO: Initialize structured logging
	log.Printf("Initializing logging for broker %s", b.ID)
	return nil
}

// createDataDirectories creates necessary data directories
func (b *Broker) createDataDirectories() error {
	// TODO: Create data directories
	log.Printf("Creating data directories at %s", b.Config.DataDir)
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
		RaftAddress: b.Config.RaftConfig.RaftAddr, // Use actual Raft address
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

	// Start Controller Raft Group
	if err := b.Controller.Start(); err != nil {
		return fmt.Errorf("failed to start controller: %v", err)
	}

	log.Printf("Controller initialized successfully")
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
