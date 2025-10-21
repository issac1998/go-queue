package testutil

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/broker"
	"github.com/issac1998/go-queue/internal/discovery"
	"github.com/issac1998/go-queue/internal/raft"
)

// TestCluster represents a test cluster with etcd and brokers
type TestCluster struct {
	EtcdProcess     *exec.Cmd
	BrokerProcesses []*exec.Cmd
	Brokers         []*broker.Broker // 直接存储 broker 实例
	BrokerConfigs   []BrokerConfig
	EtcdDataDir     string
	TestDataDir     string
	Client          *client.Client
	Admin           *client.Admin
	ctx             context.Context
	cancel          context.CancelFunc
}

// BrokerConfig represents broker configuration for testing
type BrokerConfig struct {
	NodeID   string
	BindPort int
	RaftPort int
	DataDir  string
}

// NewTestCluster creates a new test cluster
func NewTestCluster() *TestCluster {
	testDataDir := fmt.Sprintf("/tmp/go-queue-test-%d", time.Now().UnixNano())
	etcdDataDir := filepath.Join(testDataDir, "etcd")

	ctx, cancel := context.WithCancel(context.Background())

	return &TestCluster{
		TestDataDir: testDataDir,
		EtcdDataDir: etcdDataDir,
		ctx:         ctx,
		cancel:      cancel,
		BrokerConfigs: []BrokerConfig{
			{NodeID: "test-broker1", BindPort: 19092, RaftPort: 17001, DataDir: filepath.Join(testDataDir, "broker1")},
			{NodeID: "test-broker2", BindPort: 19093, RaftPort: 17002, DataDir: filepath.Join(testDataDir, "broker2")},
			{NodeID: "test-broker3", BindPort: 19094, RaftPort: 17003, DataDir: filepath.Join(testDataDir, "broker3")},
		},
	}
}

// Start starts the test cluster (etcd + brokers)
func (tc *TestCluster) Start() error {
	log.Println("Starting test cluster...")

	// Create test data directories
	if err := os.MkdirAll(tc.TestDataDir, 0755); err != nil {
		return fmt.Errorf("failed to create test data directory: %v", err)
	}

	// Start etcd
	if err := tc.startEtcd(); err != nil {
		return fmt.Errorf("failed to start etcd: %v", err)
	}
	defer tc.stopEtcd()

	// Wait for etcd to be ready
	time.Sleep(3 * time.Second)

	// Start broker cluster
	if err := tc.startBrokerCluster(); err != nil {
		return fmt.Errorf("failed to start broker cluster: %v", err)
	}
	defer tc.Stop()

	// Wait for cluster to be ready
	if err := tc.waitForClusterReady(); err != nil {
		return fmt.Errorf("cluster not ready: %v", err)
	}

	// Create client
	if err := tc.createClient(); err != nil {
		tc.Stop()
		return fmt.Errorf("failed to create client: %v", err)
	}

	log.Println("Test cluster started successfully!")
	// Wait for all brokers to start
	log.Println("Waiting for all brokers to initialize...")
	time.Sleep(15 * time.Second) // Give brokers time to fully initialize

	return nil
}

// Stop stops the test cluster and cleans up resources
func (tc *TestCluster) Stop() {
	log.Println("Stopping test cluster...")

	// Close client
	if tc.Client != nil {
		tc.Client.Close()
		tc.Client = nil
	}

	// Stop brokers
	tc.stopBrokerCluster()

	// Clean up test data directory
	if tc.TestDataDir != "" {
		os.RemoveAll(tc.TestDataDir)
	}

	log.Println("Test cluster stopped and cleaned up")
}

// startEtcd starts etcd for testing
func (tc *TestCluster) startEtcd() error {
	log.Println("Starting etcd...")

	// Clean up old data
	os.RemoveAll(tc.EtcdDataDir)

	// Check if etcd binary exists
	etcdPath := "/opt/homebrew/bin/etcd"
	if _, err := os.Stat(etcdPath); os.IsNotExist(err) {
		// Try alternative paths
		alternatives := []string{
			"/usr/local/bin/etcd",
			"/Users/a/Downloads/etcd-v3.6.4-darwin-arm64/etcd",
			"etcd", // Try system PATH
		}

		found := false
		for _, path := range alternatives {
			if _, err := exec.LookPath(path); err == nil {
				etcdPath = path
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("etcd binary not found. Please install etcd or update the path")
		}
	}

	// Start etcd
	cmd := exec.Command(etcdPath,
		"--name", "test-etcd",
		"--data-dir", tc.EtcdDataDir,
		"--listen-client-urls", "http://127.0.0.1:12379",
		"--advertise-client-urls", "http://127.0.0.1:12379",
		"--listen-peer-urls", "http://127.0.0.1:12380",
		"--initial-advertise-peer-urls", "http://127.0.0.1:12380",
		"--initial-cluster", "test-etcd=http://127.0.0.1:12380",
		"--initial-cluster-token", "test-token",
		"--initial-cluster-state", "new",
	)

	// Redirect output to avoid cluttering test output
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start etcd: %v", err)
	}

	tc.EtcdProcess = cmd
	return nil
}

// stopEtcd stops etcd
func (tc *TestCluster) stopEtcd() {
	if tc.EtcdProcess != nil {
		log.Println("Stopping etcd...")
		tc.EtcdProcess.Process.Kill()
		tc.EtcdProcess.Wait()
		tc.EtcdProcess = nil
	}
}

// startBrokerCluster starts the broker cluster
func (tc *TestCluster) startBrokerCluster() error {
	log.Println("Starting broker cluster...")

	for i, config := range tc.BrokerConfigs {
		// Clean up old data
		os.RemoveAll(config.DataDir)
		os.MkdirAll(config.DataDir, 0755)

		// Create logs directory
		logsDir := filepath.Join(config.DataDir, "logs")
		os.MkdirAll(logsDir, 0755)

		// Generate a consistent NodeID from broker ID
		hashedNodeID := tc.hashBrokerID(config.NodeID)

		// Create broker configuration
		brokerConfig := &broker.BrokerConfig{
			NodeID:   config.NodeID,
			BindAddr: "127.0.0.1",
			BindPort: config.BindPort,
			DataDir:  config.DataDir,
			LogDir:   logsDir,
			RaftConfig: &raft.RaftConfig{
				NodeID:             hashedNodeID,
				RaftAddr:           fmt.Sprintf("127.0.0.1:%d", config.RaftPort),
				RTTMillisecond:     200,
				HeartbeatRTT:       5,
				ElectionRTT:        50,
				CheckQuorum:        true,
				SnapshotEntries:    10000,
				CompactionOverhead: 5000,
			},
			Discovery: &discovery.DiscoveryConfig{
				Type:      "etcd",
				Endpoints: []string{"127.0.0.1:12379"},
			},
			Performance: &broker.PerformanceConfig{
				MaxBatchSize:       1000,
				MaxBatchBytes:      1024 * 1024, // 1MB
				ConnectionPoolSize: 100,
			},
			EnableFollowerRead: true,
		}

		// Create broker instance
		b := &broker.Broker{
			ID:      config.NodeID,
			Address: "127.0.0.1",
			Port:    config.BindPort,
			Config:  brokerConfig,
			Ctx:     tc.ctx,
			Cancel:  tc.cancel,
		}

		// Start broker in a goroutine (since Start() blocks)
		go func(broker *broker.Broker, nodeID string) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Broker %s panicked: %v", nodeID, r)
				}
			}()

			log.Printf("Starting broker %s on port %d", nodeID, broker.Port)
			if err := broker.Start(); err != nil {
				log.Printf("Broker %s failed to start: %v", nodeID, err)
			}
		}(b, config.NodeID)

		tc.Brokers = append(tc.Brokers, b)
		log.Printf("Started broker %s on port %d", config.NodeID, config.BindPort)

		// Wait between broker starts to avoid conflicts
		if i < len(tc.BrokerConfigs)-1 {
			time.Sleep(30 * time.Second)
		}
	}

	// Wait a bit for all brokers to initialize
	time.Sleep(10 * time.Second)
	return nil
}

// hashBrokerID converts a broker ID string to a uint64 node ID
func (tc *TestCluster) hashBrokerID(brokerID string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(brokerID))
	return h.Sum64()
}

// stopBrokerCluster stops all brokers
func (tc *TestCluster) stopBrokerCluster() {
	log.Println("Stopping broker cluster...")

	// Cancel context to signal all brokers to stop
	if tc.cancel != nil {
		tc.cancel()
	}

	// Wait a bit for graceful shutdown
	time.Sleep(5 * time.Second)

	// Clear broker references
	tc.Brokers = nil

	// Also stop any remaining processes (for backward compatibility)
	for _, cmd := range tc.BrokerProcesses {
		if cmd != nil && cmd.Process != nil {
			log.Printf("Stopping broker process PID %d", cmd.Process.Pid)
			cmd.Process.Kill()
			cmd.Wait()
		}
	}
	tc.BrokerProcesses = nil
}

// waitForClusterReady waits for the cluster to be ready
func (tc *TestCluster) waitForClusterReady() error {
	log.Println("Waiting for cluster to be ready...")

	// Wait for brokers to start up and form cluster
	maxRetries := 20
	retryInterval := 15 * time.Second

	for i := 0; i < maxRetries; i++ {
		allBrokersReady := true

		// Check each broker individually
		for j, config := range tc.BrokerConfigs {
			brokerAddr := fmt.Sprintf("127.0.0.1:%d", config.BindPort)
			log.Printf("Checking broker %d at %s...", j+1, brokerAddr)

			if !tc.isBrokerReady(brokerAddr) {
				log.Printf("Broker %d at %s is not ready yet", j+1, brokerAddr)
				allBrokersReady = false
				break
			}
		}

		if allBrokersReady {
			log.Println("All brokers are ready!")

			// Final verification: try to perform a cluster-wide operation
			if tc.verifyClusterOperation() {
				log.Println("Cluster is fully ready!")
				return nil
			}
			log.Println("Cluster operation verification failed, continuing to wait...")
		}

		log.Printf("Cluster not ready yet, retrying in %v... (attempt %d/%d)", retryInterval, i+1, maxRetries)
		time.Sleep(retryInterval)
	}

	return fmt.Errorf("cluster failed to become ready after %d attempts", maxRetries)
}

// isBrokerReady checks if a single broker is ready to accept connections
func (tc *TestCluster) isBrokerReady(brokerAddr string) bool {
	testClient := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{brokerAddr},
		Timeout:     5 * time.Second,
	})
	defer testClient.Close()

	if testClient == nil {
		return false
	}

	// Try to discover controller - this ensures the broker can handle basic requests
	if err := testClient.DiscoverController(); err != nil {
		log.Printf("Controller discovery failed for %s: %v", brokerAddr, err)
		return false
	}

	// Try to list topics - this ensures the broker's metadata system is working
	admin := client.NewAdmin(testClient)
	_, err := admin.ListTopics()
	if err != nil {
		log.Printf("ListTopics failed for %s: %v", brokerAddr, err)
		return false
	}

	log.Printf("Broker at %s is ready", brokerAddr)
	return true
}

// verifyClusterOperation performs a final verification that the cluster can handle operations
func (tc *TestCluster) verifyClusterOperation() bool {
	// Create a test client that can connect to any broker
	brokerAddrs := make([]string, len(tc.BrokerConfigs))
	for i, config := range tc.BrokerConfigs {
		brokerAddrs[i] = fmt.Sprintf("127.0.0.1:%d", config.BindPort)
	}

	testClient := client.NewClient(client.ClientConfig{
		BrokerAddrs: brokerAddrs,
		Timeout:     10 * time.Second,
	})
	defer testClient.Close()

	if testClient == nil {
		return false
	}

	admin := client.NewAdmin(testClient)

	// Try to create a test topic to verify the cluster can handle metadata operations
	testTopicName := "cluster-readiness-test"
	createReq := client.CreateTopicRequest{
		Name:       testTopicName,
		Partitions: 2,
		Replicas:   1,
	}

	result, err := admin.CreateTopic(createReq)
	if err != nil {
		log.Printf("Test topic creation failed: %v", err)
		return false
	}

	if result.Error != nil {
		log.Printf("Test topic creation returned error: %v", result.Error)
		return false
	}

	// Clean up the test topic
	if err := admin.DeleteTopic(testTopicName); err != nil {
		log.Printf("Warning: Failed to clean up test topic: %v", err)
		// Don't fail the verification for cleanup issues
	}

	log.Println("Cluster operation verification successful")
	return true
}

// createClient creates a client for testing
func (tc *TestCluster) createClient() error {
	brokerAddresses := make([]string, len(tc.BrokerConfigs))
	for i, config := range tc.BrokerConfigs {
		brokerAddresses[i] = fmt.Sprintf("127.0.0.1:%d", config.BindPort)
	}

	clientInstance := client.NewClient(client.ClientConfig{
		BrokerAddrs: brokerAddresses,
		Timeout:     10 * time.Second,
	})
	if clientInstance == nil {
		return fmt.Errorf("failed to create client")
	}

	tc.Client = clientInstance
	tc.Admin = client.NewAdmin(clientInstance)
	return nil
}

// CreateTopic creates a topic for testing
func (tc *TestCluster) CreateTopic(topicName string, partitions int32, replicas int32) error {
	if tc.Admin == nil {
		return fmt.Errorf("admin not initialized")
	}

	_, err := tc.Admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: partitions,
		Replicas:   replicas,
	})
	return err
}

// DeleteTopic deletes a topic
func (tc *TestCluster) DeleteTopic(topicName string) error {
	if tc.Admin == nil {
		return fmt.Errorf("admin not initialized")
	}

	return tc.Admin.DeleteTopic(topicName)
}

// GetBrokerAddress returns the address of the first broker
func (tc *TestCluster) GetBrokerAddress() string {
	if len(tc.BrokerConfigs) == 0 {
		return ""
	}
	return fmt.Sprintf("127.0.0.1:%d", tc.BrokerConfigs[0].BindPort)
}

// GetAllBrokerAddresses returns all broker addresses
func (tc *TestCluster) GetAllBrokerAddresses() []string {
	addresses := make([]string, len(tc.BrokerConfigs))
	for i, config := range tc.BrokerConfigs {
		addresses[i] = fmt.Sprintf("127.0.0.1:%d", config.BindPort)
	}
	return addresses
}
