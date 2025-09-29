package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	client "github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/broker"
	"github.com/issac1998/go-queue/internal/discovery"
	"github.com/issac1998/go-queue/internal/protocol"
	"github.com/issac1998/go-queue/internal/raft"
	"github.com/issac1998/go-queue/internal/transaction"
)

// TestEnvironment manages the test cluster setup
type TestEnvironment struct {
	brokers         []*broker.Broker
	etcdProcess     *exec.Cmd
	brokerProcesses []*exec.Cmd
	clients         []*client.Client
	topics          []string
}

// TestEnvironmentConfig configuration for test environment
type TestEnvironmentConfig struct {
	BrokerCount int
	Topics      []string // optional topics to create
	StartEtcd   bool     // whether to start etcd
}

// NewTestEnvironment creates a new test environment
func NewTestEnvironment(config TestEnvironmentConfig) *TestEnvironment {
	if config.BrokerCount == 0 {
		config.BrokerCount = 3
	}
	return &TestEnvironment{
		brokers: make([]*broker.Broker, 0, config.BrokerCount),
		clients: make([]*client.Client, 0, config.BrokerCount),
		topics:  config.Topics,
	}
}

// Setup sets up the test environment
func (te *TestEnvironment) Setup(config TestEnvironmentConfig) error {
	fmt.Println("Setting up test environment...")

	// Step 1: Start etcd if needed
	if config.StartEtcd {
		if err := te.startEtcd(); err != nil {
			return fmt.Errorf("failed to start etcd: %v", err)
		}
	}

	// Step 2: Start broker cluster
	if err := te.startBrokerCluster(config.BrokerCount); err != nil {
		return fmt.Errorf("failed to start broker cluster: %v", err)
	}

	// Step 3: Wait for cluster to be ready
	if err := te.waitForClusterReady(); err != nil {
		return fmt.Errorf("cluster not ready: %v", err)
	}

	// Step 4: Create clients

	if err := te.createClients(); err != nil {
		return fmt.Errorf("failed to create clients: %v", err)
	}

	// Step 5: Create topics if specified
	if len(config.Topics) > 0 {
		if err := te.createTopics(config.Topics); err != nil {
			return fmt.Errorf("failed to create topics: %v", err)
		}
	}
	fmt.Println("Test environment setup completed successfully!")
	return nil
}

// Cleanup cleans up the test environment
func (te *TestEnvironment) Cleanup() {
	log.Println("Cleaning up test environment...")

	// Stop brokers
	for _, b := range te.brokers {
		if b != nil {
			if err := b.Stop(); err != nil {
				log.Printf("Error stopping broker %s: %v", b.ID, err)
			}
		}
	}

	te.stopBrokerCluster()
	te.stopEtcd()

	// Close clients
	for _, c := range te.clients {
		if c != nil {
			c.Close()
		}
	}

	log.Println("Cleanup completed")
}

// startEtcd starts etcd for the test environment
func (te *TestEnvironment) startEtcd() error {
	log.Println("Starting etcd...")
	te.stopEtcd()

	time.Sleep(5)
	// Check if etcd is already running
	if err := exec.Command("pgrep", "etcd").Run(); err == nil {
		return fmt.Errorf("etcd is already running")
	}

	// Start etcd
	cmd := exec.Command("/Users/a/Downloads/etcd-v3.6.4-darwin-arm64/etcd",
		"--name", "test-etcd",
		"--data-dir", "/tmp/etcd-test",
		"--listen-client-urls", "http://127.0.0.1:2379",
		"--advertise-client-urls", "http://127.0.0.1:2379",
		"--listen-peer-urls", "http://127.0.0.1:2380",
		"--initial-advertise-peer-urls", "http://127.0.0.1:2380",
		"--initial-cluster", "test-etcd=http://127.0.0.1:2380",
		"--initial-cluster-token", "test-token",
		"--initial-cluster-state", "new",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start etcd: %v", err)
	}

	te.etcdProcess = cmd
	log.Println("etcd started successfully")
	time.Sleep(3 * time.Second)
	return nil
}

// startBrokerCluster starts broker cluster using function calls
func (te *TestEnvironment) startBrokerCluster(brokerCount int) error {
	fmt.Println("Starting broker cluster...")

	for i := 0; i < brokerCount; i++ {
		nodeID := fmt.Sprintf("broker%d", i+1)
		bindPort := 9092 + i
		raftPort := 9192 + i
		dataDir := fmt.Sprintf("/tmp/%s-data", nodeID)

		// Clean up old data
		os.RemoveAll(dataDir)
		os.MkdirAll(dataDir, 0755)
		os.MkdirAll(fmt.Sprintf("%s/logs", dataDir), 0755)

		// Convert broker ID to node ID using the same logic as the system
		h := fnv.New64a()
		h.Write([]byte(nodeID))
		numericNodeID := h.Sum64()

		// Create broker config
		config := &broker.BrokerConfig{
			NodeID:   nodeID,
			BindAddr: "127.0.0.1",
			BindPort: bindPort,
			DataDir:  dataDir,
			LogDir:   fmt.Sprintf("%s/logs", dataDir),
			RaftConfig: &raft.RaftConfig{
				NodeID:             numericNodeID,
				RaftAddr:           fmt.Sprintf("127.0.0.1:%d", raftPort),
				RTTMillisecond:     200,
				HeartbeatRTT:       5,
				ElectionRTT:        50,
				SnapshotEntries:    10000,
				CompactionOverhead: 5000,
			},
			Discovery: &discovery.DiscoveryConfig{
				Type:      "etcd",
				Endpoints: []string{"127.0.0.1:2379"},
			},
		}

		// Create and start broker
		b, err := broker.NewBroker(config)
		if err != nil {
			return fmt.Errorf("failed to create broker %s: %v", nodeID, err)
		}

		go func(broker *broker.Broker) {
			if err := broker.Start(); err != nil {
				log.Printf("Broker %s failed to start: %v", broker.ID, err)
			}
		}(b)

		te.brokers = append(te.brokers, b)
		log.Printf("Started broker %s on port %d", nodeID, bindPort)

		time.Sleep(10 * time.Second)
	}

	return nil
}

func (te *TestEnvironment) waitForClusterReady() error {
	log.Println("Waiting for cluster to be ready...")

	// Try to connect to each broker
	brokerAddrs := []string{"127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"}

	for i := 0; i < 30; i++ { // Wait up to 150 seconds
		allReady := true
		for _, addr := range brokerAddrs {
			clientConfig := client.ClientConfig{
				BrokerAddrs: []string{addr},
				Timeout:     60 * time.Second,
			}
			testClient := client.NewClient(clientConfig)
			err := testClient.DiscoverController()
			testClient.Close()

			if err != nil {
				allReady = false
				break
			}
		}

		if allReady {
			log.Println("Cluster is ready!")
			return nil
		}

		time.Sleep(5 * time.Second)
	}

	return fmt.Errorf("cluster not ready after 150 seconds")
}

// createClients creates clients for each broker
func (te *TestEnvironment) createClients() error {
	log.Println("Creating clients...")

	for i := 0; i < len(te.brokers); i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", 9092+i)
		clientInstance := client.NewClient(client.ClientConfig{
			BrokerAddrs: []string{addr},
			Timeout:     600 * time.Second,
		})
		te.clients = append(te.clients, clientInstance)
	}

	log.Printf("Created %d clients", len(te.clients))
	return nil
}

// createTopics creates the specified topics
func (te *TestEnvironment) createTopics(topics []string) error {
	log.Println("Creating topics...")

	if len(te.clients) == 0 {
		return fmt.Errorf("no clients available")
	}

	admin := client.NewAdmin(te.clients[0])

	for _, topicName := range topics {
		result, err := admin.CreateTopic(client.CreateTopicRequest{
			Name:       topicName,
			Partitions: 3,
			Replicas:   2,
		})
		if err != nil {
			return fmt.Errorf("failed to create topic %s: %v", topicName, err)
		}
		if result.Error != nil {
			return fmt.Errorf("topic creation error for %s: %v", topicName, result.Error)
		}
		log.Printf("Created topic: %s", topicName)
	}

	return nil
}

// helper to create a topic; adjust once topic creation API is available.
// ensureTestTopic: currently we rely on topic pre-existence; could add admin creation if available.
func ensureTestTopic(t *testing.T, topic string) {
	t.Logf("Using topic %s (ensure it's created externally before running test)", topic)
}

// TestExactlyOnceSemantics validates producer idempotence (duplicate sequence suppression),
// transactional commit persistence, and consumer side idempotent filtering (in-process) under re-poll.
func TestExactlyOnceSemantics(t *testing.T) {
	te := NewTestEnvironment(TestEnvironmentConfig{
		BrokerCount: 3,
		Topics:      []string{"exactly_once_test"},
		StartEtcd:   true,
	})
	defer te.Cleanup()

	if err := te.Setup(TestEnvironmentConfig{
		BrokerCount: 3,
		Topics:      []string{"exactly_once_test"},
		StartEtcd:   true,
	}); err != nil {
		t.Fatalf("Failed to setup test environment: %v", err)
	}

	// Create TransactionAwareClient instead of regular Client
	cfg := client.ClientConfig{
		BrokerAddrs: []string{"127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"},
		Timeout:     600 * time.Second,
	}

	producerGroup := "eos-producer-group"
	txnClient, err := client.NewTransactionAwareClient(cfg, producerGroup)
	if err != nil {
		t.Fatalf("Failed to create transaction-aware client: %v", err)
	}
	defer txnClient.StopTransactionCheckListener()

	// Start TCP listener for transaction check callbacks
	callbackPort := 18080
	if err := txnClient.StartTransactionCheckListener(callbackPort); err != nil {
		t.Fatalf("Failed to start transaction check listener: %v", err)
	}

	// Register producer group with callback address
	callbackAddr := fmt.Sprintf("127.0.0.1:%d", callbackPort)
	registerProducerGroup(t, "127.0.0.1:9092", producerGroup, callbackAddr)

	// Wait a bit to ensure producer group registration is processed
	time.Sleep(1 * time.Second)

	// Create regular producer for non-transactional messages
	prod := client.NewProducerWithStrategy(txnClient.Client, client.PartitionStrategyManual)

	topic := "exactly_once_test"
	partition := int32(0)

	// 1.test send unique messages
	uniqueCount := 5
	msgs := make([]client.ProduceMessage, 0, uniqueCount)
	for i := 0; i < uniqueCount; i++ {
		msgs = append(msgs, client.ProduceMessage{Topic: topic, Partition: partition, Key: []byte(fmt.Sprintf("k-%d", i)), Value: []byte(fmt.Sprintf("v-%d", i))})
	}
	if _, err := prod.SendBatchWithOptions(msgs, false); err != nil {
		t.Fatalf("produce initial batch error: %v", err)
	}

	// duplicate resend (reuse first message's seq after first batch).
	dup := []client.ProduceMessage{msgs[0]}
	if _, err := prod.SendBatchWithOptions(dup, true); err != nil {
		t.Fatalf("duplicate send error: %v", err)
	}

	// 2.test send transactional message
	listener := &alwaysCommitListener{}

	// Register transaction listener with the TransactionAwareClient
	txnClient.RegisterTransactionListener(listener)

	// Create transaction producer using the TransactionAwareClient
	txProd := client.NewTransactionProducerWithGroup(txnClient.Client, listener, producerGroup)
	txMsg := &client.TransactionMessage{Topic: topic, Partition: partition, Value: []byte("tx-commit")}
	txn, result, err := txProd.SendTransactionMessageAndDoLocal(txMsg)
	if err != nil {
		t.Fatalf("transaction send error: %v", err)
	}
	if result.Error != nil {
		t.Fatalf("transaction result error: %v", result.Error)
	}
	if txn == nil {
		t.Fatalf("nil transaction returned")
	}

	// consume via basic consumer fetch loop (group consumer infra is more complex). We'll poll sequentially from offset 0.
	cons := client.NewConsumer(txnClient.Client)
	// simple scan until we've seen expected logical messages (uniqueCount + 1 txn commit)
	expectedValues := map[string]bool{}
	for i := 0; i < uniqueCount; i++ {
		expectedValues[fmt.Sprintf("v-%d", i)] = false
	}
	expectedValues["tx-commit"] = false

	// fetch loop
	start := time.Now()
	timeout := 15 * time.Second
	nextOffset := int64(0)
	for {
		if time.Since(start) > timeout {
			t.Fatalf("timeout waiting for messages")
		}
		fr, err := cons.Fetch(client.FetchRequest{Topic: topic, Partition: partition, Offset: nextOffset, MaxBytes: 1024 * 1024})
		if err != nil {
			t.Fatalf("fetch error: %v", err)
		}
		for _, m := range fr.Messages {
			val := string(m.Value)
			expectedValues[val] = true
			log.Printf("Consumed offset=%d val=%s", m.Offset, val)
		}
		nextOffset = fr.NextOffset
		// check if all expected values observed
		all := true
		for _, seen := range expectedValues {
			if !seen {
				all = false
				break
			}
		}
		if all {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// verify no duplicate values by re-fetch earlier offsets and re-count
	seenCount := map[string]int{}
	nextOffset = 0
	for nextOffset < 100 { // safety bound
		fr, err := cons.Fetch(client.FetchRequest{Topic: topic, Partition: partition, Offset: nextOffset, MaxBytes: 1024 * 1024})
		if err != nil {
			break
		}
		for _, m := range fr.Messages {
			seenCount[string(m.Value)]++
		}
		if len(fr.Messages) == 0 {
			break
		}
		nextOffset = fr.NextOffset
	}
	// duplicate resend should not introduce second copy of msgs[0].Value
	for val, cnt := range seenCount {
		if cnt > 1 {
			t.Fatalf("value %s appears %d times (expected 1)", val, cnt)
		}
	}
}

// alwaysCommitListener commits every transaction locally
type alwaysCommitListener struct{}

func (l *alwaysCommitListener) ExecuteLocalTransaction(id transaction.TransactionID, msg transaction.HalfMessage) transaction.TransactionState {
	return transaction.StateCommit
}
func (l *alwaysCommitListener) CheckLocalTransaction(id transaction.TransactionID, msg transaction.HalfMessage) transaction.TransactionState {
	return transaction.StateCommit
}

// registerProducerGroup sends a registration request to broker
func registerProducerGroup(t *testing.T, addr, group, callback string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial broker for register group: %v", err)
	}
	defer conn.Close()

	// Write request type
	if err := binary.Write(conn, binary.BigEndian, int32(protocol.RegisterProducerGroupRequestType)); err != nil {
		t.Fatalf("write request type: %v", err)
	}

	// Build payload
	var payload bytes.Buffer
	// version
	binary.Write(&payload, binary.BigEndian, int16(1))
	// group
	binary.Write(&payload, binary.BigEndian, int16(len(group)))
	payload.Write([]byte(group))
	// callback
	binary.Write(&payload, binary.BigEndian, int16(len(callback)))
	if len(callback) > 0 {
		payload.Write([]byte(callback))
	}

	data := payload.Bytes()
	if err := binary.Write(conn, binary.BigEndian, int32(len(data))); err != nil {
		t.Fatalf("write length: %v", err)
	}
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	// Read response length
	var respLen int32
	if err := binary.Read(conn, binary.BigEndian, &respLen); err != nil {
		t.Fatalf("read resp len: %v", err)
	}
	resp := make([]byte, respLen)
	if _, err := conn.Read(resp); err != nil {
		t.Fatalf("read resp: %v", err)
	}

	code := int16(binary.BigEndian.Uint16(resp[0:2]))
	if code != 0 {
		t.Fatalf("register producer group failed, code=%d", code)
	}
	if callback != "" {
		t.Logf("Registered producer group %s with callback address %s", group, callback)
	} else {
		t.Logf("Registered producer group %s (no callback address)", group)
	}
}

// killProcessesByPattern kills processes that match the given pattern
func (te *TestEnvironment) killProcessesByPattern(pattern string) {
	cmd := exec.Command("sh", "-c", fmt.Sprintf("ps aux | grep '%s' | grep -v grep | awk '{print $2}' | xargs -r kill -9", pattern))
	if err := cmd.Run(); err != nil {
		log.Printf("Warning: failed to kill processes matching '%s': %v", pattern, err)
	}
}

func (te *TestEnvironment) stopBrokerCluster() {

	log.Println("Stopping broker cluster...")

	// Kill broker processes by name
	te.killProcessesByPattern("broker")

	// Clean up data directories
	for i := 1; i <= 3; i++ {
		// os.RemoveAll(fmt.Sprintf("/tmp/broker%d-data", i))
	}
}

func (te *TestEnvironment) stopEtcd() {
	log.Println("Stopping etcd...")

	if te.etcdProcess != nil {
		// First try to stop the tracked process
		te.etcdProcess.Process.Kill()
		te.etcdProcess.Wait()
	} else {
		// If etcdProcess is nil, try to find and kill etcd processes by pattern
		log.Println("etcdProcess is nil, searching for etcd processes in system...")
		te.killProcessesByPattern("etcd")
	}

	// Clean up data directory regardless
	os.RemoveAll("/tmp/etcd-test")
}
