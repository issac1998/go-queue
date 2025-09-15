package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/dlq"
)

type IntegrationTest struct {
	brokerProcesses []*exec.Cmd
	etcdProcess     *exec.Cmd
	clients         []*client.Client
	dlqManager      *dlq.Manager
	testTopic       string
	consumerGroup   string
}

func NewIntegrationTest() *IntegrationTest {
	return &IntegrationTest{
		testTopic:     "dlq-test-topic",
		consumerGroup: "dlq-test-group",
	}
}

func (it *IntegrationTest) Run() error {
	log.Println("Starting DLQ Integration Test...")

	// Step 3: Wait for cluster to be ready
	if err := it.waitForClusterReady(); err != nil {
		return fmt.Errorf("cluster not ready: %v", err)
	}

	// Step 4: Create clients
	if err := it.createClients(); err != nil {
		return fmt.Errorf("failed to create clients: %v", err)
	}
	defer it.closeClients()

	// Step 5: Create test topic
	if err := it.createTestTopic(); err != nil {
		return fmt.Errorf("failed to create test topic: %v", err)
	}

	// Step 6: Setup DLQ manager
	if err := it.setupDLQManager(); err != nil {
		return fmt.Errorf("failed to setup DLQ manager: %v", err)
	}

	// Step 7: Run test scenarios
	if err := it.runTestScenarios(); err != nil {
		return fmt.Errorf("test scenarios failed: %v", err)
	}

	log.Println("DLQ Integration Test completed successfully!")
	return nil
}

func (it *IntegrationTest) startEtcd() error {
	log.Println("Starting etcd...")

	// Check if etcd is already running
	if err := exec.Command("pgrep", "etcd").Run(); err == nil {
		log.Println("etcd is already running")
		return nil
	}

	// Start etcd
	cmd := exec.Command("etcd",
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

	it.etcdProcess = cmd

	// Wait for etcd to be ready
	time.Sleep(3 * time.Second)
	return nil
}

func (it *IntegrationTest) stopEtcd() {
	if it.etcdProcess != nil {
		log.Println("Stopping etcd...")
		it.etcdProcess.Process.Kill()
		it.etcdProcess.Wait()
		// Clean up data directory
		os.RemoveAll("/tmp/etcd-test")
	}
}

func (it *IntegrationTest) startBrokerCluster() error {
	log.Println("Starting broker cluster...")

	brokerConfigs := []struct {
		nodeID   string
		bindPort int
		raftPort int
		dataDir  string
	}{
		{"broker1", 9095, 7004, "/tmp/dlq-broker1-data"},
		{"broker2", 9096, 7005, "/tmp/dlq-broker2-data"},
		{"broker3", 9097, 7006, "/tmp/dlq-broker3-data"},
	}

	for _, config := range brokerConfigs {
		// Clean up old data
		os.RemoveAll(config.dataDir)
		os.MkdirAll(config.dataDir, 0755)

		// Start broker
		cmd := exec.Command("go", "run", "./cmd/broker/main.go",
			"--node-id", config.nodeID,
			"--bind-addr", "127.0.0.1",
			"--bind-port", fmt.Sprintf("%d", config.bindPort),
			"--raft-addr", fmt.Sprintf("127.0.0.1:%d", config.raftPort),
			"--data-dir", config.dataDir,
			"--discovery-type", "etcd",
			"--discovery-endpoints", "127.0.0.1:2379",
		)
		cmd.Dir = "/Users/a/go-queue"
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			return fmt.Errorf("failed to start broker %s: %v", config.nodeID, err)
		}

		it.brokerProcesses = append(it.brokerProcesses, cmd)
		log.Printf("Started broker %s on port %d", config.nodeID, config.bindPort)

		// Wait a bit between broker starts
		time.Sleep(2 * time.Second)
	}

	// Wait for brokers to fully initialize
	log.Println("Waiting for brokers to initialize...")
	time.Sleep(15 * time.Second)
	return nil
}

func (it *IntegrationTest) stopBrokerCluster() {
	log.Println("Stopping broker cluster...")
	for i, cmd := range it.brokerProcesses {
		if cmd != nil {
			log.Printf("Stopping broker %d...", i+1)
			cmd.Process.Kill()
			cmd.Wait()
		}
	}

	// Clean up data directories
	for i := 1; i <= 3; i++ {
		os.RemoveAll(fmt.Sprintf("/tmp/broker%d-data", i))
	}
}

func (it *IntegrationTest) waitForClusterReady() error {
	log.Println("Waiting for cluster to be ready...")

	// Try to connect to each broker
	brokerAddrs := []string{"127.0.0.1:9095", "127.0.0.1:9096", "127.0.0.1:9097"}

	for i := 0; i < 30; i++ { // Wait up to 30 seconds
		allReady := true
		for _, addr := range brokerAddrs {
			clientConfig := client.ClientConfig{
				BrokerAddrs: []string{addr},
				Timeout:     5 * time.Second,
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

		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("cluster not ready after 30 seconds")
}

func (it *IntegrationTest) createClients() error {
	log.Println("Creating clients...")

	brokerAddrs := []string{"127.0.0.1:9095", "127.0.0.1:9096", "127.0.0.1:9097"}

	// Create client for each broker
	for _, addr := range brokerAddrs {
		clientConfig := client.ClientConfig{
			BrokerAddrs: []string{addr},
			Timeout:     30 * time.Second,
		}
		client := client.NewClient(clientConfig)
		it.clients = append(it.clients, client)
	}

	return nil
}

func (it *IntegrationTest) closeClients() {
	log.Println("Closing clients...")
	for _, client := range it.clients {
		if client != nil {
			client.Close()
		}
	}
}

func (it *IntegrationTest) createTestTopic() error {
	log.Println("Creating test topic...")

	// Use the first client to create topic
	if len(it.clients) == 0 {
		return fmt.Errorf("no clients available")
	}

	admin := client.NewAdmin(it.clients[0])
	result, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       it.testTopic,
		Partitions: 3,
		Replicas:   2,
	})
	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}
	if result.Error != nil {
		return fmt.Errorf("topic creation error: %v", result.Error)
	}

	log.Printf("Created topic: %s", it.testTopic)
	return nil
}

func (it *IntegrationTest) setupDLQManager() error {
	log.Println("Setting up DLQ manager...")

	// Use the first client for DLQ manager
	dlqConfig := dlq.DefaultDLQConfig()
	dlqConfig.RetryPolicy.MaxRetries = 3
	dlqConfig.RetryPolicy.InitialDelay = 1 * time.Second
	dlqConfig.RetryPolicy.BackoffFactor = 2.0
	dlqConfig.RetryPolicy.MaxDelay = 10 * time.Second
	dlqConfig.TopicSuffix = ".dlq"

	manager, err := dlq.NewManager(it.clients[0], dlqConfig)
	if err != nil {
		return fmt.Errorf("failed to create DLQ manager: %v", err)
	}

	it.dlqManager = manager
	log.Println("DLQ manager setup complete")
	return nil
}

func (it *IntegrationTest) runTestScenarios() error {
	log.Println("Running test scenarios...")

	// Scenario 1: Basic message sending and receiving
	if err := it.testBasicMessaging(); err != nil {
		return fmt.Errorf("basic messaging test failed: %v", err)
	}

	// Scenario 2: Message failure and DLQ handling
	if err := it.testDLQHandling(); err != nil {
		return fmt.Errorf("DLQ handling test failed: %v", err)
	}

	// Scenario 3: Retry mechanism
	if err := it.testRetryMechanism(); err != nil {
		return fmt.Errorf("retry mechanism test failed: %v", err)
	}

	// Scenario 4: DLQ statistics
	if err := it.testDLQStatistics(); err != nil {
		return fmt.Errorf("DLQ statistics test failed: %v", err)
	}

	return nil
}

func (it *IntegrationTest) testBasicMessaging() error {
	log.Println("Testing basic messaging...")

	// Create producer
	producer := client.NewProducer(it.clients[0])

	// Send test messages
	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("Test message %d", i)
		result, err := producer.Send(client.ProduceMessage{
			Topic:     it.testTopic,
			Partition: int32(i % 3),
			Value:     []byte(message),
		})
		if err != nil {
			return fmt.Errorf("failed to send message %d: %v", i, err)
		}
		if result.Error != nil {
			return fmt.Errorf("message send error %d: %v", i, result.Error)
		}
	}

	log.Println("Sent 10 test messages")

	// Create consumer
	consumer := client.NewConsumer(it.clients[1])

	// Consume messages
	consumedCount := 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for consumedCount < 10 {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for messages, consumed %d/10", consumedCount)
		default:
			result, err := consumer.FetchFrom(it.testTopic, int32(consumedCount%3), int64(consumedCount/3))
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if result.Error != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if len(result.Messages) > 0 {
				for _, msg := range result.Messages {
					log.Printf("Consumed message: %s", string(msg.Value))
					consumedCount++
				}
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	log.Printf("Successfully consumed %d messages", consumedCount)
	return nil
}

func (it *IntegrationTest) testDLQHandling() error {
	log.Println("Testing DLQ handling...")

	// Create a message that will "fail" processing
	failedMessage := &client.Message{
		Topic:     it.testTopic,
		Partition: 0,
		Offset:    100,
		Value:     []byte("Failed message for DLQ test"),
	}

	failureInfo := &dlq.FailureInfo{
		ConsumerGroup: it.consumerGroup,
		ConsumerID:    "test-consumer-1",
		ErrorMessage:  "Simulated processing failure",
		FailureTime:   time.Now(),
	}

	// Handle the failed message
	err := it.dlqManager.HandleFailedMessage(failedMessage, failureInfo)
	if err != nil {
		return fmt.Errorf("failed to handle failed message: %v", err)
	}

	log.Println("Successfully handled failed message")

	// Verify the message is tracked for retry
	shouldRetry := it.dlqManager.ShouldRetryMessage(
		failedMessage.Topic,
		failedMessage.Partition,
		failedMessage.Offset,
		it.consumerGroup,
	)

	if !shouldRetry {
		return fmt.Errorf("message should be marked for retry")
	}

	log.Println("Message correctly marked for retry")
	return nil
}

func (it *IntegrationTest) testRetryMechanism() error {
	log.Println("Testing retry mechanism...")

	// Create multiple failed messages to test retry logic
	for i := 0; i < 5; i++ {
		failedMessage := &client.Message{
			Topic:     it.testTopic,
			Partition: int32(i % 3),
			Offset:    int64(200 + i),
			Value:     []byte(fmt.Sprintf("Retry test message %d", i)),
		}

		failureInfo := &dlq.FailureInfo{
			ConsumerGroup: it.consumerGroup,
			ConsumerID:    "test-consumer-retry",
			ErrorMessage:  fmt.Sprintf("Retry test failure %d", i),
			FailureTime:   time.Now(),
		}

		// Handle failed message multiple times to test retry exhaustion
		for retry := 0; retry < 4; retry++ { // Exceed max retries (3)
			err := it.dlqManager.HandleFailedMessage(failedMessage, failureInfo)
			if err != nil {
				return fmt.Errorf("failed to handle retry %d for message %d: %v", retry, i, err)
			}

			// Check retry delay
			delay := it.dlqManager.GetRetryDelay(
				failedMessage.Topic,
				failedMessage.Partition,
				failedMessage.Offset,
				it.consumerGroup,
			)

			log.Printf("Message %d, retry %d: delay = %v", i, retry, delay)
		}

		// After exceeding max retries, should not retry anymore
		shouldRetry := it.dlqManager.ShouldRetryMessage(
			failedMessage.Topic,
			failedMessage.Partition,
			failedMessage.Offset,
			it.consumerGroup,
		)

		if shouldRetry {
			return fmt.Errorf("message %d should not retry after exceeding max retries", i)
		}
	}

	log.Println("Retry mechanism test completed successfully")
	return nil
}

func (it *IntegrationTest) testDLQStatistics() error {
	log.Println("Testing DLQ statistics...")

	// Get DLQ statistics
	stats := it.dlqManager.GetDLQStats()

	log.Printf("DLQ Statistics:")
	log.Printf("  Total Messages: %d", stats.TotalMessages)
	log.Printf("  Messages Last Hour: %d", stats.MessagesLastHour)
	log.Printf("  Messages Last Day: %d", stats.MessagesLastDay)
	log.Printf("  Top Failure Reasons: %v", stats.TopFailureReasons)

	// Verify we have some statistics
	if stats.TotalMessages == 0 {
		return fmt.Errorf("expected some messages in DLQ statistics")
	}

	log.Println("DLQ statistics test completed successfully")
	return nil
}

func main() {
	test := NewIntegrationTest()

	if err := test.Run(); err != nil {
		log.Fatalf("Integration test failed: %v", err)
	}

	log.Println("All tests passed!")
}
