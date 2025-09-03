package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/issac1998/go-queue/internal/broker"
	"github.com/issac1998/go-queue/internal/discovery"
	"github.com/issac1998/go-queue/internal/raft"
)

func main() {
	nodeID := flag.String("node-id", "", "Unique node identifier")
	bindAddr := flag.String("bind-addr", "127.0.0.1", "Bind address")
	bindPort := flag.Int("bind-port", 9092, "Bind port")
	dataDir := flag.String("data-dir", "./data", "Data directory")
	raftAddr := flag.String("raft-addr", "", "Raft communication address")
	enableFollowerRead := flag.Bool("enable-follower-read", false, "Enable follower read on this broker")
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("node-id is required")
	}

	// Generate a consistent NodeID from broker ID
	hashedNodeID := hashBrokerID(*nodeID)

	// Create broker configuration
	config := &broker.BrokerConfig{
		NodeID:   *nodeID,
		BindAddr: *bindAddr,
		BindPort: *bindPort,
		DataDir:  *dataDir,
		RaftConfig: &raft.RaftConfig{
			NodeID:             hashedNodeID,
			RaftAddr:           *raftAddr,
			ControllerGroupID:  1, // Fixed controller group ID
			HeartbeatRTT:       5,
			ElectionRTT:        50, // Must be >= 10 * HeartbeatRTT
			CheckQuorum:        true,
			SnapshotEntries:    10000,
			CompactionOverhead: 5000,
		},
		Discovery: &discovery.DiscoveryConfig{},
		Performance: &broker.PerformanceConfig{
			MaxBatchSize:       1000,
			MaxBatchBytes:      1024 * 1024, // 1MB
			ConnectionPoolSize: 100,
		},
		EnableFollowerRead: *enableFollowerRead, // Set follower read configuration
	}

	// Create and start broker
	b := &broker.Broker{
		ID:      config.NodeID,
		Address: config.BindAddr,
		Port:    config.BindPort,
		Config:  config,
	}

	log.Printf("Starting Multi-Raft Message Queue Broker %s...", b.ID)

	if err := b.Start(); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Broker %s is running. Press Ctrl+C to exit.", b.ID)
	<-sigChan

	log.Printf("Shutting down broker %s...", b.ID)
	if err := b.Stop(); err != nil {
		log.Printf("Error stopping broker: %v", err)
	}

	log.Printf("Broker %s shutdown complete", b.ID)
}

// hashBrokerID converts a broker ID string to a uint64 node ID
func hashBrokerID(brokerID string) uint64 {
	hash := uint64(0)
	for _, b := range []byte(brokerID) {
		hash = hash*31 + uint64(b)
	}
	return hash
}
