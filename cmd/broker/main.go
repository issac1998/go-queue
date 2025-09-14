package main

import (
	"context"
	"flag"
	"hash/fnv"
	"log"
	"os"
	"os/signal"
	"strings"
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
	discoveryType := flag.String("discovery-type", "memory", "Discovery type (etcd, memory)")
	discoveryEndpoints := flag.String("discovery-endpoints", "", "Discovery endpoints (comma-separated)")
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
			RTTMillisecond:     200,
			HeartbeatRTT:       5,
			ElectionRTT:        15,
			CheckQuorum:        true,
			SnapshotEntries:    10000,
			CompactionOverhead: 5000,
		},
		Discovery: &discovery.DiscoveryConfig{
			Type:      *discoveryType,
			Endpoints: parseEndpoints(*discoveryEndpoints),
		},
		Performance: &broker.PerformanceConfig{
			MaxBatchSize:       1000,
			MaxBatchBytes:      1024 * 1024, // 1MB
			ConnectionPoolSize: 100,
		},
		EnableFollowerRead: *enableFollowerRead, // Set follower read configuration
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Create and start broker
	b := &broker.Broker{
		ID:      config.NodeID,
		Address: config.BindAddr,
		Port:    config.BindPort,
		Config:  config,
		Ctx:     ctx,
		Cancel:  cancel,
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

// parseEndpoints parses comma-separated endpoints string
func parseEndpoints(endpoints string) []string {
	if endpoints == "" {
		return nil
	}
	return strings.Split(endpoints, ",")
}

// hashBrokerID converts a broker ID string to a uint64 node ID
func hashBrokerID(brokerID string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(brokerID))
	return h.Sum64()
}
