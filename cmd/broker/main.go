package main

import (
	"encoding/binary"
	"flag"
	"log"
	"net"
	"os"

	"github.com/issac1998/go-queue/internal/cluster"
	"github.com/issac1998/go-queue/internal/config"
	"github.com/issac1998/go-queue/internal/metadata"
	"github.com/issac1998/go-queue/internal/protocol"
)

const (
	produceRequest      = 0
	fetchRequest        = 1
	createTopicRequest  = 2
	joinGroupRequest    = 3
	leaveGroupRequest   = 4
	heartbeatRequest    = 5
	commitOffsetRequest = 6
	fetchOffsetRequest  = 7
)

var (
	manager        *metadata.Manager
	clusterManager *cluster.Manager
)

func main() {
	var (
		configFile = flag.String("config", "configs/broker.json", "Configuration file path")
	)
	flag.Parse()

	// Load configuration from file
	brokerConfig, err := config.LoadBrokerConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Set log output
	if brokerConfig.Server.LogFile != "" {
		file, err := os.OpenFile(brokerConfig.Server.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Failed to open log file %s: %v", brokerConfig.Server.LogFile, err)
		}
		defer file.Close()
		log.SetOutput(file)
		log.Printf("Log output to file: %s", brokerConfig.Server.LogFile)
	}

	log.Printf("Go Queue server starting - port:%s, data directory:%s",
		brokerConfig.Server.Port, brokerConfig.Config.DataDir)

	// Initialize Manager with loaded configuration
	manager, err = metadata.NewManager(brokerConfig.Config)
	if err != nil {
		log.Fatalf("Failed to initialize Manager: %v", err)
	}

	if err := manager.Start(); err != nil {
		log.Fatalf("Failed to start Manager: %v", err)
	}

	// Initialize cluster if enabled
	if brokerConfig.Cluster.Enabled {
		log.Printf("Cluster mode enabled - NodeID: %d, RaftAddress: %s",
			brokerConfig.Cluster.NodeID, brokerConfig.Cluster.RaftAddress)

		clusterManager, err = cluster.NewManager(brokerConfig.Cluster.NodeID, &brokerConfig.Cluster, manager)
		if err != nil {
			log.Fatalf("Failed to initialize cluster manager: %v", err)
		}

		log.Printf("Cluster manager initialized successfully")
	}

	// Ensure data directory exists
	os.MkdirAll(brokerConfig.Config.DataDir, 0755)

	// Create raft data directory if cluster is enabled
	if brokerConfig.Cluster.Enabled {
		os.MkdirAll(brokerConfig.Cluster.DataDir, 0755)
	}

	log.Printf("Go Queue server started on :%s", brokerConfig.Server.Port)

	ln, err := net.Listen("tcp", ":"+brokerConfig.Server.Port)
	if err != nil {
		log.Fatalf("Failed to listen on port: %v", err)
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Read request type
		var reqType int32
		if err := binary.Read(conn, binary.BigEndian, &reqType); err != nil {
			return
		}

		switch reqType {
		case produceRequest:
			protocol.HandleProduceRequest(conn, manager, clusterManager)
		case fetchRequest:
			protocol.HandleFetchRequest(conn, manager)
		case createTopicRequest:
			protocol.HandleCreateTopicRequest(conn, manager)
		case joinGroupRequest:
			protocol.HandleJoinGroupRequest(conn, manager)
		case leaveGroupRequest:
			protocol.HandleLeaveGroupRequest(conn, manager)
		case heartbeatRequest:
			protocol.HandleHeartbeatRequest(conn, manager)
		case commitOffsetRequest:
			protocol.HandleCommitOffsetRequest(conn, manager)
		case fetchOffsetRequest:
			protocol.HandleFetchOffsetRequest(conn, manager)
		default:
			log.Printf("Unknown request type: %d", reqType)
			return
		}
	}
}
