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

// 使用 protocol 包中定义的常量
// const 定义已移至 internal/protocol/protocol.go

var (
	manager        *metadata.Manager
	clusterManager *cluster.Manager
)

func main() {
	var (
		configFile = flag.String("config", "configs/broker.json", "Configuration file path")
	)
	flag.Parse()

	brokerConfig, err := config.LoadBrokerConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

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

	manager, err = metadata.NewManager(brokerConfig.Config)
	if err != nil {
		log.Fatalf("Failed to initialize Manager: %v", err)
	}

	if err := manager.Start(); err != nil {
		log.Fatalf("Failed to start Manager: %v", err)
	}

	if brokerConfig.Cluster.Enabled {
		log.Printf("Cluster mode enabled - NodeID: %d, RaftAddress: %s",
			brokerConfig.Cluster.NodeID, brokerConfig.Cluster.RaftAddress)

		clusterManager, err = cluster.NewManager(brokerConfig.Cluster.NodeID, &brokerConfig.Cluster, manager)
		if err != nil {
			log.Fatalf("Failed to initialize cluster manager: %v", err)
		}

		log.Printf("Cluster manager initialized successfully")
	}

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
		case protocol.ProduceRequestType:
			protocol.HandleProduceRequest(conn, manager, clusterManager)
		case protocol.FetchRequestType:
			protocol.HandleFetchRequest(conn, manager, clusterManager)
		case protocol.CreateTopicRequestType:
			protocol.HandleCreateTopicRequest(conn, manager)
		case protocol.JoinGroupRequestType:
			protocol.HandleJoinGroupRequest(conn, manager)
		case protocol.LeaveGroupRequestType:
			protocol.HandleLeaveGroupRequest(conn, manager)
		case protocol.HeartbeatRequestType:
			protocol.HandleHeartbeatRequest(conn, manager)
		case protocol.CommitOffsetRequestType:
			protocol.HandleCommitOffsetRequest(conn, manager)
		case protocol.FetchOffsetRequestType:
			protocol.HandleFetchOffsetRequest(conn, manager)
		case protocol.ClusterInfoRequestType:
			protocol.HandleClusterInfoRequest(conn, clusterManager)
		default:
			log.Printf("Unknown request type: %d", reqType)
			return
		}
	}
}
