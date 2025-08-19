package main

import (
	"encoding/binary"
	"flag"
	"log"
	"net"
	"os"

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

var manager *metadata.Manager

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

	// Ensure data directory exists
	os.MkdirAll(brokerConfig.Config.DataDir, 0755)

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
	defer func() {
		conn.Close()
		if r := recover(); r != nil {
			log.Printf("Connection handling panic: %v", r)
		}
	}()

	var reqType int32
	err := binary.Read(conn, binary.BigEndian, &reqType)
	if err != nil {
		log.Printf("Failed to read request type: %v", err)
		return
	}

	log.Printf("Received request type: %d", reqType)

	switch reqType {
	case fetchRequest:
		log.Printf("Handling fetch request")
		if err := protocol.HandleFetchRequest(conn, manager); err != nil {
			log.Printf("Failed to handle fetch request: %v", err)
		}
	case produceRequest:
		log.Printf("Handling produce request")
		if err := protocol.HandleProduceRequest(conn, manager); err != nil {
			log.Printf("Failed to handle produce request: %v", err)
		}
	case createTopicRequest:
		log.Printf("Handling createTopic request")
		protocol.HandleCreateTopicRequest(conn, manager)
	case joinGroupRequest:
		log.Printf("Handling joinGroup request")
		if err := protocol.HandleJoinGroupRequest(conn, manager); err != nil {
			log.Printf("Failed to handle joinGroup request: %v", err)
		}
	case leaveGroupRequest:
		log.Printf("Handling leaveGroup request")
		if err := protocol.HandleLeaveGroupRequest(conn, manager); err != nil {
			log.Printf("Failed to handle leaveGroup request: %v", err)
		}
	case heartbeatRequest:
		log.Printf("Handling heartbeat request")
		if err := protocol.HandleHeartbeatRequest(conn, manager); err != nil {
			log.Printf("Failed to handle heartbeat request: %v", err)
		}
	case commitOffsetRequest:
		log.Printf("Handling commitOffset request")
		if err := protocol.HandleCommitOffsetRequest(conn, manager); err != nil {
			log.Printf("Failed to handle commitOffset request: %v", err)
		}
	case fetchOffsetRequest:
		log.Printf("Handling fetchOffset request")
		if err := protocol.HandleFetchOffsetRequest(conn, manager); err != nil {
			log.Printf("Failed to handle fetchOffset request: %v", err)
		}
	default:
		log.Printf("Unknown request type: %d", reqType)
	}

}
