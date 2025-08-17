package main

import (
	"encoding/binary"
	"flag"
	"log"
	"net"
	"os"
	"time"

	"github.com/issac1998/go-queue/internal/compression"
	"github.com/issac1998/go-queue/internal/metadata"
	"github.com/issac1998/go-queue/internal/protocol"
)

const (
	produceRequest     = 0
	fetchRequest       = 1
	createTopicRequest = 2
)

var manager *metadata.Manager

func main() {
	// Command line arguments
	var (
		logFile = flag.String("log", "", "Log file path (default output to console)")
		port    = flag.String("port", "9092", "Listen port")
		dataDir = flag.String("data", "./data", "Data directory")
	)
	flag.Parse()

	// Set log output
	if *logFile != "" {
		file, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Failed to open log file %s: %v", *logFile, err)
		}
		defer file.Close()
		log.SetOutput(file)
		log.Printf("Log output to file: %s", *logFile)
	}

	log.Printf("Go Queue server starting - port:%s, data directory:%s", *port, *dataDir)

	// Initialize Manager
	config := &metadata.Config{
		DataDir:            *dataDir,
		MaxTopicPartitions: 16,
		SegmentSize:        1 << 30, // 1GB
		RetentionTime:      7 * 24 * time.Hour,
		MaxStorageSize:     100 << 30, // 100GB
		FlushInterval:      time.Second,
		CleanupInterval:    time.Hour,
		MaxMessageSize:     1 << 20, // 1MB

		// Enable compression and deduplication features
		CompressionEnabled:   true,
		CompressionType:      compression.Snappy, // Use Snappy compression (high performance)
		CompressionThreshold: 100,                // Compress only messages above 100 bytes

		DeduplicationEnabled: true,
		// DeduplicationConfig will use default configuration
	}

	var err error
	manager, err = metadata.NewManager(config)
	if err != nil {
		log.Fatalf("Failed to initialize Manager: %v", err)
	}

	if err := manager.Start(); err != nil {
		log.Fatalf("Failed to start Manager: %v", err)
	}

	// Ensure data directory exists
	os.MkdirAll(*dataDir, 0755)

	log.Printf("Go Queue server started on :%s", *port)

	ln, err := net.Listen("tcp", ":"+*port)
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

	// Read request type (each connection handles only one request)
	var reqType int32
	err := binary.Read(conn, binary.BigEndian, &reqType)
	if err != nil {
		log.Printf("Failed to read request type: %v", err)
		return
	}

	log.Printf("Received request type: %d", reqType)

	// Handle request
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
	default:
		log.Printf("Unknown request type: %d", reqType)
	}

	// Request processing completed, connection will be closed automatically
}
