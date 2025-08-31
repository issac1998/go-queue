package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/issac1998/go-queue/internal/broker"
	"github.com/issac1998/go-queue/pkg/config"
)

func main() {
	var (
		configFile = flag.String("config", "configs/broker.yaml", "Configuration file path")
		nodeID     = flag.String("node-id", "", "Node ID (overrides config)")
		dataDir    = flag.String("data-dir", "", "Data directory (overrides config)")
	)
	flag.Parse()

	// Load configuration
	config, err := config.LoadBrokerConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Override with command line flags if provided
	if *nodeID != "" {
		config.NodeID = *nodeID
	}
	if *dataDir != "" {
		config.DataDir = *dataDir
	}

	// Create and start broker
	b, err := broker.NewBroker(config)
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}

	if err := b.Start(); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}

	// Wait for interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Println("Shutting down...")
	if err := b.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}

	log.Println("Broker stopped")
}
