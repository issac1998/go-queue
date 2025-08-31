package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/issac1998/go-queue/internal/broker"
	"github.com/issac1998/go-queue/internal/discovery"
	"github.com/issac1998/go-queue/internal/raft"
)

// LoadBrokerConfig loads broker configuration from file
// For demonstration purposes, this is a simplified loader
func LoadBrokerConfig(filename string) (*broker.BrokerConfig, error) {
	// Check if file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		fmt.Printf("Config file %s not found, using defaults\n", filename)
	} else {
		fmt.Printf("Loading config from %s\n", filename)
		// In real implementation, this would parse YAML
		data, err := os.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file %s: %v", filename, err)
		}

		// For demo, just print the config content
		fmt.Printf("Config file content:\n%s\n", string(data))
	}

	// Create default configuration
	config := &broker.BrokerConfig{}
	setDefaults(config)

	// Try to parse some basic values from file if it exists
	if data, err := os.ReadFile(filename); err == nil {
		parseBasicConfig(config, string(data))
	}

	return config, nil
}

// parseBasicConfig does very basic parsing for demo purposes
func parseBasicConfig(config *broker.BrokerConfig, content string) {
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "node_id:") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				config.NodeID = strings.Trim(strings.TrimSpace(parts[1]), `"`)
			}
		} else if strings.HasPrefix(line, "bind_port:") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				// Simple port parsing - in real implementation use proper YAML
				portStr := strings.TrimSpace(parts[1])
				if portStr == "9092" {
					config.BindPort = 9092
				} else if portStr == "9093" {
					config.BindPort = 9093
				} else if portStr == "9094" {
					config.BindPort = 9094
				}
			}
		} else if strings.HasPrefix(line, "data_dir:") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				config.DataDir = strings.Trim(strings.TrimSpace(parts[1]), `"`)
			}
		}
	}
}

// setDefaults sets default values for configuration
func setDefaults(config *broker.BrokerConfig) {
	if config.NodeID == "" {
		config.NodeID = "broker-1"
	}
	if config.BindAddr == "" {
		config.BindAddr = "0.0.0.0"
	}
	if config.BindPort == 0 {
		config.BindPort = 9092
	}
	if config.DataDir == "" {
		config.DataDir = "./data"
	}

	// Set Raft defaults
	if config.RaftConfig == nil {
		config.RaftConfig = &raft.RaftConfig{
			RTTMillisecond:         200,
			HeartbeatRTT:           5,
			ElectionRTT:            10,
			CheckQuorum:            true,
			SnapshotEntries:        10000,
			CompactionOverhead:     5000,
			MaxInMemLogSize:        67108864, // 64MB
			ControllerGroupID:      1,
			ControllerSnapshotFreq: 1000,
		}
	}

	// Set Discovery defaults
	if config.Discovery == nil {
		config.Discovery = &discovery.DiscoveryConfig{
			Type:    "memory",
			Timeout: "5s",
		}
	}

	// Set Performance defaults
	if config.Performance == nil {
		config.Performance = &broker.PerformanceConfig{
			MaxBatchSize:          500,
			MaxBatchBytes:         1048576, // 1MB
			BatchTimeout:          "10ms",
			WriteCacheSize:        33554432, // 32MB
			ReadCacheSize:         67108864, // 64MB
			ConnectionPoolSize:    100,
			ConnectionIdleTimeout: "5m",
			CompressionType:       "snappy",
			CompressionThreshold:  1024,
		}
	}
}
