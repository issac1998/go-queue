package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/issac1998/go-queue/internal/compression"
	"github.com/issac1998/go-queue/internal/deduplication"
	"github.com/issac1998/go-queue/internal/metadata"
)

// BrokerConfig represents the complete broker configuration
type BrokerConfig struct {
	*metadata.Config
	Server  ServerConfig  `json:"server"`
	Cluster ClusterConfig `json:"cluster"`
}

// ServerConfig represents server-specific configuration
type ServerConfig struct {
	Port    string `json:"port"`
	LogFile string `json:"log_file"`
}

// ClusterConfig represents cluster and Raft configuration
type ClusterConfig struct {
	Enabled        bool     `json:"enabled"`
	NodeID         uint64   `json:"node_id"`
	RaftAddress    string   `json:"raft_address"`
	InitialMembers []string `json:"initial_members"`
	DataDir        string   `json:"data_dir"`

	// Raft配置
	ElectionRTT        uint64 `json:"election_rtt"`
	HeartbeatRTT       uint64 `json:"heartbeat_rtt"`
	CheckQuorum        bool   `json:"check_quorum"`
	SnapshotEntries    uint64 `json:"snapshot_entries"`
	CompactionOverhead uint64 `json:"compaction_overhead"`
}

// ClientConfig represents client configuration
type ClientConfig struct {
	Broker  string        `json:"broker"`
	Timeout string        `json:"timeout"`
	LogFile string        `json:"log_file"`
	Command CommandConfig `json:"command"`
}

// CommandConfig represents command-specific configuration
type CommandConfig struct {
	Type      string `json:"type"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Message   string `json:"message"`
	Offset    int64  `json:"offset"`
	Count     int    `json:"count"`
}

// deduplicationConfigJSON is a temporary struct for JSON parsing
type deduplicationConfigJSON struct {
	HashType   deduplication.HashType `json:"hash_type"`
	MaxEntries int                    `json:"max_entries"`
	TTL        string                 `json:"ttl"`
	Enabled    bool                   `json:"enabled"`
}

// brokerConfigJSON is a temporary struct for JSON parsing
type brokerConfigJSON struct {
	DataDir            string `json:"data_dir"`
	MaxTopicPartitions int    `json:"max_topic_partitions"`
	SegmentSize        int64  `json:"segment_size"`
	RetentionTime      string `json:"retention_time"`
	MaxStorageSize     int64  `json:"max_storage_size"`
	FlushInterval      string `json:"flush_interval"`
	CleanupInterval    string `json:"cleanup_interval"`
	MaxMessageSize     int    `json:"max_message_size"`

	CompressionEnabled   bool                        `json:"compression_enabled"`
	CompressionType      compression.CompressionType `json:"compression_type"`
	CompressionThreshold int                         `json:"compression_threshold"`

	DeduplicationEnabled bool                     `json:"deduplication_enabled"`
	DeduplicationConfig  *deduplicationConfigJSON `json:"deduplication_config"`

	Server  ServerConfig  `json:"server"`
	Cluster ClusterConfig `json:"cluster"`
}

// LoadBrokerConfig loads broker configuration from JSON file
func LoadBrokerConfig(configPath string) (*BrokerConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var configJSON brokerConfigJSON
	if err := json.Unmarshal(data, &configJSON); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}

	// Parse duration fields
	retentionTime, err := time.ParseDuration(configJSON.RetentionTime)
	if err != nil {
		return nil, fmt.Errorf("invalid retention_time: %v", err)
	}

	flushInterval, err := time.ParseDuration(configJSON.FlushInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid flush_interval: %v", err)
	}

	cleanupInterval, err := time.ParseDuration(configJSON.CleanupInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid cleanup_interval: %v", err)
	}

	// Parse deduplication config if provided
	var dedupConfig *deduplication.Config
	if configJSON.DeduplicationConfig != nil {
		ttl, err := time.ParseDuration(configJSON.DeduplicationConfig.TTL)
		if err != nil {
			return nil, fmt.Errorf("invalid deduplication ttl: %v", err)
		}

		dedupConfig = &deduplication.Config{
			HashType:   configJSON.DeduplicationConfig.HashType,
			MaxEntries: configJSON.DeduplicationConfig.MaxEntries,
			TTL:        ttl,
			Enabled:    configJSON.DeduplicationConfig.Enabled,
		}
	}

	// Create the final config
	config := &BrokerConfig{
		Config: &metadata.Config{
			DataDir:            configJSON.DataDir,
			MaxTopicPartitions: configJSON.MaxTopicPartitions,
			SegmentSize:        configJSON.SegmentSize,
			RetentionTime:      retentionTime,
			MaxStorageSize:     configJSON.MaxStorageSize,
			FlushInterval:      flushInterval,
			CleanupInterval:    cleanupInterval,
			MaxMessageSize:     configJSON.MaxMessageSize,

			CompressionEnabled:   configJSON.CompressionEnabled,
			CompressionType:      configJSON.CompressionType,
			CompressionThreshold: configJSON.CompressionThreshold,

			DeduplicationEnabled: configJSON.DeduplicationEnabled,
			DeduplicationConfig:  dedupConfig,
		},
		Server:  configJSON.Server,
		Cluster: configJSON.Cluster,
	}

	// Set defaults if not provided
	if config.Server.Port == "" {
		config.Server.Port = "9092"
	}

	return config, nil
}

// LoadClientConfig loads client configuration from JSON file
func LoadClientConfig(configPath string) (*ClientConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var config ClientConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}

	// Set defaults if not provided
	if config.Broker == "" {
		config.Broker = "localhost:9092"
	}
	if config.Timeout == "" {
		config.Timeout = "10s"
	}

	return &config, nil
}

// GetTimeoutDuration parses timeout string to duration
func (c *ClientConfig) GetTimeoutDuration() (time.Duration, error) {
	return time.ParseDuration(c.Timeout)
}
