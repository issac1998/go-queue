package discovery

import (
	"time"
)

// Discovery interface defines service discovery operations
type Discovery interface {
	// RegisterBroker registers a broker to the service discovery
	RegisterBroker(broker *BrokerInfo) error

	// DiscoverBrokers discovers all registered brokers
	DiscoverBrokers() ([]*BrokerInfo, error)

	// UpdateBrokerStatus updates the status of a broker
	UpdateBrokerStatus(brokerID, status string) error

	// Close closes the discovery connection
	Close() error
}

// BrokerInfo contains information about a broker
type BrokerInfo struct {
	ID          string            `json:"id"`
	Address     string            `json:"address"`
	Port        int               `json:"port"`
	RaftAddress string            `json:"raft_address"`
	Status      string            `json:"status"`
	LoadMetrics *LoadMetrics      `json:"load_metrics,omitempty"`
	LastSeen    time.Time         `json:"last_seen"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// LoadMetrics contains broker load information
type LoadMetrics struct {
	CPU           float64 `json:"cpu"`
	Memory        int64   `json:"memory"`
	DiskIO        int64   `json:"disk_io"`
	NetworkIO     int64   `json:"network_io"`
	LeaderCount   int     `json:"leader_count"`
	PartitionLoad float64 `json:"partition_load"`
}

// DiscoveryConfig contains service discovery configuration
type DiscoveryConfig struct {
	Type      string   `yaml:"type"`
	Endpoints []string `yaml:"endpoints"`
	Username  string   `yaml:"username"`
	Password  string   `yaml:"password"`
	Timeout   string   `yaml:"timeout"`
}

// NewDiscovery creates a new Discovery instance based on configuration
func NewDiscovery(config *DiscoveryConfig) (Discovery, error) {
	if config == nil {
		return NewMemoryDiscovery(), nil
	}

	switch config.Type {
	case "etcd":
		return NewEtcdDiscovery(config)
	case "consul":
		return NewConsulDiscovery(config)
	case "memory", "":
		return NewMemoryDiscovery(), nil
	default:
		return NewMemoryDiscovery(), nil
	}
}
