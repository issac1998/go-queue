package discovery

import (
	"fmt"
	"sync"
	"time"
)

// MemoryDiscovery implements Discovery interface using in-memory storage
// This is mainly for testing and single-node development
type MemoryDiscovery struct {
	brokers map[string]*BrokerInfo
	mu      sync.RWMutex
}

// NewMemoryDiscovery creates a new memory-based discovery
func NewMemoryDiscovery() *MemoryDiscovery {
	return &MemoryDiscovery{
		brokers: make(map[string]*BrokerInfo),
	}
}

// RegisterBroker registers a broker
func (md *MemoryDiscovery) RegisterBroker(broker *BrokerInfo) error {
	md.mu.Lock()
	defer md.mu.Unlock()

	broker.LastSeen = time.Now()
	md.brokers[broker.ID] = broker

	return nil
}

// DiscoverBrokers discovers all registered brokers
func (md *MemoryDiscovery) DiscoverBrokers() ([]*BrokerInfo, error) {
	md.mu.RLock()
	defer md.mu.RUnlock()

	brokers := make([]*BrokerInfo, 0, len(md.brokers))
	for _, broker := range md.brokers {
		brokerCopy := *broker
		brokers = append(brokers, &brokerCopy)
	}

	return brokers, nil
}

// UpdateBrokerStatus updates the status of a broker
func (md *MemoryDiscovery) UpdateBrokerStatus(brokerID, status string) error {
	md.mu.Lock()
	defer md.mu.Unlock()

	broker, exists := md.brokers[brokerID]
	if !exists {
		return fmt.Errorf("broker %s not found", brokerID)
	}

	broker.Status = status
	broker.LastSeen = time.Now()

	return nil
}

// Close closes the discovery (no-op for memory implementation)
func (md *MemoryDiscovery) Close() error {
	md.mu.Lock()
	defer md.mu.Unlock()

	md.brokers = make(map[string]*BrokerInfo)
	return nil
}
