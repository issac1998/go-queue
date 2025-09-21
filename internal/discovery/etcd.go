package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdDiscovery implements Discovery interface using etcd
type EtcdDiscovery struct {
	client *clientv3.Client
	config *DiscoveryConfig
	prefix string
	ctx    context.Context
	cancel context.CancelFunc
}

// NewEtcdDiscovery creates a new etcd-based discovery
func NewEtcdDiscovery(config *DiscoveryConfig) (*EtcdDiscovery, error) {
	if config == nil {
		return nil, fmt.Errorf("etcd config cannot be nil")
	}

	if len(config.Endpoints) == 0 {
		config.Endpoints = []string{"localhost:2379"}
	}

	timeout := 5 * time.Second
	if config.Timeout != "" {
		if t, err := time.ParseDuration(config.Timeout); err == nil {
			timeout = t
		}
	}

	clientConfig := clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: timeout,
	}

	if config.Username != "" && config.Password != "" {
		clientConfig.Username = config.Username
		clientConfig.Password = config.Password
	}

	// Create etcd client
	client, err := clientv3.New(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err = client.Status(ctx, config.Endpoints[0])
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to etcd: endpoints: %v, err: %w", config.Endpoints, err)
	}

	ctx, cancel = context.WithCancel(context.Background())

	return &EtcdDiscovery{
		client: client,
		config: config,
		prefix: "/go-queue/brokers",
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// RegisterBroker registers a broker to etcd
func (ed *EtcdDiscovery) RegisterBroker(broker *BrokerInfo) error {
	if broker == nil {
		return fmt.Errorf("broker info cannot be nil")
	}

	if broker.ID == "" {
		return fmt.Errorf("broker ID cannot be empty")
	}

	// Update last seen time
	broker.LastSeen = time.Now()

	// Serialize broker info to JSON
	brokerData, err := json.Marshal(broker)
	if err != nil {
		return fmt.Errorf("failed to marshal broker info: %w", err)
	}

	// Create key for the broker
	key := fmt.Sprintf("%s/%s", ed.prefix, broker.ID)

	// Put broker info to etcd with a lease for TTL
	ctx, cancel := context.WithTimeout(ed.ctx, 5*time.Second)
	defer cancel()

	// Create a lease with 30 seconds TTL
	lease, err := ed.client.Grant(ctx, 30)
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}

	// Put the broker info with lease
	_, err = ed.client.Put(ctx, key, string(brokerData), clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to register broker: %w", err)
	}

	// Keep the lease alive
	ch, kaerr := ed.client.KeepAlive(ed.ctx, lease.ID)
	if kaerr != nil {
		return fmt.Errorf("failed to keep lease alive: %w", kaerr)
	}

	// Start a goroutine to consume keep alive responses
	go func() {
		for ka := range ch {
			// Process keep alive response if needed
			_ = ka
		}
	}()

	return nil
}

// DiscoverBrokers discovers all registered brokers from etcd
func (ed *EtcdDiscovery) DiscoverBrokers() ([]*BrokerInfo, error) {
	ctx, cancel := context.WithTimeout(ed.ctx, 15*time.Second)
	defer cancel()

	// Get all brokers with the prefix
	resp, err := ed.client.Get(ctx, ed.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to discover brokers: %w", err)
	}

	brokers := make([]*BrokerInfo, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		var broker BrokerInfo
		if err := json.Unmarshal(kv.Value, &broker); err != nil {
			// Log error but continue with other brokers
			fmt.Printf("Warning: failed to unmarshal broker data for key %s: %v\n", string(kv.Key), err)
			continue
		}

		// Note: We rely on etcd lease mechanism for broker liveness,
		// so we don't need to check LastSeen time here

		brokers = append(brokers, &broker)
	}

	return brokers, nil
}

// UpdateBrokerStatus updates the status of a broker in etcd
func (ed *EtcdDiscovery) UpdateBrokerStatus(brokerID, status string) error {
	if brokerID == "" {
		return fmt.Errorf("broker ID cannot be empty")
	}

	if status == "" {
		return fmt.Errorf("status cannot be empty")
	}

	ctx, cancel := context.WithTimeout(ed.ctx, 5*time.Second)
	defer cancel()

	// Get current broker info
	key := fmt.Sprintf("%s/%s", ed.prefix, brokerID)
	resp, err := ed.client.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to get broker info: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return fmt.Errorf("broker %s not found", brokerID)
	}

	// Unmarshal current broker info
	var broker BrokerInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &broker); err != nil {
		return fmt.Errorf("failed to unmarshal broker info: %w", err)
	}

	// Update status and last seen time
	broker.Status = status
	broker.LastSeen = time.Now()

	// Marshal updated broker info
	brokerData, err := json.Marshal(&broker)
	if err != nil {
		return fmt.Errorf("failed to marshal updated broker info: %w", err)
	}

	// Update in etcd
	_, err = ed.client.Put(ctx, key, string(brokerData))
	if err != nil {
		return fmt.Errorf("failed to update broker status: %w", err)
	}

	return nil
}

// Close closes the etcd client and cancels all operations
func (ed *EtcdDiscovery) Close() error {
	if ed.cancel != nil {
		ed.cancel()
	}

	if ed.client != nil {
		return ed.client.Close()
	}

	return nil
}

// WatchBrokers watches for broker changes in etcd
func (ed *EtcdDiscovery) WatchBrokers(callback func(*BrokerInfo, string)) error {
	watchChan := ed.client.Watch(ed.ctx, ed.prefix, clientv3.WithPrefix())

	go func() {
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				brokerID := strings.TrimPrefix(string(event.Kv.Key), ed.prefix+"/")

				switch event.Type {
				case clientv3.EventTypePut:
					var broker BrokerInfo
					if err := json.Unmarshal(event.Kv.Value, &broker); err == nil {
						callback(&broker, "updated")
					}
				case clientv3.EventTypeDelete:
					// For delete events, we only have the broker ID
					broker := &BrokerInfo{ID: brokerID}
					callback(broker, "deleted")
				}
			}
		}
	}()

	return nil
}
