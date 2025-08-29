package client

import (
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	tests := []struct {
		name     string
		config   ClientConfig
		expected ClientConfig
	}{
		{
			name:   "default config",
			config: ClientConfig{},
			expected: ClientConfig{
				BrokerAddrs: []string{"localhost:9092"},
				Timeout:     5 * time.Second,
			},
		},
		{
			name: "custom config",
			config: ClientConfig{
				BrokerAddrs: []string{"127.0.0.1:8080"},
				Timeout:     10 * time.Second,
			},
			expected: ClientConfig{
				BrokerAddrs: []string{"127.0.0.1:8080"},
				Timeout:     10 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient(tt.config)

			if len(client.brokerAddrs) != len(tt.expected.BrokerAddrs) ||
				(len(client.brokerAddrs) > 0 && client.brokerAddrs[0] != tt.expected.BrokerAddrs[0]) {
				t.Errorf("expected BrokerAddrs %v, got %v", tt.expected.BrokerAddrs, client.brokerAddrs)
			}

			if client.timeout != tt.expected.Timeout {
				t.Errorf("expected Timeout %v, got %v", tt.expected.Timeout, client.timeout)
			}
		})
	}
}

func TestClientConnect(t *testing.T) {
	client := NewClient(ClientConfig{
		BrokerAddrs: []string{"localhost:9999"}, // Use non-existent port
		Timeout:     100 * time.Millisecond,
	})

	_, err := client.connect(false)
	if err == nil {
		t.Error("expected connection error for non-existent broker")
	}
}
