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
				BrokerAddr: "localhost:9092",
				Timeout:    5 * time.Second,
			},
		},
		{
			name: "custom config",
			config: ClientConfig{
				BrokerAddr: "127.0.0.1:8080",
				Timeout:    10 * time.Second,
			},
			expected: ClientConfig{
				BrokerAddr: "127.0.0.1:8080",
				Timeout:    10 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient(tt.config)

			if client.brokerAddr != tt.expected.BrokerAddr {
				t.Errorf("expected BrokerAddr %s, got %s", tt.expected.BrokerAddr, client.brokerAddr)
			}

			if client.timeout != tt.expected.Timeout {
				t.Errorf("expected Timeout %v, got %v", tt.expected.Timeout, client.timeout)
			}
		})
	}
}

func TestClientConnect(t *testing.T) {
	client := NewClient(ClientConfig{
		BrokerAddr: "localhost:9999", // Use non-existent port
		Timeout:    100 * time.Millisecond,
	})

	conn, err := client.connect()
	if err == nil {
		t.Error("expected connection error for non-existent broker")
		if conn != nil {
			conn.Close()
		}
	}
}
