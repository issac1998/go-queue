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
			name: "Default configuration",
			config: ClientConfig{
				BrokerAddrs: []string{},
			},
			expected: ClientConfig{
				BrokerAddrs: []string{"localhost:9092"},
				Timeout:     5 * time.Second,
			},
		},
		{
			name: "Custom configuration",
			config: ClientConfig{
				BrokerAddrs: []string{"localhost:9093"},
				Timeout:     10 * time.Second,
			},
			expected: ClientConfig{
				BrokerAddrs: []string{"localhost:9093"},

				Timeout: 10 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient(tt.config)
			if !slicesEqual(client.brokerAddrs, tt.expected.BrokerAddrs) {
				t.Errorf("Expected broker addrs %v, got %v", tt.expected.BrokerAddrs, client.brokerAddrs)
			}
			if client.timeout != tt.expected.Timeout {
				t.Errorf("Expected timeout %v, got %v", tt.expected.Timeout, client.timeout)
			}
		})
	}
}

// slicesEqual 比较两个字符串切片是否相等
func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
