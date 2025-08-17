package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Client configuration
type Client struct {
	brokerAddr string
	timeout    time.Duration
	mu         sync.Mutex
}

// ClientConfig client configuration
type ClientConfig struct {
	BrokerAddr string
	Timeout    time.Duration
}

// NewClient creates a new client
func NewClient(config ClientConfig) *Client {
	if config.BrokerAddr == "" {
		config.BrokerAddr = "localhost:9092"
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Second
	}

	return &Client{
		brokerAddr: config.BrokerAddr,
		timeout:    config.Timeout,
	}
}

// connect creates a connection to broker
func (c *Client) connect() (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", c.brokerAddr, c.timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %v", err)
	}
	return conn, nil
}

// sendRequest sends request and handles response
func (c *Client) sendRequest(requestType int32, requestData []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := c.connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(c.timeout))

	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		return nil, fmt.Errorf("failed to send request type: %v", err)
	}

	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to send request data: %v", err)
	}

	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return nil, fmt.Errorf("failed to read response length: %v", err)
	}

	// Calculate actual data length based on protocol
	actualDataLen := responseLen

	responseData := make([]byte, responseLen)
	if _, err := conn.Read(responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}

	return responseData, nil
}
