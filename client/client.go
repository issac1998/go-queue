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
	brokerAddrs []string
	timeout     time.Duration
	mu          sync.Mutex
}

// ClientConfig client configuration
type ClientConfig struct {
	BrokerAddrs []string
	Timeout     time.Duration
}

// NewClient creates a new client
func NewClient(config ClientConfig) *Client {
	if len(config.BrokerAddrs) == 0 {
		config.BrokerAddrs = []string{"localhost:9092"}
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Second
	}

	return &Client{
		brokerAddrs: config.BrokerAddrs,
		timeout:     config.Timeout,
	}
}

// connect creates a connection to broker
func (c *Client) connect() (net.Conn, error) {
	// 尝试连接到第一个可用的broker
	for _, addr := range c.brokerAddrs {
		conn, err := net.DialTimeout("tcp", addr, c.timeout)
		if err == nil {
			return conn, nil
		}
	}
	return nil, fmt.Errorf("failed to connect to any broker in %v", c.brokerAddrs)
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
	if requestType == FetchRequestType {
		// For fetch requests, response length includes the 4-byte
		actualDataLen = responseLen - 4
		if actualDataLen < 0 {
			return nil, fmt.Errorf("invalid response length: %d", responseLen)
		}
	}

	// Read response data
	responseData := make([]byte, actualDataLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}

	return responseData, nil
}

// Produce 便捷方法：发送单条消息
func (c *Client) Produce(topic string, partition int32, value []byte) error {
	producer := NewProducer(c)
	_, err := producer.Send(ProduceMessage{
		Topic:     topic,
		Partition: partition,
		Value:     value,
	})
	return err
}

// CreateTopic 便捷方法：创建topic
func (c *Client) CreateTopic(name string, partitions, replicas int32) error {
	admin := NewAdmin(c)
	result, err := admin.CreateTopic(CreateTopicRequest{
		Name:       name,
		Partitions: partitions,
		Replicas:   replicas,
	})
	if err != nil {
		return err
	}
	if result.Error != nil {
		return result.Error
	}
	return nil
}
