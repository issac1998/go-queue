package client

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// Client configuration
type Client struct {
	// 基础配置
	brokerAddrs []string
	timeout     time.Duration
	mu          sync.RWMutex

	// 集群模式状态
	isClusterMode bool
	leaderAddr    string
	allBrokers    []string
	healthyNodes  map[string]bool

	stats *ClientStats
}

// ClientConfig 客户端配置
type ClientConfig struct {
	BrokerAddrs []string
	Timeout     time.Duration
}

// ClientStats 客户端统计
type ClientStats struct {
	mu             sync.RWMutex
	WriteRequests  int64  `json:"write_requests"`
	ReadRequests   int64  `json:"read_requests"`
	LeaderSwitches int64  `json:"leader_switches"`
	IsClusterMode  bool   `json:"is_cluster_mode"`
	CurrentLeader  string `json:"current_leader"`
}

// ClusterInfo 集群信息
type ClusterInfo struct {
	LeaderID    uint64                 `json:"leader_id"`
	LeaderValid bool                   `json:"leader_valid"`
	CurrentNode uint64                 `json:"current_node"`
	IsLeader    bool                   `json:"is_leader"`
	Brokers     map[uint64]interface{} `json:"brokers"`
	Topics      map[string]interface{} `json:"topics"`
}

// NewClient 创建智能客户端（自动检测集群模式）
func NewClient(config ClientConfig) *Client {
	var brokerAddrs []string
	if len(config.BrokerAddrs) > 0 {
		brokerAddrs = config.BrokerAddrs
	} else {
		brokerAddrs = []string{"localhost:9092"}
	}

	if config.Timeout == 0 {
		config.Timeout = 5 * time.Second
	}

	client := &Client{
		brokerAddrs:  brokerAddrs,
		timeout:      config.Timeout,
		healthyNodes: make(map[string]bool),
		stats:        &ClientStats{},
	}

	client.detectClusterMode()

	return client
}

// detectClusterMode 检测集群模式
func (c *Client) detectClusterMode() {
	for _, addr := range c.brokerAddrs {
		if err := c.queryClusterInfo(addr); err == nil {
			c.mu.Lock()
			c.isClusterMode = true
			c.stats.IsClusterMode = true
			c.mu.Unlock()
			log.Printf("Detected cluster mode, Leader: %s", c.leaderAddr)
			return
		}
	}

	c.mu.Lock()
	c.isClusterMode = false
	c.allBrokers = c.brokerAddrs
	for _, addr := range c.brokerAddrs {
		c.healthyNodes[addr] = true
	}
	c.mu.Unlock()
	log.Printf("Using single-node mode with brokers: %v", c.brokerAddrs)
}

// connect 创建连接（智能路由）
func (c *Client) connect(isWrite bool) (net.Conn, error) {
	var targetAddr string

	c.mu.RLock()
	if c.isClusterMode {
		if isWrite {
			targetAddr = c.leaderAddr
		} else {
			targetAddr = c.getHealthyFollower()
		}
	} else {
		// 单机模式：使用第一个可用地址
		for _, addr := range c.brokerAddrs {
			if c.healthyNodes[addr] {
				targetAddr = addr
				break
			}
		}
	}
	c.mu.RUnlock()

	if targetAddr == "" {
		return nil, fmt.Errorf("no available broker")
	}

	conn, err := net.DialTimeout("tcp", targetAddr, c.timeout)
	if err != nil {
		c.markNodeUnhealthy(targetAddr)
		return nil, fmt.Errorf("failed to connect to %s: %v", targetAddr, err)
	}
	return conn, nil
}

// sendRequest 发送请求（统一入口，智能路由）
func (c *Client) sendRequest(requestType int32, requestData []byte) ([]byte, error) {
	// 判断是否为写操作
	isWrite := c.isWriteOperation(requestType)

	conn, err := c.connect(isWrite)
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

	if requestType == protocol.FetchRequestType {
		// For fetch requests, response length includes the 4-byte length header itself
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

// isWriteOperation 判断是否为写操作
func (c *Client) isWriteOperation(requestType int32) bool {
	switch requestType {
	case protocol.ProduceRequestType:
		return true
	case protocol.CreateTopicRequestType:
		return true
	case protocol.JoinGroupRequestType, protocol.LeaveGroupRequestType, protocol.HeartbeatRequestType, protocol.CommitOffsetRequestType:
		return true
	case protocol.FetchRequestType:
		return false
	case protocol.FetchOffsetRequestType:
		return false
	case protocol.ClusterInfoRequestType:
		return false
	default:
		return true // 默认当作写操作，更安全
	}
}

// queryClusterInfo 查询集群信息
func (c *Client) queryClusterInfo(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, c.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := binary.Write(conn, binary.BigEndian, int32(protocol.ClusterInfoRequestType)); err != nil {
		return err
	}

	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return err
	}

	var errorCode int16
	if err := binary.Read(conn, binary.BigEndian, &errorCode); err != nil {
		return err
	}

	if errorCode != 0 {
		return fmt.Errorf("server error: %d", errorCode)
	}

	// 读取JSON数据
	jsonData := make([]byte, responseLen-protocol.ErrorCodeSize)
	if _, err := io.ReadFull(conn, jsonData); err != nil {
		return err
	}

	var clusterInfo ClusterInfo
	if err := json.Unmarshal(jsonData, &clusterInfo); err != nil {
		return err
	}

	// 更新集群状态
	c.mu.Lock()
	defer c.mu.Unlock()

	c.allBrokers = c.brokerAddrs
	for _, broker := range c.allBrokers {
		c.healthyNodes[broker] = true
	}

	if clusterInfo.IsLeader {
		c.leaderAddr = addr
		c.stats.CurrentLeader = addr
	}

	return nil
}

// getHealthyFollower 获取健康的Follower节点
func (c *Client) getHealthyFollower() string {
	// 收集非Leader的健康节点
	var followers []string
	for _, addr := range c.allBrokers {
		if addr != c.leaderAddr && c.healthyNodes[addr] {
			followers = append(followers, addr)
		}
	}

	if len(followers) > 0 {
		return followers[rand.Intn(len(followers))]
	}

	// 如果没有健康的Follower，返回Leader
	if c.healthyNodes[c.leaderAddr] {
		return c.leaderAddr
	}

	return ""
}

// markNodeUnhealthy 标记节点不健康
func (c *Client) markNodeUnhealthy(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.healthyNodes[addr] = false
	if addr == c.leaderAddr {
		c.leaderAddr = ""
		c.stats.mu.Lock()
		c.stats.LeaderSwitches++
		c.stats.CurrentLeader = ""
		c.stats.mu.Unlock()
		log.Printf("Leader %s failed, need to rediscover", addr)
	}
}

// GetStats 获取统计信息
func (c *Client) GetStats() *ClientStats {
	c.stats.mu.RLock()
	defer c.stats.mu.RUnlock()

	return &ClientStats{
		WriteRequests:  c.stats.WriteRequests,
		ReadRequests:   c.stats.ReadRequests,
		LeaderSwitches: c.stats.LeaderSwitches,
		IsClusterMode:  c.stats.IsClusterMode,
		CurrentLeader:  c.stats.CurrentLeader,
	}
}
