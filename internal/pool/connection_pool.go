package pool

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// ConnectionPool connection pool
type ConnectionPool struct {
	maxConnections    int32
	minConnections    int32
	connectionTimeout time.Duration
	idleTimeout       time.Duration
	maxLifetime       time.Duration

	connections map[string]*BrokerPool
	mu          sync.RWMutex

	totalConnections  int64
	activeConnections int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// BrokerPool
type BrokerPool struct {
	address     string
	connections chan *PooledConnection
	mu          sync.RWMutex
	closed      bool

	maxConns    int32
	minConns    int32
	connTimeout time.Duration
	idleTimeout time.Duration
	maxLifetime time.Duration

	totalConns  int64
	activeConns int64

	ctx    context.Context
	cancel context.CancelFunc
}

// PooledConnection pooled connection
type PooledConnection struct {
	net.Conn
	pool       *BrokerPool
	createdAt  time.Time
	lastUsedAt time.Time
	inUse      bool
	mu         sync.RWMutex
}

// ConnectionPoolConfig config
type ConnectionPoolConfig struct {
	MaxConnections    int32
	MinConnections    int32
	ConnectionTimeout time.Duration
	IdleTimeout       time.Duration
	MaxLifetime       time.Duration
}

// NewConnectionPool
func NewConnectionPool(config ConnectionPoolConfig) *ConnectionPool {
	if config.MaxConnections <= 0 {
		config.MaxConnections = 10
	}
	if config.MinConnections <= 0 {
		config.MinConnections = 2
	}
	if config.ConnectionTimeout <= 0 {
		config.ConnectionTimeout = 5 * time.Second
	}
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = 30 * time.Minute
	}
	if config.MaxLifetime <= 0 {
		config.MaxLifetime = 1 * time.Hour
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &ConnectionPool{
		maxConnections:    config.MaxConnections,
		minConnections:    config.MinConnections,
		connectionTimeout: config.ConnectionTimeout,
		idleTimeout:       config.IdleTimeout,
		maxLifetime:       config.MaxLifetime,
		connections:       make(map[string]*BrokerPool),
		ctx:               ctx,
		cancel:            cancel,
	}

	pool.wg.Add(1)
	go pool.cleanupLoop()

	return pool
}

// GetConnection get connection from connection pool
func (cp *ConnectionPool) GetConnection(brokerAddr string) (*PooledConnection, error) {
	cp.mu.RLock()
	brokerPool, exists := cp.connections[brokerAddr]
	cp.mu.RUnlock()

	if !exists {
		brokerPool = cp.createBrokerPool(brokerAddr)

		cp.mu.Lock()
		if existing, exists := cp.connections[brokerAddr]; exists {
			cp.mu.Unlock()
			brokerPool.Close()
			brokerPool = existing
		} else {
			cp.connections[brokerAddr] = brokerPool
			cp.mu.Unlock()
		}
	}

	return brokerPool.GetConnection()
}

func (cp *ConnectionPool) createBrokerPool(brokerAddr string) *BrokerPool {
	ctx, cancel := context.WithCancel(cp.ctx)

	bp := &BrokerPool{
		address:     brokerAddr,
		connections: make(chan *PooledConnection, cp.maxConnections),
		maxConns:    cp.maxConnections,
		minConns:    cp.minConnections,
		connTimeout: cp.connectionTimeout,
		idleTimeout: cp.idleTimeout,
		maxLifetime: cp.maxLifetime,
		ctx:         ctx,
		cancel:      cancel,
	}

	// Pre-create minimum connections
	go bp.preCreateConnections()

	return bp
}

// GetConnection gets connection from broker pool
func (bp *BrokerPool) GetConnection() (*PooledConnection, error) {
	bp.mu.RLock()
	if bp.closed {
		bp.mu.RUnlock()
		return nil, fmt.Errorf("broker pool is closed")
	}
	bp.mu.RUnlock()

	select {
	case conn := <-bp.connections:
		if bp.isConnectionValid(conn) {
			conn.mu.Lock()
			conn.inUse = true
			conn.lastUsedAt = time.Now()
			conn.mu.Unlock()

			atomic.AddInt64(&bp.activeConns, 1)
			return conn, nil
		} else {
			conn.Close()
			atomic.AddInt64(&bp.totalConns, -1)
			return bp.createConnection()
		}
	default:
		return bp.createConnection()
	}
}

// createConnection creates new connection
func (bp *BrokerPool) createConnection() (*PooledConnection, error) {
	if atomic.LoadInt64(&bp.totalConns) >= int64(bp.maxConns) {
		return nil, fmt.Errorf("max connections reached for broker %s", bp.address)
	}

	rawConn, err := protocol.ConnectToSpecificBroker(bp.address, bp.connTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection to %s: %v", bp.address, err)
	}

	conn := &PooledConnection{
		Conn:       rawConn,
		pool:       bp,
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
		inUse:      true,
	}

	atomic.AddInt64(&bp.totalConns, 1)
	atomic.AddInt64(&bp.activeConns, 1)

	return conn, nil
}

// Return return connection to pool
func (pc *PooledConnection) Return() {
	pc.mu.Lock()
	if !pc.inUse {
		pc.mu.Unlock()
		return
	}
	pc.inUse = false
	pc.lastUsedAt = time.Now()
	pc.mu.Unlock()

	atomic.AddInt64(&pc.pool.activeConns, -1)

	// Check if pool is closed
	pc.pool.mu.RLock()
	closed := pc.pool.closed
	pc.pool.mu.RUnlock()
	
	if closed {
		pc.Close()
		atomic.AddInt64(&pc.pool.totalConns, -1)
		return
	}

	if pc.pool.isConnectionValid(pc) {
		select {
		case pc.pool.connections <- pc:
		default:
			pc.Close()
			atomic.AddInt64(&pc.pool.totalConns, -1)
		}
	} else {
		pc.Close()
		atomic.AddInt64(&pc.pool.totalConns, -1)
	}
}

// Close close pooled connection
func (pc *PooledConnection) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.Conn != nil {
		err := pc.Conn.Close()
		pc.Conn = nil
		return err
	}
	return nil
}

// isConnectionValid check connection is valid
func (bp *BrokerPool) isConnectionValid(conn *PooledConnection) bool {
	conn.mu.RLock()
	defer conn.mu.RUnlock()

	if conn.Conn == nil {
		return false
	}

	now := time.Now()

	if now.Sub(conn.createdAt) > bp.maxLifetime {
		return false
	}

	if !conn.inUse && now.Sub(conn.lastUsedAt) > bp.idleTimeout {
		return false
	}

	return true
}

// preCreateConnections pre create minimum connections
func (bp *BrokerPool) preCreateConnections() {
	for i := int32(0); i < bp.minConns; i++ {
		select {
		case <-bp.ctx.Done():
			return
		default:
			conn, err := bp.createConnection()
			if err != nil {
				continue
			}
			conn.Return()
		}
	}
}

// Close close broker connection pool
func (bp *BrokerPool) Close() {
	bp.mu.Lock()
	if bp.closed {
		bp.mu.Unlock()
		return
	}
	bp.closed = true
	bp.mu.Unlock()

	bp.cancel()

	close(bp.connections)
	for conn := range bp.connections {
		conn.Close()
	}
}

// cleanupLoop cleanup loop, periodically clean up expired connections
func (cp *ConnectionPool) cleanupLoop() {
	defer cp.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-cp.ctx.Done():
			return
		case <-ticker.C:
			cp.cleanup()
		}
	}
}

// cleanup clean up expired connections
func (cp *ConnectionPool) cleanup() {
	cp.mu.RLock()
	brokerPools := make([]*BrokerPool, 0, len(cp.connections))
	for _, bp := range cp.connections {
		brokerPools = append(brokerPools, bp)
	}
	cp.mu.RUnlock()

	for _, bp := range brokerPools {
		bp.cleanupConnections()
	}
}

// cleanupConnections clean up expired connections in broker pool
func (bp *BrokerPool) cleanupConnections() {
	var validConns []*PooledConnection

	for {
		select {
		case conn := <-bp.connections:
			if bp.isConnectionValid(conn) {
				validConns = append(validConns, conn)
			} else {
				conn.Close()
				atomic.AddInt64(&bp.totalConns, -1)
			}
		default:
			goto done
		}
	}

done:
	for _, conn := range validConns {
		select {
		case bp.connections <- conn:
		default:
			conn.Close()
			atomic.AddInt64(&bp.totalConns, -1)
		}
	}
}

// Close close whole connection pool
func (cp *ConnectionPool) Close() {
	cp.cancel()
	cp.wg.Wait()

	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _, bp := range cp.connections {
		bp.Close()
	}
	cp.connections = make(map[string]*BrokerPool)
}

// Stats connection pool statistics
type Stats struct {
	TotalConnections  int64                  `json:"total_connections"`
	ActiveConnections int64                  `json:"active_connections"`
	BrokerStats       map[string]BrokerStats `json:"broker_stats"`
}

// BrokerStats broker connection pool statistics
type BrokerStats struct {
	Address           string `json:"address"`
	TotalConnections  int64  `json:"total_connections"`
	ActiveConnections int64  `json:"active_connections"`
	PoolSize          int    `json:"pool_size"`
}

// GetStats
func (cp *ConnectionPool) GetStats() Stats {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	stats := Stats{
		TotalConnections:  atomic.LoadInt64(&cp.totalConnections),
		ActiveConnections: atomic.LoadInt64(&cp.activeConnections),
		BrokerStats:       make(map[string]BrokerStats),
	}

	for addr, bp := range cp.connections {
		stats.BrokerStats[addr] = BrokerStats{
			Address:           addr,
			TotalConnections:  atomic.LoadInt64(&bp.totalConns),
			ActiveConnections: atomic.LoadInt64(&bp.activeConns),
			PoolSize:          len(bp.connections),
		}
	}

	return stats
}
