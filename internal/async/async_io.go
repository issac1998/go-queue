package async

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// AsyncIO represents a user-space io_uring-like implementation
type AsyncIO struct {
	submissionQueue *SubmissionQueue
	completionQueue *CompletionQueue
	workersCnt      int
	connections     map[int64]*AsyncConnection
	connMutex       sync.RWMutex
	connCounter     int64
	config          AsyncIOConfig
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	running         int32
}

// AsyncIOConfig configuration for async IO
type AsyncIOConfig struct {
	WorkerCount    int
	SQSize         int
	CQSize         int
	BatchSize      int
	PollTimeout    time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	MaxConnections int
}

// SubmissionQueue represents the submission queue (like io_uring SQ)
type SubmissionQueue struct {
	entries  []SubmissionEntry
	head     uint32
	tail     uint32
	mask     uint32
	mu       sync.Mutex
	notEmpty *sync.Cond
}

// CompletionQueue represents the completion queue (like io_uring CQ)
type CompletionQueue struct {
	entries  []CompletionEntry
	head     uint32
	tail     uint32
	mask     uint32
	mu       sync.Mutex
	notEmpty *sync.Cond
}

// SubmissionEntry represents an IO operation to be submitted
type SubmissionEntry struct {
	opcode   OpCode
	conn     *AsyncConnection
	buffer   []byte
	userData uint64
	flags    uint32
	callback interface{}
}

// CompletionEntry represents a completed IO operation
type CompletionEntry struct {
	userData uint64
	result   int32
	flags    uint32
	error    error
	data     []byte
	callback interface{}
	conn     net.Conn // Add connection reference for stream callbacks
}

// OpCode represents operation types
type OpCode uint8

const (
	OpRead OpCode = iota
	OpWrite
	OpClose
	OpStreamRead // Stream read with direct conn access
)

// AsyncConnection represents an async connection
type AsyncConnection struct {
	id      int64
	conn    net.Conn
	asyncIO *AsyncIO
	closed  int32
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.RWMutex
}

// Callback types
type ReadCallback func(userData uint64, data []byte, n int, err error)
type WriteCallback func(userData uint64, conn net.Conn, n int, err error)
type StreamReadCallback func(userData uint64, conn net.Conn, err error)
type AcceptCallback func(conn net.Conn, err error)
type ConnectCallback func(err error)
type CloseCallback func(userData uint64, err error)

// NewAsyncIO creates a new AsyncIO instance
func NewAsyncIO(config AsyncIOConfig) *AsyncIO {
	if config.SQSize == 0 {
		config.SQSize = 1024
	}
	if config.CQSize == 0 {
		config.CQSize = 2048
	}
	if config.WorkerCount == 0 {
		config.WorkerCount = 4
	}
	if config.BatchSize == 0 {
		config.BatchSize = 32
	}
	if config.PollTimeout == 0 {
		config.PollTimeout = 100 * time.Millisecond
	}
	if config.MaxConnections == 0 {
		config.MaxConnections = 10000
	}

	ctx, cancel := context.WithCancel(context.Background())

	sq := &SubmissionQueue{
		entries: make([]SubmissionEntry, config.SQSize),
		mask:    uint32(config.SQSize - 1),
	}
	sq.notEmpty = sync.NewCond(&sq.mu)

	cq := &CompletionQueue{
		entries: make([]CompletionEntry, config.CQSize),
		mask:    uint32(config.CQSize - 1),
	}
	cq.notEmpty = sync.NewCond(&cq.mu)

	aio := &AsyncIO{
		submissionQueue: sq,
		completionQueue: cq,
		connections:     make(map[int64]*AsyncConnection),
		config:          config,
		ctx:             ctx,
		cancel:          cancel,
		workersCnt:      config.WorkerCount,
	}

	return aio
}

// Start starts the AsyncIO system
func (aio *AsyncIO) Start() error {
	if !atomic.CompareAndSwapInt32(&aio.running, 0, 1) {
		return fmt.Errorf("AsyncIO already running")
	}

	for range aio.workersCnt {
		aio.wg.Add(2)
		go aio.processCQ()
		go aio.processSQ()
	}

	return nil
}

// AddConnection adds a connection to async IO management
func (aio *AsyncIO) AddConnection(conn net.Conn) *AsyncConnection {
	id := atomic.AddInt64(&aio.connCounter, 1)
	ctx, cancel := context.WithCancel(aio.ctx)

	asyncConn := &AsyncConnection{
		id:      id,
		conn:    conn,
		asyncIO: aio,
		ctx:     ctx,
		cancel:  cancel,
	}

	aio.connMutex.Lock()
	aio.connections[id] = asyncConn
	aio.connMutex.Unlock()

	return asyncConn
}

// ReadAsync submits an async read operation
func (ac *AsyncConnection) ReadAsync(buffer []byte, callback ReadCallback) error {
	if atomic.LoadInt32(&ac.closed) == 1 {
		return fmt.Errorf("connection closed")
	}

	userData := uint64(ac.id)<<32 | uint64(time.Now().UnixNano()&0xFFFFFFFF)

	entry := SubmissionEntry{
		opcode:   OpRead,
		conn:     ac,
		buffer:   buffer,
		userData: userData,
		callback: callback,
	}

	return ac.asyncIO.submitSQE(entry)
}

// WriteAsync submits an async write operation
func (ac *AsyncConnection) WriteAsync(data []byte, userData uint64, callback WriteCallback) error {
	if atomic.LoadInt32(&ac.closed) == 1 {
		return fmt.Errorf("connection closed")
	}

	// If no userData provided, generate one
	if userData == 0 {
		userData = uint64(ac.id)<<32 | uint64(time.Now().UnixNano()&0xFFFFFFFF)
	}

	entry := SubmissionEntry{
		opcode:   OpWrite,
		conn:     ac,
		buffer:   data,
		userData: userData,
		callback: callback,
	}

	return ac.asyncIO.submitSQE(entry)
}

// ReadAsyncStream performs async read with direct conn access in callback
// This allows for streaming reads without buffer size limitations
func (ac *AsyncConnection) ReadAsyncStream(userData uint64, callback StreamReadCallback) error {
	if atomic.LoadInt32(&ac.closed) == 1 {
		return fmt.Errorf("connection closed")
	}

	if userData == 0 {
		userData = uint64(ac.id)<<32 | uint64(time.Now().UnixNano()&0xFFFFFFFF)
	}

	entry := SubmissionEntry{
		opcode:   OpStreamRead,
		conn:     ac,
		buffer:   nil, // No buffer needed for stream read
		userData: userData,
		callback: callback,
	}

	return ac.asyncIO.submitSQE(entry)
}

// Close closes the async connection
func (ac *AsyncConnection) Close() error {
	if !atomic.CompareAndSwapInt32(&ac.closed, 0, 1) {
		return nil
	}

	ac.cancel()

	// Close the underlying connection first
	err := ac.conn.Close()

	// Remove from connections map without holding connection lock
	ac.asyncIO.connMutex.Lock()
	delete(ac.asyncIO.connections, ac.id)
	ac.asyncIO.connMutex.Unlock()

	return err
}

// submitSQE submits a submission queue entry
func (aio *AsyncIO) submitSQE(entry SubmissionEntry) error {
	sq := aio.submissionQueue
	sq.mu.Lock()
	defer sq.mu.Unlock()

	if sq.tail-sq.head >= uint32(len(sq.entries)) {
		return fmt.Errorf("submission queue full")
	}

	sq.entries[sq.tail&sq.mask] = entry
	sq.tail++

	sq.notEmpty.Signal()

	return nil
}

func (aio *AsyncIO) executeOperation(entry SubmissionEntry) {
	var result int32
	var err error
	var data []byte

	entry.conn.mu.Lock()
	defer entry.conn.mu.Unlock()

	if atomic.LoadInt32(&entry.conn.closed) == 1 {
		err = fmt.Errorf("connection is closed")
		result = -1
	} else {
		switch entry.opcode {
		case OpRead:
			n, readErr := entry.conn.conn.Read(entry.buffer)
			result = int32(n)
			err = readErr
			if n > 0 {
				data = entry.buffer[:n]
			}

		case OpWrite:
			n, writeErr := entry.conn.conn.Write(entry.buffer)
			result = int32(n)
			err = writeErr

		case OpClose:
			err = entry.conn.conn.Close()
			atomic.StoreInt32(&entry.conn.closed, 1)
			result = 0

		default:
			err = fmt.Errorf("unsupported operation: %d", entry.opcode)
			result = -1
		}
	}

	cqe := CompletionEntry{
		userData: entry.userData,
		result:   result,
		error:    err,
		data:     data,
		callback: entry.callback,
		conn:     entry.conn.conn,
	}

	aio.submitCQE(cqe)
}

// submitCQE submits a completion queue entry
func (aio *AsyncIO) submitCQE(entry CompletionEntry) {
	cq := aio.completionQueue
	cq.mu.Lock()
	defer cq.mu.Unlock()

	if cq.tail-cq.head >= uint32(len(cq.entries)) {
		return
	}

	cq.entries[cq.tail&cq.mask] = entry
	cq.tail++

	cq.notEmpty.Signal()
}

func (aio *AsyncIO) processCQ() {
	defer aio.wg.Done()

	cq := aio.completionQueue
	batch := make([]CompletionEntry, aio.config.BatchSize)

	for {
		select {
		case <-aio.ctx.Done():
			return
		default:
		}

		cq.mu.Lock()
		for cq.head == cq.tail {
			select {
			case <-aio.ctx.Done():
				cq.mu.Unlock()
				return
			default:
				cq.notEmpty.Wait()
			}
		}

		batchSize := 0
		for cq.head != cq.tail && batchSize < aio.config.BatchSize {
			batch[batchSize] = cq.entries[cq.head&cq.mask]
			cq.head++
			batchSize++
		}
		cq.mu.Unlock()

		// Execute callbacks
		for i := 0; i < batchSize; i++ {
			aio.executeCallback(batch[i])
		}
	}
}

func (aio *AsyncIO) executeCallback(entry CompletionEntry) {
	switch cb := entry.callback.(type) {
	case ReadCallback:
		cb(entry.userData, entry.data, int(entry.result), entry.error)
	case WriteCallback:
		cb(entry.userData, entry.conn, int(entry.result), entry.error)
	case CloseCallback:
		cb(entry.userData, entry.error)
	}
}

// runWorker runs a worker goroutine
func (aio *AsyncIO) processSQ() {
	defer aio.wg.Done()

	sq := aio.submissionQueue
	batch := make([]SubmissionEntry, aio.config.BatchSize)

	for {
		select {
		case <-aio.ctx.Done():
			return
		default:
		}

		sq.mu.Lock()
		for sq.head == sq.tail {
			select {
			case <-aio.ctx.Done():
				sq.mu.Unlock()
				return
			default:
				sq.notEmpty.Wait()
			}
		}

		// Get batch of work
		batchSize := 0
		for sq.head != sq.tail && batchSize < aio.config.BatchSize {
			batch[batchSize] = sq.entries[sq.head&sq.mask]
			sq.head++
			batchSize++
		}
		sq.mu.Unlock()

		// Execute operations
		for i := 0; i < batchSize; i++ {
			aio.executeOperation(batch[i])
		}
	}
}

// Close closes the AsyncIO system
func (aio *AsyncIO) Close() {
	if !atomic.CompareAndSwapInt32(&aio.running, 1, 0) {
		return
	}

	aio.cancel()

	// Wake up waiting goroutines
	aio.submissionQueue.notEmpty.Broadcast()
	aio.completionQueue.notEmpty.Broadcast()

	// Close all connections - copy list first to avoid deadlock
	aio.connMutex.Lock()
	connections := make([]*AsyncConnection, 0, len(aio.connections))
	for _, conn := range aio.connections {
		connections = append(connections, conn)
	}
	aio.connMutex.Unlock()

	// Close connections without holding the mutex
	for _, conn := range connections {
		conn.Close()
	}

	aio.wg.Wait()
}

// GetStats returns AsyncIO statistics
func (aio *AsyncIO) GetStats() AsyncStats {
	aio.connMutex.RLock()
	activeConns := len(aio.connections)
	aio.connMutex.RUnlock()

	sq := aio.submissionQueue
	sq.mu.Lock()
	sqSize := int(sq.tail - sq.head)
	sq.mu.Unlock()

	cq := aio.completionQueue
	cq.mu.Lock()
	cqSize := int(cq.tail - cq.head)
	cq.mu.Unlock()

	return AsyncStats{
		ActiveConnections: int64(activeConns),
		SQSize:            sqSize,
		CQSize:            cqSize,
		TotalConnections:  atomic.LoadInt64(&aio.connCounter),
	}
}

// AsyncStats represents AsyncIO statistics
type AsyncStats struct {
	ActiveConnections int64 `json:"active_connections"`
	TotalConnections  int64 `json:"total_connections"`
	WorkerCount       int   `json:"worker_count"`
	SQSize            int   `json:"sq_size"`
	CQSize            int   `json:"cq_size"`
}
