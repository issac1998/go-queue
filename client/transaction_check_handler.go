package client

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
	"github.com/issac1998/go-queue/internal/transaction"
)

// TransactionCheckHandler handles transaction check requests from server
type TransactionCheckHandler struct {
	listeners map[string]transaction.TransactionListener
	mu        sync.RWMutex
}

// NewTransactionCheckHandler creates a new transaction check handler
func NewTransactionCheckHandler() *TransactionCheckHandler {
	return &TransactionCheckHandler{
		listeners: make(map[string]transaction.TransactionListener),
	}
}

// RegisterListener registers a transaction listener for producer group
func (tch *TransactionCheckHandler) RegisterListener(producerGroup string, listener transaction.TransactionListener) {
	tch.mu.Lock()
	defer tch.mu.Unlock()

	tch.listeners[producerGroup] = listener
	log.Printf("Registered transaction listener for producer group: %s", producerGroup)
}

// UnregisterListener unregisters a transaction listener
func (tch *TransactionCheckHandler) UnregisterListener(producerGroup string) {
	tch.mu.Lock()
	defer tch.mu.Unlock()

	delete(tch.listeners, producerGroup)
	log.Printf("Unregistered transaction listener for producer group: %s", producerGroup)
}

// HandleCheckRequest handles transaction check request
func (tch *TransactionCheckHandler) HandleCheckRequest(conn net.Conn) error {
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	var requestLen int32
	if err := binary.Read(conn, binary.BigEndian, &requestLen); err != nil {
		return fmt.Errorf("failed to read request length: %w", err)
	}

	requestData := make([]byte, requestLen)
	if _, err := io.ReadFull(conn, requestData); err != nil {
		return fmt.Errorf("failed to read request data: %w", err)
	}

	var checkRequest transaction.TransactionCheckRequest
	if err := json.Unmarshal(requestData, &checkRequest); err != nil {
		return fmt.Errorf("failed to parse check request: %w", err)
	}

	log.Printf("Received transaction check request: %s", checkRequest.TransactionID)

	response := tch.processCheckRequest(&checkRequest)
	return tch.sendCheckResponse(conn, response)
}

// processCheckRequest processes the check request
func (tch *TransactionCheckHandler) processCheckRequest(req *transaction.TransactionCheckRequest) *transaction.TransactionCheckResponse {
	tch.mu.RLock()
	defer tch.mu.RUnlock()

	listener, exists := tch.listeners[req.ProducerGroup]
	if !exists {
		log.Printf("No transaction listener found for producer group: %s", req.ProducerGroup)
		return &transaction.TransactionCheckResponse{
			TransactionID: req.TransactionID,
			State:         transaction.StateUnknown,
			ErrorCode:     protocol.ErrorUnknownGroup,
			Error:         fmt.Sprintf("no listener for producer group: %s", req.ProducerGroup),
		}
	}

	state := listener.CheckLocalTransaction(req.TransactionID, req.OriginalMessage)
	log.Printf("Transaction check result: %s -> %d", req.TransactionID, state)

	return &transaction.TransactionCheckResponse{
		TransactionID: req.TransactionID,
		State:         state,
		ErrorCode:     protocol.ErrorNone,
	}
}

// sendCheckResponse sends check response
func (tch *TransactionCheckHandler) sendCheckResponse(conn net.Conn, response *transaction.TransactionCheckResponse) error {
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetWriteDeadline(time.Time{})

	responseData, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	if err := binary.Write(conn, binary.BigEndian, int32(len(responseData))); err != nil {
		return fmt.Errorf("failed to send response length: %w", err)
	}

	if _, err := conn.Write(responseData); err != nil {
		return fmt.Errorf("failed to send response data: %w", err)
	}

	log.Printf("Sent transaction check response: %s", response.TransactionID)
	return nil
}

// TransactionAwareClient is a client that supports transaction checking
type TransactionAwareClient struct {
	*Client
	checkHandler  *TransactionCheckHandler
	checkListener net.Listener
	producerGroup string
	isListening   bool
	stopChan      chan struct{}
	wg            sync.WaitGroup
}

// NewTransactionAwareClient creates a new transaction-aware client
func NewTransactionAwareClient(config ClientConfig, producerGroup string) (*TransactionAwareClient, error) {
	client := NewClient(config)

	tac := &TransactionAwareClient{
		Client:        client,
		checkHandler:  NewTransactionCheckHandler(),
		producerGroup: producerGroup,
		stopChan:      make(chan struct{}),
	}

	return tac, nil
}

// StartTransactionCheckListener starts the transaction check listener
func (tac *TransactionAwareClient) StartTransactionCheckListener(port int) error {
	if tac.isListening {
		return fmt.Errorf("transaction check listener already started")
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to start transaction check listener: %w", err)
	}

	tac.checkListener = listener
	tac.isListening = true

	log.Printf("Transaction check listener started on port %d", port)

	tac.wg.Add(1)
	go tac.handleConnections()

	return nil
}

// StopTransactionCheckListener stops the transaction check listener
func (tac *TransactionAwareClient) StopTransactionCheckListener() error {
	if !tac.isListening {
		return nil
	}

	close(tac.stopChan)

	if tac.checkListener != nil {
		tac.checkListener.Close()
	}

	tac.wg.Wait()
	tac.isListening = false

	log.Printf("Transaction check listener stopped")
	return nil
}

// RegisterTransactionListener registers a transaction listener
func (tac *TransactionAwareClient) RegisterTransactionListener(listener transaction.TransactionListener) {
	tac.checkHandler.RegisterListener(tac.producerGroup, listener)
}

// handleConnections handles incoming connections
func (tac *TransactionAwareClient) handleConnections() {
	defer tac.wg.Done()

	for {
		select {
		case <-tac.stopChan:
			return
		default:
			if tcpListener, ok := tac.checkListener.(*net.TCPListener); ok {
				tcpListener.SetDeadline(time.Now().Add(1 * time.Second))
			}

			conn, err := tac.checkListener.Accept()
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				select {
				case <-tac.stopChan:
					return
				default:
					log.Printf("Failed to accept connection: %v", err)
					continue
				}
			}

			tac.wg.Add(1)
			go func(c net.Conn) {
				defer tac.wg.Done()
				defer c.Close()

				if err := tac.handleConnection(c); err != nil {
					log.Printf("Error handling transaction check connection: %v", err)
				}
			}(conn)
		}
	}
}

// handleConnection handles a single connection
func (tac *TransactionAwareClient) handleConnection(conn net.Conn) error {
	var requestType int32
	if err := binary.Read(conn, binary.BigEndian, &requestType); err != nil {
		return fmt.Errorf("failed to read request type: %w", err)
	}

	if requestType != protocol.TransactionCheckRequestType {
		return fmt.Errorf("unexpected request type: %d", requestType)
	}

	return tac.checkHandler.HandleCheckRequest(conn)
}
