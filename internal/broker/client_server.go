package broker

import (
	"fmt"
	"log"
	"net"
)

// ClientServer handles client connections and requests
type ClientServer struct {
	broker   *Broker
	listener net.Listener
}

// NewClientServer creates a new ClientServer
func NewClientServer(broker *Broker) (*ClientServer, error) {
	return &ClientServer{
		broker: broker,
	}, nil
}

// Start starts the client server
func (cs *ClientServer) Start() error {
	addr := fmt.Sprintf("%s:%d", cs.broker.Address, cs.broker.Port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	cs.listener = listener

	// Start accepting connections in a goroutine
	go cs.acceptConnections()

	log.Printf("Client server listening on %s", addr)
	return nil
}

// Stop stops the client server
func (cs *ClientServer) Stop() error {
	if cs.listener != nil {
		return cs.listener.Close()
	}
	return nil
}

// acceptConnections accepts and handles client connections
func (cs *ClientServer) acceptConnections() {
	for {
		conn, err := cs.listener.Accept()
		if err != nil {
			// Server is probably shutting down
			return
		}

		// Handle connection in a goroutine
		go cs.handleConnection(conn)
	}
}

// handleConnection handles a single client connection
func (cs *ClientServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// TODO: Implement protocol handling
	log.Printf("New client connection from %s", conn.RemoteAddr())
}
