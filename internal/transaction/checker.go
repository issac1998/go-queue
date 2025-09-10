package transaction

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// DefaultTransactionChecker  check txn
type DefaultTransactionChecker struct {
	producerGroupBrokers map[string]string
	connectTimeout       time.Duration
	requestTimeout       time.Duration
}

// NewDefaultTransactionChecker create checker
func NewDefaultTransactionChecker() *DefaultTransactionChecker {
	return &DefaultTransactionChecker{
		producerGroupBrokers: make(map[string]string),
		connectTimeout:       5 * time.Second,
		requestTimeout:       10 * time.Second,
	}
}

// RegisterProducerGroup register checker
func (c *DefaultTransactionChecker) RegisterProducerGroup(group, brokerAddr string) {
	c.producerGroupBrokers[group] = brokerAddr
}

// CheckTransactionState check txn
// ->broker->produce linstener
func (c *DefaultTransactionChecker) CheckTransactionState(transactionID TransactionID, originalMessage HalfMessage) TransactionState {
	log.Printf("Checking transaction state for: %s", transactionID)

	brokerAddr, exists := c.producerGroupBrokers[originalMessage.ProducerGroup]
	if !exists {
		log.Printf("No broker registered for producer group: %s", originalMessage.ProducerGroup)
		return StateUnknown
	}

	return c.CheckTransactionStateWithBroker(brokerAddr, transactionID, originalMessage)
}

// CheckTransactionStateWithBroker check status
func (c *DefaultTransactionChecker) CheckTransactionStateWithBroker(brokerAddr string, transactionID TransactionID, originalMessage HalfMessage) TransactionState {
	conn, err := net.DialTimeout("tcp", brokerAddr, c.connectTimeout)
	if err != nil {
		log.Printf("Failed to connect to broker %s for transaction check: %v", brokerAddr, err)
		return StateUnknown
	}
	defer conn.Close()

	deadline := time.Now().Add(c.requestTimeout)
	conn.SetDeadline(deadline)

	request := &TransactionCheckRequest{
		TransactionID:   transactionID,
		Topic:           originalMessage.Topic,
		Partition:       originalMessage.Partition,
		OriginalMessage: originalMessage,
	}

	if err := c.sendCheckRequest(conn, request); err != nil {
		log.Printf("Failed to send transaction check request: %v", err)
		return StateUnknown
	}

	response, err := c.readCheckResponse(conn)
	if err != nil {
		log.Printf("Failed to read transaction check response: %v", err)
		return StateUnknown
	}

	if response.ErrorCode != protocol.ErrorNone {
		log.Printf("Transaction check failed with error: %s", response.Error)
		return StateUnknown
	}

	return response.State
}

func (c *DefaultTransactionChecker) sendCheckRequest(conn io.Writer, request *TransactionCheckRequest) error {
	if err := binary.Write(conn, binary.BigEndian, protocol.TransactionCheckRequestType); err != nil {
		return fmt.Errorf("failed to write request type: %v", err)
	}

	data, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	if err := binary.Write(conn, binary.BigEndian, int32(len(data))); err != nil {
		return fmt.Errorf("failed to write data length: %v", err)
	}

	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %v", err)
	}

	return nil
}

func (c *DefaultTransactionChecker) readCheckResponse(conn io.Reader) (*TransactionCheckResponse, error) {
	var dataLength int32
	if err := binary.Read(conn, binary.BigEndian, &dataLength); err != nil {
		return nil, fmt.Errorf("failed to read data length: %v", err)
	}

	data := make([]byte, dataLength)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, fmt.Errorf("failed to read data: %v", err)
	}

	var response TransactionCheckResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return &response, nil
}
