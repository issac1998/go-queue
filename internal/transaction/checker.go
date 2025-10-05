package transaction

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net"
	"time"

	typederrors "github.com/issac1998/go-queue/internal/errors"
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
func (c *DefaultTransactionChecker) RegisterProducerGroup(group, callbackAddr string) {
	c.producerGroupBrokers[group] = callbackAddr
}


// CheckTransactionStateWithBroker check status
func (c *DefaultTransactionChecker) CheckTransactionStateNet(callbackAddr string, transactionID TransactionID, originalMessage HalfMessage) TransactionState {
	conn, err := net.DialTimeout("tcp", callbackAddr, c.connectTimeout)
	if err != nil {
		log.Printf("Failed to connect to broker %s for transaction check: %v", callbackAddr, err)
		return StateUnknown
	}
	defer conn.Close()

	deadline := time.Now().Add(c.requestTimeout)
	conn.SetDeadline(deadline)

	request := &TransactionCheckRequest{
		TransactionID: transactionID,
		Topic:         originalMessage.Topic,
		Partition:     originalMessage.Partition,
		MessageID:     string(originalMessage.TransactionID), // 使用TransactionID作为MessageID
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
		return typederrors.NewTypedError(typederrors.ConnectionError, "failed to write request type", err)
	}

	data, err := json.Marshal(request)
	if err != nil {
		return typederrors.NewTypedError(typederrors.GeneralError, "failed to marshal request", err)
	}

	if err := binary.Write(conn, binary.BigEndian, int32(len(data))); err != nil {
		return typederrors.NewTypedError(typederrors.ConnectionError, "failed to write data length", err)
	}

	if _, err := conn.Write(data); err != nil {
		return typederrors.NewTypedError(typederrors.ConnectionError, "failed to write data", err)
	}

	return nil
}

func (c *DefaultTransactionChecker) readCheckResponse(conn io.Reader) (*TransactionCheckResponse, error) {
	var dataLength int32
	if err := binary.Read(conn, binary.BigEndian, &dataLength); err != nil {
		return nil, typederrors.NewTypedError(typederrors.ConnectionError, "failed to read data length", err)
	}

	data := make([]byte, dataLength)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, typederrors.NewTypedError(typederrors.ConnectionError, "failed to read data", err)
	}

	var response TransactionCheckResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, typederrors.NewTypedError(typederrors.GeneralError, "failed to unmarshal response", err)
	}

	return &response, nil
}

// GetRegisteredProducerGroups returns all registered producer groups
func (c *DefaultTransactionChecker) GetRegisteredProducerGroups() map[string]string {
	result := make(map[string]string)
	for group, addr := range c.producerGroupBrokers {
		result[group] = addr
	}
	return result
}

// GetProducerGroupBroker returns the broker address for a specific producer group
func (c *DefaultTransactionChecker) GetProducerGroupBroker(group string) (string, bool) {
	addr, exists := c.producerGroupBrokers[group]
	return addr, exists
}

// UnregisterProducerGroup removes a producer group from the checker
func (c *DefaultTransactionChecker) UnregisterProducerGroup(group string) {
	delete(c.producerGroupBrokers, group)
}
