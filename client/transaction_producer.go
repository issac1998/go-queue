package client

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/errors"
	"github.com/issac1998/go-queue/internal/protocol"
	"github.com/issac1998/go-queue/internal/transaction"
)

// TransactionProducer produce
type TransactionProducer struct {
	client          *Client
	listener        transaction.TransactionListener
	group           string
	callbackAddress string 
	partitioner     Partitioner 
	
	tcpListener   net.Listener
	isListening   bool
	stopChan      chan struct{}
	wg            sync.WaitGroup
	mu            sync.RWMutex
}

// TransactionMessage massage
type TransactionMessage struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Key       []byte            `json:"key,omitempty"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timeout   time.Duration     `json:"timeout"`
}

// Transaction txn
type Transaction struct {
	ID            transaction.TransactionID
	producer      *TransactionProducer
	producerGroup string
	prepared      bool
	topic         string
	partition     int32
}

// TransactionResult txn result
type TransactionResult struct {
	TransactionID transaction.TransactionID
	Offset        int64
	Timestamp     time.Time
	Error         error
}


// NewTransactionProducerWithGroup creates producer with explicit producer group
func NewTransactionProducer(client *Client, listener transaction.TransactionListener, group string) *TransactionProducer {
	return &TransactionProducer{
		client:          client,
		listener:        listener,
		group:           group,
		partitioner:     &HashPartitioner{}, 
		stopChan:        make(chan struct{}),
	}
}

// NewTransactionProducerWithStrategy create a new transaction producer with specific partitioning strategy
func NewTransactionProducerWithStrategy(client *Client, listener transaction.TransactionListener, group string, strategy PartitionStrategy) *TransactionProducer {
	var partitioner Partitioner
	switch strategy {
	case PartitionStrategyManual:
		partitioner = &ManualPartitioner{}
	case PartitionStrategyRoundRobin:
		partitioner = &RoundRobinPartitioner{}
	case PartitionStrategyRandom:
		partitioner = &RandomPartitioner{}
	case PartitionStrategyHash:
		partitioner = &HashPartitioner{}
	default:
		partitioner = &HashPartitioner{} 
	}

	return &TransactionProducer{
		client:          client,
		listener:        listener,
		group:           group,
		partitioner:     partitioner,
		stopChan:        make(chan struct{}),
	}
}


// StartTransactionListener start transaction listener
func (tp *TransactionProducer) StartTransactionListener(port string) error {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	
	if tp.isListening {
		return fmt.Errorf("transaction listener is already running")
	}
	
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to start transaction listener on port %s: %w", port, err)
	}
	
	tp.tcpListener = listener
	tp.isListening = true
	
	addr := listener.Addr().(*net.TCPAddr)
	tp.callbackAddress = fmt.Sprintf("localhost:%d", addr.Port)
	
	tp.wg.Add(1)
	go tp.handleTransactionCallbacks()
	
	log.Printf("Transaction listener started on %s", tp.callbackAddress)
	return nil
}

// StopTransactionListener stop transaction listener
func (tp *TransactionProducer) StopTransactionListener() error {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	
	if !tp.isListening {
		return nil
	}
	
	close(tp.stopChan)
	
	if tp.tcpListener != nil {
		tp.tcpListener.Close()
	}
	
	tp.wg.Wait()
	tp.isListening = false
	
	log.Printf("Transaction listener stopped")
	return nil
}


func (tp *TransactionProducer) handleTransactionCallbacks() {
	defer tp.wg.Done()
	
	for {
		select {
		case <-tp.stopChan:
			return
		default:
			conn, err := tp.tcpListener.Accept()
			if err != nil {
				select {
				case <-tp.stopChan:
					return
				default:
					log.Printf("Failed to accept connection: %v", err)
					continue
				}
			}
			
			go tp.handleConnection(conn)
		}
	}
}

func (tp *TransactionProducer) handleConnection(conn net.Conn) {
	defer conn.Close()
	
	var requestType int32
	if err := binary.Read(conn, binary.BigEndian, &requestType); err != nil {
		log.Printf("Failed to read request type: %v", err)
		return
	}
	
	if requestType != protocol.TransactionCheckRequestType {
		log.Printf("Unexpected request type: %d", requestType)
		return
	}
	
	var dataLength int32
	if err := binary.Read(conn, binary.BigEndian, &dataLength); err != nil {
		log.Printf("Failed to read data length: %v", err)
		return
	}
	
	data := make([]byte, dataLength)
	if _, err := io.ReadFull(conn, data); err != nil {
		log.Printf("Failed to read request data: %v", err)
		return
	}
	
	var request transaction.TransactionCheckRequest
	if err := json.Unmarshal(data, &request); err != nil {
		log.Printf("Failed to unmarshal request: %v", err)
		return
	}
	
	state := tp.listener.CheckLocalTransaction(request.TransactionID, request.MessageID)
	
	response := transaction.TransactionCheckResponse{
		TransactionID: request.TransactionID,
		State:         state,
		ErrorCode:     protocol.ErrorNone,
	}
	
	responseData, err := json.Marshal(response)
	if err != nil {
		log.Printf("Failed to marshal response: %v", err)
		return
	}
	
	if err := binary.Write(conn, binary.BigEndian, int32(len(responseData))); err != nil {
		log.Printf("Failed to write response length: %v", err)
		return
	}
	
	if _, err := conn.Write(responseData); err != nil {
		log.Printf("Failed to write response data: %v", err)
		return
	}
	
	log.Printf("Transaction check completed for %s: %v", request.TransactionID, state)
}

// BeginTransaction start new txn
func (tp *TransactionProducer) BeginTransaction() (*Transaction, error) {
	transactionID := tp.generateTransactionID()

	return &Transaction{
		ID:            transactionID,
		producer:      tp,
		prepared:      false,
		producerGroup: tp.group,
	}, nil
}

// SendHalfMessageAndDoLocal send half message, then do local txn
func (tp *TransactionProducer) SendHalfMessageAndDoLocal(msg *TransactionMessage) (*Transaction, *TransactionResult, error) {
	txn, err := tp.BeginTransaction()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	result, err := txn.SendHalfMessageAndDoLocal(msg)
	if err != nil {
		return txn, nil, fmt.Errorf("failed to send half message: %w", err)
	}

	return txn, result, nil
}

// SendBatchHalfMessagesAndDoLocal sends multiple messages as a single transaction for better performance
func (tp *TransactionProducer) SendBatchHalfMessagesAndDoLocal(msgs []*TransactionMessage) (*Transaction, *TransactionResult, error) {
	if len(msgs) == 0 {
		return nil, nil, fmt.Errorf("no messages provided for batch transaction")
	}

	txn, err := tp.BeginTransaction()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	result, err := txn.SendBatchHalfMessagesAndDoLocal(msgs)
	if err != nil {
		return txn, nil, fmt.Errorf("failed to send batch half messages: %w", err)
	}

	return txn, result, nil
}

// selectPartition selects appropriate partition for a transaction message
func (t *Transaction) selectPartition(msg *TransactionMessage) (int32, error) {
	if msg.Partition >= 0 {
		return msg.Partition, nil
	}

	topicMeta, err := t.producer.client.getTopicMetadata(msg.Topic)
	if err != nil {
		return 0, &errors.TypedError{
			Type:    errors.GeneralError,
			Message: "failed to get topic metadata",
			Cause:   err,
		}
	}

	numPartitions := int32(len(topicMeta.Partitions))
	
	// 创建一个临时的ProduceMessage来适配partitioner接口
	produceMsg := &ProduceMessage{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Key:       msg.Key,
		Value:     msg.Value,
	}
	
	return t.producer.partitioner.Partition(produceMsg, numPartitions)
}

// SendHalfMessage send half message
func (t *Transaction) SendHalfMessageAndDoLocal(msg *TransactionMessage) (*TransactionResult, error) {
	halfMessage := transaction.HalfMessage{
		TransactionID: t.ID,
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Key:           msg.Key,
		Value:         msg.Value,
		Headers:       msg.Headers,
		CreatedAt:     time.Now(),
		Timeout:       msg.Timeout,
		State:         transaction.StatePrepared,
	}

	err := t.sendHalfMessage(&halfMessage, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to send half message: %w", err)
	}

	t.prepared = true
	t.topic = msg.Topic
	t.partition = msg.Partition

	// do local 
	localTxnState := t.producer.listener.ExecuteLocalTransaction(t.ID, string(t.ID))

	var finalResult *TransactionResult
	switch localTxnState {
	case transaction.StateCommit:
		finalResult, err := t.Commit()
		if err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
		return finalResult, nil
	case transaction.StateRollback:
		err := t.Rollback()
		if err != nil {
			return nil, fmt.Errorf("failed to rollback transaction: %w", err)
		}
		finalResult = &TransactionResult{
			TransactionID: t.ID,
			Error:         fmt.Errorf("transaction rolled back"),
		}
	case transaction.StateUnknown:
		finalResult = &TransactionResult{
			TransactionID: t.ID,
			Error:         fmt.Errorf("transaction state unknown, will be checked later"),
		}
	default:
		err := t.Rollback()
		if err != nil {
			return nil, fmt.Errorf("failed to rollback transaction: %w", err)
		}
		finalResult = &TransactionResult{
			TransactionID: t.ID,
			Error:         fmt.Errorf("unknown transaction state, rolled back"),
		}
	}

	return finalResult, nil
}

// SendBatchHalfMessagesAndDoLocal sends multiple messages as a single transaction for better performance
func (t *Transaction) SendBatchHalfMessagesAndDoLocal(msgs []*TransactionMessage) (*TransactionResult, error) {
	if len(msgs) == 0 {
		return nil, fmt.Errorf("no messages provided for batch transaction")
	}

	// Convert TransactionMessage to MessageRequest for batch request
	messageRequests := make([]transaction.MessageRequest, len(msgs))
	topicPartitionMap := make(map[string]bool) // 用于去重
	var topicPartitions []transaction.TopicPartition

	for i, msg := range msgs {
		// Use the unified partition selection logic
		selectedPartition, err := t.selectPartition(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to select partition for message %d: %w", i, err)
		}

		messageRequests[i] = transaction.MessageRequest{
			Topic:     msg.Topic,
			Partition: selectedPartition,
			Key:       msg.Key,
			Value:     msg.Value,
			Headers:   msg.Headers,
		}

		// 收集唯一的topic-partition组合
		key := fmt.Sprintf("%s-%d", msg.Topic, selectedPartition)
		if !topicPartitionMap[key] {
			topicPartitionMap[key] = true
			topicPartitions = append(topicPartitions, transaction.TopicPartition{
				Topic:     msg.Topic,
				Partition: selectedPartition,
			})
		}
	}

	// Send batch transaction prepare request
	err := t.sendBatchTransactionPrepareRequest(messageRequests)
	if err != nil {
		return nil, fmt.Errorf("failed to send batch transaction prepare request: %w", err)
	}

	t.prepared = true
	t.topic = msgs[0].Topic
	t.partition = messageRequests[0].Partition // Use the selected partition

	// Execute batch local transaction - 构造messageIDs数组
	messageIDs := make([]string, len(messageRequests))
	for i := range messageRequests {
		messageIDs[i] = string(t.ID) + "_" + fmt.Sprintf("%d", i)
	}
	localTxnState := t.producer.listener.ExecuteBatchLocalTransaction(t.ID, messageIDs)

	var finalResult *TransactionResult
	switch localTxnState {
	case transaction.StateCommit:
		finalResult, err := t.BatchCommit(topicPartitions)
		if err != nil {
			return nil, fmt.Errorf("failed to commit batch transaction: %w", err)
		}
		return finalResult, nil
	case transaction.StateRollback:
		err := t.BatchRollback(topicPartitions)
		if err != nil {
			return nil, fmt.Errorf("failed to rollback batch transaction: %w", err)
		}
		finalResult = &TransactionResult{
			TransactionID: t.ID,
			Error:         fmt.Errorf("batch transaction rolled back"),
		}
	case transaction.StateUnknown:
		finalResult = &TransactionResult{
			TransactionID: t.ID,
			Error:         fmt.Errorf("batch transaction state unknown, will be checked later"),
		}
	default:
		err := t.BatchRollback(topicPartitions)
		if err != nil {
			return nil, fmt.Errorf("failed to rollback batch transaction: %w", err)
		}
		finalResult = &TransactionResult{
			TransactionID: t.ID,
			Error:         fmt.Errorf("batch transaction rolled back due to unknown state"),
		}
	}

	return finalResult, nil
}

// sendBatchTransactionPrepareRequest sends a batch transaction prepare request
func (t *Transaction) sendBatchTransactionPrepareRequest(messageRequests []transaction.MessageRequest) error {
	// Use the first message's topic and partition to determine the coordinator
	firstMsg := messageRequests[0]
	
	req := &transaction.BatchTransactionPrepareRequest{
		TransactionID:   t.ID,
		Messages:        messageRequests,
		Timeout:         30000, // 30 seconds in milliseconds
		ProducerGroup:   t.producerGroup,
		CallbackAddress: t.producer.callbackAddress,
	}

	requestData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal batch transaction prepare request: %w", err)
	}

	// Connect to the coordinator (first partition leader)
	conn, err := t.producer.client.connectForDataOperation(firstMsg.Topic, firstMsg.Partition, true)
	if err != nil {
		return fmt.Errorf("failed to connect for batch transaction prepare: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(t.producer.client.timeout))

	// Send request type
	if err := binary.Write(conn, binary.BigEndian, protocol.BatchTransactionPrepareRequestType); err != nil {
		return fmt.Errorf("failed to write request type: %w", err)
	}

	// Send request length
	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return fmt.Errorf("failed to write request length: %w", err)
	}

	// Send request data
	if _, err := conn.Write(requestData); err != nil {
		return fmt.Errorf("failed to write request data: %w", err)
	}

	// Read response length
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return fmt.Errorf("failed to read response length: %w", err)
	}

	// Read response data
	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return fmt.Errorf("failed to read response data: %w", err)
	}

	// Parse response
	var response transaction.BatchTransactionPrepareResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// Check for errors
	if response.ErrorCode != protocol.ErrorNone {
		return fmt.Errorf("batch transaction prepare failed: %s", response.Error)
	}

	return nil
}

func (t *Transaction) sendHalfMessage(halfMessage *transaction.HalfMessage, msg *TransactionMessage) error {
	// 使用统一的partition选择逻辑
	selectedPartition, err := t.selectPartition(msg)
	if err != nil {
		return fmt.Errorf("failed to select partition: %w", err)
	}

	req := &transaction.TransactionPrepareRequest{
		TransactionID:   t.ID,
		Topic:           msg.Topic,
		Partition:       selectedPartition, // 使用选择的partition
		Key:             msg.Key,
		Value:           msg.Value,
		Headers:         msg.Headers,
		Timeout:         int64(msg.Timeout / time.Millisecond),
		ProducerGroup:   t.producerGroup,
		CallbackAddress: t.producer.callbackAddress, 
	}

	conn, err := t.producer.client.connectForDataOperation(msg.Topic, selectedPartition, false)
	if err != nil {
		return &errors.TypedError{
			Type:    errors.PartitionLeaderError,
			Message: errors.FailedToConnectToPartitionMsg,
			Cause:   err,
		}
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(t.producer.client.timeout))

	if err := t.sendTransactionPrepareRequest(conn, req); err != nil {
		return fmt.Errorf("failed to send prepare request: %w", err)
	}

	response, err := t.readTransactionPrepareResponse(conn)
	if err != nil {
		return fmt.Errorf("failed to read prepare response: %w", err)
	}

	if response.ErrorCode != protocol.ErrorNone {
		return fmt.Errorf("prepare failed: %s", response.Error)
	}

	return nil
}

// Commit commits
func (t *Transaction) Commit() (*TransactionResult, error) {
	if !t.prepared {
		return nil, fmt.Errorf("transaction not prepared")
	}

	req := &transaction.TransactionCommitRequest{
		TransactionID: t.ID,
		Topic:         t.topic,
		Partition:     t.partition,
	}

	requestData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal commit request: %w", err)
	}

	// Connect to partition leader for data operation
	conn, err := t.producer.client.connectForDataOperation(t.topic, t.partition, true)
	if err != nil {
		return nil, fmt.Errorf("failed to connect for transaction commit: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(t.producer.client.timeout))

	// Send request type
	if err := binary.Write(conn, binary.BigEndian, protocol.TransactionCommitRequestType); err != nil {
		return nil, fmt.Errorf("failed to write request type: %w", err)
	}

	// Send request length
	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return nil, fmt.Errorf("failed to write request length: %w", err)
	}

	// Send request data
	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to write request data: %w", err)
	}

	// Read response length
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}

	// Read response data
	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}

	var response transaction.TransactionCommitResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, fmt.Errorf("failed to parse commit response: %w", err)
	}

	if response.ErrorCode != protocol.ErrorNone {
		return nil, fmt.Errorf("commit failed: %s", response.Error)
	}

	return &TransactionResult{
		TransactionID: response.TransactionID,
		Offset:        response.Offset,
		Timestamp:     response.Timestamp,
	}, nil
}

// Rollback rollback transaction
func (t *Transaction) Rollback() error {
	if !t.prepared {
		return fmt.Errorf("transaction not prepared")
	}

	req := &transaction.TransactionRollbackRequest{
		TransactionID: t.ID,
		Topic:         t.topic,
		Partition:     t.partition,
	}

	requestData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal rollback request: %w", err)
	}

	// Connect to partition leader for data operation
	conn, err := t.producer.client.connectForDataOperation(t.topic, t.partition, true)
	if err != nil {
		return fmt.Errorf("failed to connect for transaction rollback: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(t.producer.client.timeout))

	// Send request type
	if err := binary.Write(conn, binary.BigEndian, protocol.TransactionRollbackRequestType); err != nil {
		return fmt.Errorf("failed to write request type: %w", err)
	}

	// Send request length
	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return fmt.Errorf("failed to write request length: %w", err)
	}

	// Send request data
	if _, err := conn.Write(requestData); err != nil {
		return fmt.Errorf("failed to write request data: %w", err)
	}

	// Read response length
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return fmt.Errorf("failed to read response length: %w", err)
	}

	// Read response data
	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return fmt.Errorf("failed to read response data: %w", err)
	}

	var response transaction.TransactionRollbackResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return fmt.Errorf("failed to parse rollback response: %w", err)
	}

	if response.ErrorCode != protocol.ErrorNone {
		return fmt.Errorf("rollback failed: %s", response.Error)
	}

	return nil
}

func (tp *TransactionProducer) generateTransactionID() transaction.TransactionID {
	timestamp := time.Now().UnixNano()
	randomBytes := make([]byte, 8)
	// TODO: How to generate a unique ID?
	rand.Read(randomBytes)

	id := fmt.Sprintf("txn_%d_%s", timestamp, hex.EncodeToString(randomBytes))
	return transaction.TransactionID(id)
}

func (t *Transaction) sendTransactionPrepareRequest(conn io.Writer, req *transaction.TransactionPrepareRequest) error {
	requestData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	if err := binary.Write(conn, binary.BigEndian, protocol.TransactionPrepareRequestType); err != nil {
		return fmt.Errorf("failed to send request type: %w", err)
	}

	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return fmt.Errorf("failed to send request length: %w", err)
	}

	if _, err := conn.Write(requestData); err != nil {
		return fmt.Errorf("failed to send request data: %w", err)
	}

	return nil
}

func (t *Transaction) readTransactionPrepareResponse(conn io.Reader) (*transaction.TransactionPrepareResponse, error) {
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}

	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}

	var response transaction.TransactionPrepareResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &response, nil
}

// BatchCommit commits a batch transaction
func (t *Transaction) BatchCommit(topicPartitions []transaction.TopicPartition) (*TransactionResult, error) {
	if !t.prepared {
		return nil, fmt.Errorf("transaction not prepared")
	}

	req := &transaction.BatchTransactionCommitRequest{
		TransactionID:    t.ID,
		TopicPartitions:  topicPartitions,
	}

	requestData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch commit request: %w", err)
	}

	// Connect to the first partition leader as coordinator
	conn, err := t.producer.client.connectForDataOperation(t.topic, t.partition, true)
	if err != nil {
		return nil, fmt.Errorf("failed to connect for batch transaction commit: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(t.producer.client.timeout))

	// Send request type
	if err := binary.Write(conn, binary.BigEndian, protocol.BatchTransactionCommitRequestType); err != nil {
		return nil, fmt.Errorf("failed to write request type: %w", err)
	}

	// Send request length
	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return nil, fmt.Errorf("failed to write request length: %w", err)
	}

	// Send request data
	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to write request data: %w", err)
	}

	// Read response length
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}

	// Read response data
	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}

	var response transaction.BatchTransactionCommitResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, fmt.Errorf("failed to parse batch commit response: %w", err)
	}

	if response.ErrorCode != protocol.ErrorNone {
		return nil, fmt.Errorf("batch commit failed: %s", response.Error)
	}

	// 返回第一个结果作为代表（批量事务的结果包含多个分区的信息）
	if len(response.Results) > 0 {
		return &TransactionResult{
			TransactionID: response.TransactionID,
			Offset:        response.Results[0].Offset,
			Timestamp:     response.Results[0].Timestamp,
		}, nil
	}

	return &TransactionResult{
		TransactionID: response.TransactionID,
	}, nil
}

// BatchRollback rolls back a batch transaction
func (t *Transaction) BatchRollback(topicPartitions []transaction.TopicPartition) error {
	if !t.prepared {
		return fmt.Errorf("transaction not prepared")
	}

	req := &transaction.BatchTransactionRollbackRequest{
		TransactionID:   t.ID,
		TopicPartitions: topicPartitions,
	}

	requestData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal batch rollback request: %w", err)
	}

	// Connect to the first partition leader as coordinator
	conn, err := t.producer.client.connectForDataOperation(t.topic, t.partition, true)
	if err != nil {
		return fmt.Errorf("failed to connect for batch transaction rollback: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(t.producer.client.timeout))

	// Send request type
	if err := binary.Write(conn, binary.BigEndian, protocol.BatchTransactionRollbackRequestType); err != nil {
		return fmt.Errorf("failed to write request type: %w", err)
	}

	// Send request length
	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return fmt.Errorf("failed to write request length: %w", err)
	}

	// Send request data
	if _, err := conn.Write(requestData); err != nil {
		return fmt.Errorf("failed to write request data: %w", err)
	}

	// Read response length
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return fmt.Errorf("failed to read response length: %w", err)
	}

	// Read response data
	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return fmt.Errorf("failed to read response data: %w", err)
	}

	var response transaction.BatchTransactionRollbackResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return fmt.Errorf("failed to parse batch rollback response: %w", err)
	}

	if response.ErrorCode != protocol.ErrorNone {
		return fmt.Errorf("batch rollback failed: %s", response.Error)
	}

	return nil
}
