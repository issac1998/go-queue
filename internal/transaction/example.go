package transaction

import (
	"log"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// ExampleUsage 展示如何使用事务管理器
func ExampleUsage() {
	tm := NewTransactionManager()
	defer tm.Stop()

	err := tm.RegisterProducerGroup("producer-group-1", "localhost:8081")
	if err != nil {
		log.Printf("Failed to register producer group: %v", err)
		return
	}

	err = tm.RegisterProducerGroup("producer-group-2", "localhost:8082")
	if err != nil {
		log.Printf("Failed to register producer group: %v", err)
		return
	}

	req := &TransactionPrepareRequest{
		TransactionID: "txn-001",
		Topic:         "test-topic",
		Partition:     0,
		Key:           []byte("test-key"),
		Value:         []byte("test-value"),
		Headers:       map[string]string{"source": "test"},
		Timeout:       30000, // 30秒
		ProducerGroup: "producer-group-1",
	}

	resp, err := tm.PrepareTransaction(req)
	if err != nil {
		log.Printf("Failed to prepare transaction: %v", err)
		return
	}

	if resp.ErrorCode != protocol.ErrorNone {
		log.Printf("Prepare transaction failed: %s", resp.Error)
		return
	}

	log.Printf("Transaction prepared successfully: %s", resp.TransactionID)

	time.Sleep(2 * time.Second)

	commitResp, err := tm.CommitTransaction("txn-001")
	if err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return
	}

	if commitResp.ErrorCode != protocol.ErrorNone {
		log.Printf("Commit transaction failed: %s", commitResp.Error)
		return
	}

	log.Printf("Transaction committed successfully: %s", commitResp.TransactionID)
}

func ExampleRollback() {
	tm := NewTransactionManager()
	defer tm.Stop()

	req := &TransactionPrepareRequest{
		TransactionID: "txn-002",
		Topic:         "test-topic",
		Partition:     0,
		Value:         []byte("test-value-2"),
		ProducerGroup: "producer-group-1",
	}

	resp, err := tm.PrepareTransaction(req)
	if err != nil {
		log.Printf("Failed to prepare transaction: %v", err)
		return
	}

	if resp.ErrorCode != protocol.ErrorNone {
		log.Printf("Prepare transaction failed: %s", resp.Error)
		return
	}


	rollbackResp, err := tm.RollbackTransaction("txn-002")
	if err != nil {
		log.Printf("Failed to rollback transaction: %v", err)
		return
	}

	if rollbackResp.ErrorCode != protocol.ErrorNone {
		log.Printf("Rollback transaction failed: %s", rollbackResp.Error)
		return
	}

	log.Printf("Transaction rolled back successfully: %s", rollbackResp.TransactionID)
}

func ExampleMultipleProducerGroups() {
	tm := NewTransactionManager()
	defer tm.Stop()

	tm.RegisterProducerGroup("order-service", "localhost:8081")
	tm.RegisterProducerGroup("payment-service", "localhost:8082")
	tm.RegisterProducerGroup("inventory-service", "localhost:8083")

	transactions := []*TransactionPrepareRequest{
		{
			TransactionID: "order-txn-001",
			Topic:         "orders",
			Partition:     0,
			Value:         []byte("order-data"),
			ProducerGroup: "order-service",
			Timeout:       30000,
		},
		{
			TransactionID: "payment-txn-001",
			Topic:         "payments",
			Partition:     0,
			Value:         []byte("payment-data"),
			ProducerGroup: "payment-service",
			Timeout:       30000,
		},
		{
			TransactionID: "inventory-txn-001",
			Topic:         "inventory",
			Partition:     0,
			Value:         []byte("inventory-data"),
			ProducerGroup: "inventory-service",
			Timeout:       30000,
		},
	}

	for _, req := range transactions {
		resp, err := tm.PrepareTransaction(req)
		if err != nil {
			log.Printf("Failed to prepare transaction %s: %v", req.TransactionID, err)
			continue
		}

		if resp.ErrorCode != protocol.ErrorNone {
			log.Printf("Prepare transaction %s failed: %s", req.TransactionID, resp.Error)
			continue
		}

		log.Printf("Transaction prepared: %s from %s", resp.TransactionID, req.ProducerGroup)
	}

	groups := tm.GetRegisteredProducerGroups()
	log.Printf("Registered producer groups: %+v", groups)

	tm.CommitTransaction("order-txn-001")
	tm.RollbackTransaction("payment-txn-001")

	log.Printf("Transactions processed")
}
