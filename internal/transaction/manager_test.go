package transaction

import (
	"fmt"
	"testing"

	"github.com/issac1998/go-queue/internal/protocol"
)

func TestTransactionManager_PrepareTransaction(t *testing.T) {
	tm := NewTransactionManager()
	defer tm.Stop()

	// 先注册生产者组
	err := tm.RegisterProducerGroup("test-group", "localhost:8081")
	if err != nil {
		t.Fatalf("Failed to register producer group: %v", err)
	}

	req := &TransactionPrepareRequest{
		TransactionID: "test-txn-001",
		Topic:         "test-topic",
		Partition:     0,
		Key:           []byte("test-key"),
		Value:         []byte("test-value"),
		Headers:       map[string]string{"source": "test"},
		Timeout:       30000,
		ProducerGroup: "test-group",
	}

	resp, err := tm.PrepareTransaction(req)
	if err != nil {
		t.Fatalf("PrepareTransaction failed: %v", err)
	}

	if resp.ErrorCode != protocol.ErrorNone {
		t.Fatalf("Expected ErrorNone, got %d: %s", resp.ErrorCode, resp.Error)
	}

	if resp.TransactionID != req.TransactionID {
		t.Fatalf("Expected transaction ID %s, got %s", req.TransactionID, resp.TransactionID)
	}

	// 验证半消息是否正确存储
	halfMsg, exists := tm.GetHalfMessage("test-txn-001")
	if !exists {
		t.Fatalf("Half message not found")
	}

	if halfMsg.Topic != req.Topic {
		t.Fatalf("Expected topic %s, got %s", req.Topic, halfMsg.Topic)
	}

	if halfMsg.ProducerGroup != req.ProducerGroup {
		t.Fatalf("Expected producer group %s, got %s", req.ProducerGroup, halfMsg.ProducerGroup)
	}
}

func TestTransactionManager_CommitTransaction(t *testing.T) {
	tm := NewTransactionManager()
	defer tm.Stop()

	// 先注册生产者组
	err := tm.RegisterProducerGroup("test-group", "localhost:8081")
	if err != nil {
		t.Fatalf("Failed to register producer group: %v", err)
	}

	// 先准备事务
	req := &TransactionPrepareRequest{
		TransactionID: "test-txn-002",
		Topic:         "test-topic",
		Partition:     0,
		Value:         []byte("test-value"),
		ProducerGroup: "test-group",
	}

	_, err = tm.PrepareTransaction(req)
	if err != nil {
		t.Fatalf("PrepareTransaction failed: %v", err)
	}

	// 提交事务
	commitResp, err := tm.CommitTransaction("test-txn-002")
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	if commitResp.ErrorCode != protocol.ErrorNone {
		t.Fatalf("Expected ErrorNone, got %d: %s", commitResp.ErrorCode, commitResp.Error)
	}

	// 验证半消息已被删除
	_, exists := tm.GetHalfMessage("test-txn-002")
	if exists {
		t.Fatalf("Half message should be deleted after commit")
	}
}

func TestTransactionManager_RollbackTransaction(t *testing.T) {
	tm := NewTransactionManager()
	defer tm.Stop()

	// 先注册生产者组
	err := tm.RegisterProducerGroup("test-group", "localhost:8081")
	if err != nil {
		t.Fatalf("Failed to register producer group: %v", err)
	}

	// 先准备事务
	req := &TransactionPrepareRequest{
		TransactionID: "test-txn-003",
		Topic:         "test-topic",
		Partition:     0,
		Value:         []byte("test-value"),
		ProducerGroup: "test-group",
	}

	_, err = tm.PrepareTransaction(req)
	if err != nil {
		t.Fatalf("PrepareTransaction failed: %v", err)
	}

	// 回滚事务
	rollbackResp, err := tm.RollbackTransaction("test-txn-003")
	if err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}

	if rollbackResp.ErrorCode != protocol.ErrorNone {
		t.Fatalf("Expected ErrorNone, got %d: %s", rollbackResp.ErrorCode, rollbackResp.Error)
	}

	// 验证半消息已被删除
	_, exists := tm.GetHalfMessage("test-txn-003")
	if exists {
		t.Fatalf("Half message should be deleted after rollback")
	}
}

func TestTransactionManager_RegisterProducerGroup(t *testing.T) {
	tm := NewTransactionManager()
	defer tm.Stop()

	// 测试注册生产者组
	err := tm.RegisterProducerGroup("test-group", "localhost:8081")
	if err != nil {
		t.Fatalf("RegisterProducerGroup failed: %v", err)
	}

	// 验证生产者组已注册
	groups := tm.GetRegisteredProducerGroups()
	if len(groups) != 1 {
		t.Fatalf("Expected 1 registered group, got %d", len(groups))
	}

	if groups["test-group"] != "localhost:8081" {
		t.Fatalf("Expected callback localhost:8081, got %s", groups["test-group"])
	}

	// 准备事务
	req := &TransactionPrepareRequest{
		TransactionID: "test-txn-005",
		Topic:         "test-topic",
		Partition:     0,
		Value:         []byte("test-value"),
		ProducerGroup: "test-group",
	}

	_, err = tm.PrepareTransaction(req)
	if err != nil {
		t.Fatalf("PrepareTransaction failed: %v", err)
	}

	// 获取半消息并验证检查器是否设置
	halfMsg, exists := tm.GetHalfMessage("test-txn-005")
	if !exists {
		t.Fatalf("Half message not found")
	}

	// 验证可以找到对应的检查器
	_, exists = tm.producerGroupCheckers[halfMsg.ProducerGroup]
	if !exists {
		t.Fatalf("Producer group checker not found")
	}
}

func TestTransactionManager_DuplicateTransaction(t *testing.T) {
	tm := NewTransactionManager()
	defer tm.Stop()

	// 先注册生产者组
	err := tm.RegisterProducerGroup("test-group", "localhost:8081")
	if err != nil {
		t.Fatalf("Failed to register producer group: %v", err)
	}

	req := &TransactionPrepareRequest{
		TransactionID: "test-txn-duplicate",
		Topic:         "test-topic",
		Partition:     0,
		Value:         []byte("test-value"),
		ProducerGroup: "test-group",
	}

	// 第一次准备事务
	resp1, err := tm.PrepareTransaction(req)
	if err != nil {
		t.Fatalf("First PrepareTransaction failed: %v", err)
	}

	if resp1.ErrorCode != protocol.ErrorNone {
		t.Fatalf("Expected ErrorNone for first prepare, got %d: %s", resp1.ErrorCode, resp1.Error)
	}

	// 第二次准备相同的事务，应该返回错误
	resp2, err := tm.PrepareTransaction(req)
	if err != nil {
		t.Fatalf("Second PrepareTransaction failed: %v", err)
	}

	if resp2.ErrorCode != protocol.ErrorInvalidRequest {
		t.Fatalf("Expected ErrorInvalidRequest for duplicate prepare, got %d: %s", resp2.ErrorCode, resp2.Error)
	}
}

func TestDefaultTransactionChecker(t *testing.T) {
	checker := NewDefaultTransactionChecker()

	// 注册生产者组
	checker.RegisterProducerGroup("test-group", "localhost:9092")

	// 测试检查事务状态（没有实际的broker，应该返回Unknown）
	halfMsg := HalfMessage{
		TransactionID: "test-txn",
		Topic:         "test-topic",
		Partition:     0,
		ProducerGroup: "test-group",
		Value:         []byte("test-value"),
	}

	state := checker.CheckTransactionState("test-txn", halfMsg)

	// 由于没有实际的broker运行，应该返回Unknown
	if state != StateUnknown {
		t.Fatalf("Expected StateUnknown, got %d", state)
	}

	// 测试未注册的生产者组
	halfMsg.ProducerGroup = "unknown-group"
	state = checker.CheckTransactionState("test-txn", halfMsg)

	if state != StateUnknown {
		t.Fatalf("Expected StateUnknown for unknown group, got %d", state)
	}
}

func TestTransactionManager_UnregisteredProducerGroup(t *testing.T) {
	tm := NewTransactionManager()
	defer tm.Stop()

	// 尝试准备事务但未注册生产者组
	req := &TransactionPrepareRequest{
		TransactionID: "test-txn-unregistered",
		Topic:         "test-topic",
		Partition:     0,
		Value:         []byte("test-value"),
		ProducerGroup: "unregistered-group",
	}

	resp, err := tm.PrepareTransaction(req)
	if err != nil {
		t.Fatalf("PrepareTransaction should not fail with error: %v", err)
	}

	// 应该返回错误码表示生产者组未注册
	if resp.ErrorCode != protocol.ErrorInvalidRequest {
		t.Fatalf("Expected ErrorInvalidRequest for unregistered group, got %d: %s", resp.ErrorCode, resp.Error)
	}

	if resp.Error == "" {
		t.Fatalf("Expected error message for unregistered group")
	}
}

func TestTransactionManager_MultipleProducerGroups(t *testing.T) {
	tm := NewTransactionManager()
	defer tm.Stop()

	// 注册多个生产者组
	groups := map[string]string{
		"service-a": "localhost:8081",
		"service-b": "localhost:8082",
		"service-c": "localhost:8083",
	}

	for group, callback := range groups {
		err := tm.RegisterProducerGroup(group, callback)
		if err != nil {
			t.Fatalf("Failed to register group %s: %v", group, err)
		}
	}

	// 验证所有组都已注册
	registered := tm.GetRegisteredProducerGroups()
	if len(registered) != len(groups) {
		t.Fatalf("Expected %d registered groups, got %d", len(groups), len(registered))
	}

	for group, expectedCallback := range groups {
		actualCallback, exists := registered[group]
		if !exists {
			t.Fatalf("Group %s not found in registered groups", group)
		}
		if actualCallback != expectedCallback {
			t.Fatalf("Group %s: expected callback %s, got %s", group, expectedCallback, actualCallback)
		}
	}

	// 为每个组准备事务
	for i, group := range []string{"service-a", "service-b", "service-c"} {
		req := &TransactionPrepareRequest{
			TransactionID: TransactionID(fmt.Sprintf("txn-%s-%d", group, i)),
			Topic:         "test-topic",
			Partition:     0,
			Value:         []byte(fmt.Sprintf("test-value-%d", i)),
			ProducerGroup: group,
		}

		resp, err := tm.PrepareTransaction(req)
		if err != nil {
			t.Fatalf("PrepareTransaction failed for group %s: %v", group, err)
		}

		if resp.ErrorCode != protocol.ErrorNone {
			t.Fatalf("Expected ErrorNone for group %s, got %d: %s", group, resp.ErrorCode, resp.Error)
		}
	}

	// 测试注销生产者组
	err := tm.UnregisterProducerGroup("service-b")
	if err != nil {
		t.Fatalf("Failed to unregister group: %v", err)
	}

	// 验证组已被注销
	registered = tm.GetRegisteredProducerGroups()
	if len(registered) != 2 {
		t.Fatalf("Expected 2 registered groups after unregister, got %d", len(registered))
	}

	if _, exists := registered["service-b"]; exists {
		t.Fatalf("service-b should be unregistered")
	}
}
