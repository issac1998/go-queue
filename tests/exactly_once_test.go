package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	client "github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/transaction"
)

// helper to create a topic; adjust once topic creation API is available.
// ensureTestTopic: currently we rely on topic pre-existence; could add admin creation if available.
func ensureTestTopic(t *testing.T, topic string) {
	t.Logf("Using topic %s (ensure it's created externally before running test)", topic)
}

// TestExactlyOnceSemantics validates producer idempotence (duplicate sequence suppression),
// transactional commit persistence, and consumer side idempotent filtering (in-process) under re-poll.
func TestExactlyOnceSemantics(t *testing.T) {
	cfg := client.ClientConfig{
		BrokerAddrs: []string{"127.0.0.1:9092"},
		Timeout:     10 * time.Second,
	}
	c := client.NewClient(cfg)

	// Register producer group formally before transactional operations
	registerProducerGroup(t, "127.0.0.1:9092", "eos-producer-group", "")
	prod := client.NewProducerWithStrategy(c, client.PartitionStrategyManual)
	topic := "exactly_once_test"
	partition := int32(0)
	ensureTestTopic(t, topic)

	uniqueCount := 5
	msgs := make([]client.ProduceMessage, 0, uniqueCount)
	for i := 0; i < uniqueCount; i++ {
		msgs = append(msgs, client.ProduceMessage{Topic: topic, Partition: partition, Key: []byte(fmt.Sprintf("k-%d", i)), Value: []byte(fmt.Sprintf("v-%d", i))})
	}
	if _, err := prod.SendBatchWithOptions(msgs, false); err != nil {
		t.Fatalf("produce initial batch error: %v", err)
	}

	// duplicate resend (reuse first message's seq after first batch). We can't directly access seq numbers recorded inside broker, but we can reuse messages slice (they now contain sequence fields assigned).
	dup := []client.ProduceMessage{msgs[0]} // single duplicate
	if _, err := prod.SendBatchWithOptions(dup, true); err != nil {
		t.Fatalf("duplicate send error: %v", err)
	}

	// transactional part: implement minimal listener that always commits
	listener := &alwaysCommitListener{}
	txProd := client.NewTransactionProducer(c, listener, "eos-producer-group")
	txMsg := &client.TransactionMessage{Topic: topic, Partition: partition, Value: []byte("tx-commit")}
	txn, result, err := txProd.SendHalfMessageAndDoLocal(txMsg)
	if err != nil {
		t.Fatalf("transaction send error: %v", err)
	}
	if result.Error != nil {
		t.Fatalf("transaction result error: %v", result.Error)
	}
	if txn == nil {
		t.Fatalf("nil transaction returned")
	}

	// consume via basic consumer fetch loop (group consumer infra is more complex). We'll poll sequentially from offset 0.
	cons := client.NewConsumer(c)
	// simple scan until we've seen expected logical messages (uniqueCount + 1 txn commit)
	expectedValues := map[string]bool{}
	for i := 0; i < uniqueCount; i++ {
		expectedValues[fmt.Sprintf("v-%d", i)] = false
	}
	expectedValues["tx-commit"] = false

	// fetch loop
	start := time.Now()
	timeout := 15 * time.Second
	nextOffset := int64(0)
	for {
		if time.Since(start) > timeout {
			t.Fatalf("timeout waiting for messages")
		}
		fr, err := cons.Fetch(client.FetchRequest{Topic: topic, Partition: partition, Offset: nextOffset, MaxBytes: 1024 * 1024})
		if err != nil {
			t.Fatalf("fetch error: %v", err)
		}
		for _, m := range fr.Messages {
			val := string(m.Value)
			expectedValues[val] = true
			log.Printf("Consumed offset=%d val=%s", m.Offset, val)
		}
		nextOffset = fr.NextOffset
		// check if all expected values observed
		all := true
		for _, seen := range expectedValues {
			if !seen {
				all = false
				break
			}
		}
		if all {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// verify no duplicate values by re-fetch earlier offsets and re-count
	seenCount := map[string]int{}
	nextOffset = 0
	for nextOffset < 100 { // safety bound
		fr, err := cons.Fetch(client.FetchRequest{Topic: topic, Partition: partition, Offset: nextOffset, MaxBytes: 1024 * 1024})
		if err != nil {
			break
		}
		for _, m := range fr.Messages {
			seenCount[string(m.Value)]++
		}
		if len(fr.Messages) == 0 {
			break
		}
		nextOffset = fr.NextOffset
	}
	// duplicate resend should not introduce second copy of msgs[0].Value
	for val, cnt := range seenCount {
		if cnt > 1 {
			t.Fatalf("value %s appears %d times (expected 1)", val, cnt)
		}
	}
}

// alwaysCommitListener commits every transaction locally
type alwaysCommitListener struct{}

func (l *alwaysCommitListener) ExecuteLocalTransaction(id transaction.TransactionID, messageID string) transaction.TransactionState {
	return transaction.StateCommit
}
func (l *alwaysCommitListener) CheckLocalTransaction(id transaction.TransactionID, messageID string) transaction.TransactionState {
	return transaction.StateCommit
}

// ExecuteBatchLocalTransaction 执行批量本地事务
func (l *alwaysCommitListener) ExecuteBatchLocalTransaction(id transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	return transaction.StateCommit
}
// CheckBatchLocalTransaction 检查批量本地事务状态
func (l *alwaysCommitListener) CheckBatchLocalTransaction(id transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	return transaction.StateCommit
}

// registerProducerGroup sends a registration request to broker
func registerProducerGroup(t *testing.T, addr, group, callback string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial broker for register group: %v", err)
	}
	defer conn.Close()

	// Write request type
	if err := binary.Write(conn, binary.BigEndian, int32(32)); err != nil {
		t.Fatalf("write request type: %v", err)
	} // 32 = RegisterProducerGroupRequestType

	// Build payload
	var payload bytes.Buffer
	// version
	binary.Write(&payload, binary.BigEndian, int16(1))
	// group
	binary.Write(&payload, binary.BigEndian, int16(len(group)))
	payload.Write([]byte(group))
	// callback
	binary.Write(&payload, binary.BigEndian, int16(len(callback)))
	if len(callback) > 0 {
		payload.Write([]byte(callback))
	}

	data := payload.Bytes()
	if err := binary.Write(conn, binary.BigEndian, int32(len(data))); err != nil {
		t.Fatalf("write length: %v", err)
	}
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	// Read response length
	var respLen int32
	if err := binary.Read(conn, binary.BigEndian, &respLen); err != nil {
		t.Fatalf("read resp len: %v", err)
	}
	resp := make([]byte, respLen)
	if _, err := conn.Read(resp); err != nil {
		t.Fatalf("read resp: %v", err)
	}
	// First two bytes: error code (int16) then maybe second int16 (as we mirrored pattern)
	if len(resp) < 4 {
		t.Fatalf("invalid response length: %d", len(resp))
	}
	code := int16(binary.BigEndian.Uint16(resp[0:2]))
	if code != 0 {
		// Treat ErrorInternalError (14) as retriable if group already registered concurrently
		if code == 14 {
			t.Logf("producer group already registered or transient issue (code=%d), continuing", code)
		} else {
			t.Fatalf("register producer group failed, code=%d", code)
		}
	}
	t.Logf("Registered producer group %s", group)
}
