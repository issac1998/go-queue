package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/transaction"
)

// OrderBatchTransactionListener 订单批量事务监听器
// 模拟电商场景中的批量订单处理
type OrderBatchTransactionListener struct {
	mu           sync.RWMutex
	orders       map[string]*Order
	inventory    map[string]int
	transactions map[transaction.TransactionID]*BatchTransactionRecord
}

type Order struct {
	ID       string
	Product  string
	Quantity int
	UserID   string
	Amount   float64
	Status   string
}

type BatchTransactionRecord struct {
	TransactionID transaction.TransactionID
	Orders        []*Order
	TotalAmount   float64
	ProcessedAt   time.Time
	Status        string
}

func NewOrderBatchTransactionListener() *OrderBatchTransactionListener {
	return &OrderBatchTransactionListener{
		orders:       make(map[string]*Order),
		inventory:    map[string]int{
			"laptop":     50,
			"smartphone": 100,
			"tablet":     30,
			"headphones": 200,
		},
		transactions: make(map[transaction.TransactionID]*BatchTransactionRecord),
	}
}

// 单消息事务处理（保持向后兼容）
func (l *OrderBatchTransactionListener) ExecuteLocalTransaction(transactionID transaction.TransactionID, messageID string) transaction.TransactionState {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 基于messageID创建模拟订单数据
	order := &Order{
		ID:       messageID,
		Product:  "product_0", // 默认产品
		Quantity: 1,           // 默认数量
		UserID:   "user_0",    // 默认用户
		Amount:   100.0,       // 默认金额
		Status:   "pending",
	}

	// 检查库存
	if l.inventory[order.Product] < order.Quantity {
		log.Printf("Insufficient inventory for product %s. Required: %d, Available: %d", 
			order.Product, order.Quantity, l.inventory[order.Product])
		return transaction.StateRollback
	}

	// 扣减库存
	l.inventory[order.Product] -= order.Quantity
	order.Status = "confirmed"
	l.orders[order.ID] = order

	log.Printf("Single order processed successfully: %s, Product: %s, Quantity: %d", 
		order.ID, order.Product, order.Quantity)
	
	return transaction.StateCommit
}

func (l *OrderBatchTransactionListener) CheckLocalTransaction(transactionID transaction.TransactionID, messageID string) transaction.TransactionState {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// 简单的状态检查逻辑
	if record, exists := l.transactions[transactionID]; exists {
		if record.Status == "committed" {
			return transaction.StateCommit
		} else if record.Status == "rolled_back" {
			return transaction.StateRollback
		}
	}

	return transaction.StateUnknown
}

// 批量事务处理
func (l *OrderBatchTransactionListener) ExecuteBatchLocalTransaction(transactionID transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	l.mu.Lock()
	defer l.mu.Unlock()

	log.Printf("Processing batch transaction %s with %d orders", transactionID, len(messageIDs))

	// 基于messageID创建模拟订单
	orders := make([]*Order, 0, len(messageIDs))
	totalAmount := 0.0

	for i, messageID := range messageIDs {
		// 基于messageID创建模拟订单数据
		order := &Order{
			ID:       messageID,
			Product:  fmt.Sprintf("product_%d", i%3), // 循环使用3种产品
			Quantity: (i%5) + 1,                     // 数量1-5
			UserID:   fmt.Sprintf("user_%d", i%10),  // 循环使用10个用户
			Amount:   float64((i%100)+50) * 10.0,    // 金额500-1490
			Status:   "pending",
		}
		orders = append(orders, order)
		totalAmount += order.Amount
	}

	// 创建批量事务记录
	record := &BatchTransactionRecord{
		TransactionID: transactionID,
		Orders:        orders,
		TotalAmount:   totalAmount,
		ProcessedAt:   time.Now(),
		Status:        "processing",
	}

	// 批量检查库存
	inventorySnapshot := make(map[string]int)
	for product, stock := range l.inventory {
		inventorySnapshot[product] = stock
	}

	for _, order := range orders {
		if inventorySnapshot[order.Product] < order.Quantity {
			log.Printf("Batch transaction %s failed: insufficient inventory for product %s. Required: %d, Available: %d", 
				transactionID, order.Product, order.Quantity, inventorySnapshot[order.Product])
			record.Status = "rolled_back"
			l.transactions[transactionID] = record
			return transaction.StateRollback
		}
		inventorySnapshot[order.Product] -= order.Quantity
	}

	// 模拟业务逻辑检查（例如：用户信用检查、支付验证等）
	if totalAmount > 10000 {
		log.Printf("Batch transaction %s failed: total amount %.2f exceeds limit", transactionID, totalAmount)
		record.Status = "rolled_back"
		l.transactions[transactionID] = record
		return transaction.StateRollback
	}

	// 批量扣减库存并确认订单
	for _, order := range orders {
		l.inventory[order.Product] -= order.Quantity
		order.Status = "confirmed"
		l.orders[order.ID] = order
	}

	record.Status = "committed"
	l.transactions[transactionID] = record

	log.Printf("Batch transaction %s processed successfully: %d orders, total amount: %.2f", 
		transactionID, len(orders), totalAmount)
	
	return transaction.StateCommit
}

func (l *OrderBatchTransactionListener) CheckBatchLocalTransaction(transactionID transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if record, exists := l.transactions[transactionID]; exists {
		if record.Status == "committed" {
			return transaction.StateCommit
		} else if record.Status == "rolled_back" {
			return transaction.StateRollback
		}
	}

	return transaction.StateUnknown
}

// 辅助方法：从消息中解析订单
func (l *OrderBatchTransactionListener) parseOrderFromMessage(message transaction.HalfMessage) *Order {
	// 简化的解析逻辑，实际应用中可能使用JSON或其他序列化格式
	value := string(message.Value)
	
	// 假设消息格式为: "orderID:product:quantity:userID:amount"
	// 例如: "order-001:laptop:2:user-123:2999.99"
	parts := make([]string, 0)
	current := ""
	for _, char := range value {
		if char == ':' {
			parts = append(parts, current)
			current = ""
		} else {
			current += string(char)
		}
	}
	parts = append(parts, current)

	if len(parts) != 5 {
		return nil
	}

	quantity := 0
	amount := 0.0
	
	// 简单的数字解析
	for _, char := range parts[2] {
		if char >= '0' && char <= '9' {
			quantity = quantity*10 + int(char-'0')
		}
	}
	
	// 简单的浮点数解析
	dotFound := false
	decimal := 0.1
	for _, char := range parts[4] {
		if char == '.' {
			dotFound = true
		} else if char >= '0' && char <= '9' {
			if dotFound {
				amount += float64(char-'0') * decimal
				decimal *= 0.1
			} else {
				amount = amount*10 + float64(char-'0')
			}
		}
	}

	return &Order{
		ID:       parts[0],
		Product:  parts[1],
		Quantity: quantity,
		UserID:   parts[3],
		Amount:   amount,
		Status:   "pending",
	}
}

// 获取库存信息
func (l *OrderBatchTransactionListener) GetInventory() map[string]int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	inventory := make(map[string]int)
	for product, stock := range l.inventory {
		inventory[product] = stock
	}
	return inventory
}

// 获取订单信息
func (l *OrderBatchTransactionListener) GetOrders() map[string]*Order {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	orders := make(map[string]*Order)
	for id, order := range l.orders {
		orders[id] = order
	}
	return orders
}

// 获取批量事务记录
func (l *OrderBatchTransactionListener) GetBatchTransactions() map[transaction.TransactionID]*BatchTransactionRecord {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	transactions := make(map[transaction.TransactionID]*BatchTransactionRecord)
	for id, record := range l.transactions {
		transactions[id] = record
	}
	return transactions
}

func main() {
	fmt.Println("=== 批量事务示例演示 ===")
	fmt.Println("模拟电商场景中的批量订单处理")
	fmt.Println()

	// 创建虚拟客户端（在实际应用中，这里应该是真实的客户端连接）
	c := &client.Client{}
	
	// 创建批量事务监听器
	listener := NewOrderBatchTransactionListener()
	
	// 创建事务生产者
	producer := client.NewTransactionProducer(c, listener, "order-batch-producer")
	
	fmt.Println("初始库存状态:")
	for product, stock := range listener.GetInventory() {
		fmt.Printf("  %s: %d\n", product, stock)
	}
	fmt.Println()

	// 示例1: 成功的批量事务
	fmt.Println("=== 示例1: 成功的批量订单处理 ===")
	successMessages := []*client.TransactionMessage{
		{
			Topic:     "orders",
			Partition: 0,
			Key:       []byte("batch-1"),
			Value:     []byte("order-001:laptop:2:user-123:5999.98"),
			Headers:   map[string]string{"type": "order", "batch": "success-demo"},
			Timeout:   10 * time.Second,
		},
		{
			Topic:     "orders",
			Partition: 0,
			Key:       []byte("batch-1"),
			Value:     []byte("order-002:smartphone:3:user-456:2999.97"),
			Headers:   map[string]string{"type": "order", "batch": "success-demo"},
			Timeout:   10 * time.Second,
		},
		{
			Topic:     "orders",
			Partition: 1,
			Key:       []byte("batch-1"),
			Value:     []byte("order-003:headphones:5:user-789:999.95"),
			Headers:   map[string]string{"type": "order", "batch": "success-demo"},
			Timeout:   10 * time.Second,
		},
	}

	txn1, result1, err1 := producer.SendBatchHalfMessagesAndDoLocal(successMessages)
	if err1 != nil {
		log.Printf("批量事务执行失败: %v", err1)
	} else if result1.Error != nil {
		log.Printf("批量事务业务逻辑失败: %v", result1.Error)
	} else {
		fmt.Printf("✅ 批量事务成功! 事务ID: %s\n", result1.TransactionID)
		fmt.Printf("   处理了 %d 个订单\n", len(successMessages))
	}
	
	fmt.Println("\n处理后库存状态:")
	for product, stock := range listener.GetInventory() {
		fmt.Printf("  %s: %d\n", product, stock)
	}
	fmt.Println()

	// 示例2: 库存不足导致的批量事务回滚
	fmt.Println("=== 示例2: 库存不足导致的批量事务回滚 ===")
	failMessages := []*client.TransactionMessage{
		{
			Topic:     "orders",
			Partition: 0,
			Key:       []byte("batch-2"),
			Value:     []byte("order-004:tablet:25:user-111:7499.75"), // 库存不足
			Headers:   map[string]string{"type": "order", "batch": "fail-demo"},
			Timeout:   10 * time.Second,
		},
		{
			Topic:     "orders",
			Partition: 0,
			Key:       []byte("batch-2"),
			Value:     []byte("order-005:laptop:10:user-222:29999.90"), // 库存不足
			Headers:   map[string]string{"type": "order", "batch": "fail-demo"},
			Timeout:   10 * time.Second,
		},
	}

	txn2, result2, err2 := producer.SendBatchHalfMessagesAndDoLocal(failMessages)
	if err2 != nil {
		log.Printf("批量事务执行失败: %v", err2)
	} else if result2.Error != nil {
		fmt.Printf("❌ 批量事务回滚! 原因: %v\n", result2.Error)
		fmt.Printf("   事务ID: %s\n", result2.TransactionID)
	} else {
		fmt.Printf("✅ 批量事务成功! 事务ID: %s\n", result2.TransactionID)
	}

	fmt.Println("\n回滚后库存状态（应该保持不变）:")
	for product, stock := range listener.GetInventory() {
		fmt.Printf("  %s: %d\n", product, stock)
	}
	fmt.Println()

	// 示例3: 金额超限导致的批量事务回滚
	fmt.Println("=== 示例3: 金额超限导致的批量事务回滚 ===")
	limitMessages := []*client.TransactionMessage{
		{
			Topic:     "orders",
			Partition: 0,
			Key:       []byte("batch-3"),
			Value:     []byte("order-006:laptop:5:user-333:15000.00"), // 总金额会超过限制
			Headers:   map[string]string{"type": "order", "batch": "limit-demo"},
			Timeout:   10 * time.Second,
		},
	}

	txn3, result3, err3 := producer.SendBatchHalfMessagesAndDoLocal(limitMessages)
	if err3 != nil {
		log.Printf("批量事务执行失败: %v", err3)
	} else if result3.Error != nil {
		fmt.Printf("❌ 批量事务回滚! 原因: %v\n", result3.Error)
		fmt.Printf("   事务ID: %s\n", result3.TransactionID)
	} else {
		fmt.Printf("✅ 批量事务成功! 事务ID: %s\n", result3.TransactionID)
	}
	fmt.Println()

	// 示例4: 对比单个事务与批量事务的性能
	fmt.Println("=== 示例4: 性能对比演示 ===")
	
	// 准备测试数据
	testMessages := []*client.TransactionMessage{
		{
			Topic:     "orders",
			Partition: 0,
			Key:       []byte("perf-test"),
			Value:     []byte("order-101:smartphone:1:user-perf1:999.99"),
			Headers:   map[string]string{"type": "order", "test": "performance"},
			Timeout:   5 * time.Second,
		},
		{
			Topic:     "orders",
			Partition: 0,
			Key:       []byte("perf-test"),
			Value:     []byte("order-102:headphones:2:user-perf2:199.98"),
			Headers:   map[string]string{"type": "order", "test": "performance"},
			Timeout:   5 * time.Second,
		},
		{
			Topic:     "orders",
			Partition: 1,
			Key:       []byte("perf-test"),
			Value:     []byte("order-103:tablet:1:user-perf3:799.99"),
			Headers:   map[string]string{"type": "order", "test": "performance"},
			Timeout:   5 * time.Second,
		},
	}

	// 测试单个事务
	fmt.Println("执行单个事务...")
	start := time.Now()
	for i, msg := range testMessages {
		_, _, err := producer.SendHalfMessageAndDoLocal(msg)
		if err != nil {
			log.Printf("单个事务 %d 失败: %v", i, err)
		}
	}
	singleDuration := time.Since(start)

	// 重置库存用于批量事务测试
	listener = NewOrderBatchTransactionListener()
	producer = client.NewTransactionProducer(c, listener, "perf-batch-producer")

	// 测试批量事务
	fmt.Println("执行批量事务...")
	start = time.Now()
	_, _, err := producer.SendBatchHalfMessagesAndDoLocal(testMessages)
	if err != nil {
		log.Printf("批量事务失败: %v", err)
	}
	batchDuration := time.Since(start)

	fmt.Printf("\n性能对比结果:\n")
	fmt.Printf("  单个事务总耗时: %v\n", singleDuration)
	fmt.Printf("  批量事务总耗时: %v\n", batchDuration)
	if singleDuration > batchDuration {
		fmt.Printf("  批量事务性能提升: %.2fx\n", float64(singleDuration)/float64(batchDuration))
	} else {
		fmt.Printf("  批量事务性能比率: %.2fx\n", float64(batchDuration)/float64(singleDuration))
	}
	fmt.Println()

	// 显示最终状态
	fmt.Println("=== 最终状态总结 ===")
	fmt.Println("库存状态:")
	for product, stock := range listener.GetInventory() {
		fmt.Printf("  %s: %d\n", product, stock)
	}
	
	fmt.Println("\n已确认订单:")
	orders := listener.GetOrders()
	for orderID, order := range orders {
		fmt.Printf("  %s: %s x%d (用户: %s, 金额: %.2f, 状态: %s)\n", 
			orderID, order.Product, order.Quantity, order.UserID, order.Amount, order.Status)
	}
	
	fmt.Println("\n批量事务记录:")
	batchTxns := listener.GetBatchTransactions()
	for txnID, record := range batchTxns {
		fmt.Printf("  事务ID: %s\n", txnID)
		fmt.Printf("    订单数量: %d\n", len(record.Orders))
		fmt.Printf("    总金额: %.2f\n", record.TotalAmount)
		fmt.Printf("    状态: %s\n", record.Status)
		fmt.Printf("    处理时间: %s\n", record.ProcessedAt.Format("2006-01-02 15:04:05"))
	}

	// 避免编译器警告
	_ = txn1
	_ = txn2
	_ = txn3

	fmt.Println("\n=== 演示完成 ===")
	fmt.Println("批量事务功能可以显著提高处理多个相关消息的效率，")
	fmt.Println("同时保证所有消息的原子性处理。")
}