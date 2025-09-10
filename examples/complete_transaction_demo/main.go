package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/transaction"
)

// OrderService 模拟订单服务
type OrderService struct {
	orders map[string]*Order
	mu     sync.RWMutex
}

// Order 订单信息
type Order struct {
	ID     string
	Amount float64
	Status string // pending, processing, paid, cancelled, unknown
	TxnID  string // 关联的事务ID
}

// CompleteTransactionListener 完整的事务监听器实现
type CompleteTransactionListener struct {
	orderService *OrderService
}

// ExecuteLocalTransaction 执行本地事务
func (l *CompleteTransactionListener) ExecuteLocalTransaction(transactionID transaction.TransactionID, message transaction.HalfMessage) transaction.TransactionState {
	orderID := string(message.Key)
	log.Printf("💰 执行本地事务 - 订单ID: %s, 事务ID: %s", orderID, transactionID)

	l.orderService.mu.Lock()
	defer l.orderService.mu.Unlock()

	// 查找订单
	order, exists := l.orderService.orders[orderID]
	if !exists {
		log.Printf("❌ 订单不存在: %s", orderID)
		return transaction.StateRollback
	}

	// 关联事务ID
	order.TxnID = string(transactionID)

	// 模拟支付处理逻辑
	random := rand.Float32()
	if random < 0.5 { // 50% 成功
		order.Status = "paid"
		log.Printf("✅ 订单支付成功: %s, 金额: %.2f", orderID, order.Amount)
		return transaction.StateCommit
	} else if random < 0.8 { // 30% 失败
		order.Status = "cancelled"
		log.Printf("❌ 订单支付失败: %s", orderID)
		return transaction.StateRollback
	} else { // 20% 状态未知（模拟网络超时等情况）
		order.Status = "processing"
		log.Printf("❓ 订单支付状态未知: %s（将等待回查）", orderID)

		// 异步模拟支付结果
		go func() {
			time.Sleep(5 * time.Second) // 模拟延迟确认
			l.orderService.mu.Lock()
			defer l.orderService.mu.Unlock()

			if rand.Float32() < 0.7 { // 70% 最终成功
				order.Status = "paid"
				log.Printf("🔄 延迟确认 - 订单支付成功: %s", orderID)
			} else {
				order.Status = "cancelled"
				log.Printf("🔄 延迟确认 - 订单支付失败: %s", orderID)
			}
		}()

		return transaction.StateUnknown
	}
}

// CheckLocalTransaction 检查本地事务状态（回查）
func (l *CompleteTransactionListener) CheckLocalTransaction(transactionID transaction.TransactionID, message transaction.HalfMessage) transaction.TransactionState {
	orderID := string(message.Key)
	log.Printf("🔍 事务状态回查 - 订单ID: %s, 事务ID: %s", orderID, transactionID)

	l.orderService.mu.RLock()
	defer l.orderService.mu.RUnlock()

	order, exists := l.orderService.orders[orderID]
	if !exists {
		log.Printf("❓ 回查时订单不存在，回滚事务: %s", orderID)
		return transaction.StateRollback
	}

	// 验证事务ID匹配
	if order.TxnID != string(transactionID) {
		log.Printf("❓ 事务ID不匹配，回滚事务: %s (expected: %s, got: %s)", orderID, order.TxnID, transactionID)
		return transaction.StateRollback
	}

	switch order.Status {
	case "paid":
		log.Printf("✅ 回查结果 - 订单已支付，提交事务: %s", orderID)
		return transaction.StateCommit
	case "cancelled":
		log.Printf("❌ 回查结果 - 订单已取消，回滚事务: %s", orderID)
		return transaction.StateRollback
	case "processing":
		log.Printf("⏳ 回查结果 - 订单仍在处理中，保持未知状态: %s", orderID)
		return transaction.StateUnknown
	default:
		log.Printf("❓ 回查结果 - 订单状态未知，回滚事务: %s (status: %s)", orderID, order.Status)
		return transaction.StateRollback
	}
}

func main() {
	fmt.Println("🏪 完整事务消息演示（含回查机制）")
	fmt.Println("==========================================")
	fmt.Println()

	// 创建订单服务
	orderService := &OrderService{
		orders: make(map[string]*Order),
	}

	// 创建一些测试订单
	orders := []*Order{
		{ID: "order-001", Amount: 99.99, Status: "pending"},
		{ID: "order-002", Amount: 199.50, Status: "pending"},
		{ID: "order-003", Amount: 299.00, Status: "pending"},
		{ID: "order-004", Amount: 399.99, Status: "pending"},
		{ID: "order-005", Amount: 499.50, Status: "pending"},
	}

	for _, order := range orders {
		orderService.orders[order.ID] = order
		fmt.Printf("📝 创建订单: %s, 金额: %.2f\n", order.ID, order.Amount)
	}
	fmt.Println()

	// 创建支持事务检查的客户端
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     10 * time.Second,
	}

	producerGroup := "payment-producer-group"
	txnClient, err := client.NewTransactionAwareClient(clientConfig, producerGroup)
	if err != nil {
		log.Fatalf("❌ 创建事务客户端失败: %v", err)
	}

	// 启动事务检查监听器
	checkPort := 8081
	if err := txnClient.StartTransactionCheckListener(checkPort); err != nil {
		log.Fatalf("❌ 启动事务检查监听器失败: %v", err)
	}
	defer txnClient.StopTransactionCheckListener()

	fmt.Printf("🎧 事务检查监听器已启动，端口: %d\n", checkPort)

	// 创建事务监听器
	listener := &CompleteTransactionListener{
		orderService: orderService,
	}

	// 注册事务监听器
	txnClient.RegisterTransactionListener(listener)

	// 创建事务生产者
	txnProducer := client.NewTransactionProducer(txnClient.Client, listener)

	fmt.Println("💳 开始处理订单支付...")
	fmt.Println()

	var wg sync.WaitGroup
	results := make(chan struct{}, len(orders))

	// 并发处理订单
	for _, order := range orders {
		wg.Add(1)
		go func(o *Order) {
			defer wg.Done()
			defer func() { results <- struct{}{} }()

			fmt.Printf("🔄 处理订单: %s\n", o.ID)

			// 构造事务消息
			txnMessage := &client.TransactionMessage{
				Topic:     "payment-topic",
				Partition: 0,
				Key:       []byte(o.ID),
				Value:     []byte(fmt.Sprintf(`{"order_id":"%s","amount":%.2f,"status":"processing"}`, o.ID, o.Amount)),
				Headers: map[string]string{
					"order_id":       o.ID,
					"type":           "payment",
					"producer_group": producerGroup,
				},
				Timeout: 30 * time.Second,
			}

			// 发送事务消息
			txn, result, err := txnProducer.SendTransactionMessageAndDoLocal(txnMessage)
			if err != nil {
				log.Printf("❌ 发送事务消息失败: %v", err)
				return
			}

			if result.Error != nil {
				log.Printf("⚠️ 事务执行结果: %v", result.Error)
			} else {
				log.Printf("✅ 事务处理完成: %s, offset: %d", result.TransactionID, result.Offset)
			}

			fmt.Printf("   事务ID: %s\n", txn.ID)
			fmt.Printf("   订单状态: %s\n", orderService.orders[o.ID].Status)
			fmt.Println()
		}(order)

		// 间隔一下避免过于密集
		time.Sleep(500 * time.Millisecond)
	}

	// 等待所有订单处理完成
	go func() {
		wg.Wait()
		close(results)
	}()

	// 收集结果
	processed := 0
	for range results {
		processed++
		fmt.Printf("📊 已处理 %d/%d 个订单\n", processed, len(orders))
	}

	fmt.Println()
	fmt.Println("⏰ 等待可能的事务状态回查...")
	time.Sleep(10 * time.Second) // 等待回查完成

	fmt.Println()
	fmt.Println("📊 最终订单状态:")
	for _, order := range orders {
		orderService.mu.RLock()
		currentOrder := orderService.orders[order.ID]
		status := currentOrder.Status
		txnID := currentOrder.TxnID
		orderService.mu.RUnlock()

		statusIcon := ""
		switch status {
		case "paid":
			statusIcon = "✅"
		case "cancelled":
			statusIcon = "❌"
		case "processing":
			statusIcon = "⏳"
		default:
			statusIcon = "❓"
		}
		fmt.Printf("   %s %s: %.2f - %s (TxnID: %s)\n", statusIcon, order.ID, order.Amount, status, txnID)
	}
	fmt.Println()

	fmt.Println("🎯 完整事务消息特性演示:")
	fmt.Println("   ✅ 半消息机制: 消息先发送但对消费者不可见")
	fmt.Println("   ✅ 本地事务: 根据业务逻辑返回 Commit/Rollback/Unknown")
	fmt.Println("   ✅ 事务回查: 服务端主动向客户端查询事务状态")
	fmt.Println("   ✅ 超时重试: 多次回查with指数退避策略")
	fmt.Println("   ✅ 最终一致性: 保证消息发送与本地事务的一致性")
	fmt.Println("   ✅ 故障恢复: 网络中断、进程重启等异常场景的处理")
	fmt.Println()

	fmt.Println("💡 回查机制说明:")
	fmt.Println("   • 当本地事务返回 Unknown 时，服务端会发起回查")
	fmt.Println("   • 回查请求发送到客户端的监听端口")
	fmt.Println("   • 客户端根据业务状态返回最终的事务决定")
	fmt.Println("   • 支持多次重试，避免网络抖动导致的失败")
	fmt.Println("   • 超过最大重试次数后自动回滚，防止资源占用")
}
