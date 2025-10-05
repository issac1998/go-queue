package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/transaction"
)

// OrderService 模拟订单服务
type OrderService struct {
	orders map[string]*Order
}

// Order 订单信息
type Order struct {
	ID     string
	Amount float64
	Status string // pending, paid, cancelled
}

// TransactionListener 实现事务监听器
type MyTransactionListener struct {
	orderService *OrderService
}

// ExecuteLocalTransaction 执行本地事务
func (l *MyTransactionListener) ExecuteLocalTransaction(transactionID transaction.TransactionID, messageID string) transaction.TransactionState {
	log.Printf("💰 执行本地事务 - 消息ID: %s, 事务ID: %s", messageID, transactionID)

	// 模拟订单处理
	order, exists := l.orderService.orders[messageID]
	if !exists {
		log.Printf("❌ 订单不存在: %s", messageID)
		return transaction.StateRollback
	}

	// 模拟支付处理（随机成功/失败）
	if rand.Float32() < 0.8 { // 80% 成功率
		// 支付成功
		order.Status = "paid"
		log.Printf("✅ 订单支付成功: %s, 金额: %.2f", messageID, order.Amount)
		return transaction.StateCommit
	} else {
		// 支付失败
		order.Status = "cancelled"
		log.Printf("❌ 订单支付失败: %s", messageID)
		return transaction.StateRollback
	}
}

// CheckLocalTransaction 检查本地事务状态（用于回查）
func (l *MyTransactionListener) CheckLocalTransaction(transactionID transaction.TransactionID, messageID string) transaction.TransactionState {
	log.Printf("🔍 检查本地事务状态 - 消息ID: %s, 事务ID: %s", messageID, transactionID)

	order, exists := l.orderService.orders[messageID]
	if !exists {
		log.Printf("❓ 订单不存在，回滚事务: %s", messageID)
		return transaction.StateRollback
	}

	switch order.Status {
	case "paid":
		log.Printf("✅ 订单已支付，提交事务: %s", messageID)
		return transaction.StateCommit
	case "cancelled":
		log.Printf("❌ 订单已取消，回滚事务: %s", messageID)
		return transaction.StateRollback
	default:
		log.Printf("❓ 订单状态未知，保持等待: %s", messageID)
		return transaction.StateUnknown
	}
}

// ExecuteBatchLocalTransaction 执行批量本地事务
func (l *MyTransactionListener) ExecuteBatchLocalTransaction(transactionID transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	log.Printf("💰 执行批量本地事务 - 事务ID: %s, 消息数量: %d", transactionID, len(messageIDs))

	// 检查所有订单是否存在
	for _, messageID := range messageIDs {
		if _, exists := l.orderService.orders[messageID]; !exists {
			log.Printf("❌ 批量事务中订单不存在: %s", messageID)
			return transaction.StateRollback
		}
	}

	// 模拟批量支付处理（随机成功/失败）
	if rand.Float32() < 0.8 { // 80% 成功率
		// 批量支付成功
		for _, messageID := range messageIDs {
			order := l.orderService.orders[messageID]
			order.Status = "paid"
			log.Printf("✅ 批量订单支付成功: %s, 金额: %.2f", messageID, order.Amount)
		}
		return transaction.StateCommit
	} else {
		// 批量支付失败
		for _, messageID := range messageIDs {
			order := l.orderService.orders[messageID]
			order.Status = "cancelled"
			log.Printf("❌ 批量订单支付失败: %s", messageID)
		}
		return transaction.StateRollback
	}
}

// CheckBatchLocalTransaction 检查批量本地事务状态（用于回查）
func (l *MyTransactionListener) CheckBatchLocalTransaction(transactionID transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	log.Printf("🔍 检查批量本地事务状态 - 事务ID: %s, 消息数量: %d", transactionID, len(messageIDs))

	var commitCount, rollbackCount, unknownCount int

	for _, messageID := range messageIDs {
		order, exists := l.orderService.orders[messageID]
		if !exists {
			log.Printf("❓ 批量事务中订单不存在，回滚: %s", messageID)
			rollbackCount++
			continue
		}

		switch order.Status {
		case "paid":
			log.Printf("✅ 批量事务中订单已支付: %s", messageID)
			commitCount++
		case "cancelled":
			log.Printf("❌ 批量事务中订单已取消: %s", messageID)
			rollbackCount++
		default:
			log.Printf("❓ 批量事务中订单状态未知: %s", messageID)
			unknownCount++
		}
	}

	// 批量事务的决策逻辑：只有所有订单都成功才提交，否则回滚
	if rollbackCount > 0 {
		log.Printf("❌ 批量事务回滚 - 有 %d 个订单失败", rollbackCount)
		return transaction.StateRollback
	} else if unknownCount > 0 {
		log.Printf("❓ 批量事务状态未知 - 有 %d 个订单状态未知", unknownCount)
		return transaction.StateUnknown
	} else {
		log.Printf("✅ 批量事务提交 - 所有 %d 个订单都成功", commitCount)
		return transaction.StateCommit
	}
}

func main() {
	fmt.Println("🏪 RocketMQ 风格事务消息演示")
	fmt.Println("================================")
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
	}

	for _, order := range orders {
		orderService.orders[order.ID] = order
		fmt.Printf("📝 创建订单: %s, 金额: %.2f\n", order.ID, order.Amount)
	}
	fmt.Println()

	// 创建客户端
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     10 * time.Second,
	}

	clientInstance := client.NewClient(clientConfig)

	// 创建事务监听器
	listener := &MyTransactionListener{
		orderService: orderService,
	}

	// 创建事务生产者
	txnProducer := client.NewTransactionProducer(clientInstance, listener, "default-txn-group")

	fmt.Println("💳 开始处理订单支付...")
	fmt.Println()

	// 处理每个订单
	for _, order := range orders {
		fmt.Printf("🔄 处理订单: %s\n", order.ID)

		// 构造事务消息
		txnMessage := &client.TransactionMessage{
			Topic:     "payment-topic",
			Partition: 0,
			Key:       []byte(order.ID),
			Value:     []byte(fmt.Sprintf(`{"order_id":"%s","amount":%.2f,"status":"processing"}`, order.ID, order.Amount)),
			Headers: map[string]string{
				"order_id": order.ID,
				"type":     "payment",
			},
			Timeout: 30 * time.Second,
		}

		txn, result, err := txnProducer.SendHalfMessageAndDoLocal(txnMessage)
		if err != nil {
			log.Printf("❌ 发送事务消息失败: %v", err)
			continue
		}

		if result.Error != nil {
			log.Printf("⚠️ 事务执行结果: %v", result.Error)
		} else {
			log.Printf("✅ 事务提交成功: %s, offset: %d", result.TransactionID, result.Offset)
		}

		fmt.Printf("   事务ID: %s\n", txn.ID)
		fmt.Printf("   订单状态: %s\n", orderService.orders[order.ID].Status)
		fmt.Println()

		// 间隔一秒处理下一个订单
		time.Sleep(1 * time.Second)
	}

	fmt.Println("📊 最终订单状态:")
	for _, order := range orders {
		status := orderService.orders[order.ID].Status
		statusIcon := ""
		switch status {
		case "paid":
			statusIcon = "✅"
		case "cancelled":
			statusIcon = "❌"
		default:
			statusIcon = "⏳"
		}
		fmt.Printf("   %s %s: %.2f - %s\n", statusIcon, order.ID, order.Amount, status)
	}
	fmt.Println()

	fmt.Println("🎯 事务消息特性展示:")
	fmt.Println("   ✅ 半消息机制: 先发送半消息，再执行本地事务")
	fmt.Println("   ✅ 本地事务: 根据本地业务逻辑决定提交或回滚")
	fmt.Println("   ✅ 事务状态: Commit/Rollback/Unknown")
	fmt.Println("   ✅ 事务回查: 超时或状态未知时的补偿机制")
	fmt.Println("   ✅ 最终一致性: 保证消息发送与本地事务的一致性")
	fmt.Println()

	fmt.Println("💡 使用场景:")
	fmt.Println("   • 订单支付: 支付成功后发送物流消息")
	fmt.Println("   • 账户转账: 扣款成功后发送到账通知")
	fmt.Println("   • 库存扣减: 下单成功后发送库存更新消息")
	fmt.Println("   • 积分奖励: 购买成功后发送积分增加消息")
}
