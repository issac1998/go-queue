# 🔄 完整事务消息架构与回查机制

## 📖 架构概览

本文档详细描述了完整的 RocketMQ 风格事务消息系统，包括完整的事务状态回查机制。

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           完整事务消息架构                                      │
└─────────────────────────────────────────────────────────────────────────────┘

    客户端（生产者）                 服务端（Broker）                 消费者端
┌─────────────────────┐         ┌──────────────────────┐        ┌─────────────┐
│                     │         │                      │        │             │
│ TransactionProducer │◄────────┤ TransactionManager   │        │  Consumer   │
│                     │         │                      │        │             │
│ ┌─────────────────┐ │         │ ┌──────────────────┐ │        │             │
│ │ Local Business  │ │         │ │  Half Messages   │ │        │             │
│ │ Transaction     │ │         │ │  Storage         │ │        │             │
│ └─────────────────┘ │         │ └──────────────────┘ │        │             │
│                     │         │                      │        │             │
│ ┌─────────────────┐ │         │ ┌──────────────────┐ │        │             │
│ │ Check Listener  │◄┼─────────┤ │ Transaction      │ │        │             │
│ │ (Port: 8081)    │ │         │ │ Checker          │ │        │             │
│ └─────────────────┘ │         │ └──────────────────┘ │        │             │
│                     │         │                      │        │             │
└─────────────────────┘         └──────────────────────┘        └─────────────┘
```

## 🔧 核心组件详解

### 1. 服务端组件

#### TransactionManager（事务管理器）
```go
type TransactionManager struct {
    halfMessages     map[TransactionID]*HalfMessage  // 半消息存储
    defaultTimeout   time.Duration                   // 默认超时时间
    maxCheckCount    int                             // 最大回查次数
    checkInterval    time.Duration                   // 回查间隔
    checkCallback    func(TransactionID, *HalfMessage) TransactionState
}
```

**主要职责**：
- 管理半消息的完整生命周期
- 超时检查和自动清理
- 触发事务状态回查
- 处理回查结果并执行最终操作

#### TransactionChecker（事务检查器）
```go
type TransactionChecker struct {
    broker               *Broker
    connectionManager    *ProducerConnectionManager  // 生产者连接管理
    checkTimeout         time.Duration               // 单次检查超时
    maxCheckAttempts     int                         // 最大检查尝试次数
    activeChecks         map[TransactionID]*CheckContext  // 活跃检查
}
```

**主要职责**：
- 管理与生产者的连接
- 发送事务状态检查请求
- 处理检查响应和重试逻辑
- 实现指数退避策略

#### ProducerConnectionManager（生产者连接管理器）
```go
type ProducerConnectionManager struct {
    connections map[string][]net.Conn  // producerGroup -> connections
    mu          sync.RWMutex
}
```

**主要职责**：
- 管理不同生产者组的连接
- 支持连接的注册和注销
- 提供负载均衡的连接选择

### 2. 客户端组件

#### TransactionAwareClient（事务感知客户端）
```go
type TransactionAwareClient struct {
    *Client
    checkHandler    *TransactionCheckHandler
    checkListener   net.Listener              // 监听回查请求
    producerGroup   string
    isListening     bool
}
```

**主要职责**：
- 监听服务端的回查请求
- 管理事务监听器
- 处理并发的检查请求

#### TransactionCheckHandler（事务检查处理器）
```go
type TransactionCheckHandler struct {
    listeners map[string]TransactionListener  // producerGroup -> listener
    mu        sync.RWMutex
}
```

**主要职责**：
- 处理服务端发来的检查请求
- 调用业务逻辑进行状态检查
- 返回检查结果给服务端

## 📋 完整工作流程

### 阶段一：事务消息发送

```
客户端                    服务端                    事务管理器
   │                        │                         │
   │──① PrepareRequest────→ │                         │
   │                        │──② Store HalfMessage──→ │
   │ ←─③ PrepareResponse─── │                         │
   │                        │                         │
   │──④ Execute Local Txn   │                         │
   │                        │                         │
   │──⑤ Commit/Rollback───→ │                         │
   │                        │──⑥ Process Result─────→ │
   │ ←─⑦ Final Response──── │                         │
```

### 阶段二：事务状态回查（关键创新）

```
事务管理器           事务检查器           生产者连接管理器        客户端检查监听器
    │                    │                      │                      │
    │──① Timeout Check── │                      │                      │
    │                    │──② Get Connection──→ │                      │
    │                    │ ←─③ Return Conn───── │                      │
    │                    │──④ Send Check Request─────────────────────→ │
    │                    │                      │                      │
    │                    │                      │  ┌─────────────────┐ │
    │                    │                      │  │ Business Logic  │ │
    │                    │                      │  │ State Check     │ │
    │                    │                      │  └─────────────────┘ │
    │                    │                      │                      │
    │                    │ ←─⑤ Check Response───────────────────────── │
    │ ←─⑥ Process Result │                      │                      │
    │──⑦ Final Action──→ │                      │                      │
```

### 阶段三：故障处理和重试

```
事务检查器                     客户端
     │                          │
     │──① First Attempt────────→ │
     │ ←─② Network Error────────│ │
     │                          │
     │──③ Wait (2s)             │
     │                          │
     │──④ Second Attempt───────→ │
     │ ←─⑤ Timeout──────────────│ │
     │                          │
     │──⑥ Wait (4s)             │
     │                          │
     │──⑦ Third Attempt────────→ │
     │ ←─⑧ Success──────────────│ │
     │                          │
     │──⑨ Process Result        │
```

## 🛠️ 详细实现机制

### 1. 半消息存储和管理

```go
// 半消息存储结构
type HalfMessage struct {
    TransactionID TransactionID     `json:"transaction_id"`
    Topic         string            `json:"topic"`
    Partition     int32             `json:"partition"`
    Key           []byte            `json:"key,omitempty"`
    Value         []byte            `json:"value"`
    Headers       map[string]string `json:"headers,omitempty"`
    CreatedAt     time.Time         `json:"created_at"`
    Timeout       time.Duration     `json:"timeout"`
    CheckCount    int               `json:"check_count"`
    LastCheck     time.Time         `json:"last_check"`
    State         TransactionState  `json:"state"`
}
```

### 2. 超时检查策略

```go
func (tm *TransactionManager) checkTimeoutTransactions() {
    for transactionID, halfMessage := range tm.halfMessages {
        if time.Since(halfMessage.CreatedAt) > halfMessage.Timeout {
            if halfMessage.CheckCount >= tm.maxCheckCount {
                // 超过最大回查次数，自动回滚
                tm.autoRollbackTransaction(transactionID)
            } else {
                // 执行状态回查
                tm.initiateTransactionCheck(transactionID, halfMessage)
            }
        }
    }
}
```

### 3. 指数退避重试策略

```go
func (tc *TransactionChecker) calculateRetryInterval(attempts int) time.Duration {
    baseInterval := 2 * time.Second
    maxInterval := 30 * time.Second
    
    interval := baseInterval * time.Duration(1<<uint(attempts))
    if interval > maxInterval {
        interval = maxInterval
    }
    return interval
}
```

### 4. 连接管理和负载均衡

```go
func (pcm *ProducerConnectionManager) selectConnection(producerGroup string) (net.Conn, error) {
    connections := pcm.connections[producerGroup]
    if len(connections) == 0 {
        return nil, ErrNoAvailableConnection
    }
    
    // 简单轮询（可扩展为更复杂的负载均衡策略）
    selectedConn := connections[pcm.roundRobinIndex[producerGroup]]
    pcm.roundRobinIndex[producerGroup] = (pcm.roundRobinIndex[producerGroup] + 1) % len(connections)
    
    return selectedConn, nil
}
```

## 🚀 高级特性

### 1. 并发检查控制

```go
type CheckContext struct {
    TransactionID transaction.TransactionID
    HalfMessage   *transaction.HalfMessage
    ProducerGroup string
    Attempts      int
    LastAttempt   time.Time
    ResultChan    chan transaction.TransactionState
    mutex         sync.Mutex  // 防止重复检查
}
```

### 2. 检查结果缓存

```go
type CheckResultCache struct {
    cache map[TransactionID]*CachedResult
    ttl   time.Duration
    mu    sync.RWMutex
}

type CachedResult struct {
    State     TransactionState
    Timestamp time.Time
}
```

### 3. 监控和指标

```go
type TransactionMetrics struct {
    TotalTransactions    int64  // 总事务数
    CommittedTransactions int64  // 已提交事务数
    RolledBackTransactions int64  // 已回滚事务数
    CheckRequestsSent    int64  // 发送的检查请求数
    CheckRequestsFailed  int64  // 失败的检查请求数
    AverageCheckLatency  time.Duration  // 平均检查延迟
}
```

## 🔒 安全和可靠性

### 1. 事务ID验证

```go
func (l *BusinessTransactionListener) CheckLocalTransaction(
    transactionID transaction.TransactionID, 
    message transaction.HalfMessage) transaction.TransactionState {
    
    // 验证事务ID是否属于当前业务
    if !l.isValidTransactionID(transactionID) {
        return transaction.StateRollback
    }
    
    // 验证消息完整性
    if !l.validateMessageIntegrity(message) {
        return transaction.StateRollback
    }
    
    // 执行业务逻辑检查
    return l.performBusinessCheck(transactionID, message)
}
```

### 2. 网络异常处理

```go
func (tc *TransactionChecker) handleNetworkError(err error, ctx *CheckContext) bool {
    if netErr, ok := err.(net.Error); ok {
        if netErr.Timeout() {
            log.Printf("Network timeout for transaction %s, will retry", ctx.TransactionID)
            return true  // 可以重试
        }
        if netErr.Temporary() {
            log.Printf("Temporary network error for transaction %s, will retry", ctx.TransactionID)
            return true  // 可以重试
        }
    }
    
    log.Printf("Permanent network error for transaction %s: %v", ctx.TransactionID, err)
    return false  // 不可重试
}
```

### 3. 状态一致性保证

```go
func (tm *TransactionManager) processCheckResult(
    transactionID TransactionID, 
    state TransactionState) error {
    
    tm.mu.Lock()
    defer tm.mu.Unlock()
    
    halfMessage, exists := tm.halfMessages[transactionID]
    if !exists {
        return ErrTransactionNotFound
    }
    
    // 原子性地更新状态
    switch state {
    case StateCommit:
        if err := tm.commitMessageToQueue(halfMessage); err != nil {
            return err
        }
        delete(tm.halfMessages, transactionID)
        
    case StateRollback:
        delete(tm.halfMessages, transactionID)
        
    case StateUnknown:
        // 重置检查计数，等待下次检查
        halfMessage.State = StatePrepared
        halfMessage.LastCheck = time.Time{}
    }
    
    return nil
}
```

## 📊 性能优化

### 1. 批量检查优化

```go
func (tc *TransactionChecker) batchCheckTransactions(
    transactions []TransactionID) map[TransactionID]TransactionState {
    
    results := make(map[TransactionID]TransactionState)
    
    // 按生产者组分组
    groupedTxns := tc.groupByProducerGroup(transactions)
    
    // 并发检查不同的生产者组
    var wg sync.WaitGroup
    for producerGroup, txnList := range groupedTxns {
        wg.Add(1)
        go func(group string, txns []TransactionID) {
            defer wg.Done()
            groupResults := tc.checkTransactionGroup(group, txns)
            
            tc.resultMu.Lock()
            for txnID, state := range groupResults {
                results[txnID] = state
            }
            tc.resultMu.Unlock()
        }(producerGroup, txnList)
    }
    
    wg.Wait()
    return results
}
```

### 2. 连接池优化

```go
type ConnectionPool struct {
    connections chan net.Conn
    factory     func() (net.Conn, error)
    maxIdle     int
    maxActive   int
}

func (cp *ConnectionPool) Get() (net.Conn, error) {
    select {
    case conn := <-cp.connections:
        if cp.isValidConnection(conn) {
            return conn, nil
        }
        conn.Close()
        fallthrough
    default:
        return cp.factory()
    }
}
```

## 🎯 使用最佳实践

### 1. 业务逻辑设计

```go
// ✅ 好的实践：幂等性检查
func (l *PaymentTransactionListener) CheckLocalTransaction(
    txnID transaction.TransactionID, 
    msg transaction.HalfMessage) transaction.TransactionState {
    
    orderID := string(msg.Key)
    
    // 1. 检查订单是否存在
    order, exists := l.orderService.GetOrder(orderID)
    if !exists {
        return transaction.StateRollback
    }
    
    // 2. 检查事务ID是否匹配（防止重复处理）
    if order.TransactionID != string(txnID) {
        return transaction.StateRollback
    }
    
    // 3. 检查最终业务状态
    switch order.PaymentStatus {
    case "PAID":
        return transaction.StateCommit
    case "FAILED", "CANCELLED":
        return transaction.StateRollback
    default:
        return transaction.StateUnknown
    }
}
```

### 2. 超时配置建议

```go
// 推荐的超时配置
const (
    TransactionTimeout    = 30 * time.Second   // 事务总超时
    CheckTimeout         = 10 * time.Second   // 单次检查超时
    CheckInterval        = 5 * time.Second    // 检查间隔
    MaxCheckAttempts     = 3                  // 最大检查次数
    ConnectionTimeout    = 5 * time.Second    // 连接超时
)
```

### 3. 错误处理模式

```go
func (l *BusinessListener) ExecuteLocalTransaction(
    txnID transaction.TransactionID, 
    msg transaction.HalfMessage) transaction.TransactionState {
    
    defer func() {
        if r := recover(); r != nil {
            log.Printf("Panic in local transaction %s: %v", txnID, r)
            // 发生 panic 时回滚事务
        }
    }()
    
    // 使用超时上下文
    ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
    defer cancel()
    
    result, err := l.businessService.ProcessWithContext(ctx, msg)
    if err != nil {
        if errors.Is(err, context.DeadlineExceeded) {
            return transaction.StateUnknown  // 超时，等待回查
        }
        return transaction.StateRollback  // 其他错误，回滚
    }
    
    return l.mapBusinessResultToTxnState(result)
}
```

## 🏆 总结

这个完整的事务消息实现提供了：

1. **🔒 强一致性保证**: 通过2PC协议确保消息发送与本地事务的一致性
2. **🔄 完整回查机制**: 服务端主动向客户端查询事务状态
3. **🛡️ 故障容错能力**: 网络中断、进程重启、超时等异常场景的处理
4. **⚡ 高性能设计**: 异步处理、连接复用、批量操作等优化
5. **📊 可观测性**: 完整的日志记录和指标监控
6. **🎛️ 灵活配置**: 支持各种超时、重试策略的自定义配置

这是一个生产级别的分布式事务消息实现，完全符合 RocketMQ 的事务模型，可以有效解决分布式系统中的数据一致性问题。 