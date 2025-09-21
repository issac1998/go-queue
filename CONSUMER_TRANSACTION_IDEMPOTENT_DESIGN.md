# 🔄 Consumer端事务性消费和幂等处理实现方案

## 📖 概述

基于当前系统架构分析，Producer端已通过Raft日志和快照实现了exactly-once语义的持久化保证。Consumer端需要实现事务性消费和幂等处理，确保消息处理的exactly-once语义。

## 🎯 设计目标

1. **事务性消费**: 消息处理与offset提交的原子性
2. **幂等处理**: 防止重复消费导致的副作用
3. **故障恢复**: 系统重启后能正确恢复消费状态
4. **性能优化**: 最小化事务开销，保持高吞吐量

## 🏗️ 整体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Consumer端事务性消费架构                                │
└─────────────────────────────────────────────────────────────────────────────┘

    Consumer端                    Broker端                    外部存储
┌─────────────────────┐         ┌──────────────────────┐        ┌─────────────┐
│                     │         │                      │        │             │
│ TransactionalConsumer│◄────────┤ ConsumerGroupManager │        │ 业务数据库   │
│                     │         │                      │        │             │
│ ┌─────────────────┐ │         │ ┌──────────────────┐ │        │ ┌─────────┐ │
│ │ IdempotentManager│ │         │ │  OffsetStorage   │ │        │ │ 消费记录 │ │
│ └─────────────────┘ │         │ └──────────────────┘ │        │ └─────────┘ │
│                     │         │                      │        │             │
│ ┌─────────────────┐ │         │ ┌──────────────────┐ │        │ ┌─────────┐ │
│ │ TransactionMgr  │ │         │ │ TransactionSM    │ │        │ │ 事务日志 │ │
│ └─────────────────┘ │         │ └──────────────────┘ │        │ └─────────┘ │
└─────────────────────┘         └──────────────────────┘        └─────────────┘
```

## 🔧 核心组件设计

### 1. TransactionalConsumer（事务性消费者）

```go
type TransactionalConsumer struct {
    consumer          *GroupConsumer
    idempotentManager *IdempotentManager
    transactionMgr    *ConsumerTransactionManager
    
    // 配置参数
    enableTransaction bool
    enableIdempotent  bool
    transactionTimeout time.Duration
    
    // 状态管理
    processingMessages map[string]*ProcessingContext
    mu                sync.RWMutex
}

type ProcessingContext struct {
    MessageID     string
    Topic         string
    Partition     int32
    Offset        int64
    TransactionID string
    StartTime     time.Time
    Status        ProcessingStatus
}

type ProcessingStatus int

const (
    StatusProcessing ProcessingStatus = iota
    StatusCommitted
    StatusRolledBack
    StatusFailed
)
```

**主要功能**：
- 封装GroupConsumer，提供事务性消费接口
- 管理消息处理的生命周期
- 协调幂等检查和事务提交
- 提供故障恢复机制

### 2. IdempotentManager（幂等管理器）

```go
type IdempotentManager struct {
    // 本地缓存（性能优化）
    processedMessages map[string]*ProcessedRecord
    cacheMu          sync.RWMutex
    cacheSize        int
    
    // 持久化存储
    storage          IdempotentStorage
    
    // 配置
    enablePersistence bool
    cacheExpiry      time.Duration
    cleanupInterval  time.Duration
}

type ProcessedRecord struct {
    MessageID   string
    Topic       string
    Partition   int32
    Offset      int64
    ProcessedAt time.Time
    Result      ProcessingResult
    Checksum    string  // 消息内容校验和
}

type ProcessingResult struct {
    Success   bool
    Error     string
    Metadata  map[string]interface{}
}

type IdempotentStorage interface {
    IsProcessed(messageID string) (bool, *ProcessedRecord, error)
    MarkProcessed(record *ProcessedRecord) error
    CleanupExpired(before time.Time) error
    GetProcessedCount() (int64, error)
}
```

**主要功能**：
- 基于MessageID的幂等性检查
- 支持内存缓存 + 持久化存储的双层架构
- 自动清理过期记录
- 提供处理结果的存储和查询

### 3. ConsumerTransactionManager（消费者事务管理器）

```go
type ConsumerTransactionManager struct {
    // 事务状态管理
    activeTransactions map[string]*ConsumerTransaction
    transactionMu     sync.RWMutex
    
    // 与Broker的通信
    client            *Client
    groupID           string
    
    // 配置
    transactionTimeout time.Duration
    maxRetryCount     int
    
    // 事务日志（可选）
    transactionLog    TransactionLog
}

type ConsumerTransaction struct {
    ID            string
    GroupID       string
    Topic         string
    Partition     int32
    StartOffset   int64
    EndOffset     int64
    Status        TransactionStatus
    StartTime     time.Time
    LastUpdate    time.Time
    
    // 处理的消息列表
    Messages      []*ConsumedMessage
    
    // 业务处理结果
    BusinessResult interface{}
    Error         error
}

type TransactionStatus int

const (
    TxnStatusActive TransactionStatus = iota
    TxnStatusPrepared
    TxnStatusCommitted
    TxnStatusRolledBack
    TxnStatusFailed
)

type ConsumedMessage struct {
    MessageID string
    Offset    int64
    Key       []byte
    Value     []byte
    Headers   map[string]string
    Timestamp time.Time
}
```

**主要功能**：
- 管理消费者端的事务生命周期
- 协调消息处理与offset提交的原子性
- 提供事务回滚和重试机制
- 支持批量消息的事务性处理

## 📋 详细工作流程

### 1. 事务性消费流程

```
TransactionalConsumer    IdempotentManager    ConsumerTransactionMgr    Broker
        │                        │                      │                │
        │──① Poll Messages───────────────────────────────────────────────→│
        │←─② Return Messages────────────────────────────────────────────── │
        │                        │                      │                │
        │──③ Check Idempotent───→│                      │                │
        │←─④ Return Result───────│                      │                │
        │                        │                      │                │
        │──⑤ Begin Transaction──────────────────────────→│                │
        │←─⑥ Transaction ID─────────────────────────────│                │
        │                        │                      │                │
        │──⑦ Process Business Logic                     │                │
        │                        │                      │                │
        │──⑧ Prepare Commit─────────────────────────────→│                │
        │                        │                      │──⑨ Commit Offset→│
        │                        │                      │←─⑩ Ack──────────│
        │←─⑪ Commit Success─────────────────────────────│                │
        │                        │                      │                │
        │──⑫ Mark Processed─────→│                      │                │
```

### 2. 幂等性检查流程

```
IdempotentManager        Local Cache        Persistent Storage
        │                      │                      │
        │──① Check Cache───────→│                      │
        │←─② Cache Miss────────│                      │
        │                      │                      │
        │──③ Query Storage─────────────────────────────→│
        │←─④ Return Result────────────────────────────│
        │                      │                      │
        │──⑤ Update Cache──────→│                      │
        │←─⑥ Cache Updated─────│                      │
```

### 3. 故障恢复流程

```
TransactionalConsumer    ConsumerTransactionMgr    Broker
        │                          │                  │
        │──① Startup Recovery──────→│                  │
        │                          │──② Query Uncommitted Txns→│
        │                          │←─③ Return Txn List────────│
        │←─④ Recovery Plan─────────│                  │
        │                          │                  │
        │──⑤ Rollback/Retry───────→│                  │
        │                          │──⑥ Update Offsets────────→│
        │←─⑦ Recovery Complete─────│                  │
```

## 🛠️ 具体实现方案

### 1. 消息ID生成策略

```go
// 基于消息内容和位置的唯一ID生成
func GenerateMessageID(topic string, partition int32, offset int64, key []byte, value []byte) string {
    hasher := sha256.New()
    hasher.Write([]byte(topic))
    hasher.Write([]byte(fmt.Sprintf("%d", partition)))
    hasher.Write([]byte(fmt.Sprintf("%d", offset)))
    if key != nil {
        hasher.Write(key)
    }
    hasher.Write(value)
    
    return fmt.Sprintf("%s-%d-%d-%x", 
        topic, partition, offset, hasher.Sum(nil)[:8])
}
```

### 2. 事务性消费API设计

```go
// 基础事务性消费接口
type TransactionalMessageHandler interface {
    HandleMessage(ctx context.Context, message *ConsumedMessage) error
}

// 批量事务性消费接口
type BatchTransactionalMessageHandler interface {
    HandleMessages(ctx context.Context, messages []*ConsumedMessage) error
}

// 使用示例
func (tc *TransactionalConsumer) ConsumeWithTransaction(
    handler TransactionalMessageHandler) error {
    
    for {
        // 1. 拉取消息
        messages, err := tc.consumer.Poll(tc.pollTimeout)
        if err != nil {
            return err
        }
        
        for _, message := range messages {
            // 2. 生成消息ID
            messageID := GenerateMessageID(
                message.Topic, message.Partition, 
                message.Offset, message.Key, message.Value)
            
            // 3. 幂等性检查
            if processed, record, err := tc.idempotentManager.IsProcessed(messageID); err != nil {
                log.Printf("Idempotent check failed: %v", err)
                continue
            } else if processed {
                log.Printf("Message %s already processed, skipping", messageID)
                // 直接提交offset
                tc.consumer.CommitOffset(message.Topic, message.Partition, message.Offset+1)
                continue
            }
            
            // 4. 开始事务
            txn, err := tc.transactionMgr.BeginTransaction(
                tc.consumer.GroupID, message.Topic, message.Partition, message.Offset)
            if err != nil {
                log.Printf("Failed to begin transaction: %v", err)
                continue
            }
            
            // 5. 处理消息
            ctx, cancel := context.WithTimeout(context.Background(), tc.transactionTimeout)
            err = handler.HandleMessage(ctx, &ConsumedMessage{
                MessageID: messageID,
                Offset:    message.Offset,
                Key:       message.Key,
                Value:     message.Value,
                Headers:   message.Headers,
                Timestamp: time.Now(),
            })
            cancel()
            
            // 6. 根据处理结果提交或回滚
            if err != nil {
                // 回滚事务
                if rollbackErr := tc.transactionMgr.RollbackTransaction(txn.ID); rollbackErr != nil {
                    log.Printf("Failed to rollback transaction %s: %v", txn.ID, rollbackErr)
                }
                log.Printf("Message processing failed, transaction rolled back: %v", err)
            } else {
                // 提交事务
                if commitErr := tc.transactionMgr.CommitTransaction(txn.ID); commitErr != nil {
                    log.Printf("Failed to commit transaction %s: %v", txn.ID, commitErr)
                    continue
                }
                
                // 标记为已处理
                record := &ProcessedRecord{
                    MessageID:   messageID,
                    Topic:       message.Topic,
                    Partition:   message.Partition,
                    Offset:      message.Offset,
                    ProcessedAt: time.Now(),
                    Result: ProcessingResult{
                        Success:  true,
                        Metadata: map[string]interface{}{"txn_id": txn.ID},
                    },
                    Checksum: generateChecksum(message.Value),
                }
                
                if markErr := tc.idempotentManager.MarkProcessed(record); markErr != nil {
                    log.Printf("Failed to mark message as processed: %v", markErr)
                }
                
                log.Printf("Message %s processed successfully in transaction %s", messageID, txn.ID)
            }
        }
    }
}
```

### 3. 持久化存储实现

#### 基于数据库的IdempotentStorage

```go
type DatabaseIdempotentStorage struct {
    db     *sql.DB
    table  string
}

func (s *DatabaseIdempotentStorage) IsProcessed(messageID string) (bool, *ProcessedRecord, error) {
    query := `SELECT message_id, topic, partition, offset, processed_at, success, error, metadata, checksum 
              FROM ` + s.table + ` WHERE message_id = ?`
    
    row := s.db.QueryRow(query, messageID)
    
    var record ProcessedRecord
    var success bool
    var errorStr sql.NullString
    var metadataJSON sql.NullString
    
    err := row.Scan(
        &record.MessageID, &record.Topic, &record.Partition, &record.Offset,
        &record.ProcessedAt, &success, &errorStr, &metadataJSON, &record.Checksum)
    
    if err == sql.ErrNoRows {
        return false, nil, nil
    }
    if err != nil {
        return false, nil, err
    }
    
    record.Result.Success = success
    if errorStr.Valid {
        record.Result.Error = errorStr.String
    }
    
    if metadataJSON.Valid {
        json.Unmarshal([]byte(metadataJSON.String), &record.Result.Metadata)
    }
    
    return true, &record, nil
}

func (s *DatabaseIdempotentStorage) MarkProcessed(record *ProcessedRecord) error {
    metadataJSON, _ := json.Marshal(record.Result.Metadata)
    
    query := `INSERT INTO ` + s.table + ` 
              (message_id, topic, partition, offset, processed_at, success, error, metadata, checksum) 
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON DUPLICATE KEY UPDATE 
              processed_at = VALUES(processed_at), success = VALUES(success), 
              error = VALUES(error), metadata = VALUES(metadata)`
    
    _, err := s.db.Exec(query,
        record.MessageID, record.Topic, record.Partition, record.Offset,
        record.ProcessedAt, record.Result.Success, record.Result.Error,
        string(metadataJSON), record.Checksum)
    
    return err
}
```

#### 基于Raft的IdempotentStorage

```go
type RaftIdempotentStorage struct {
    raftClient *raft.Client
    groupID    uint64
}

func (s *RaftIdempotentStorage) IsProcessed(messageID string) (bool, *ProcessedRecord, error) {
    // 查询Raft状态机
    result, err := s.raftClient.QueryMetadata(protocol.RaftQueryGetProcessedMessage, map[string]interface{}{
        "message_id": messageID,
    })
    if err != nil {
        return false, nil, err
    }
    
    var response map[string]interface{}
    if err := json.Unmarshal(result, &response); err != nil {
        return false, nil, err
    }
    
    if found, exists := response["found"].(bool); !exists || !found {
        return false, nil, nil
    }
    
    // 解析记录
    recordData := response["record"].(map[string]interface{})
    record := &ProcessedRecord{
        MessageID:   recordData["message_id"].(string),
        Topic:       recordData["topic"].(string),
        Partition:   int32(recordData["partition"].(float64)),
        Offset:      int64(recordData["offset"].(float64)),
        ProcessedAt: time.Unix(int64(recordData["processed_at"].(float64)), 0),
        Checksum:    recordData["checksum"].(string),
    }
    
    return true, record, nil
}

func (s *RaftIdempotentStorage) MarkProcessed(record *ProcessedRecord) error {
    // 通过Raft提交
    cmd := &raft.ControllerCommand{
        Type:      protocol.RaftCmdMarkMessageProcessed,
        ID:        fmt.Sprintf("mark-processed-%s-%d", record.MessageID, time.Now().UnixNano()),
        Timestamp: time.Now(),
        Data: map[string]interface{}{
            "message_id":   record.MessageID,
            "topic":        record.Topic,
            "partition":    record.Partition,
            "offset":       record.Offset,
            "processed_at": record.ProcessedAt.Unix(),
            "success":      record.Result.Success,
            "error":        record.Result.Error,
            "metadata":     record.Result.Metadata,
            "checksum":     record.Checksum,
        },
    }
    
    return s.raftClient.ExecuteCommand(cmd)
}
```

## ⚡ 性能优化策略

### 1. 批量处理优化

```go
func (tc *TransactionalConsumer) BatchConsumeWithTransaction(
    handler BatchTransactionalMessageHandler, batchSize int) error {
    
    for {
        // 批量拉取消息
        messages, err := tc.consumer.PollBatch(batchSize, tc.pollTimeout)
        if err != nil {
            return err
        }
        
        if len(messages) == 0 {
            continue
        }
        
        // 批量幂等性检查
        messageIDs := make([]string, len(messages))
        for i, msg := range messages {
            messageIDs[i] = GenerateMessageID(msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
        }
        
        processedMap, err := tc.idempotentManager.BatchIsProcessed(messageIDs)
        if err != nil {
            log.Printf("Batch idempotent check failed: %v", err)
            continue
        }
        
        // 过滤未处理的消息
        var unprocessedMessages []*ConsumedMessage
        for i, msg := range messages {
            if !processedMap[messageIDs[i]] {
                unprocessedMessages = append(unprocessedMessages, &ConsumedMessage{
                    MessageID: messageIDs[i],
                    Offset:    msg.Offset,
                    Key:       msg.Key,
                    Value:     msg.Value,
                    Headers:   msg.Headers,
                    Timestamp: time.Now(),
                })
            }
        }
        
        if len(unprocessedMessages) == 0 {
            // 所有消息都已处理，直接提交offset
            lastMessage := messages[len(messages)-1]
            tc.consumer.CommitOffset(lastMessage.Topic, lastMessage.Partition, lastMessage.Offset+1)
            continue
        }
        
        // 开始批量事务
        firstMsg := unprocessedMessages[0]
        lastMsg := unprocessedMessages[len(unprocessedMessages)-1]
        
        txn, err := tc.transactionMgr.BeginBatchTransaction(
            tc.consumer.GroupID, firstMsg.Topic, firstMsg.Partition, 
            firstMsg.Offset, lastMsg.Offset)
        if err != nil {
            log.Printf("Failed to begin batch transaction: %v", err)
            continue
        }
        
        // 批量处理消息
        ctx, cancel := context.WithTimeout(context.Background(), tc.transactionTimeout)
        err = handler.HandleMessages(ctx, unprocessedMessages)
        cancel()
        
        if err != nil {
            // 回滚事务
            tc.transactionMgr.RollbackTransaction(txn.ID)
            log.Printf("Batch processing failed, transaction rolled back: %v", err)
        } else {
            // 提交事务
            if commitErr := tc.transactionMgr.CommitTransaction(txn.ID); commitErr != nil {
                log.Printf("Failed to commit batch transaction %s: %v", txn.ID, commitErr)
                continue
            }
            
            // 批量标记为已处理
            var records []*ProcessedRecord
            for _, msg := range unprocessedMessages {
                records = append(records, &ProcessedRecord{
                    MessageID:   msg.MessageID,
                    Topic:       firstMsg.Topic,
                    Partition:   firstMsg.Partition,
                    Offset:      msg.Offset,
                    ProcessedAt: time.Now(),
                    Result: ProcessingResult{
                        Success:  true,
                        Metadata: map[string]interface{}{"txn_id": txn.ID},
                    },
                    Checksum: generateChecksum(msg.Value),
                })
            }
            
            tc.idempotentManager.BatchMarkProcessed(records)
            log.Printf("Batch processed %d messages in transaction %s", len(unprocessedMessages), txn.ID)
        }
    }
}
```

### 2. 缓存优化策略

```go
type LRUIdempotentCache struct {
    cache    *lru.Cache
    maxSize  int
    ttl      time.Duration
    mu       sync.RWMutex
}

type CacheEntry struct {
    Record    *ProcessedRecord
    ExpiresAt time.Time
}

func (c *LRUIdempotentCache) Get(messageID string) (*ProcessedRecord, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    
    if entry, exists := c.cache.Get(messageID); exists {
        cacheEntry := entry.(*CacheEntry)
        if time.Now().Before(cacheEntry.ExpiresAt) {
            return cacheEntry.Record, true
        }
        // 过期，删除
        c.cache.Remove(messageID)
    }
    
    return nil, false
}

func (c *LRUIdempotentCache) Put(messageID string, record *ProcessedRecord) {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    entry := &CacheEntry{
        Record:    record,
        ExpiresAt: time.Now().Add(c.ttl),
    }
    
    c.cache.Add(messageID, entry)
}
```

## 🔧 配置和部署

### 1. 配置参数

```go
type TransactionalConsumerConfig struct {
    // 基础配置
    GroupID           string        `json:"group_id"`
    Topics            []string      `json:"topics"`
    
    // 事务配置
    EnableTransaction   bool          `json:"enable_transaction"`
    TransactionTimeout  time.Duration `json:"transaction_timeout"`
    MaxRetryCount      int           `json:"max_retry_count"`
    
    // 幂等配置
    EnableIdempotent    bool          `json:"enable_idempotent"`
    IdempotentStorage   string        `json:"idempotent_storage"` // "memory", "database", "raft"
    CacheSize          int           `json:"cache_size"`
    CacheExpiry        time.Duration `json:"cache_expiry"`
    
    // 性能配置
    BatchSize          int           `json:"batch_size"`
    PollTimeout        time.Duration `json:"poll_timeout"`
    
    // 存储配置
    DatabaseConfig     *DatabaseConfig `json:"database_config,omitempty"`
    RaftConfig        *RaftConfig     `json:"raft_config,omitempty"`
}

type DatabaseConfig struct {
    Driver   string `json:"driver"`
    DSN      string `json:"dsn"`
    Table    string `json:"table"`
}

type RaftConfig struct {
    GroupID uint64 `json:"group_id"`
}
```

### 2. 使用示例

```go
// 配置文件示例 (config.json)
{
    "group_id": "order-processing-group",
    "topics": ["order-events", "payment-events"],
    "enable_transaction": true,
    "transaction_timeout": "30s",
    "max_retry_count": 3,
    "enable_idempotent": true,
    "idempotent_storage": "database",
    "cache_size": 10000,
    "cache_expiry": "1h",
    "batch_size": 100,
    "poll_timeout": "5s",
    "database_config": {
        "driver": "mysql",
        "dsn": "user:password@tcp(localhost:3306)/queue_db",
        "table": "processed_messages"
    }
}

// 应用代码示例
func main() {
    // 加载配置
    config, err := LoadTransactionalConsumerConfig("config.json")
    if err != nil {
        log.Fatal(err)
    }
    
    // 创建客户端
    client, err := client.NewClient("localhost:8080")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()
    
    // 创建事务性消费者
    consumer, err := NewTransactionalConsumer(client, config)
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()
    
    // 定义消息处理器
    handler := &OrderMessageHandler{
        orderService: orderService,
        paymentService: paymentService,
    }
    
    // 开始消费
    log.Println("Starting transactional consumer...")
    if err := consumer.ConsumeWithTransaction(handler); err != nil {
        log.Fatal(err)
    }
}

type OrderMessageHandler struct {
    orderService   *OrderService
    paymentService *PaymentService
}

func (h *OrderMessageHandler) HandleMessage(ctx context.Context, message *ConsumedMessage) error {
    // 解析消息
    var event OrderEvent
    if err := json.Unmarshal(message.Value, &event); err != nil {
        return fmt.Errorf("failed to unmarshal message: %w", err)
    }
    
    // 根据事件类型处理
    switch event.Type {
    case "order_created":
        return h.handleOrderCreated(ctx, &event)
    case "payment_completed":
        return h.handlePaymentCompleted(ctx, &event)
    default:
        return fmt.Errorf("unknown event type: %s", event.Type)
    }
}

func (h *OrderMessageHandler) handleOrderCreated(ctx context.Context, event *OrderEvent) error {
    // 业务逻辑：创建订单记录
    order := &Order{
        ID:       event.OrderID,
        Amount:   event.Amount,
        Status:   "created",
        CreateAt: time.Now(),
    }
    
    // 在事务中执行业务操作
    return h.orderService.CreateOrderWithTx(ctx, order)
}

func (h *OrderMessageHandler) handlePaymentCompleted(ctx context.Context, event *OrderEvent) error {
    // 业务逻辑：更新订单状态
    return h.orderService.UpdateOrderStatusWithTx(ctx, event.OrderID, "paid")
}
```

## 📊 监控和运维

### 1. 关键指标

```go
type TransactionalConsumerMetrics struct {
    // 消费指标
    MessagesConsumed      int64 `json:"messages_consumed"`
    MessagesProcessed     int64 `json:"messages_processed"`
    MessagesFailed        int64 `json:"messages_failed"`
    
    // 事务指标
    TransactionsStarted   int64 `json:"transactions_started"`
    TransactionsCommitted int64 `json:"transactions_committed"`
    TransactionsRolledBack int64 `json:"transactions_rolled_back"`
    
    // 幂等指标
    IdempotentHits        int64 `json:"idempotent_hits"`
    IdempotentMisses      int64 `json:"idempotent_misses"`
    CacheHitRate         float64 `json:"cache_hit_rate"`
    
    // 性能指标
    AvgProcessingTime    time.Duration `json:"avg_processing_time"`
    AvgTransactionTime   time.Duration `json:"avg_transaction_time"`
    
    // 错误指标
    LastError            string    `json:"last_error"`
    ErrorCount           int64     `json:"error_count"`
    LastErrorTime        time.Time `json:"last_error_time"`
}
```

### 2. 健康检查

```go
func (tc *TransactionalConsumer) HealthCheck() *HealthStatus {
    status := &HealthStatus{
        Status:    "healthy",
        Timestamp: time.Now(),
        Details:   make(map[string]interface{}),
    }
    
    // 检查消费者状态
    if !tc.consumer.IsConnected() {
        status.Status = "unhealthy"
        status.Details["consumer"] = "disconnected"
    }
    
    // 检查事务管理器状态
    activeTransactions := tc.transactionMgr.GetActiveTransactionCount()
    if activeTransactions > 1000 { // 阈值检查
        status.Status = "warning"
        status.Details["active_transactions"] = activeTransactions
    }
    
    // 检查幂等存储状态
    if tc.enableIdempotent {
        if err := tc.idempotentManager.HealthCheck(); err != nil {
            status.Status = "unhealthy"
            status.Details["idempotent_storage"] = err.Error()
        }
    }
    
    return status
}

type HealthStatus struct {
    Status    string                 `json:"status"`
    Timestamp time.Time              `json:"timestamp"`
    Details   map[string]interface{} `json:"details"`
}
```

## 🎯 总结

### 实现亮点

1. **完整的事务性保证**: 通过事务管理器确保消息处理与offset提交的原子性
2. **高效的幂等处理**: 双层缓存架构，支持多种持久化存储
3. **灵活的配置选项**: 支持内存、数据库、Raft等多种存储后端
4. **优秀的性能优化**: 批量处理、缓存优化、连接复用等策略
5. **完善的监控体系**: 丰富的指标和健康检查机制
6. **故障恢复能力**: 支持系统重启后的状态恢复

### 技术优势

- **架构清晰**: 模块化设计，职责分离
- **扩展性强**: 支持插件化的存储后端
- **性能优秀**: 多种优化策略，适应高吞吐场景
- **可靠性高**: 完整的错误处理和重试机制
- **易于使用**: 简洁的API设计和丰富的配置选项

### 适用场景

- **订单处理系统**: 确保订单状态变更的一致性
- **支付系统**: 防止重复扣款和状态不一致
- **库存管理**: 确保库存操作的原子性
- **数据同步**: 保证跨系统数据的最终一致性

这个实现方案为Consumer端提供了完整的exactly-once语义保证，与Producer端的实现形成完整的端到端exactly-once消息传递系统。