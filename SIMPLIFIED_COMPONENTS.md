# 简化组件文档

本文档介绍了 go-queue 项目中新增的简化组件，这些组件提供了更轻量级的替代方案，适用于简单的使用场景。

## 1. 简化延迟消息管理器 (SimpleDelayedMessageManager)

### 概述
`SimpleDelayedMessageManager` 是 `DelayedMessageManager` 的简化版本，提供基本的延迟消息调度和处理功能。

### 特性
- 基于内存的消息存储
- 简单的时间轮算法
- 支持基本的延迟消息调度
- 轻量级实现，适合小规模应用

### 使用示例
```go
// 创建简化的延迟消息管理器
manager := delayed.NewSimpleDelayedMessageManager()

// 启动管理器
err := manager.Start()
if err != nil {
    log.Fatal(err)
}

// 调度延迟消息
request := &delayed.DelayedProduceRequest{
    Topic:     "test-topic",
    Partition: 0,
    Value:     []byte("delayed message"),
    DelayTime: time.Now().Add(5 * time.Second).Unix(), // 5秒后投递
}

err = manager.ScheduleMessage(request)
if err != nil {
    log.Printf("Failed to schedule message: %v", err)
}

// 停止管理器
manager.Stop()
```

### 配置选项
- 处理间隔：100ms（固定）
- 存储类型：内存
- 并发安全：是

## 2. 幂等管理器

### 概述
项目提供了多种幂等管理器实现：
- `DatabaseIdempotentStorage`: 基于数据库的持久化存储
- `PebbleIdempotentStorage`: 基于PebbleDB的本地存储

### 特性
- 持久化存储，数据安全
- 支持TTL过期清理
- 支持最大容量限制
- 线程安全
- 支持批量操作

### 使用示例
```go
// 使用PebbleDB存储（推荐）
storage, err := client.NewPebbleIdempotentStorage("./data/idempotent")
if err != nil {
    log.Fatal(err)
}
manager := client.NewIdempotentManager(storage)

// 或使用数据库存储
dbStorage, err := client.NewDatabaseIdempotentStorage(db)
if err != nil {
    log.Fatal(err)
}
manager := client.NewIdempotentManager(dbStorage)

// 生成消息ID
messageID := manager.GenerateMessageID("topic", 0, []byte("message"))

// 检查消息是否已处理
if manager.IsProcessed(messageID) {
    log.Println("Message already processed")
    return
}

// 标记消息为已处理
err := manager.MarkProcessed(messageID, map[string]interface{}{
    "topic":     "test-topic",
    "partition": 0,
    "offset":    123,
})
if err != nil {
    log.Printf("Failed to mark message as processed: %v", err)
}
```

## 3. 事务处理器

### 概述
项目提供了基于PebbleDB的事务处理器实现，支持分布式事务状态管理。

### 特性
- 基于PebbleDB的持久化存储
- 支持分布式事务状态管理
- 标准的TransactionListener接口
- 支持事务状态缓存和回查

### 核心接口
```go
// 使用标准的TransactionListener接口
// 详见 internal/transaction/transaction.go
```

### 使用示例

#### 基本使用
```go
// 创建PebbleDB事务处理器
handler, err := client.NewPebbleTransactionHandler("./data/transactions")
if err != nil {
    log.Fatal(err)
}

// 创建事务监听器
listener := &MyTransactionListener{}

// 注册监听器
handler.RegisterListener("producer-group", listener)

// 检查事务状态
state := handler.CheckTransaction("producer-group", "tx-1", []byte("key"), []byte("value"))
```

#### 使用事务客户端
```go
// 创建事务客户端
config := client.ClientConfig{
    BrokerAddrs: []string{"localhost:9092"},
}
client, err := client.NewClient(config)
if err != nil {
    log.Fatal(err)
}

// 创建事务感知客户端
txnClient, err := client.NewTransactionAwareClient(client, "producer-group")
if err != nil {
    log.Fatal(err)
}

// 注册事务监听器
listener := &MyTransactionListener{}
txnClient.RegisterTransactionListener(listener)

// 启动事务检查监听器
err = txnClient.StartTransactionCheckListener(8081)
if err != nil {
    log.Fatal(err)
}
```

## 4. 组件对比

| 特性 | 完整版本 | 当前版本 |
|------|----------|----------|
| **延迟消息** | 支持持久化存储 | 仅内存存储 |
| | 复杂的时间轮算法 | 简单的定时器 |
| | 支持集群部署 | 单机部署 |
| **幂等管理** | 多种存储后端 | PebbleDB/数据库存储 |
| | 数据库持久化 | 持久化存储 |
| | 支持分布式 | 本地/数据库存储 |
| **事务处理** | 网络通信检查 | 网络通信检查 |
| | 复杂的连接管理 | 完整的连接管理 |
| | 支持分布式事务 | 支持分布式事务 |

## 5. 选择指南

### 使用当前版本的场景
- 中小规模应用
- 需要数据持久化
- 分布式事务支持
- 生产环境部署

### 使用完整版本的场景
- 大规模生产环境
- 复杂的集群部署
- 高可用性要求
- 复杂的业务逻辑

## 6. 配置指南

### 延迟消息
```go
// 使用内存存储的延迟消息管理器
manager := delayed.NewSimpleDelayedMessageManager()
```

### 幂等管理
```go
// 使用PebbleDB存储（推荐）
storage, err := client.NewPebbleIdempotentStorage("./data/idempotent")
if err != nil {
    log.Fatal(err)
}
manager := client.NewIdempotentManager(storage)

// 或使用数据库存储
dbStorage, err := client.NewDatabaseIdempotentStorage(db)
if err != nil {
    log.Fatal(err)
}
manager := client.NewIdempotentManager(dbStorage)
```

### 事务处理
```go
// 创建PebbleDB事务处理器
handler, err := client.NewPebbleTransactionHandler("./data/transactions")
if err != nil {
    log.Fatal(err)
}

// 创建事务感知客户端
txnClient, err := client.NewTransactionAwareClient(client, "producer-group")
if err != nil {
    log.Fatal(err)
}
```

## 7. 性能特征

### 延迟消息管理器
- 内存使用：O(n) n为待处理消息数
- 处理延迟：100ms 精度
- 吞吐量：~1000 消息/秒

### 幂等管理器
- 存储：持久化存储
- 查询延迟：取决于存储后端
- 支持并发：是

### 事务处理器
- 存储：PebbleDB持久化
- 检查延迟：网络+存储延迟
- 支持并发：是

## 8. 注意事项

1. **延迟消息**：仍使用内存存储，重启后数据会丢失
2. **幂等管理**：使用持久化存储，数据安全
3. **事务处理**：支持完整的分布式事务
4. **生产环境**：当前版本适合生产环境使用

## 9. 故障排除

### 常见问题

#### 延迟消息不被处理
- 检查管理器是否已启动：`manager.Start()`
- 确认延迟时间设置正确
- 查看日志输出

#### 幂等检查失效
- 确认消息ID生成一致
- 检查TTL设置是否过短
- 验证并发访问是否正确

#### 事务状态检查失败
- 确认监听器已注册
- 检查事务ID是否正确
- 验证回调函数逻辑

### 调试技巧
1. 启用详细日志
2. 使用测试工具验证功能
3. 监控内存使用情况
4. 检查并发访问模式

## 10. 最佳实践

1. **合理设置容量限制**：避免内存溢出
2. **适当的TTL设置**：平衡内存使用和功能需求
3. **错误处理**：妥善处理各种异常情况
4. **监控和告警**：监控组件运行状态
5. **测试覆盖**：确保充分的测试覆盖率