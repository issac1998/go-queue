# 事务性消费者示例

本示例展示了如何使用Go-Queue的事务性消费者实现exact-once语义的消息处理。

## 功能特性

- **Exact-Once语义**: 确保每条消息只被处理一次
- **事务性处理**: 消息处理和offset提交的原子性
- **幂等性保证**: 自动检测和跳过重复消息
- **错误恢复**: 处理失败时自动回滚事务

## 使用方法

### 1. 启动Broker

```bash
# 启动Go-Queue broker
go run main.go
```

### 2. 创建测试主题

```bash
cd cmd/client
go run main.go -cmd=create-topic -topic=test-topic
```

### 3. 发送测试消息

```bash
# 发送一些测试消息
go run main.go -cmd=produce -topic=test-topic -partition=0 -message="Hello World 1"
go run main.go -cmd=produce -topic=test-topic -partition=0 -message="Hello World 2"
go run main.go -cmd=produce -topic=test-topic -partition=0 -message="Hello World 3"
```

### 4. 运行事务性消费者

```bash
cd examples/transactional_consumer
go run main.go
```

## 代码说明

### 配置

```go
config := client.TransactionalConsumerConfig{
    GroupConsumerConfig: client.GroupConsumerConfig{
        GroupID:        "example-group",
        ConsumerID:     "consumer-1",
        Topics:         []string{"test-topic"},
        SessionTimeout: 30 * time.Second,
    },
    TransactionTimeout: 30 * time.Second,
    BatchSize:         10,
    EnableIdempotent:  true,
    IdempotentStorageConfig: client.IdempotentStorageConfig{
        StorageType:     "memory",
        MaxRecords:      1000,
        CleanupInterval: 5 * time.Minute,
    },
}
```

### 事务性消费

```go
err = consumer.ConsumeTransactionally(ctx, func(messages []*client.ConsumeMessage) error {
    // 处理消息批次
    for _, msg := range messages {
        err := processMessage(msg)
        if err != nil {
            return err // 返回错误会触发事务回滚
        }
    }
    return nil // 返回nil会提交事务
})
```

## 工作原理

1. **消息拉取**: 从分配的分区拉取消息批次
2. **事务开始**: 为每个批次开始一个新事务
3. **幂等检查**: 检查消息是否已被处理过
4. **业务处理**: 调用用户定义的处理函数
5. **事务提交**: 处理成功时提交事务和offset
6. **错误回滚**: 处理失败时回滚事务

## 注意事项

- 确保broker已启动并可访问
- 消费者组ID在集群中应该唯一
- 处理函数应该是幂等的
- 长时间运行的处理逻辑可能导致事务超时

## 错误处理

- 网络错误: 自动重试连接
- 处理错误: 事务回滚，消息重新处理
- 超时错误: 事务自动回滚
- 重复消息: 自动跳过，不重复处理