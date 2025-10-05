# Go-Queue 使用方法说明

## 概述

Go-Queue 是一个基于 Multi-Raft 架构的高性能分布式消息队列系统，支持多种高级特性如事务消息、死信队列、消费者组、延迟消息等。

## 系统架构

### 核心组件
- **Broker**: 消息存储和处理节点
- **Controller**: 集群元数据管理和协调
- **Client**: 生产者和消费者客户端
- **Raft**: 分布式一致性协议实现

### 主要特性
- ✅ **Multi-Raft 架构**: 每个分区独立的 Raft 组
- ✅ **事务消息**: 支持分布式事务和精确一次语义
- ✅ **消费者组**: 自动负载均衡和故障转移
- ✅ **死信队列 (DLQ)**: 失败消息处理和重试机制
- ✅ **延迟消息**: 定时消息投递
- ✅ **有序消息**: 分区内消息顺序保证
- ✅ **异步 I/O**: 高性能连接池和批处理
- ✅ **压缩**: 多种压缩算法支持
- ✅ **去重**: 生产者幂等性保证

## 快速开始

### 1. 编译项目

```bash
git clone https://github.com/issac1998/go-queue.git
cd go-queue
go mod tidy
go build -o go-queue-client ./cmd/client/main.go
```

### 2. 启动 Broker

#### 单节点模式

创建配置文件 `broker.yaml`:

```yaml
# Broker 基础配置
node_id: "broker-1"
bind_addr: "0.0.0.0"
bind_port: 9092
data_dir: "./data/broker-1"

# Raft 配置
raft:
  rtt_millisecond: 200
  heartbeat_rtt: 5
  election_rtt: 10
  check_quorum: true
  snapshot_entries: 10000
  compaction_overhead: 5000
  max_in_mem_log_size: 67108864  # 64MB
  controller_group_id: 1
  controller_snapshot_freq: 1000

# 服务发现配置
discovery:
  type: "memory"  # 单节点使用内存模式
  endpoints: []
  timeout: "5s"

# 性能配置
performance:
  max_batch_size: 500
  max_batch_bytes: 1048576  # 1MB
  batch_timeout: "10ms"
  write_cache_size: 33554432  # 32MB
  read_cache_size: 67108864   # 64MB
  connection_pool_size: 100
  connection_idle_timeout: "5m"

# 功能开关
compression_enabled: true
compression_type: "snappy"
compression_threshold: 1024
deduplication_enabled: true
deduplication_ttl: 24
deduplication_max_size: 100000
```

启动 Broker:
```bash
# 编译 broker
go build -o go-queue-broker ./cmd/broker/main.go

# 启动单节点 Broker
./go-queue-broker -node-id=broker-1 -bind-addr=127.0.0.1 -bind-port=9092 -data-dir=./data/broker-1 -log-dir=./logs -discovery-type=memory
```

#### 集群模式

集群模式不需要创建单独的配置文件，只需要指定 etcd 作为服务发现，后续启动的 broker 会自动加入集群。

启动集群（需要先启动 etcd）:
```bash
# 1. 启动 etcd
etcd --data-dir=./etcd-data

# 2. 编译 broker
go build -o go-queue-broker ./cmd/broker/main.go

# 3. 启动第一个 Broker 节点
./go-queue-broker \
  -node-id=broker1 \
  -bind-addr=127.0.0.1 \
  -bind-port=9092 \
  -raft-addr=127.0.0.1:7001 \
  -data-dir=./data/broker1 \
  -log-dir=./data/broker1/logs \
  -discovery-type=etcd \
  -discovery-endpoints=127.0.0.1:2379

# 4. 启动第二个 Broker 节点
./go-queue-broker \
  -node-id=broker2 \
  -bind-addr=127.0.0.1 \
  -bind-port=9093 \
  -raft-addr=127.0.0.1:7002 \
  -data-dir=./data/broker2 \
  -log-dir=./data/broker2/logs \
  -discovery-type=etcd \
  -discovery-endpoints=127.0.0.1:2379

# 5. 启动第三个 Broker 节点
./go-queue-broker \
  -node-id=broker3 \
  -bind-addr=127.0.0.1 \
  -bind-port=9094 \
  -raft-addr=127.0.0.1:7003 \
  -data-dir=./data/broker3 \
  -log-dir=./data/broker3/logs \
  -discovery-type=etcd \
  -discovery-endpoints=127.0.0.1:2379 \
  -enable-follower-read
```

#### Broker 启动参数说明

| 参数 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| `-node-id` | ✅ | - | 唯一的节点标识符 |
| `-bind-addr` | ❌ | 127.0.0.1 | 客户端连接地址 |
| `-bind-port` | ❌ | 9092 | 客户端连接端口 |
| `-data-dir` | ❌ | ./data | 数据存储目录 |
| `-log-dir` | ❌ | - | 日志目录（为空则输出到控制台） |
| `-raft-addr` | ❌ | - | Raft 通信地址（集群模式必需） |
| `-discovery-type` | ❌ | memory | 服务发现类型（memory/etcd） |
| `-discovery-endpoints` | ❌ | - | 服务发现端点（etcd 地址，逗号分隔） |
| `-enable-follower-read` | ❌ | false | 启用从节点读取 |

### 3. 客户端配置

创建客户端配置文件 `client.json`:

```json
{
  "broker_addrs": ["localhost:9092", "localhost:9093", "localhost:9094"],
  "timeout": "10s",
  "log_file": "logs/client.log",
  "command": {
    "type": "produce",
    "topic": "test-topic",
    "partition": 0,
    "message": "Hello World from Go-Queue!",
    "offset": 0,
    "count": 5
  }
}
```

## 客户端 API 使用

### 基础客户端配置

```go
package main

import (
    "time"
    "github.com/issac1998/go-queue/client"
    "github.com/issac1998/go-queue/internal/async"
    "github.com/issac1998/go-queue/internal/pool"
)

func main() {
    // 创建客户端配置
    config := client.ClientConfig{
        BrokerAddrs: []string{"localhost:9092", "localhost:9093"},
        Timeout:     5 * time.Second,
        
        // 性能优化配置
        EnableConnectionPool: true,
        EnableAsyncIO:        true,
        ConnectionPool: pool.ConnectionPoolConfig{
            MaxConnections:    10,
            MinConnections:    2,
            ConnectionTimeout: 3 * time.Second,
            IdleTimeout:       5 * time.Minute,
            MaxLifetime:       15 * time.Minute,
        },
        AsyncIO: async.AsyncIOConfig{
            WorkerCount:    4,
            SQSize:         512,
            CQSize:         512,
            BatchSize:      100,
            PollTimeout:    10 * time.Millisecond,
            ReadTimeout:    10 * time.Second,
            WriteTimeout:   10 * time.Second,
            MaxConnections: 100,
        },
        BatchSize:       50,
        BatchTimeout:    5 * time.Millisecond,
        MaxPendingBatch: 500,
    }
    
    // 创建客户端
    c := client.NewClient(config)
    defer c.Close()
}
```

### 主题管理

```go
// 创建管理员客户端
admin := client.NewAdmin(c)

// 创建主题
createResult, err := admin.CreateTopic(client.CreateTopicRequest{
    Name:       "my-topic",
    Partitions: 3,
    Replicas:   2,
})
if err != nil {
    log.Printf("创建主题失败: %v", err)
} else {
    fmt.Printf("主题创建成功: %s\n", createResult.Name)
}

// 列出所有主题
topics, err := admin.ListTopics()
if err != nil {
    log.Printf("列出主题失败: %v", err)
} else {
    for _, topic := range topics {
        fmt.Printf("主题: %s, 分区数: %d\n", topic.Name, topic.Partitions)
    }
}

// 获取主题信息
topicInfo, err := admin.GetTopicInfo("my-topic")
if err != nil {
    log.Printf("获取主题信息失败: %v", err)
} else {
    fmt.Printf("主题: %s, 分区数: %d, 副本数: %d\n", 
        topicInfo.Name, topicInfo.Partitions, topicInfo.Replicas)
}

// 删除主题
err = admin.DeleteTopic("my-topic")
if err != nil {
    log.Printf("删除主题失败: %v", err)
} else {
    fmt.Println("主题删除成功")
}
```

### 生产者使用

#### 基础生产者

```go
// 创建生产者
producer := client.NewProducer(c)

// 发送单条消息
message := client.ProduceMessage{
    Topic:     "my-topic",
    Partition: 0,  // 指定分区
    Value:     []byte("Hello, Go-Queue!"),
}

result, err := producer.Send(message)
if err != nil {
    log.Printf("发送消息失败: %v", err)
} else {
    fmt.Printf("消息发送成功 - 主题: %s, 分区: %d, 偏移量: %d\n", 
        result.Topic, result.Partition, result.Offset)
}
```

#### 分区策略

```go
// 手动分区（默认）
producer := client.NewProducer(c)

// 轮询分区
producer := client.NewProducerWithStrategy(c, client.PartitionStrategyRoundRobin)

// 随机分区
producer := client.NewProducerWithStrategy(c, client.PartitionStrategyRandom)

// 哈希分区
producer := client.NewProducerWithStrategy(c, client.PartitionStrategyHash)

// 使用哈希分区发送消息
message := client.ProduceMessage{
    Topic: "my-topic",
    Key:   []byte("user-123"),  // 根据 key 进行哈希分区
    Value: []byte("User data"),
}
```

#### 批量发送

```go
// 批量发送消息
messages := []client.ProduceMessage{
    {Topic: "my-topic", Partition: 0, Value: []byte("Message 1")},
    {Topic: "my-topic", Partition: 0, Value: []byte("Message 2")},
    {Topic: "my-topic", Partition: 0, Value: []byte("Message 3")},
}

result, err := producer.SendBatch(messages)
if err != nil {
    log.Printf("批量发送失败: %v", err)
} else {
    fmt.Printf("批量发送成功，最后偏移量: %d\n", result.Offset)
}
```

### 消费者使用

#### 基础消费者

```go
// 创建消费者
consumer := client.NewConsumer(c)

// 从指定偏移量开始消费
result, err := consumer.FetchFrom("my-topic", 0, 0)  // 从偏移量 0 开始
if err != nil {
    log.Printf("消费消息失败: %v", err)
} else {
    for _, msg := range result.Messages {
        fmt.Printf("收到消息 - 偏移量: %d, 内容: %s\n", 
            msg.Offset, string(msg.Value))
    }
}

// 自定义 Fetch 请求
fetchResult, err := consumer.Fetch(client.FetchRequest{
    Topic:     "my-topic",
    Partition: 0,
    Offset:    10,
    MaxBytes:  1024 * 1024,  // 最大 1MB
})
```

#### 订阅模式消费

```go
// 订阅主题
err := consumer.Subscribe([]string{"my-topic", "another-topic"})
if err != nil {
    log.Printf("订阅失败: %v", err)
}

// 轮询消息
for {
    messages, err := consumer.Poll(1 * time.Second)
    if err != nil {
        log.Printf("轮询失败: %v", err)
        continue
    }
    
    for _, msg := range messages {
        fmt.Printf("收到消息: %s\n", string(msg.Value))
    }
}
```

### 消费者组

```go
// 创建消费者组配置
groupConsumer := client.NewGroupConsumer(c, client.GroupConsumerConfig{
    GroupID:        "my-consumer-group",
    ConsumerID:     "consumer-1",
    Topics:         []string{"my-topic"},
    SessionTimeout: 30 * time.Second,
})

// 加入消费者组
err := groupConsumer.JoinGroup()
if err != nil {
    log.Printf("加入消费者组失败: %v", err)
    return
}

// 获取分区分配
assignment := groupConsumer.GetAssignment()
fmt.Printf("分区分配: %v\n", assignment)

// 开始消费
for {
    messages, err := groupConsumer.Poll(1 * time.Second)
    if err != nil {
        log.Printf("消费失败: %v", err)
        continue
    }
    
    for _, msg := range messages {
        fmt.Printf("消费者组收到消息: %s\n", string(msg.Value))
        
        // 提交偏移量
        err := groupConsumer.CommitOffset(msg.Topic, msg.Partition, msg.Offset+1)
        if err != nil {
            log.Printf("提交偏移量失败: %v", err)
        }
    }
}
```

### 事务消息

```go
import (
    "github.com/issac1998/go-queue/client"
    "github.com/issac1998/go-queue/internal/transaction"
)

// 实现事务监听器
type MyTransactionListener struct {
    // 业务逻辑相关字段
}

func (l *MyTransactionListener) ExecuteLocalTransaction(
    transactionID transaction.TransactionID, 
    messageID string,
) transaction.TransactionState {
    // 执行本地事务逻辑
    // 返回 StateCommit, StateRollback, 或 StateUnknown
    return transaction.StateCommit
}

func (l *MyTransactionListener) CheckLocalTransaction(
    transactionID transaction.TransactionID, 
    messageID string,
) transaction.TransactionState {
    // 检查本地事务状态（用于回查）
    return transaction.StateCommit
}

// 创建事务生产者
listener := &MyTransactionListener{}
txProducer := client.NewTransactionProducer(c, listener)

// 发送事务消息
message := client.ProduceMessage{
    Topic: "transaction-topic",
    Value: []byte("Transaction message"),
}

result, err := txProducer.SendTransactional(message)
if err != nil {
    log.Printf("发送事务消息失败: %v", err)
} else {
    fmt.Printf("事务消息发送成功: %d\n", result.Offset)
}
```

### 死信队列 (DLQ)

```go
import "github.com/issac1998/go-queue/internal/dlq"

// 创建 DLQ 配置
dlqConfig := dlq.DefaultDLQConfig()
dlqConfig.RetryPolicy.MaxRetries = 3
dlqConfig.RetryPolicy.InitialDelay = 2 * time.Second
dlqConfig.RetryPolicy.BackoffFactor = 2.0
dlqConfig.RetryPolicy.MaxDelay = 30 * time.Second
dlqConfig.TopicSuffix = ".dlq"

// 创建 DLQ 管理器
dlqManager, err := dlq.NewManager(c, dlqConfig)
if err != nil {
    log.Fatalf("创建 DLQ 管理器失败: %v", err)
}

// 处理失败消息
message := &client.Message{
    Topic:     "orders",
    Partition: 0,
    Offset:    100,
    Value:     []byte("Failed order message"),
}

failureInfo := &dlq.FailureInfo{
    ConsumerGroup: "order-processor",
    ConsumerID:    "consumer-1",
    ErrorMessage:  "Database connection timeout",
    FailureTime:   time.Now(),
}

err = dlqManager.HandleFailedMessage(message, failureInfo)
if err != nil {
    log.Printf("处理失败消息失败: %v", err)
}
```

### 延迟消息

```go
// 创建延迟生产者
delayedProducer := client.NewDelayedProducer(c)

// 发送延迟消息
message := client.ProduceMessage{
    Topic: "delayed-topic",
    Value: []byte("This message will be delivered later"),
}

// 延迟 5 分钟投递
deliveryTime := time.Now().Add(5 * time.Minute)
result, err := delayedProducer.SendDelayed(message, deliveryTime)
if err != nil {
    log.Printf("发送延迟消息失败: %v", err)
} else {
    fmt.Printf("延迟消息发送成功，将在 %v 投递\n", deliveryTime)
}
```

### 有序消息

```go
// 创建有序生产者
orderedProducer := client.NewOrderedProducer(c)

// 发送有序消息（同一个 orderKey 的消息会保证顺序）
messages := []client.OrderedMessage{
    {
        OrderKey: "user-123",
        Message: client.ProduceMessage{
            Topic: "user-events",
            Value: []byte("User login"),
        },
    },
    {
        OrderKey: "user-123",
        Message: client.ProduceMessage{
            Topic: "user-events", 
            Value: []byte("User purchase"),
        },
    },
}

for _, msg := range messages {
    result, err := orderedProducer.SendOrdered(msg)
    if err != nil {
        log.Printf("发送有序消息失败: %v", err)
    } else {
        fmt.Printf("有序消息发送成功: %d\n", result.Offset)
    }
}
```

## 命令行工具使用

### 基础命令

```bash
# 编译客户端
go build -o go-queue-client ./cmd/client/main.go

# 创建主题
./go-queue-client -config=client.json -cmd=create-topic -topic=my-topic -partitions=3 -replicas=2

# 发送消息
./go-queue-client -config=client.json -cmd=produce -topic=my-topic -partition=0 -message="Hello World" -count=10

# 消费消息
./go-queue-client -config=client.json -cmd=consume -topic=my-topic -partition=0 -offset=0

# 列出主题
./go-queue-client -config=client.json -cmd=list-topics

# 获取主题信息
./go-queue-client -config=client.json -cmd=describe-topic -topic=my-topic

# 删除主题
./go-queue-client -config=client.json -cmd=delete-topic -topic=my-topic
```

### 高级命令

```bash
# 指定 Broker 地址（覆盖配置文件）
./go-queue-client -broker=localhost:9092 -cmd=produce -topic=test -message="Direct broker"

# 设置日志文件（覆盖配置文件）
./go-queue-client -log=./logs/client.log -cmd=consume -topic=test -partition=0

# 批量发送消息
./go-queue-client -cmd=produce -topic=test -partition=0 -count=1000 -message="Batch message"

# 指定副本数和分区数创建主题
./go-queue-client -cmd=create-topic -topic=new-topic -partitions=5 -replicas=3

# 从指定偏移量开始消费
./go-queue-client -cmd=consume -topic=test -partition=0 -offset=100 -count=50
```

### 客户端启动参数说明

| 参数 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| `-config` | ❌ | configs/client.json | 配置文件路径 |
| `-cmd` | ❌ | - | 命令类型（覆盖配置文件） |
| `-topic` | ❌ | - | 主题名称（覆盖配置文件） |
| `-partition` | ❌ | -1 | 分区ID（覆盖配置文件） |
| `-message` | ❌ | - | 发送的消息内容（覆盖配置文件） |
| `-offset` | ❌ | -1 | 消费起始偏移量（覆盖配置文件） |
| `-count` | ❌ | -1 | 消息数量（覆盖配置文件） |
| `-broker` | ❌ | - | Broker 地址（覆盖配置文件） |
| `-log` | ❌ | - | 日志文件路径（覆盖配置文件） |
| `-partitions` | ❌ | 1 | 创建主题时的分区数 |
| `-replicas` | ❌ | 1 | 创建主题时的副本数 |

## 性能调优

### Broker 性能配置

```yaml
performance:
  # 批处理配置
  max_batch_size: 1000        # 最大批处理大小
  max_batch_bytes: 10485760   # 最大批处理字节数 (10MB)
  batch_timeout: "5ms"        # 批处理超时时间
  
  # 缓存配置
  write_cache_size: 67108864  # 写缓存大小 (64MB)
  read_cache_size: 134217728  # 读缓存大小 (128MB)
  
  # 连接池配置
  connection_pool_size: 200
  connection_idle_timeout: "10m"
  
  # 压缩配置
  compression_type: "snappy"     # 压缩算法: gzip, zlib, snappy, zstd
  compression_threshold: 1024    # 压缩阈值
```

### 客户端性能配置

```go
config := client.ClientConfig{
    // 连接池配置
    EnableConnectionPool: true,
    ConnectionPool: pool.ConnectionPoolConfig{
        MaxConnections:    50,
        MinConnections:    5,
        ConnectionTimeout: 3 * time.Second,
        IdleTimeout:       10 * time.Minute,
        MaxLifetime:       30 * time.Minute,
    },
    
    // 异步 I/O 配置
    EnableAsyncIO: true,
    AsyncIO: async.AsyncIOConfig{
        WorkerCount:    8,          // 工作线程数
        SQSize:         1024,       // 发送队列大小
        CQSize:         1024,       // 完成队列大小
        BatchSize:      200,        // 批处理大小
        PollTimeout:    5 * time.Millisecond,
        MaxConnections: 200,
    },
    
    // 批处理配置
    BatchSize:       100,
    BatchTimeout:    10 * time.Millisecond,
    MaxPendingBatch: 1000,
}
```

## 监控和运维

### 日志配置

```yaml
# Broker 日志配置
logging:
  level: "info"              # debug, info, warn, error
  file: "./logs/broker.log"
  max_size: 100              # MB
  max_backups: 10
  max_age: 30                # days
  compress: true
```

### 健康检查

```bash
# 检查 Broker 状态
curl http://localhost:9092/health

# 检查集群状态
curl http://localhost:9092/cluster/status

# 获取指标
curl http://localhost:9092/metrics
```

### 故障排查

1. **连接问题**
   - 检查 Broker 是否启动
   - 验证网络连通性
   - 检查防火墙设置

2. **性能问题**
   - 监控 CPU 和内存使用
   - 检查磁盘 I/O
   - 调整批处理参数

3. **数据一致性问题**
   - 检查 Raft 日志
   - 验证副本同步状态
   - 查看 Controller 日志

## 最佳实践

### 主题设计

1. **分区数量**: 根据并发需求设置，通常为消费者数量的 2-3 倍
2. **副本数量**: 生产环境建议至少 3 个副本
3. **命名规范**: 使用有意义的主题名称，如 `user.events`, `order.payments`

### 生产者最佳实践

1. **批量发送**: 使用批量 API 提高吞吐量
2. **异步发送**: 启用异步 I/O 提高性能
3. **错误处理**: 实现重试机制和错误回调
4. **分区策略**: 根据业务需求选择合适的分区策略

### 消费者最佳实践

1. **消费者组**: 使用消费者组实现负载均衡
2. **偏移量管理**: 及时提交偏移量避免重复消费
3. **异常处理**: 实现死信队列处理失败消息
4. **背压控制**: 根据处理能力调整拉取频率

### 运维最佳实践

1. **监控**: 设置关键指标监控和告警
2. **备份**: 定期备份重要数据
3. **升级**: 制定滚动升级策略
4. **容量规划**: 根据业务增长预估容量需求

## 常见问题

### Q: 如何保证消息不丢失？
A: 
1. 设置足够的副本数量（至少 3 个）
2. 启用同步复制
3. 生产者使用同步发送模式
4. 消费者及时提交偏移量

### Q: 如何处理消息重复？
A: 
1. 启用生产者幂等性
2. 消费者实现幂等处理逻辑
3. 使用事务消息保证精确一次语义

### Q: 如何提高性能？
A: 
1. 启用批处理和异步 I/O
2. 调整缓存大小
3. 使用压缩减少网络传输
4. 合理设置分区数量

### Q: 如何进行集群扩容？
A: 
1. 添加新的 Broker 节点
2. 重新平衡分区分配
3. 等待数据同步完成
4. 验证集群状态

## 总结

Go-Queue 提供了完整的分布式消息队列解决方案，支持多种高级特性和灵活的配置选项。通过合理的配置和使用最佳实践，可以构建高性能、高可用的消息系统。

更多详细信息请参考项目文档和示例代码。