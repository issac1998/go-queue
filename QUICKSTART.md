# Go-Queue 快速入门指南

## 🚀 5分钟快速上手

### 步骤 1: 编译项目

```bash
git clone https://github.com/issac1998/go-queue.git
cd go-queue
go build -o go-queue ./cmd/broker/main.go
```

### 步骤 2: 创建配置文件

创建 `broker.json`:
```json
{
  "data_dir": "./data",
  "max_topic_partitions": 100,
  "segment_size": 10485760,
  "retention_time": "24h",
  "max_message_size": 1048576,
  "compression_enabled": true,
  "compression_type": "gzip",
  "server": {
    "port": "9092"
  },
  "enable_follower_read": true
}
```

### 步骤 3: 启动 Broker

```bash
./go-queue -config=broker.json -broker-id=broker1 -raft-addr=localhost:7001 -client-addr=localhost:9092
```

### 步骤 4: 使用客户端

```go
// 创建 Topic
admin := client.NewAdmin(client.NewClient(client.ClientConfig{
    BrokerAddrs: []string{"localhost:9092"},
}))

admin.CreateTopic(client.CreateTopicRequest{
    Name: "test-topic",
    Partitions: 3,
    Replicas: 1,
})

// 生产消息
producer := client.NewProducer(client.NewClient(client.ClientConfig{
    BrokerAddrs: []string{"localhost:9092"},
}))

producer.ProduceMessage(client.ProduceRequest{
    Topic: "test-topic",
    Partition: 0,
    Messages: []client.Message{{Value: []byte("Hello World")}},
})

// 消费消息
consumer := client.NewConsumer(client.NewClient(client.ClientConfig{
    BrokerAddrs: []string{"localhost:9092"},
}))

result, _ := consumer.FetchFrom(client.FetchRequest{
    Topic: "test-topic",
    Partition: 0,
    Offset: 0,
})
```

## 🏗️ 架构概览

Go-Queue 是基于 Multi-Raft 的分布式消息队列：

- **Controller**: 负责集群元数据管理
- **Partition**: 每个分区是独立的 Raft 组
- **FollowerRead**: 支持从 Follower 读取提升性能

## 📋 主要功能

- ✅ 消息生产和消费
- ✅ Topic 和分区管理  
- ✅ 多副本高可用
- ✅ 自动 Leader 选举
- ✅ 消息压缩和去重
- ✅ 消费者组支持

## 🔧 配置选项

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `data_dir` | 数据存储目录 | `./data` |
| `segment_size` | 分段文件大小 | `10MB` |
| `retention_time` | 数据保留时间 | `24h` |
| `compression_enabled` | 启用压缩 | `true` |
| `enable_follower_read` | 启用 Follower 读 | `true` |

## 🚨 生产环境建议

1. **至少 3 个节点** 确保高可用
2. **SSD 存储** 提升 I/O 性能
3. **监控内存使用** 合理配置 JVM 参数
4. **网络延迟 < 10ms** 确保 Raft 性能
5. **定期备份** 数据目录

## 📞 帮助

- 查看 `PROJECT_DOCUMENTATION.md` 获取完整文档
- 通过 GitHub Issues 报告问题
- 参考 `examples/` 目录查看更多示例