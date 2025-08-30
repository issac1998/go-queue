# Go Queue 分布式消息队列

一个用Go语言实现的高性能分布式消息队列系统，提供类似Apache Kafka的功能。

## 🚀 特性

- **高性能消息存储**: Segment-based存储架构，支持快速读写
- **分区支持**: 每个Topic可包含多个分区，提供水平扩展能力
- **持久化**: 消息数据持久化到磁盘，保证数据不丢失
- **客户端SDK**: 提供易用的Go客户端库
- **TCP协议**: 自定义二进制协议，保证高性能通信
- **🆕 集群和高可用**: 基于Raft算法的集群模式，提供高可用性和数据一致性
- **🆕 消息副本同步**: 基于纯Raft算法的消息数据同步，无需传统ISR机制

## 📦 项目结构

```
go-queue/
├── cmd/
│   ├── broker/          # 消息队列服务端
│   └── client/          # 命令行客户端工具
├── pkg/
│   └── client/            # 客户端SDK
├── internal/
│   ├── metadata/       # 元数据管理
│   ├── protocol/       # 网络协议
│   └── storage/        # 存储引擎
└── examples/           # 使用示例
```

## 🔧 安装和使用

### 1. 启动服务端

```bash
# 编译并启动服务端
cd cmd/broker
go build -o broker
./broker

# 或者直接运行
go run cmd/broker/main.go
```

服务端将在 `localhost:9092` 启动。

### 2. 使用客户端SDK

#### 基本使用示例

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/issac1998/go-queue/pkg/client"
)

func main() {
    // 统一智能客户端（自动检测集群/单机模式）
    
    // 单机模式
    client := client.NewClient(client.ClientConfig{
        BrokerAddrs: []string{"localhost:9092"},
        Timeout:     5 * time.Second,
    })
    
    // 集群模式（自动启用负载均衡）
    // client := client.NewClient(client.ClientConfig{
    //     BrokerAddrs: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
    //     Timeout:     10 * time.Second,
    // })

    // 创建管理员客户端
    admin := client.NewAdmin(client)
    
    // 创建主题
    result, err := admin.CreateTopic(client.CreateTopicRequest{
        Name:       "my-topic",
        Partitions: 3,
        Replicas:   1,
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("主题创建成功: %s\n", result.Name)

    // 创建生产者
    producer := client.NewProducer(client)
    
    // 发送消息
    msg := client.ProduceMessage{
        Topic:     "my-topic",
        Partition: 0,
        Value:     []byte("Hello, Go Queue!"),
    }
    
    sendResult, err := producer.Send(msg)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("消息发送成功，Offset: %d\n", sendResult.Offset)

    // 创建消费者
    consumer := client.NewConsumer(client)
    
    // 拉取消息
    fetchResult, err := consumer.FetchFrom("my-topic", 0, 0)
    if err != nil {
        log.Fatal(err)
    }
    
    for _, msg := range fetchResult.Messages {
        fmt.Printf("收到消息: %s\n", string(msg.Value))
    }
}
```

#### 批量发送消息

```go
producer := client.NewProducer(client)

messages := []client.ProduceMessage{
    {Topic: "my-topic", Partition: 0, Value: []byte("消息1")},
    {Topic: "my-topic", Partition: 0, Value: []byte("消息2")},
    {Topic: "my-topic", Partition: 0, Value: []byte("消息3")},
}

result, err := producer.SendBatch(messages)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("批量发送成功，起始Offset: %d\n", result.Offset)
```

#### 订阅消息

```go
consumer := client.NewConsumer(client)

// 定义消息处理函数
handler := func(msg client.Message) error {
    fmt.Printf("处理消息: %s, Offset: %d\n", string(msg.Value), msg.Offset)
    return nil
}

// 订阅主题（阻塞式）
err := consumer.Subscribe("my-topic", 0, handler)
if err != nil {
    log.Fatal(err)
}
```

### 3. 使用命令行工具

```bash
cd cmd/client

# 创建主题
go run main.go -cmd=create-topic -topic=test-topic

# 发送单条消息
go run main.go -cmd=produce -topic=test-topic -partition=0 -message="Hello World"

# 批量发送消息
go run main.go -cmd=produce -topic=test-topic -partition=0 -message="Test" -count=5

# 消费消息
go run main.go -cmd=consume -topic=test-topic -partition=0 -offset=0
```

## 📖 API 文档

### Client

```go
type ClientConfig struct {
    BrokerAddrs []string      // Broker地址列表，单个地址表示单Broker模式，多个地址表示集群模式
    Timeout     time.Duration // 连接超时时间，默认5秒
}

func NewClient(config ClientConfig) *Client
```

### Producer

```go
type Producer struct {
    client *Client
}

type ProduceMessage struct {
    Topic     string
    Partition int32
    Value     []byte
}

type ProduceResult struct {
    Topic     string
    Partition int32
    Offset    int64
    Error     error
}

func NewProducer(client *Client) *Producer
func (p *Producer) Send(msg ProduceMessage) (*ProduceResult, error)
func (p *Producer) SendBatch(messages []ProduceMessage) (*ProduceResult, error)
```

### Consumer

```go
type Consumer struct {
    client *Client
}

type Message struct {
    Topic     string
    Partition int32
    Offset    int64
    Value     []byte
}

type FetchResult struct {
    Topic      string
    Partition  int32
    Messages   []Message
    NextOffset int64
    Error      error
}

func NewConsumer(client *Client) *Consumer
func (c *Consumer) FetchFrom(topic string, partition int32, offset int64) (*FetchResult, error)
func (c *Consumer) Subscribe(topic string, partition int32, handler func(Message) error) error
```

### Admin

```go
type Admin struct {
    client *Client
}

type CreateTopicRequest struct {
    Name       string
    Partitions int32
    Replicas   int32
}

type CreateTopicResult struct {
    Name  string
    Error error
}

func NewAdmin(client *Client) *Admin
func (a *Admin) CreateTopic(req CreateTopicRequest) (*CreateTopicResult, error)
```

### Consumer Groups (消费者组)

```go
// 创建消费者组消费者
groupConsumer := client.NewGroupConsumer(c, client.GroupConsumerConfig{
    GroupID:        "my-consumer-group",
    ConsumerID:     "consumer-1", 
    Topics:         []string{"my-topic"},
    SessionTimeout: 30 * time.Second,
})

// 加入消费者组
err := groupConsumer.JoinGroup()
if err != nil {
    log.Fatal(err)
}

// 查看分区分配
assignment := groupConsumer.GetAssignment()
fmt.Printf("Assigned partitions: %v\n", assignment)

// 提交offset
err = groupConsumer.CommitOffset("my-topic", 0, 100, "")
if err != nil {
    log.Fatal(err)
}

// 离开消费者组
defer groupConsumer.LeaveGroup()
```

## 🎯 完整示例

查看示例获取完整的使用示例：

```bash
# 基础功能示例
go run examples/simple/main.go

# Consumer Groups示例
go run examples/consumer_groups/main.go
```

## 🏗️ 集群模式

### 快速启动集群

```bash
# 1. 编译broker
go build -o broker cmd/broker/main.go

# 2. 启动3节点集群
./scripts/start-cluster.sh

# 3. 测试集群功能
./scripts/test-cluster.sh
```

### 集群配置示例

```json
{
  "cluster": {
    "enabled": true,
    "node_id": 1,
    "raft_address": "localhost:8001",
    "initial_members": [
      "localhost:8001",
      "localhost:8002", 
      "localhost:8003"
    ],
    "data_dir": "./raft-data/node1"
  }
}
```

详细的集群配置和使用说明请参考 [集群指南](CLUSTER_GUIDE.md)。

## 🔮 架构说明

### 存储架构
- **Segment**: 消息数据按Segment分片存储，每个Segment包含数据文件和索引文件
- **Partition**: 每个Topic可以有多个分区，分区内消息有序
- **Index**: 支持Offset索引和时间索引，实现快速查找

### 网络协议
- **TCP连接**: 客户端与服务端通过TCP连接通信
- **二进制协议**: 自定义二进制协议，支持版本控制
- **请求类型**: 支持Produce、Fetch、CreateTopic等请求类型

## 🚧 开发状态

当前实现的功能：
- ✅ 基础的生产者/消费者功能
- ✅ Topic和Partition管理
- ✅ 持久化存储
- ✅ 客户端SDK
- ✅ 命令行工具
- ✅ 消费者组（Consumer Groups）
- ✅ 自动分区分配和重平衡
- ✅ Offset管理和提交
- ✅ 心跳和故障检测
- ✅ **集群和高可用性** (基于Raft算法)
- ✅ **元数据一致性** (使用dragonboat实现)
- ✅ **自动Leader选举** (故障自动恢复)
- ✅ 消息数据副本同步

计划实现的功能：
- ⏳ 动态成员管理
- ⏳ HTTP API接口
- ⏳ 监控和度量指标
- ⏳ 客户端负载均衡

## 🤝 贡献

欢迎提交Issue和Pull Request来改进项目！

## �� 许可证

MIT License
