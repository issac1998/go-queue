# 集群模式消费者组 (Cluster Consumer Groups)

Go Queue现在完全支持集群模式下的消费者组功能！这是分布式消息队列的高级特性，提供跨节点的消费者组协调和一致性保证。

## 🌟 主要特性

### 集群模式增强
- **Raft一致性**: 所有消费者组状态通过Raft协议同步到集群所有节点
- **Leader协调**: 只有集群Leader节点执行消费者组管理操作
- **故障恢复**: 节点故障时自动从其他节点恢复消费者组状态
- **分布式重平衡**: 跨节点的分区重新分配

### 核心功能
- **跨节点消费者组**: 消费者可以连接到集群中的任意节点
- **一致性保证**: 消费者组状态在所有节点间保持一致
- **Offset同步**: 消费进度通过Raft同步，确保不丢失
- **自动故障转移**: Leader节点故障时自动选择新Leader

## 🏗️ 架构设计

### 集群组件层次

```
┌─────────────────────────────────────────────────────────┐
│                    客户端层                              │
│  GroupConsumer -> 任意Broker节点                         │
└─────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────┐
│                   协议处理层                             │
│  HandleJoinGroupRequest (支持集群模式)                   │
│  HandleLeaveGroupRequest / HandleHeartbeatRequest       │
└─────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────┐
│                  集群管理层                              │
│  ClusterManager.JoinConsumerGroup()                    │
│  ClusterManager.LeaveConsumerGroup()                   │
│  ClusterManager.RebalanceConsumerGroupPartitions()     │
└─────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────┐
│                  Raft状态机                              │
│  ClusterStateMachine                                   │
│  - consumerGroups (一致性存储)                           │
│  - groupOffsets (一致性存储)                             │
└─────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────┐
│                   Raft协议                               │
│  跨节点同步 & Leader选举                                 │
└─────────────────────────────────────────────────────────┘
```

## 📊 操作类型

### 新增的Raft操作
- `OpJoinConsumerGroup (7)`: 消费者加入组
- `OpLeaveConsumerGroup (8)`: 消费者离开组  
- `OpRebalancePartitions (9)`: 分区重平衡
- `OpCommitOffset (10)`: 提交消费进度
- `OpHeartbeatConsumer (11)`: 消费者心跳

## 🔧 使用方法

### 1. 启动集群

创建3个broker配置文件:

**configs/broker1.json**:
```json
{
  "server": {
    "port": "9092",
    "log_file": "logs/broker1.log"
  },
  "config": {
    "data_dir": "data/broker1"
  },
  "cluster": {
    "enabled": true,
    "node_id": 1,
    "raft_address": "localhost:8091",
    "data_dir": "raft_data/node1",
    "peers": [
      {"id": 1, "address": "localhost:8091"},
      {"id": 2, "address": "localhost:8092"},
      {"id": 3, "address": "localhost:8093"}
    ]
  }
}
```

**configs/broker2.json** (修改端口和节点ID):
```json
{
  "server": {
    "port": "9093",
    "log_file": "logs/broker2.log"
  },
  "config": {
    "data_dir": "data/broker2"
  },
  "cluster": {
    "enabled": true,
    "node_id": 2,
    "raft_address": "localhost:8092",
    "data_dir": "raft_data/node2",
    "peers": [
      {"id": 1, "address": "localhost:8091"},
      {"id": 2, "address": "localhost:8092"},
      {"id": 3, "address": "localhost:8093"}
    ]
  }
}
```

**configs/broker3.json** (修改端口和节点ID):
```json
{
  "server": {
    "port": "9094", 
    "log_file": "logs/broker3.log"
  },
  "config": {
    "data_dir": "data/broker3"
  },
  "cluster": {
    "enabled": true,
    "node_id": 3,
    "raft_address": "localhost:8093",
    "data_dir": "raft_data/node3",
    "peers": [
      {"id": 1, "address": "localhost:8091"},
      {"id": 2, "address": "localhost:8092"},
      {"id": 3, "address": "localhost:8093"}
    ]
  }
}
```

### 2. 启动Broker节点

```bash
# 终端1
./bin/broker -config configs/broker1.json

# 终端2  
./bin/broker -config configs/broker2.json

# 终端3
./bin/broker -config configs/broker3.json
```

### 3. 集群模式消费者组示例

```go
package main

import (
    "log"
    "time"
    "github.com/issac1998/go-queue/client"
)

func main() {
    // 连接到集群
    c := client.NewClient(client.ClientConfig{
        BrokerAddrs: []string{
            "localhost:9092", 
            "localhost:9093", 
            "localhost:9094",
        },
        Timeout: 10 * time.Second,
    })

    // 创建消费者组
    groupConsumer := client.NewGroupConsumer(c, client.GroupConsumerConfig{
        GroupID:        "my-cluster-group",
        ConsumerID:     "consumer-1",
        Topics:         []string{"my-topic"},
        SessionTimeout: 30 * time.Second,
    })

    // 加入组 (会自动路由到Leader节点)
    err := groupConsumer.JoinGroup()
    if err != nil {
        log.Fatalf("Failed to join group: %v", err)
    }

    // 启动心跳
    err = groupConsumer.StartHeartbeat()
    if err != nil {
        log.Fatalf("Failed to start heartbeat: %v", err)
    }

    // 消费消息
    for {
        messages, err := groupConsumer.Subscribe()
        if err != nil {
            log.Printf("Subscribe error: %v", err)
            continue
        }

        for _, msg := range messages {
            log.Printf("Consumed: %s", string(msg.Value))
            
            // 提交offset (同步到集群)
            err = groupConsumer.CommitOffset(msg.Topic, msg.Partition, msg.Offset+1)
            if err != nil {
                log.Printf("Failed to commit offset: %v", err)
            }
        }
    }
}
```

## 🔄 集群模式vs单机模式

### 单机模式
- 消费者组状态存储在单个节点
- 节点故障 = 数据丢失
- 简单快速，适合开发测试

### 集群模式  
- 消费者组状态通过Raft复制
- 自动故障恢复
- 强一致性保证
- 适合生产环境

## ⚡ 性能特性

### 一致性保证
- **强一致性**: 所有消费者组操作通过Raft达成一致
- **Leader写入**: 只有Leader节点执行写操作，避免冲突
- **读取优化**: 可以从任意节点读取状态

### 故障处理
- **节点故障**: 自动从其他节点恢复
- **网络分区**: Raft算法确保数据一致性
- **Leader故障**: 自动选举新Leader继续服务

## 📈 监控和调试

### 日志信息
```bash
# 查看消费者组操作日志
tail -f logs/broker1.log | grep "JoinGroup\|LeaveGroup\|Heartbeat"

# 查看Raft状态
tail -f logs/broker1.log | grep "raft\|cluster"
```

### 状态查询
集群模式下可以通过任意节点查询消费者组状态，所有节点保持一致。

## 🚀 最佳实践

### 1. 集群配置
- 至少3个节点 (支持1个节点故障)
- 奇数个节点 (避免脑裂)
- 合理的心跳间隔 (建议30s)

### 2. 客户端配置
- 配置多个broker地址实现故障转移
- 合理的超时时间
- 适当的重试机制

### 3. 监控指标
- Raft Leader状态
- 消费者组数量和状态
- Offset提交延迟
- 节点健康状况

## 🔧 故障排除

### 常见问题

1. **"only leader can handle consumer group operations"**
   - 当前节点不是Leader，请等待Leader选举完成

2. **连接超时**
   - 检查集群节点是否正常启动
   - 验证网络连接

3. **消费者组操作失败**
   - 确认集群状态健康
   - 检查Raft日志

### 调试命令
```bash
# 检查进程
ps aux | grep broker

# 检查端口占用
netstat -tlnp | grep :909

# 查看数据目录
ls -la data/ raft_data/
```

## 🎯 总结

集群模式消费者组提供了：
- **高可用性**: 节点故障不影响服务
- **强一致性**: Raft协议保证数据一致性
- **自动恢复**: 故障自动检测和恢复
- **横向扩展**: 支持更多消费者和更高吞吐量

这使得Go Queue成为真正的企业级分布式消息队列！🎉 