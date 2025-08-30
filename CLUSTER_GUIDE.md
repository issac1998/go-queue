# Go Queue 集群和高可用性指南

## 🎯 概述

Go Queue 现在支持基于 Raft 算法的集群模式，提供高可用性和数据一致性保证。集群模式使用 [dragonboat](https://github.com/lni/dragonboat) 作为 Raft 实现。

## 🏗️ 架构设计

### 核心组件

1. **Raft 状态机** (`ClusterStateMachine`)
   - 管理集群元数据（broker信息、topic配置、分区分配）
   - 保证元数据的强一致性
   - 处理集群操作（创建topic、消息写入等）

2. **集群管理器** (`cluster.Manager`)
   - 初始化和管理 Raft 节点
   - 监控领导者变化
   - 处理集群操作的路由

3. **Broker 节点**
   - 每个 broker 是一个 Raft 节点
   - 存储本地数据和参与 Raft 协议
   - 提供客户端服务接口

### 数据一致性

- **元数据**: 使用 Raft 保证强一致性
- **消息数据**: 当前版本仍使用本地存储，后续版本将支持副本同步

## 🚀 快速开始

### 1. 配置文件

为每个节点创建配置文件，例如 `configs/broker-cluster-node1.json`:

```json
{
  "data_dir": "./data-node1",
  "max_topic_partitions": 16,
  "segment_size": 1073741824,
  "retention_time": "168h",
  // ... 其他配置 ...
  
  "server": {
    "port": "9092",
    "log_file": "logs/node1.log"
  },
  
  "cluster": {
    "enabled": true,
    "node_id": 1,
    "raft_address": "localhost:8001",
    "initial_members": [
      "localhost:8001",
      "localhost:8002", 
      "localhost:8003"
    ],
    "data_dir": "./raft-data/node1",
    "election_rtt": 10,
    "heartbeat_rtt": 1,
    "check_quorum": true,
    "snapshot_entries": 10000,
    "compaction_overhead": 5000
  }
}
```

### 2. 启动集群

```bash
# 方式1: 使用提供的脚本
./scripts/start-cluster.sh

# 方式2: 手动启动各节点
./broker -config=configs/broker-cluster-node1.json &
./broker -config=configs/broker-cluster-node2.json &
./broker -config=configs/broker-cluster-node3.json &
```

### 3. 测试集群

```bash
# 运行集群测试
./scripts/test-cluster.sh
```

## 📋 配置说明

### 集群配置参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `enabled` | 是否启用集群模式 | false |
| `node_id` | 节点ID（必须唯一） | - |
| `raft_address` | Raft通信地址 | - |
| `initial_members` | 初始成员列表 | - |
| `data_dir` | Raft数据目录 | - |
| `election_rtt` | 选举超时RTT | 10 |
| `heartbeat_rtt` | 心跳RTT | 1 |
| `check_quorum` | 是否检查法定人数 | true |
| `snapshot_entries` | 快照条目数 | 10000 |
| `compaction_overhead` | 压缩开销 | 5000 |

### 端口规划

- **业务端口**: 9092, 9093, 9094 (客户端连接)
- **Raft端口**: 8001, 8002, 8003 (节点间通信)

## 🔧 运维操作

### 查看集群状态

```bash
# 检查节点日志
tail -f logs/node1.log

# 查看Raft数据目录
ls -la raft-data/node1/
```

### 添加节点

1. 创建新节点的配置文件
2. 启动新节点
3. 通过集群API添加成员（功能开发中）

### 故障恢复

- **单节点故障**: 自动选举新Leader，服务继续
- **多节点故障**: 需要法定人数节点才能恢复服务
- **数据恢复**: 从Raft日志和快照自动恢复

## 📊 监控和诊断

### 日志监控

```bash
# 监控Leader选举
grep "became leader\|lost leadership" logs/*.log

# 监控Raft状态
grep "Raft cluster started" logs/*.log
```

### 性能指标

- Raft 日志条目数
- 快照频率
- 选举次数
- 网络延迟

## 🚧 当前限制

1. **数据副本**: 消息数据暂未实现跨节点副本
2. **动态成员**: 不支持动态添加/删除节点
3. **负载均衡**: 客户端需要手动选择连接的节点
4. **监控界面**: 缺少Web管理界面

## 🛣️ 后续规划

### 短期目标
- [ ] 消息数据副本同步
- [ ] 动态成员管理
- [ ] 客户端负载均衡
- [ ] 故障自动切换

### 长期目标
- [ ] 跨数据中心部署
- [ ] 性能监控仪表板
- [ ] 自动扩缩容
- [ ] 多租户支持

## 🤝 贡献

欢迎提交Issue和PR来改进集群功能！

## �� 许可证

MIT License 