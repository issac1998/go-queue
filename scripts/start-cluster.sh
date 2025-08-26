#!/bin/bash

# 创建必要的目录
mkdir -p logs
mkdir -p data-node1 data-node2 data-node3
mkdir -p raft-data/node1 raft-data/node2 raft-data/node3

echo "启动Go Queue集群..."

# 启动第一个节点
echo "启动节点1 (端口9092, Raft端口8001)..."
./broker -config=configs/broker-cluster-node1.json &
NODE1_PID=$!

# 等待一下
sleep 2

# 启动第二个节点
echo "启动节点2 (端口9093, Raft端口8002)..."
./broker -config=configs/broker-cluster-node2.json &
NODE2_PID=$!

# 等待一下
sleep 2

# 启动第三个节点
echo "启动节点3 (端口9094, Raft端口8003)..."
./broker -config=configs/broker-cluster-node3.json &
NODE3_PID=$!

echo "集群启动完成！"
echo "节点1 PID: $NODE1_PID (端口9092)"
echo "节点2 PID: $NODE2_PID (端口9093)"  
echo "节点3 PID: $NODE3_PID (端口9094)"
echo ""
echo "要停止集群，请运行: kill $NODE1_PID $NODE2_PID $NODE3_PID"
echo "或者运行: pkill -f broker"

# 等待用户输入以停止集群
echo ""
echo "按Enter键停止集群..."
read

echo "正在停止集群..."
kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null
wait

echo "集群已停止" 