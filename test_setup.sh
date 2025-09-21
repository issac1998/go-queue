#!/bin/bash

# Go-Queue 测试环境搭建脚本
echo "=== Go-Queue 测试环境搭建 ==="

# 清理历史数据
echo "清理历史数据..."
rm -rf ./data*
rm -rf ./logs/*
rm -rf ./pids/*

# 启动etcd集群
echo "启动etcd集群..."
ETCD_PATH="/Users/a/Downloads/etcd-v3.6.4-darwin-arm64"

# 启动3个etcd节点
$ETCD_PATH/etcd --name etcd1 --data-dir data1 \
  --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379 \
  --listen-peer-urls http://127.0.0.1:2380 \
  --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --initial-cluster etcd1=http://127.0.0.1:2380,etcd2=http://127.0.0.1:2382,etcd3=http://127.0.0.1:2384 \
  --initial-cluster-token etcd-cluster-1 \
  --initial-cluster-state new > logs/etcd1.log 2>&1 &
echo $! > pids/etcd1.pid

$ETCD_PATH/etcd --name etcd2 --data-dir data2 \
  --listen-client-urls http://127.0.0.1:2381 \
  --advertise-client-urls http://127.0.0.1:2381 \
  --listen-peer-urls http://127.0.0.1:2382 \
  --initial-advertise-peer-urls http://127.0.0.1:2382 \
  --initial-cluster etcd1=http://127.0.0.1:2380,etcd2=http://127.0.0.1:2382,etcd3=http://127.0.0.1:2384 \
  --initial-cluster-token etcd-cluster-1 \
  --initial-cluster-state new > logs/etcd2.log 2>&1 &
echo $! > pids/etcd2.pid

$ETCD_PATH/etcd --name etcd3 --data-dir data3 \
  --listen-client-urls http://127.0.0.1:2383 \
  --advertise-client-urls http://127.0.0.1:2383 \
  --listen-peer-urls http://127.0.0.1:2384 \
  --initial-advertise-peer-urls http://127.0.0.1:2384 \
  --initial-cluster etcd1=http://127.0.0.1:2380,etcd2=http://127.0.0.1:2382,etcd3=http://127.0.0.1:2384 \
  --initial-cluster-token etcd-cluster-1 \
  --initial-cluster-state new > logs/etcd3.log 2>&1 &
echo $! > pids/etcd3.pid

echo "等待etcd集群启动..."
sleep 5

# 启动broker集群
echo "启动broker集群..."

# 编译broker
go build -o bin/broker cmd/broker/main.go

# 启动3个broker节点
./bin/broker \
  --node-id=broker-1 \
  --bind-addr=127.0.0.1 \
  --bind-port=9092 \
  --data-dir=./data/broker-1 \
  --raft-addr=127.0.0.1:9192 \
  --discovery-type=etcd \
  --discovery-endpoints=http://127.0.0.1:2379,http://127.0.0.1:2381,http://127.0.0.1:2383 > logs/broker1.log 2>&1 &
echo $! > pids/broker1.pid

./bin/broker \
  --node-id=broker-2 \
  --bind-addr=127.0.0.1 \
  --bind-port=9093 \
  --data-dir=./data/broker-2 \
  --raft-addr=127.0.0.1:9193 \
  --discovery-type=etcd \
  --discovery-endpoints=http://127.0.0.1:2379,http://127.0.0.1:2381,http://127.0.0.1:2383 > logs/broker2.log 2>&1 &
echo $! > pids/broker2.pid

./bin/broker \
  --node-id=broker-3 \
  --bind-addr=127.0.0.1 \
  --bind-port=9094 \
  --data-dir=./data/broker-3 \
  --raft-addr=127.0.0.1:9194 \
  --discovery-type=etcd \
  --discovery-endpoints=http://127.0.0.1:2379,http://127.0.0.1:2381,http://127.0.0.1:2383 > logs/broker3.log 2>&1 &
echo $! > pids/broker3.pid

echo "等待broker集群启动..."
sleep 10

echo "=== 测试环境搭建完成 ==="
echo "etcd节点: http://127.0.0.1:2379, http://127.0.0.1:2381, http://127.0.0.1:2383"
echo "broker节点: 127.0.0.1:9092, 127.0.0.1:9093, 127.0.0.1:9094"
echo ""
echo "使用 ./test_cleanup.sh 清理测试环境" 