#!/bin/bash

echo "测试Go Queue集群功能..."

# 编译客户端工具
echo "编译客户端工具..."
go build -o client cmd/client/main.go

echo ""
echo "1. 创建topic (连接到节点1:9092)..."
./client -config=configs/client.json -cmd=create-topic -topic=cluster-test -broker=localhost:9092

echo ""
echo "2. 发送消息到节点1..."
./client -config=configs/client.json -cmd=produce -topic=cluster-test -partition=0 -message="Hello from Node 1" -broker=localhost:9092

echo ""
echo "3. 发送消息到节点2..."
./client -config=configs/client.json -cmd=produce -topic=cluster-test -partition=0 -message="Hello from Node 2" -broker=localhost:9093

echo ""
echo "4. 发送消息到节点3..."
./client -config=configs/client.json -cmd=produce -topic=cluster-test -partition=0 -message="Hello from Node 3" -broker=localhost:9094

echo ""
echo "5. 从节点1消费消息..."
./client -config=configs/client.json -cmd=consume -topic=cluster-test -partition=0 -offset=0 -broker=localhost:9092

echo ""
echo "6. 从节点2消费消息..."
./client -config=configs/client.json -cmd=consume -topic=cluster-test -partition=0 -offset=0 -broker=localhost:9093

echo ""
echo "7. 从节点3消费消息..."
./client -config=configs/client.json -cmd=consume -topic=cluster-test -partition=0 -offset=0 -broker=localhost:9094

echo ""
echo "集群测试完成！" 