#!/bin/bash

# Go-Queue 测试环境清理脚本
echo "=== Go-Queue 测试环境清理 ==="

# 停止broker进程
echo "停止broker进程..."
if [ -f pids/broker1.pid ]; then
    kill $(cat pids/broker1.pid) 2>/dev/null
    rm -f pids/broker1.pid
fi
if [ -f pids/broker2.pid ]; then
    kill $(cat pids/broker2.pid) 2>/dev/null
    rm -f pids/broker2.pid
fi
if [ -f pids/broker3.pid ]; then
    kill $(cat pids/broker3.pid) 2>/dev/null
    rm -f pids/broker3.pid
fi

# 停止etcd进程
echo "停止etcd进程..."
if [ -f pids/etcd1.pid ]; then
    kill $(cat pids/etcd1.pid) 2>/dev/null
    rm -f pids/etcd1.pid
fi
if [ -f pids/etcd2.pid ]; then
    kill $(cat pids/etcd2.pid) 2>/dev/null
    rm -f pids/etcd2.pid
fi
if [ -f pids/etcd3.pid ]; then
    kill $(cat pids/etcd3.pid) 2>/dev/null
    rm -f pids/etcd3.pid
fi

# 等待进程完全停止
sleep 2

# 强制清理可能残留的进程
pkill -f "etcd --name"
pkill -f "./bin/broker"

# 清理数据目录
echo "清理数据目录..."
rm -rf ./data*
rm -rf ./logs/*
rm -rf ./pids/*

echo "=== 测试环境清理完成 ===" 