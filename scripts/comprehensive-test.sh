#!/bin/bash

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 测试结果统计
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((PASSED_TESTS++))
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((FAILED_TESTS++))
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# 测试函数
run_test() {
    local test_name="$1"
    local test_command="$2"
    
    ((TOTAL_TESTS++))
    log_info "运行测试: $test_name"
    
    if eval "$test_command"; then
        log_success "$test_name"
        return 0
    else
        log_error "$test_name"
        return 1
    fi
}

# 清理函数
cleanup() {
    log_info "清理测试环境..."
    pkill -f broker 2>/dev/null || true
    pkill -f client 2>/dev/null || true
    sleep 2
    
    # 清理数据目录
    rm -rf data* raft-data* logs/*.log 2>/dev/null || true
}

# 等待服务启动
wait_for_service() {
    local port=$1
    local timeout=30
    local count=0
    
    log_info "等待端口 $port 启动..."
    while ! nc -z localhost $port 2>/dev/null; do
        sleep 1
        ((count++))
        if [ $count -gt $timeout ]; then
            log_error "端口 $port 启动超时"
            return 1
        fi
    done
    log_success "端口 $port 已启动"
}

# 编译测试
test_compilation() {
    log_info "开始编译测试..."
    
    run_test "编译broker" "go build -o broker cmd/broker/main.go"
    run_test "编译client" "go build -o client cmd/client/main.go"
    run_test "编译consumer_groups示例" "go build -o consumer_groups examples/consumer_groups/main.go"
    run_test "编译compression_dedup示例" "go build -o compression_dedup examples/compression_dedup/main.go"
}

# 单机模式测试
test_standalone_mode() {
    log_info "开始单机模式测试..."
    
    # 启动单机broker
    log_info "启动单机broker..."
    ./broker -config=configs/broker.json > logs/standalone.log 2>&1 &
    BROKER_PID=$!
    
    if ! wait_for_service 9092; then
        kill $BROKER_PID 2>/dev/null || true
        return 1
    fi
    
    # 基础功能测试
    run_test "创建topic" "./client -config=configs/client.json -cmd=create-topic -topic=test-topic"
    run_test "生产消息" "./client -config=configs/client.json -cmd=produce -topic=test-topic -partition=0 -message='Hello World'"
    run_test "消费消息" "./client -config=configs/client.json -cmd=consume -topic=test-topic -partition=0 -offset=0"
    
    # 多分区测试
    run_test "创建多分区topic" "./client -config=configs/client.json -cmd=create-topic -topic=multi-partition -partitions=3"
    run_test "分区0生产消息" "./client -config=configs/client.json -cmd=produce -topic=multi-partition -partition=0 -message='Partition 0'"
    run_test "分区1生产消息" "./client -config=configs/client.json -cmd=produce -topic=multi-partition -partition=1 -message='Partition 1'"
    run_test "分区2生产消息" "./client -config=configs/client.json -cmd=produce -topic=multi-partition -partition=2 -message='Partition 2'"
    
    # 批量消息测试
    log_info "批量消息测试..."
    for i in {1..10}; do
        ./client -config=configs/client.json -cmd=produce -topic=test-topic -partition=0 -message="Batch message $i" >/dev/null
    done
    run_test "批量消息生产" "true"  # 上面的循环如果成功就算通过
    
    # 清理
    kill $BROKER_PID 2>/dev/null || true
    wait
}

# 集群模式测试
test_cluster_mode() {
    log_info "开始集群模式测试..."
    
    # 创建必要目录
    mkdir -p logs data-node1 data-node2 data-node3 raft-data/node1 raft-data/node2 raft-data/node3
    
    # 启动集群
    log_info "启动集群节点..."
    ./broker -config=configs/broker-cluster-node1.json > logs/node1.log 2>&1 &
    NODE1_PID=$!
    
    ./broker -config=configs/broker-cluster-node2.json > logs/node2.log 2>&1 &
    NODE2_PID=$!
    
    ./broker -config=configs/broker-cluster-node3.json > logs/node3.log 2>&1 &
    NODE3_PID=$!
    
    # 等待所有节点启动
    if ! wait_for_service 9092 || ! wait_for_service 9093 || ! wait_for_service 9094; then
        kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true
        return 1
    fi
    
    # 等待Raft集群稳定
    log_info "等待Raft集群稳定..."
    sleep 5
    
    # 集群功能测试
    run_test "节点1创建topic" "./client -config=configs/client.json -cmd=create-topic -topic=cluster-test -broker=localhost:9092"
    run_test "节点1生产消息" "./client -config=configs/client.json -cmd=produce -topic=cluster-test -partition=0 -message='From Node 1' -broker=localhost:9092"
    run_test "节点2生产消息" "./client -config=configs/client.json -cmd=produce -topic=cluster-test -partition=0 -message='From Node 2' -broker=localhost:9093"
    run_test "节点3生产消息" "./client -config=configs/client.json -cmd=produce -topic=cluster-test -partition=0 -message='From Node 3' -broker=localhost:9094"
    
    # 从不同节点消费测试
    run_test "从节点1消费" "./client -config=configs/client.json -cmd=consume -topic=cluster-test -partition=0 -offset=0 -broker=localhost:9092"
    run_test "从节点2消费" "./client -config=configs/client.json -cmd=consume -topic=cluster-test -partition=0 -offset=0 -broker=localhost:9093"
    run_test "从节点3消费" "./client -config=configs/client.json -cmd=consume -topic=cluster-test -partition=0 -offset=0 -broker=localhost:9094"
    
    # 故障恢复测试
    log_info "故障恢复测试..."
    log_info "杀死节点3 (PID: $NODE3_PID)..."
    kill $NODE3_PID 2>/dev/null || true
    sleep 3
    
    run_test "故障后节点1仍可服务" "./client -config=configs/client.json -cmd=produce -topic=cluster-test -partition=0 -message='After failure' -broker=localhost:9092"
    run_test "故障后节点2仍可服务" "./client -config=configs/client.json -cmd=produce -topic=cluster-test -partition=0 -message='After failure' -broker=localhost:9093"
    
    # 重启故障节点
    log_info "重启故障节点..."
    ./broker -config=configs/broker-cluster-node3.json > logs/node3-restart.log 2>&1 &
    NODE3_PID=$!
    
    if wait_for_service 9094; then
        sleep 3
        run_test "故障恢复后节点3可服务" "./client -config=configs/client.json -cmd=produce -topic=cluster-test -partition=0 -message='After recovery' -broker=localhost:9094"
    fi
    
    # 清理
    kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true
    wait
}

# 消费者组测试
test_consumer_groups() {
    log_info "开始消费者组测试..."
    
    # 启动broker
    ./broker -config=configs/broker.json > logs/consumer-group-test.log 2>&1 &
    BROKER_PID=$!
    
    if ! wait_for_service 9092; then
        kill $BROKER_PID 2>/dev/null || true
        return 1
    fi
    
    # 创建topic
    ./client -config=configs/client.json -cmd=create-topic -topic=consumer-group-test -partitions=3
    
    # 生产一些消息
    for i in {1..9}; do
        partition=$((($i - 1) % 3))
        ./client -config=configs/client.json -cmd=produce -topic=consumer-group-test -partition=$partition -message="Message $i" >/dev/null
    done
    
    # 启动消费者组示例（后台运行）
    timeout 10s ./consumer_groups > logs/consumer-groups-output.log 2>&1 &
    CG_PID=$!
    
    sleep 5
    
    # 检查消费者组是否正常工作
    if kill -0 $CG_PID 2>/dev/null; then
        run_test "消费者组运行正常" "true"
        kill $CG_PID 2>/dev/null || true
    else
        run_test "消费者组运行正常" "false"
    fi
    
    # 清理
    kill $BROKER_PID 2>/dev/null || true
    wait
}

# 压缩和去重测试
test_compression_deduplication() {
    log_info "开始压缩和去重测试..."
    
    # 启动broker
    ./broker -config=configs/broker.json > logs/comp-dedup-test.log 2>&1 &
    BROKER_PID=$!
    
    if ! wait_for_service 9092; then
        kill $BROKER_PID 2>/dev/null || true
        return 1
    fi
    
    # 运行压缩去重示例
    timeout 10s ./compression_dedup > logs/compression-dedup-output.log 2>&1
    
    run_test "压缩去重示例执行" "[ $? -eq 0 ] || [ $? -eq 124 ]"  # 0=正常退出, 124=timeout
    
    # 清理
    kill $BROKER_PID 2>/dev/null || true
    wait
}

# 性能测试
test_performance() {
    log_info "开始性能测试..."
    
    ./broker -config=configs/broker.json > logs/performance-test.log 2>&1 &
    BROKER_PID=$!
    
    if ! wait_for_service 9092; then
        kill $BROKER_PID 2>/dev/null || true
        return 1
    fi
    
    # 创建测试topic
    ./client -config=configs/client.json -cmd=create-topic -topic=perf-test
    
    # 批量生产测试
    log_info "批量生产性能测试..."
    start_time=$(date +%s)
    for i in {1..1000}; do
        ./client -config=configs/client.json -cmd=produce -topic=perf-test -partition=0 -message="Performance test message $i" >/dev/null
    done
    end_time=$(date +%s)
    
    duration=$((end_time - start_time))
    log_info "生产1000条消息耗时: ${duration}秒"
    
    run_test "性能测试完成" "[ $duration -lt 60 ]"  # 应该在60秒内完成
    
    # 清理
    kill $BROKER_PID 2>/dev/null || true
    wait
}

# 错误处理测试
test_error_handling() {
    log_info "开始错误处理测试..."
    
    ./broker -config=configs/broker.json > logs/error-test.log 2>&1 &
    BROKER_PID=$!
    
    if ! wait_for_service 9092; then
        kill $BROKER_PID 2>/dev/null || true
        return 1
    fi
    
    # 测试不存在的topic
    if ./client -config=configs/client.json -cmd=consume -topic=non-existent -partition=0 -offset=0 2>/dev/null; then
        run_test "不存在topic的错误处理" "false"
    else
        run_test "不存在topic的错误处理" "true"
    fi
    
    # 测试无效分区
    ./client -config=configs/client.json -cmd=create-topic -topic=error-test
    if ./client -config=configs/client.json -cmd=produce -topic=error-test -partition=999 -message="test" 2>/dev/null; then
        run_test "无效分区的错误处理" "false"
    else
        run_test "无效分区的错误处理" "true"
    fi
    
    # 清理
    kill $BROKER_PID 2>/dev/null || true
    wait
}

# 主测试流程
main() {
    log_info "开始Go Queue完整功能测试..."
    log_info "=================================================="
    
    # 初始化
    cleanup
    mkdir -p logs
    
    # 运行所有测试
    test_compilation
    test_standalone_mode
    test_cluster_mode
    test_consumer_groups
    test_compression_deduplication
    test_performance
    test_error_handling
    
    # 最终清理
    cleanup
    
    # 测试结果汇总
    log_info "=================================================="
    log_info "测试结果汇总:"
    log_info "总测试数: $TOTAL_TESTS"
    log_success "通过: $PASSED_TESTS"
    log_error "失败: $FAILED_TESTS"
    
    if [ $FAILED_TESTS -eq 0 ]; then
        log_success "🎉 所有测试通过！"
        exit 0
    else
        log_error "❌ 有测试失败，请检查日志"
        exit 1
    fi
}

# 信号处理
trap cleanup EXIT

# 检查依赖
if ! command -v nc &> /dev/null; then
    log_warning "nc命令不存在，某些网络检查可能失败"
fi

# 运行主测试
main "$@" 