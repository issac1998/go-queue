#!/bin/bash

set -e

echo "🚀 Go-Queue 统一客户端回归测试"
echo "================================"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 测试结果统计
TESTS_PASSED=0
TESTS_FAILED=0

# 日志函数
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
    ((TESTS_PASSED++))
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
    ((TESTS_FAILED++))
}

# 清理函数
cleanup() {
    log_info "清理测试环境..."
    pkill -f "go-queue" || true
    sleep 1
}

# 启动单机broker
start_single_broker() {
    log_info "启动单机broker (端口9092)..."
    cd /Users/a/go-queue
    go run cmd/broker/main.go -port=9092 -data-dir=/tmp/go-queue-test-single > /tmp/broker-single.log 2>&1 &
    SINGLE_BROKER_PID=$!
    sleep 3
    
    if ps -p $SINGLE_BROKER_PID > /dev/null; then
        log_success "单机broker启动成功 (PID: $SINGLE_BROKER_PID)"
        return 0
    else
        log_error "单机broker启动失败"
        return 1
    fi
}

# 启动集群brokers
start_cluster_brokers() {
    log_info "启动集群brokers..."
    cd /Users/a/go-queue
    
    # 启动三个broker节点
    go run cmd/broker/main.go -port=9092 -node-id=1 -cluster-mode=true \
        -raft-address=localhost:9100 -data-dir=/tmp/go-queue-cluster-1 \
        -initial-members="1:localhost:9100,2:localhost:9101,3:localhost:9102" > /tmp/broker-1.log 2>&1 &
    BROKER1_PID=$!
    
    go run cmd/broker/main.go -port=9093 -node-id=2 -cluster-mode=true \
        -raft-address=localhost:9101 -data-dir=/tmp/go-queue-cluster-2 \
        -initial-members="1:localhost:9100,2:localhost:9101,3:localhost:9102" > /tmp/broker-2.log 2>&1 &
    BROKER2_PID=$!
    
    go run cmd/broker/main.go -port=9094 -node-id=3 -cluster-mode=true \
        -raft-address=localhost:9102 -data-dir=/tmp/go-queue-cluster-3 \
        -initial-members="1:localhost:9100,2:localhost:9101,3:localhost:9102" > /tmp/broker-3.log 2>&1 &
    BROKER3_PID=$!
    
    sleep 5  # 等待集群初始化
    
    # 检查所有节点是否启动
    local failed=0
    for pid in $BROKER1_PID $BROKER2_PID $BROKER3_PID; do
        if ! ps -p $pid > /dev/null; then
            failed=1
        fi
    done
    
    if [ $failed -eq 0 ]; then
        log_success "集群brokers启动成功 (PIDs: $BROKER1_PID, $BROKER2_PID, $BROKER3_PID)"
        return 0
    else
        log_error "集群brokers启动失败"
        return 1
    fi
}

# 运行Go测试
run_go_tests() {
    log_info "运行Go单元测试..."
    cd /Users/a/go-queue
    
    if go test -v ./test/ -timeout=60s; then
        log_success "Go单元测试通过"
    else
        log_error "Go单元测试失败"
    fi
}

# 运行基准测试
run_benchmark() {
    log_info "运行性能基准测试..."
    cd /Users/a/go-queue
    
    if go test -bench=BenchmarkUnifiedClient -benchtime=5s ./test/ > /tmp/benchmark.log 2>&1; then
        log_success "基准测试完成"
        echo "基准测试结果:"
        cat /tmp/benchmark.log | grep -E "(Benchmark|ns/op|统计)"
    else
        log_error "基准测试失败"
        cat /tmp/benchmark.log
    fi
}

# 测试向后兼容性
test_compatibility() {
    log_info "测试向后兼容性..."
    cd /Users/a/go-queue
    
    # 创建临时测试文件
    cat > /tmp/compatibility_test.go << 'EOF'
package main

import (
    "log"
    "time"
    "github.com/issac1998/go-queue/client"
)

func main() {
    // 测试原有的单机配置方式
    c := client.NewClient(client.ClientConfig{
        BrokerAddrs: []string{"localhost:9092"},
        Timeout:    5 * time.Second,
    })
    
    stats := c.GetStats()
    log.Printf("兼容性测试: 集群模式=%v", stats.IsClusterMode)
    
    // 简单的producer测试
    producer := client.NewProducer(c)
    result, err := producer.Send(client.ProduceMessage{
        Topic:     "compatibility-topic",
        Partition: 0,
        Value:     []byte("compatibility-test"),
    })
    
    if err != nil {
        log.Fatalf("兼容性测试失败: %v", err)
    }
    
    log.Printf("兼容性测试成功: offset=%d", result.Offset)
}
EOF

    if go run /tmp/compatibility_test.go > /tmp/compatibility.log 2>&1; then
        log_success "向后兼容性测试通过"
        cat /tmp/compatibility.log
    else
        log_error "向后兼容性测试失败"
        cat /tmp/compatibility.log
    fi
    
    rm -f /tmp/compatibility_test.go
}

# 测试集群功能
test_cluster_features() {
    log_info "测试集群特有功能..."
    cd /Users/a/go-queue
    
    # 创建集群测试文件
    cat > /tmp/cluster_test.go << 'EOF'
package main

import (
    "log"
    "time"
    "github.com/issac1998/go-queue/client"
)

func main() {
    // 测试集群模式
    c := client.NewClient(client.ClientConfig{
        BrokerAddrs: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
        Timeout:     10 * time.Second,
    })
    
    time.Sleep(2 * time.Second) // 等待集群发现
    
    stats := c.GetStats()
    log.Printf("集群测试: 集群模式=%v, Leader=%s", stats.IsClusterMode, stats.CurrentLeader)
    
    if !stats.IsClusterMode {
        log.Fatalf("应该检测到集群模式")
    }
    
    // 测试读写分离
    producer := client.NewProducer(c)
    consumer := client.NewConsumer(c)
    
    // 写操作（应该路由到Leader）
    for i := 0; i < 5; i++ {
        _, err := producer.Send(client.ProduceMessage{
            Topic:     "cluster-topic",
            Partition: 0,
            Value:     []byte("cluster-test-message"),
        })
        if err != nil {
            log.Printf("写操作失败: %v", err)
        }
    }
    
    // 读操作（应该负载均衡）
    for i := 0; i < 5; i++ {
        _, err := consumer.Fetch(client.FetchRequest{
            Topic:     "cluster-topic",
            Partition: 0,
            Offset:    int64(i),
            MaxBytes:  1024,
        })
        if err != nil {
            log.Printf("读操作失败: %v", err)
        }
    }
    
    finalStats := c.GetStats()
    log.Printf("最终统计: 写请求=%d, 读请求=%d, Leader切换=%d", 
        finalStats.WriteRequests, finalStats.ReadRequests, finalStats.LeaderSwitches)
    
    log.Println("集群功能测试完成")
}
EOF

    if go run /tmp/cluster_test.go > /tmp/cluster.log 2>&1; then
        log_success "集群功能测试通过"
        cat /tmp/cluster.log
    else
        log_error "集群功能测试失败"
        cat /tmp/cluster.log
    fi
    
    rm -f /tmp/cluster_test.go
}

# 主测试流程
main() {
    echo "开始回归测试..."
    echo
    
    # 清理环境
    cleanup
    rm -rf /tmp/go-queue-*
    
    echo "=== 阶段1: 单机模式测试 ==="
    if start_single_broker; then
        sleep 2
        test_compatibility
        cleanup
    else
        log_error "无法启动单机broker，跳过单机测试"
    fi
    
    echo
    echo "=== 阶段2: 集群模式测试 ==="
    if start_cluster_brokers; then
        sleep 3
        test_cluster_features
        cleanup
    else
        log_error "无法启动集群brokers，跳过集群测试"
    fi
    
    echo
    echo "=== 阶段3: 单元测试 ==="
    # 重新启动单机broker进行单元测试
    if start_single_broker; then
        run_go_tests
        run_benchmark
        cleanup
    fi
    
    echo
    echo "=== 测试总结 ==="
    echo -e "通过: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "失败: ${RED}$TESTS_FAILED${NC}"
    
    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}🎉 所有测试通过！${NC}"
        exit 0
    else
        echo -e "${RED}💥 有测试失败，请检查日志${NC}"
        exit 1
    fi
}

# 信号处理
trap cleanup EXIT INT TERM

# 执行主函数
main "$@" 