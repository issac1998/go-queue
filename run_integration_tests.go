package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/issac1998/go-queue/client"
)

// 集成测试运行器
func main() {
	log.Println("=== Go-Queue 集成测试运行器 ===")

	// 检查broker是否运行
	if !checkBrokerAvailability() {
		log.Println("警告: Broker集群未运行，请先运行 ./test_setup.sh")
		log.Println("继续运行单元测试...")
	}

	// 运行单元测试
	log.Println("运行单元测试...")
	runUnitTests()

	// 如果broker可用，运行集成测试
	if checkBrokerAvailability() {
		log.Println("运行集成测试...")
		runIntegrationTests()
	}

	log.Println("=== 测试完成 ===")
}

func checkBrokerAvailability() bool {
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"},
		Timeout:     5 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 尝试发现Controller
	select {
	case <-ctx.Done():
		return false
	default:
	}

	err := c.DiscoverController()
	if err == nil {
		log.Printf("Broker集群可用，Controller发现成功")
		return true
	}

	return false
}

func runUnitTests() {
	tests := []string{
		"./client",
		"./internal/storage",
		"./internal/metadata",
		"./internal/protocol",
	}

	for _, testPath := range tests {
		log.Printf("运行 %s 单元测试...", testPath)
		cmd := exec.Command("go", "test", "-v", testPath)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			log.Printf("警告: %s 测试失败: %v", testPath, err)
		}
	}
}

func runIntegrationTests() {
	log.Println("运行集成测试...")
	cmd := exec.Command("go", "test", "-v", "./tests")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("集成测试失败: %v", err)
	}
}

func printTestSummary() { // intentionally kept for potential future use
	 fmt.Print(`
=== 测试要求说明 ===

1. 环境搭建:
	- 运行 chmod +x test_setup.sh && ./test_setup.sh 搭建测试环境
	- 包含3个etcd节点和3个broker节点

2. 测试覆盖:
	- 基础生产消费功能
	- 批量操作
	- 消费者组
	- Topic管理
	- 事务支持
	- 错误处理
	- 高负载场景
	- 集群弹性

3. 清理环境:
	- 运行 ./test_cleanup.sh 清理测试环境

4. 测试说明:
	- 所有测试使用真实的etcd和broker集群
	- 每个测试前会清理历史数据
	- 测试涵盖完整的消息队列功能
`)
}
