package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/issac1998/go-queue/internal/broker"
	"github.com/issac1998/go-queue/internal/discovery"
	"github.com/issac1998/go-queue/internal/raft"
)

// panicHandler 全局 panic 处理器
func panicHandler(component string) {
	if r := recover(); r != nil {
		stack := debug.Stack()
		log.Printf("PANIC in %s: %v\nStack trace:\n%s", component, r, string(stack))
		
		// 记录到文件
		if logFile, err := os.OpenFile("panic.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err == nil {
			fmt.Fprintf(logFile, "[%s] PANIC in %s: %v\nStack trace:\n%s\n\n", 
				time.Now().Format("2006-01-02 15:04:05"), component, r, string(stack))
			logFile.Close()
		}
		
		// 如果是主 goroutine panic，退出程序
		if component == "main" {
			log.Printf("Main goroutine panic, exiting...")
			os.Exit(1)
		}
	}
}

// safeGoroutine 安全的 goroutine 包装器
func safeGoroutine(name string, fn func()) {
	go func() {
		defer panicHandler(name)
		fn()
	}()
}

func main() {
	// 主 goroutine panic 捕获
	defer panicHandler("main")
	
	nodeID := flag.String("node-id", "", "Unique node identifier")
	bindAddr := flag.String("bind-addr", "127.0.0.1", "Bind address")
	bindPort := flag.Int("bind-port", 9092, "Bind port")
	dataDir := flag.String("data-dir", "./data", "Data directory")
	logDir := flag.String("log-dir", "", "Log directory (if empty, logs to stdout)")
	raftAddr := flag.String("raft-addr", "", "Raft communication address")
	discoveryType := flag.String("discovery-type", "memory", "Discovery type (etcd, memory)")
	discoveryEndpoints := flag.String("discovery-endpoints", "", "Discovery endpoints (comma-separated)")
	enableFollowerRead := flag.Bool("enable-follower-read", false, "Enable follower read on this broker")
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("node-id is required")
	}

	// Generate a consistent NodeID from broker ID
	hashedNodeID := hashBrokerID(*nodeID)

	// Create broker configuration
	config := &broker.BrokerConfig{
		NodeID:   *nodeID,
		BindAddr: *bindAddr,
		BindPort: *bindPort,
		DataDir:  *dataDir,
		LogDir:   *logDir,
		RaftConfig: &raft.RaftConfig{
			NodeID:             hashedNodeID,
			RaftAddr:           *raftAddr,
			RTTMillisecond:     200,
			HeartbeatRTT:       5,
			ElectionRTT:        50,
			CheckQuorum:        true,
			SnapshotEntries:    10000,
			CompactionOverhead: 5000,
		},
		Discovery: &discovery.DiscoveryConfig{
			Type:      *discoveryType,
			Endpoints: parseEndpoints(*discoveryEndpoints),
		},
		Performance: &broker.PerformanceConfig{
			MaxBatchSize:       1000,
			MaxBatchBytes:      1024 * 1024, // 1MB
			ConnectionPoolSize: 100,
		},
		EnableFollowerRead: *enableFollowerRead, // Set follower read configuration
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Create and start broker
	b := &broker.Broker{
		ID:      config.NodeID,
		Address: config.BindAddr,
		Port:    config.BindPort,
		Config:  config,
		Ctx:     ctx,
		Cancel:  cancel,
	}

	log.Printf("Starting Multi-Raft Message Queue Broker %s...", b.ID)

	// 启动 broker（在安全的 goroutine 中）
	brokerDone := make(chan error, 1)
	safeGoroutine("broker-start", func() {
		if err := b.Start(); err != nil {
			brokerDone <- fmt.Errorf("broker start failed: %v", err)
		} else {
			brokerDone <- nil
		}
	})

	// 启动内存监控（在安全的 goroutine 中）
	safeGoroutine("memory-monitor", func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Printf("Memory Stats - Alloc: %d KB, Sys: %d KB, NumGC: %d", 
					m.Alloc/1024, m.Sys/1024, m.NumGC)
			case <-ctx.Done():
				return
			}
		}
	})

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Broker %s is running. Press Ctrl+C to exit.", b.ID)
	
	// 等待退出信号或 broker 错误
	select {
	case sig := <-sigChan:
		log.Printf("Received signal %v, shutting down...", sig)
		cancel()
		
		// 给 broker 一些时间优雅关闭
		shutdownTimer := time.NewTimer(30 * time.Second)
		select {
		case <-brokerDone:
			log.Println("Broker shutdown completed")
		case <-shutdownTimer.C:
			log.Println("Broker shutdown timeout, forcing exit")
		}
		
	case err := <-brokerDone:
		if err != nil {
			log.Printf("Broker error: %v", err)
			os.Exit(1)
		}
	}

	log.Printf("Broker %s shutdown complete", b.ID)
}

// parseEndpoints parses comma-separated endpoints string
func parseEndpoints(endpoints string) []string {
	if endpoints == "" {
		return nil
	}
	return strings.Split(endpoints, ",")
}

// hashBrokerID converts a broker ID string to a uint64 node ID
func hashBrokerID(brokerID string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(brokerID))
	return h.Sum64()
}
