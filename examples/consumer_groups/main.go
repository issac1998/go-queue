package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("=== Go Queue Consumer Groups Demo ===")

	// 创建客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	})

	// 1. 创建测试topic
	fmt.Println("\n1. Creating test topic...")
	admin := client.NewAdmin(c)
	createResult, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       "consumer-group-topic",
		Partitions: 4, // 创建4个分区用于演示
		Replicas:   1,
	})
	if err != nil {
		log.Printf("Failed to create topic: %v", err)
	} else if createResult.Error != nil {
		log.Printf("Topic creation error: %v", createResult.Error)
	} else {
		fmt.Printf("✓ Topic '%s' created successfully with 4 partitions!\n", createResult.Name)
	}

	// 2. 发送一些测试消息
	fmt.Println("\n2. Producing test messages...")
	producer := client.NewProducer(c)

	// 发送20条消息到不同分区
	for i := 0; i < 20; i++ {
		partition := int32(i % 4) // 轮询分配到4个分区
		message := fmt.Sprintf("Message %d for consumer groups", i+1)

		result, err := producer.Send(client.ProduceMessage{
			Topic:     "consumer-group-topic",
			Partition: partition,
			Value:     []byte(message),
		})
		if err != nil {
			log.Printf("Failed to send message %d: %v", i+1, err)
			continue
		}
		if result.Error != nil {
			log.Printf("Message %d send error: %v", i+1, result.Error)
			continue
		}

		fmt.Printf("  ✓ Sent message %d to partition %d (offset: %d)\n", i+1, partition, result.Offset)
	}

	// 等待消息持久化
	time.Sleep(100 * time.Millisecond)

	// 3. 创建消费者组
	fmt.Println("\n3. Creating consumer group with multiple consumers...")

	groupID := "demo-consumer-group"
	topics := []string{"consumer-group-topic"}

	// 创建3个消费者
	var wg sync.WaitGroup
	consumers := make([]*client.GroupConsumer, 3)

	for i := 0; i < 3; i++ {
		consumerID := fmt.Sprintf("consumer-%d", i+1)

		// 创建Group Consumer
		consumers[i] = client.NewGroupConsumer(c, client.GroupConsumerConfig{
			GroupID:        groupID,
			ConsumerID:     consumerID,
			Topics:         topics,
			SessionTimeout: 30 * time.Second,
		})

		// 加入消费者组
		fmt.Printf("  Consumer %s joining group...\n", consumerID)
		if err := consumers[i].JoinGroup(); err != nil {
			log.Printf("Consumer %s failed to join group: %v", consumerID, err)
			continue
		}

		// 查看分区分配
		assignment := consumers[i].GetAssignment()
		fmt.Printf("  ✓ Consumer %s joined! Assignment: %v\n", consumerID, assignment)
	}

	// 等待一下让分区分配稳定
	time.Sleep(2 * time.Second)

	// 4. 每个消费者开始消费消息
	fmt.Println("\n4. Starting message consumption...")

	// 用于统计消费的消息
	consumedMessages := make(map[string][]string) // consumerID -> messages
	var mu sync.Mutex

	for i, consumer := range consumers {
		wg.Add(1)
		consumerID := fmt.Sprintf("consumer-%d", i+1)

		go func(gc *client.GroupConsumer, cid string) {
			defer wg.Done()

			// 消息处理函数
			messageHandler := func(msg client.Message) error {
				mu.Lock()
				if consumedMessages[cid] == nil {
					consumedMessages[cid] = make([]string, 0)
				}
				consumedMessages[cid] = append(consumedMessages[cid], string(msg.Value))
				mu.Unlock()

				fmt.Printf("  [%s] Consumed: %s (Topic: %s, Partition: %d, Offset: %d)\n",
					cid, string(msg.Value), msg.Topic, msg.Partition, msg.Offset)
				return nil
			}

			// 开始订阅消费（这是阻塞的，在实际应用中可能需要更复杂的控制）
			fmt.Printf("  %s starting subscription...\n", cid)

			// 由于Subscribe是阻塞的，我们需要用goroutine控制消费时间
			done := make(chan struct{})
			go func() {
				time.Sleep(10 * time.Second) // 消费10秒
				close(done)
			}()

			// 这里我们模拟一个简化的消费循环
			assignment := gc.GetAssignment()
			regularConsumer := client.NewConsumer(c)

			for topic, partitions := range assignment {
				for _, partition := range partitions {
					go func(t string, p int32) {
						// 获取已提交的offset
						startOffset, err := gc.FetchCommittedOffset(t, p)
						if err != nil {
							startOffset = 0 // 如果没有提交的offset，从头开始
						}

						offset := startOffset
						for {
							select {
							case <-done:
								return
							default:
								// 拉取消息
								result, err := regularConsumer.FetchFrom(t, p, offset)
								if err != nil {
									log.Printf("Error fetching from %s:%d: %v", t, p, err)
									time.Sleep(100 * time.Millisecond)
									continue
								}

								if result.Error != nil {
									log.Printf("Server error for %s:%d: %v", t, p, result.Error)
									time.Sleep(100 * time.Millisecond)
									continue
								}

								// 处理消息
								for _, msg := range result.Messages {
									messageHandler(msg)
									offset = msg.Offset + 1

									// 提交offset
									gc.CommitOffset(t, p, offset, "")
								}

								// 如果没有消息，稍等
								if len(result.Messages) == 0 {
									time.Sleep(100 * time.Millisecond)
								}
							}
						}
					}(topic, partition)
				}
			}

			// 等待消费完成
			<-done
		}(consumer, consumerID)
	}

	// 5. 等待消费一段时间
	fmt.Println("\n  Consuming messages for 10 seconds...")
	wg.Wait()

	// 6. 显示消费统计
	fmt.Println("\n5. Consumption statistics:")
	totalConsumed := 0
	for consumerID, messages := range consumedMessages {
		fmt.Printf("  %s consumed %d messages\n", consumerID, len(messages))
		totalConsumed += len(messages)
	}
	fmt.Printf("  Total messages consumed: %d\n", totalConsumed)

	// 7. 模拟一个消费者离开
	fmt.Println("\n6. Simulating consumer leave and rebalance...")
	if len(consumers) > 0 {
		leavingConsumer := consumers[0]
		fmt.Printf("  Consumer consumer-1 leaving group...\n")

		if err := leavingConsumer.LeaveGroup(); err != nil {
			log.Printf("Failed to leave group: %v", err)
		} else {
			fmt.Printf("  ✓ Consumer consumer-1 left group successfully\n")
		}

		// 等待其他消费者重新平衡
		time.Sleep(2 * time.Second)

		// 显示剩余消费者的新分配
		for i := 1; i < len(consumers); i++ {
			consumerID := fmt.Sprintf("consumer-%d", i+1)
			assignment := consumers[i].GetAssignment()
			fmt.Printf("  %s new assignment after rebalance: %v\n", consumerID, assignment)
		}
	}

	// 8. 清理剩余消费者
	fmt.Println("\n7. Cleaning up remaining consumers...")
	for i := 1; i < len(consumers); i++ {
		consumerID := fmt.Sprintf("consumer-%d", i+1)
		fmt.Printf("  %s leaving group...\n", consumerID)

		if err := consumers[i].LeaveGroup(); err != nil {
			log.Printf("Consumer %s failed to leave group: %v", consumerID, err)
		} else {
			fmt.Printf("  ✓ %s left group successfully\n", consumerID)
		}
	}

	fmt.Println("\n=== Consumer Groups Demo Completed! ===")
	fmt.Println("\n✅ Demonstrated features:")
	fmt.Println("  - Consumer group creation and joining")
	fmt.Println("  - Automatic partition assignment (round-robin)")
	fmt.Println("  - Cooperative message consumption")
	fmt.Println("  - Offset tracking and committing")
	fmt.Println("  - Consumer heartbeat mechanism")
	fmt.Println("  - Dynamic rebalancing when consumers leave")
	fmt.Println("  - Graceful consumer group cleanup")
}
