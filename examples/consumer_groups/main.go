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

	
	c := client.NewClient(client.ClientConfig{
		BrokerAddr: "localhost:9092",
		Timeout:    5 * time.Second,
	})

	
	fmt.Println("\n1. Creating test topic...")
	admin := client.NewAdmin(c)
	createResult, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       "consumer-group-topic",
		Partitions: 4, 
		Replicas:   1,
	})
	if err != nil {
		log.Printf("Failed to create topic: %v", err)
	} else if createResult.Error != nil {
		log.Printf("Topic creation error: %v", createResult.Error)
	} else {
		fmt.Printf("✓ Topic '%s' created successfully with 4 partitions!\n", createResult.Name)
	}

	
	fmt.Println("\n2. Producing test messages...")
	producer := client.NewProducer(c)

	
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

	
	fmt.Println("\n3. Creating consumer group with multiple consumers...")

	groupID := "demo-consumer-group"
	topics := []string{"consumer-group-topic"}

	
	var wg sync.WaitGroup
	consumers := make([]*client.GroupConsumer, 3)

	for i := 0; i < 3; i++ {
		consumerID := fmt.Sprintf("consumer-%d", i+1)

		consumers[i] = client.NewGroupConsumer(c, client.GroupConsumerConfig{
			GroupID:        groupID,
			ConsumerID:     consumerID,
			Topics:         topics,
			SessionTimeout: 30 * time.Second,
		})

		fmt.Printf("  Consumer %s joining group...\n", consumerID)
		if err := consumers[i].JoinGroup(); err != nil {
			log.Printf("Consumer %s failed to join group: %v", consumerID, err)
			continue
		}

		
		assignment := consumers[i].GetAssignment()
		fmt.Printf("  ✓ Consumer %s joined! Assignment: %v\n", consumerID, assignment)
	}

	
	time.Sleep(2 * time.Second)

	
	fmt.Println("\n4. Starting message consumption...")

	
	consumedMessages := make(map[string][]string) // consumerID -> messages
	var mu sync.Mutex

	for i, consumer := range consumers {
		wg.Add(1)
		consumerID := fmt.Sprintf("consumer-%d", i+1)

		go func(gc *client.GroupConsumer, cid string) {
			defer wg.Done()

			
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

			
			fmt.Printf("  %s starting subscription...\n", cid)

			
			done := make(chan struct{})
			go func() {
				time.Sleep(10 * time.Second) // 消费10秒
				close(done)
			}()

			assignment := gc.GetAssignment()
			regularConsumer := client.NewConsumer(c)

			for topic, partitions := range assignment {
				for _, partition := range partitions {
					go func(t string, p int32) {
						
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

								
								for _, msg := range result.Messages {
									messageHandler(msg)
									offset = msg.Offset + 1

									
									gc.CommitOffset(t, p, offset, "")
								}

							}
						}
					}(topic, partition)
				}
			}

			
			<-done
		}(consumer, consumerID)
	}

	
	fmt.Println("\n  Consuming messages for 10 seconds...")
	wg.Wait()

	
	fmt.Println("\n5. Consumption statistics:")
	totalConsumed := 0
	for consumerID, messages := range consumedMessages {
		fmt.Printf("  %s consumed %d messages\n", consumerID, len(messages))
		totalConsumed += len(messages)
	}
	fmt.Printf("  Total messages consumed: %d\n", totalConsumed)

	
	fmt.Println("\n6. Simulating consumer leave and rebalance...")
	if len(consumers) > 0 {
		leavingConsumer := consumers[0]
		fmt.Printf("  Consumer consumer-1 leaving group...\n")

		if err := leavingConsumer.LeaveGroup(); err != nil {
			log.Printf("Failed to leave group: %v", err)
		} else {
			fmt.Printf("  ✓ Consumer consumer-1 left group successfully\n")
		}

		
		time.Sleep(2 * time.Second)

		
		for i := 1; i < len(consumers); i++ {
			consumerID := fmt.Sprintf("consumer-%d", i+1)
			assignment := consumers[i].GetAssignment()
			fmt.Printf("  %s new assignment after rebalance: %v\n", consumerID, assignment)
		}
	}

	
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
