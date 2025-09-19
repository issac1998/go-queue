package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/config"
)

func main() {
	var (
		configFile = flag.String("config", "configs/client.json", "Configuration file path")
		command    = flag.String("cmd", "", "Command: create-topic, produce, consume (overrides config)")
		topic      = flag.String("topic", "", "Topic name (overrides config)")
		partition  = flag.Int("partition", -1, "Partition ID (overrides config)")
		message    = flag.String("message", "", "Message to send (overrides config)")
		offset     = flag.Int64("offset", -1, "Consume start offset (overrides config)")
		count      = flag.Int("count", -1, "Message count (overrides config)")
		broker     = flag.String("broker", "", "Broker address (overrides config)")
		logFile    = flag.String("log", "", "Log file path (overrides config)")
		partitions = flag.Int("partitions", 1, "Number of partitions for topic creation")
		replicas   = flag.Int("replicas", 1, "Number of replicas for topic creation")
	)
	flag.Parse()

	clientConfig, err := config.LoadClientConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	if *broker != "" {
		clientConfig.BrokerAddrs = []string{*broker}
	}
	if *logFile != "" {
		clientConfig.LogFile = *logFile
	}
	if *command != "" {
		clientConfig.Command.Type = *command
	}
	if *topic != "" {
		clientConfig.Command.Topic = *topic
	}
	if *partition >= 0 {
		clientConfig.Command.Partition = *partition
	}
	if *message != "" {
		clientConfig.Command.Message = *message
	}
	if *offset >= 0 {
		clientConfig.Command.Offset = *offset
	}
	if *count >= 0 {
		clientConfig.Command.Count = *count
	}
	// Note: partitions and replicas are only used for topic creation
	// Don't override the partition setting here

	if clientConfig.LogFile != "" {
		file, err := os.OpenFile(clientConfig.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Failed to open log file %s: %v", clientConfig.LogFile, err)
		}
		defer file.Close()
		log.SetOutput(file)
		log.Printf("Client log output to file: %s", clientConfig.LogFile)
	}

	if clientConfig.Command.Type == "" {
		printUsage()
		os.Exit(1)
	}

	brokerAddrs := clientConfig.GetBrokerAddrs()
	log.Printf("Go Queue client starting - Brokers: %v, Command: %s",
		brokerAddrs, clientConfig.Command.Type)

	timeout, err := clientConfig.GetTimeoutDuration()
	if err != nil {
		log.Fatalf("Invalid timeout configuration: %v", err)
	}

	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: brokerAddrs,
		Timeout:     timeout,
	})

	switch clientConfig.Command.Type {
	case "create-topic":
		partitionCount := int32(1)
		replicaCount := int32(1)
		if *partitions > 0 {
			partitionCount = int32(*partitions)
		}
		if *replicas > 0 {
			replicaCount = int32(*replicas)
		}
		createTopic(c, clientConfig.Command.Topic, partitionCount, replicaCount)
	case "list-topics":
		listTopics(c)
	case "describe-topic":
		describeTopic(c, clientConfig.Command.Topic)
	case "delete-topic":
		deleteTopic(c, clientConfig.Command.Topic)
	case "topic-info":
		getTopicInfo(c, clientConfig.Command.Topic)
	case "produce":
		produce(c, clientConfig.Command.Topic, int32(clientConfig.Command.Partition),
			clientConfig.Command.Message, clientConfig.Command.Count)
	case "consume":
		consume(c, clientConfig.Command.Topic, int32(clientConfig.Command.Partition),
			clientConfig.Command.Offset)
	default:
		fmt.Printf("Unknown command: %s\n", clientConfig.Command.Type)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Go Queue Client Tool")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  With config file: go run main.go -config=configs/client.json")
	fmt.Println("  Override config: go run main.go -config=configs/client.json -cmd=produce -topic=my-topic")
	fmt.Println("  Command line only: go run main.go -cmd=create-topic -topic=my-topic -broker=localhost:9092")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  Topic Management:")
	fmt.Println("    create-topic: Create a new topic")
	fmt.Println("    list-topics: List all topics")
	fmt.Println("    describe-topic: Get detailed topic information")
	fmt.Println("    delete-topic: Delete a topic")
	fmt.Println("    topic-info: Get basic topic information")
	fmt.Println("  Message Operations:")
	fmt.Println("    produce: Send messages to a topic")
	fmt.Println("    consume: Consume messages from a topic")
	fmt.Println()
	fmt.Println("Parameters:")
	flag.PrintDefaults()
}

func createTopic(c *client.Client, topicName string, partitions int32, replicas int32) {
	if topicName == "" {
		log.Fatal("Please specify topic name")
	}

	// Set defaults if not specified
	if partitions <= 0 {
		partitions = 1
	}
	if replicas <= 0 {
		replicas = 1
	}

	log.Printf("Starting to create topic: %s with %d partitions and %d replicas", topicName, partitions, replicas)
	admin := client.NewAdmin(c)
	result, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: partitions,
		Replicas:   replicas,
	})

	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	if result.Error != nil {
		log.Fatalf("Failed to create topic: %v", result.Error)
	}

	log.Printf("Topic '%s' created successfully with %d partitions and %d replicas", result.Name, partitions, replicas)
	fmt.Printf("Topic '%s' created successfully with %d partitions and %d replicas!\n", result.Name, partitions, replicas)
}

func produce(c *client.Client, topicName string, partition int32, message string, count int) {
	if topicName == "" {
		log.Fatal("Please specify topic name")
	}
	if message == "" {
		log.Fatal("Please specify message to send")
	}

	log.Printf("Starting to send messages to topic: %s, partition: %d, count: %d", topicName, partition, count)
	producer := client.NewProducer(c)

	if count == 1 {
		msg := client.ProduceMessage{
			Topic:     topicName,
			Partition: partition,
			Value:     []byte(message),
		}

		log.Printf("Sending single message: %s", message)
		result, err := producer.Send(msg)
		if err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}

		if result.Error != nil {
			log.Fatalf("Failed to send message: %v", result.Error)
		}

		log.Printf("Message sent successfully! Topic: %s, Partition: %d, Offset: %d", result.Topic, result.Partition, result.Offset)
		fmt.Printf("Message sent successfully! Topic: %s, Partition: %d, Offset: %d\n",
			result.Topic, result.Partition, result.Offset)
	} else {
		messages := make([]client.ProduceMessage, count)
		for i := 0; i < count; i++ {
			messages[i] = client.ProduceMessage{
				Topic:     topicName,
				Partition: partition,
				Value:     []byte(fmt.Sprintf("%s-%d", message, i+1)),
			}
		}

		log.Printf("Sending batch of %d messages", count)
		result, err := producer.SendBatch(messages)
		if err != nil {
			log.Fatalf("Failed to send batch messages: %v", err)
		}

		if result.Error != nil {
			log.Fatalf("Failed to send batch messages: %v", result.Error)
		}

		log.Printf("Batch messages sent successfully! Topic: %s, Partition: %d, Start Offset: %d, Count: %d", result.Topic, result.Partition, result.Offset, count)
		fmt.Printf("Batch messages sent successfully! Topic: %s, Partition: %d, Start Offset: %d, Count: %d\n",
			result.Topic, result.Partition, result.Offset, count)
	}
}

func consume(c *client.Client, topicName string, partition int32, offset int64) {
	if topicName == "" {
		log.Fatal("Please specify topic name")
	}

	log.Printf("Starting to consume messages - Topic: %s, Partition: %d, Offset: %d", topicName, partition, offset)
	consumer := client.NewConsumer(c)

	fmt.Printf("Starting to consume messages from Topic: %s, Partition: %d, Offset: %d...\n",
		topicName, partition, offset)

	result, err := consumer.FetchFrom(topicName, partition, offset)
	if err != nil {
		log.Fatalf("Failed to fetch messages: %v", err)
	}

	if result.Error != nil {
		log.Fatalf("Failed to fetch messages: %v", result.Error)
	}

	if len(result.Messages) == 0 {
		log.Printf("No messages found")
		fmt.Println("No messages found")
		return
	}

	log.Printf("Successfully fetched %d messages", len(result.Messages))
	fmt.Printf("Successfully fetched %d messages:\n", len(result.Messages))
	fmt.Println(strings.Repeat("-", 60))

	for i, msg := range result.Messages {
		log.Printf("Message %d: Offset=%d, Content=%s, Length=%d bytes", i+1, msg.Offset, string(msg.Value), len(msg.Value))
		fmt.Printf("Message %d:\n", i+1)
		fmt.Printf("  Offset: %d\n", msg.Offset)
		fmt.Printf("  Content: %s\n", string(msg.Value))
		fmt.Printf("  Length: %d bytes\n", len(msg.Value))
		if i < len(result.Messages)-1 {
			fmt.Println()
		}
	}

	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Next Offset: %d\n", result.NextOffset)
	log.Printf("Consumption completed, next Offset: %d", result.NextOffset)
}

func listTopics(c *client.Client) {
	log.Printf("Listing all topics...")
	admin := client.NewAdmin(c)

	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("Failed to list topics: %v", err)
	}

	if len(topics) == 0 {
		fmt.Println("No topics found")
		return
	}

	fmt.Printf("Found %d topics:\n", len(topics))
	fmt.Println(strings.Repeat("=", 80))

	for i, topic := range topics {
		fmt.Printf("Topic %d: %s\n", i+1, topic.Name)
		fmt.Printf("  Partitions: %d\n", topic.Partitions)
		fmt.Printf("  Replicas: %d\n", topic.Replicas)

		fmt.Printf("  Created: %s\n", topic.CreatedAt.Format("2006-01-02 15:04:05"))
		if i < len(topics)-1 {
			fmt.Println(strings.Repeat("-", 40))
		}
	}
	fmt.Println(strings.Repeat("=", 80))
	log.Printf("Listed %d topics successfully", len(topics))
}

func describeTopic(c *client.Client, topicName string) {
	if topicName == "" {
		log.Fatal("Please specify topic name")
	}

	log.Printf("Describing topic: %s", topicName)
	admin := client.NewAdmin(c)

	topic, err := admin.GetTopicInfo(topicName)
	if err != nil {
		log.Fatalf("Failed to describe topic: %v", err)
	}

	fmt.Printf("Topic Details: %s\n", topic.Name)
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Basic Information:\n")
	fmt.Printf("  Name: %s\n", topic.Name)
	fmt.Printf("  Partitions: %d\n", topic.Partitions)
	fmt.Printf("  Replicas: %d\n", topic.Replicas)

	fmt.Printf("  Created At: %s\n", topic.CreatedAt.Format("2006-01-02 15:04:05"))

	fmt.Printf("\nPartition Details:\n")
	fmt.Println(strings.Repeat("-", 80))
	for _, partition := range topic.PartitionDetails {
		fmt.Printf("Partition %d:\n", partition.ID)
		fmt.Printf("  Leader: %d\n", partition.Leader)
		fmt.Printf("  Messages: %d\n", partition.MessageCount)
		fmt.Printf("  Size: %.2f KB\n", float64(partition.Size)/1024)
		fmt.Printf("  Offset Range: %d - %d\n", partition.StartOffset, partition.EndOffset)
		fmt.Printf("  Replicas: %v\n", partition.Replicas)
		fmt.Printf("  ISR: %v\n", partition.ISR)
		fmt.Println()
	}
	fmt.Println(strings.Repeat("=", 80))
	log.Printf("Topic %s described successfully", topicName)
}

func deleteTopic(c *client.Client, topicName string) {
	if topicName == "" {
		log.Fatal("Please specify topic name")
	}

	log.Printf("Deleting topic: %s", topicName)
	admin := client.NewAdmin(c)

	err := admin.DeleteTopic(topicName)
	if err != nil {
		log.Fatalf("Failed to delete topic: %v", err)
	}

	fmt.Printf("Topic '%s' deleted successfully!\n", topicName)
	log.Printf("Topic %s deleted successfully", topicName)
}

func getTopicInfo(c *client.Client, topicName string) {
	if topicName == "" {
		log.Fatal("Please specify topic name")
	}

	log.Printf("Getting topic info: %s", topicName)
	admin := client.NewAdmin(c)

	info, err := admin.GetTopicInfo(topicName)
	if err != nil {
		log.Fatalf("Failed to get topic info: %v", err)
	}

	fmt.Printf("Topic Information: %s\n", info.Name)
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("  Name: %s\n", info.Name)
	fmt.Printf("  Partitions: %d\n", info.Partitions)

	fmt.Println(strings.Repeat("=", 50))
	log.Printf("Topic info for %s retrieved successfully", topicName)
}

func parseTopicPartition(topicPartition string) (string, int32, error) {
	parts := strings.Split(topicPartition, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("format error, should be topic:partition")
	}

	topic := parts[0]
	partition, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("failed to parse partition ID: %v", err)
	}

	return topic, int32(partition), nil
}
