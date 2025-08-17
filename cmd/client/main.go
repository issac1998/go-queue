package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	var (
		broker    = flag.String("broker", "localhost:9092", "Broker address")
		command   = flag.String("cmd", "", "Command: create-topic, produce, consume")
		topic     = flag.String("topic", "", "Topic name")
		partition = flag.Int("partition", 0, "Partition ID")
		message   = flag.String("message", "", "Message to send")
		offset    = flag.Int64("offset", 0, "Consume start offset")
		count     = flag.Int("count", 1, "Message count")
		logFile   = flag.String("log", "", "Log file path (default output to console)")
	)
	flag.Parse()

	if *logFile != "" {
		file, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Failed to open log file %s: %v", *logFile, err)
		}
		defer file.Close()
		log.SetOutput(file)
		log.Printf("Client log output to file: %s", *logFile)
	}

	if *command == "" {
		printUsage()
		os.Exit(1)
	}

	log.Printf("Go Queue client starting - Broker: %s, Command: %s", *broker, *command)

	client := client.NewClient(client.ClientConfig{
		BrokerAddr: *broker,
		Timeout:    10 * time.Second,
	})

	switch *command {
	case "create-topic":
		createTopic(client, *topic)
	case "produce":
		produce(client, *topic, int32(*partition), *message, *count)
	case "consume":
		consume(client, *topic, int32(*partition), *offset)
	default:
		fmt.Printf("Unknown command: %s\n", *command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Go Queue Client Tool")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  Create topic: go run main.go -cmd=create-topic -topic=my-topic")
	fmt.Println("  Send message: go run main.go -cmd=produce -topic=my-topic -partition=0 -message='Hello World'")
	fmt.Println("  Batch send: go run main.go -cmd=produce -topic=my-topic -partition=0 -message='Hello' -count=5")
	fmt.Println("  Consume message: go run main.go -cmd=consume -topic=my-topic -partition=0 -offset=0")
	fmt.Println()
	fmt.Println("Parameters:")
	flag.PrintDefaults()
}

func createTopic(c *client.Client, topicName string) {
	if topicName == "" {
		log.Fatal("Please specify topic name")
	}

	log.Printf("Starting to create topic: %s", topicName)
	admin := client.NewAdmin(c)
	result, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
		Replicas:   1,
	})

	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	if result.Error != nil {
		log.Fatalf("Failed to create topic: %v", result.Error)
	}

	log.Printf("Topic '%s' created successfully", result.Name)
	fmt.Printf("Topic '%s' created successfully!\n", result.Name)
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
