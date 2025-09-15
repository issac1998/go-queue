# Dead Letter Queue (DLQ) Implementation

This package provides a comprehensive Dead Letter Queue implementation for the go-queue message broker system.

## Features

- **Automatic Retry Logic**: Configurable retry policies with exponential backoff
- **Dead Letter Storage**: Failed messages are stored in dedicated DLQ topics
- **Message Tracking**: Complete tracking of message retry attempts and failure reasons
- **Statistics & Monitoring**: Real-time statistics and monitoring capabilities
- **Flexible Configuration**: Customizable retry policies and DLQ settings

## Core Components

### Manager
The main DLQ manager that handles failed message processing, retry logic, and dead letter storage.

### Types
- `DeadLetterMessage`: Represents a message in the dead letter queue
- `RetryPolicy`: Defines retry behavior (max retries, delays, backoff)
- `DLQConfig`: Configuration for DLQ functionality
- `FailureInfo`: Information about message processing failures
- `MessageRetryState`: Tracks retry state for individual messages
- `DLQStats`: Statistics about DLQ operations

### Client API
Client-side API for interacting with DLQ functionality, including message failure handling and retry management.

## Usage Example

```go
package main

import (
    "github.com/issac1998/go-queue/client"
    "github.com/issac1998/go-queue/internal/dlq"
)

func main() {
    // Create client
    client := client.NewClient(client.ClientConfig{
        BrokerAddrs: []string{"localhost:9092"},
        Timeout:     10 * time.Second,
    })
    defer client.Close()

    // Create DLQ manager
    dlqConfig := dlq.DefaultDLQConfig()
    dlqConfig.RetryPolicy.MaxRetries = 3
    dlqConfig.RetryPolicy.InitialDelay = 2 * time.Second
    
    dlqManager, err := dlq.NewManager(client, dlqConfig)
    if err != nil {
        log.Fatal(err)
    }

    // Handle failed message
    failureInfo := &dlq.FailureInfo{
        ConsumerGroup: "my-group",
        ConsumerID:    "consumer-1",
        ErrorMessage:  "Processing failed",
        FailureTime:   time.Now(),
    }

    err = dlqManager.HandleFailedMessage(message, failureInfo)
    if err != nil {
        log.Printf("Failed to handle message: %v", err)
    }

    // Check retry status
    shouldRetry := dlqManager.ShouldRetryMessage(topic, partition, offset, consumerGroup)
    if shouldRetry {
        delay := dlqManager.GetRetryDelay(topic, partition, offset, consumerGroup)
        // Wait for delay before retrying
    }

    // Get statistics
    stats := dlqManager.GetDLQStats()
    fmt.Printf("Total DLQ messages: %d\n", stats.TotalMessages)
}
```

## Configuration

### Default Retry Policy
- Max Retries: 3
- Initial Delay: 1 second
- Backoff Factor: 2.0
- Max Delay: 5 minutes

### Default DLQ Config
- Enabled: true
- Topic Suffix: ".dlq"
- Retention Period: 7 days
- Cleanup Interval: 1 hour

## Testing

Run the test suite:
```bash
go test ./internal/dlq/ -v
```

## Example Demo

See the complete example in `examples/dlq_demo/main.go` for a comprehensive demonstration of DLQ functionality.

## Architecture

The DLQ implementation follows these principles:

1. **Separation of Concerns**: Core logic, client API, and types are separated
2. **Configurable Policies**: Retry behavior is fully configurable
3. **Observability**: Comprehensive statistics and monitoring
4. **Error Handling**: Robust error handling with detailed failure information
5. **Performance**: Efficient message tracking and storage

## Integration

The DLQ system integrates seamlessly with the existing go-queue infrastructure:
- Uses the same client for broker communication
- Follows existing message and topic patterns
- Maintains consistency with the overall architecture