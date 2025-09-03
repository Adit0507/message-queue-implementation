package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Adit0507/message-queue-implementation/internal/client"
)

func main() {
	producer := client.NewProducer("ws://localhost:8080/ws")
	if err := producer.Connect(); err != nil{
		log.Fatalf("Failed to connect producer: %v", err)
	}
	defer producer.Disconnect()

	fmt.Println("=== Simple Message Publishing ===")
	for i :=0; i < 5; i++ {
		payload := map[string]interface{}{
			"message_number": i + 1,
			"content":        fmt.Sprintf("Hello from producer! Message #%d", i+1),
			"timestamp":      time.Now().Format(time.RFC3339),
		}

		headers := map[string] string{
			"source":      "example-producer",
			"priority":    "normal",
			"message_type": "greeting",
		}

		resp, err := producer.Publish("test-queue", payload, headers)
		if err != nil{
			log.Printf("Failed to publish message %d: %v", i+1, err)
			continue
		}

		if resp.Success {
			fmt.Printf("😋 Published message %d with ID: %s\n", i+1, resp.MessageID)
		} else {
			fmt.Printf("😔 failed to publish message %d: %s \n", i +1, resp.Error)
		}

		time.Sleep(1* time.Second)
	}

	fmt.Println("\n=== Batch Message Publishing ===")
	batchMessages := []map[string] interface{} {
		{
			"user_id": "user_001",
			"action":  "login",
			"ip":      "192.168.1.100",
		},
		{
			"user_id": "user_002", 
			"action":  "purchase",
			"product": "laptop",
			"amount":  999.99,
		},
		{
			"user_id": "user_003",
			"action":  "logout",
		},
	}

	if err := producer.PublishBatch("user-events", batchMessages); err != nil {
		log.Printf("batch publishing failed %v", err)
	} else {
		fmt.Printf("✓ Published batch of %d messages to user-events queue\n", len(batchMessages))
	}

	fmt.Println("\n=== Multi-Queue Publishing ===")
	queues := []string{"notifications", "analytics", "audit-log"}
	
	for _, queue := range queues {
		payload := map[string]interface{}{
			"queue_name": queue,
			"data":       fmt.Sprintf("Sample data for %s", queue),
			"generated":  time.Now().Unix(),
		}

		resp, err := producer.Publish(queue, payload, nil)
		if err != nil{
			log.Printf("Failed to publish to %s: %v", queue, err)
		} else if resp.Success {
			fmt.Printf("✓ Published to %s (ID: %s)\n", queue, resp.MessageID)
		}
	}

	fmt.Println("\n=== Producer finished ===")
}