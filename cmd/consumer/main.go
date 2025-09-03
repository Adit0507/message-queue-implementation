package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Adit0507/message-queue-implementation/internal/client"
)

func main() {
	consumer := client.NewConsumer("ws://localhost:8080/ws")
	if err := consumer.Connect(); err != nil {
		log.Fatalf("Failed to connect consumer: %v", err)
	}
	defer consumer.Connect()

	// start heartbeat
	consumer.StartHeartbeat(30 * time.Second)

	// subscribin to test queue with handler
	err := consumer.Subscribe("test-queue", func(message *client.ConsumedMessage) error {
		fmt.Printf("📨 [test-queue] Received message %s:\n", message.ID)
		fmt.Printf("   Content: %v\n", message.Payload["content"])
		fmt.Printf("   Number: %v\n", message.Payload["message_number"])
		fmt.Printf("   Headers: %v\n", message.Headers)
		fmt.Printf("   Attempts: %d\n", message.Attempts)
		fmt.Printf("   Created: %v\n", message.CreatedAt.Format(time.RFC3339))

		time.Sleep(500 * time.Millisecond)

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe to test-queue: %v", err)
	}

	err = consumer.Subscribe("User-events", func(message *client.ConsumedMessage) error {
		action, hasAction := message.Payload["action"]
		userId, hasUserID := message.Payload["user_id"]

		if !hasAction || !hasUserID {
			fmt.Printf("⚠️  [user-events] Invalid message format: %s\n", message.ID)
			return fmt.Errorf("missing required fields")
		}

		fmt.Printf("[user-events] User %v performed action: %v\n", userId, action)

		switch action {
		case "login":
			ip := message.Payload["ip"]
			fmt.Printf("   Login from IP: %v\n", ip)
		
		case "purchase":
			product := message.Payload["product"]
			amt := message.Payload["amount"]
			fmt.Printf("   Purchased %v for $%.2f\n", product, amt)

		case "logout":
			fmt.Printf("User logged out \n")

		default:
			fmt.Printf("unknown action %v \n", action)
		}

		return nil
	})

	if err != nil{
		log.Fatalf("Failed to subscribe to user-events: %v", err)
	}

	// subscribe to notifications queue
	err = consumer.Subscribe("notifications", func(message *client.ConsumedMessage) error {
		fmt.Printf("🔔 [notifications] Processing notification %s\n", message.ID)
		fmt.Printf("Data: %v\n", message.Payload)
	
		time.Sleep(200 * time.Millisecond)
		fmt.Printf("   ✓ Notification sent!\n")

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe to notifications: %v", err)
	}

	// susbcribe to analytics queu
	err = consumer.Subscribe("analytics", func(message *client.ConsumedMessage) error {
		fmt.Printf("📊 [analytics] Processing analytics data %s\n", message.ID)
		
		time.Sleep(1 *time.Second)
		fmt.Printf("Analytics data processed and stored \n")

		return nil
	})
	if err != nil{
		log.Fatalf("Failed to subscribe to analytics: %v", err)
	}

	err = consumer.Subscribe("audit-log", func(message *client.ConsumedMessage) error {
		fmt.Printf("📋 [audit-log] Processing audit log %s\n", message.ID)

		if message.Attempts > 0{
			fmt.Printf("⚠️  Retry attempt #%d\n", message.Attempts)
		}

		if message.Attempts == 0 && time.Now().UnixNano()%10 < 3 {
			fmt.Printf("Simulated processing error\n")
			return fmt.Errorf("simulated processing error")
		}
		
		time.Sleep(300 * time.Millisecond)
		fmt.Printf("Audit log entry created\n")

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe to audit-log: %v", err)
	}

	fmt.Println("🚀 Consumer started! Listening for messages...")
	fmt.Println("📋 Subscribed to: test-queue, user-events, notifications, analytics, audit-log")
	fmt.Println("Press Ctrl+C to stop...")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n🛑 Shutting down consumer...")
}
