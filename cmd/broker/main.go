package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Adit0507/message-queue-implementation/internal/broker"
	"github.com/Adit0507/message-queue-implementation/pkg/config"
)

func main() {
	cfg := config.LoadBrokerConfig()
	b := broker.NewBroker(cfg)

	// graceful shtdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func ()  {
		<- sigChan
		log.Println("Shutdown signal received, stopping broker...")
		os.Exit(0)
	}()

	if err := b.Start(); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}
}