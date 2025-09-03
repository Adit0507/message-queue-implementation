package client

import (
	"fmt"
	"log"
	"time"

	"github.com/Adit0507/message-queue-implementation/internal/protocol"
	"github.com/gorilla/websocket"
)

type MessageHandler func(message *ConsumedMessage) error

type Consumer struct {
	conn          *websocket.Conn
	brokerURL     string
	connected     bool
	subscriptions map[string]MessageHandler
	stopChan      chan bool
}

type ConsumedMessage struct {
	ID        string                 `json:"id"`
	Queue     string                 `json:"queue"`
	Payload   map[string]interface{} `json:"payload"`
	Headers   map[string]string      `json:"headers"`
	CreatedAt time.Time              `json:"created_at"`
	Attempts  int                    `json:"attempts"`
	consumer  *Consumer
}

func (m *ConsumedMessage) Ack() error {
	return m.consumer.ackMessage(m.ID)
}

func (m *ConsumedMessage) Nack() error {
	return m.consumer.nackMessage(m.ID)
}

func NewConsumer(brokerURL string) *Consumer {
	return &Consumer{
		brokerURL:     brokerURL,
		connected:     false,
		subscriptions: make(map[string]MessageHandler),
		stopChan:      make(chan bool),
	}
}

func (c *Consumer) Connect() error {
	var err error

	c.conn, _, err = websocket.DefaultDialer.Dial(c.brokerURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to broker %v", err)
	}

	c.connected = true
	log.Printf("Consumer connected to %s", c.brokerURL)

	// message processing routines
	go c.processMessages()

	return nil
}

func (c *Consumer) Disconnect() error {
	if c.connected {
		c.stopChan <- true
		c.connected = false
	}

	if c.conn != nil {
		return c.conn.Close()
	}

	return nil
}

func (c *Consumer) Subscribe(queue string, handler MessageHandler) error {
	if !c.connected {
		return fmt.Errorf("consumer isnt connected")
	}

	cmd := &protocol.Command{
		Type:      protocol.TypeSubscribe,
		Queue:     queue,
		Timestamp: time.Now(),
	}

	data, err := cmd.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize command: %v", err)
	}

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("failed to send subscribe command: %v", err)
	}

	// read response
	_, responseData, err := c.conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read subscribe response: %v", err)
	}

	response, err := protocol.ParseResponse(responseData)
	if err != nil {
		return fmt.Errorf("failed to parse subscribe response %v", err)
	}

	if !response.Success {
		return fmt.Errorf("subscription failed %s", response.Error)
	}

	c.subscriptions[queue] = handler
	log.Printf("Subscribed to queue: %s", queue)

	return nil
}

func (c *Consumer) Unsubscribe(queue string) error {
	if !c.connected {
		return fmt.Errorf("consumer is not connected")
	}

	cmd := &protocol.Command{
		Type:      protocol.TypeUnsubscribe,
		Queue:     queue,
		Timestamp: time.Now(),
	}

	data, err := cmd.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize command %v", err)
	}

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("failed to send unsubscribe command %v", err)
	}

	delete(c.subscriptions, queue)
	log.Printf("Unsubscribed from queue: %s", queue)

	return nil
}

func (c *Consumer) processMessages() {
	for {
		select {
		case <-c.stopChan:
			log.Println("Stopping message processing")
			return

		default:
			_, data, err := c.conn.ReadMessage()
			if err != nil {
				if c.connected {
					log.Printf("Error reading message: %v", err)
				}

				return
			}

			response, err := protocol.ParseResponse(data)
			if err != nil {
				log.Printf("Error parsing response: %v", err)
				continue
			}

			if response.Type == protocol.TypeHeartbeat {
				c.handleMessage(response)
			}
		}
	}
}

func (c *Consumer) handleMessage(response *protocol.Response) {
	queueName, ok := response.Data["queue"].(string)
	if !ok {
		log.Printf("Invalid queue name in message")
		return
	}

	handler, exists := c.subscriptions[queueName]
	if !exists {
		log.Printf("No handler for queue: %s", queueName)
		return
	}

	// extract message data
	payload, _ := response.Data["payload"].(map[string]interface{})
	headers, _ := response.Data["headers"].(map[string]string)
	attempts, _ := response.Data["attempts"].(float64)
	createdAtStr, _ := response.Data["created_at"].(string)

	createdAt := time.Now()
	if createdAtStr != "" {
		if parsed, err := time.Parse(time.RFC3339, createdAtStr); err != nil {
			createdAt = parsed
		}
	}

	message := &ConsumedMessage{
		ID:        response.MessageID,
		Queue:     queueName,
		Payload:   payload,
		Headers:   headers,
		CreatedAt: createdAt,
		Attempts:  int(attempts),
		consumer:  c,
	}

	// process message with handler
	if err := handler(message); err != nil {
		log.Printf("Error processing message %s: %v", message.ID, err)
		message.Nack()
	} else {
		message.Ack()
	}
}

func (c *Consumer) ackMessage(messageID string) error {
	cmd := &protocol.Command{
		Type:      protocol.TypeACK,
		MessageID: messageID,
		Timestamp: time.Now(),
	}

	data, err := cmd.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize ack command %v", err)
	}

	return c.conn.WriteMessage(websocket.TextMessage, data)
}

func (c *Consumer) nackMessage(messageID string) error {
	cmd := &protocol.Command{
		Type:      protocol.TypeNACK,
		MessageID: messageID,
		Timestamp: time.Now(),
	}

	data, err := cmd.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize nack command %v", err)
	}

	return c.conn.WriteMessage(websocket.TextMessage, data)
}

func (c *Consumer) StartHeartbeat(interval time.Duration) {
	if !c.connected {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-c.stopChan:
				return

			case <-ticker.C:
				cmd := &protocol.Command{
					Type:      protocol.TypeHeartbeat,
					Timestamp: time.Now(),
				}

				data, err := cmd.ToJSON()
				if err != nil {
					log.Printf("Failed to serialize heartbeat: %v", err)
					continue
				}

				if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
					log.Printf("Failed to send heartbeat: %v", err)
					return
				}
			}
		}
	}()
}
