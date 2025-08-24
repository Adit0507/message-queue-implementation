package broker

import (
	"fmt"
	"sync/atomic"
	"time"
)

var messageIDCounter int64

type DeliveryStatus int

const (
	StatusPending DeliveryStatus = iota
	StatusDelivered
	StatusAcknowledged
	StatusFailed
)

type Message struct {
	ID             string                 `json:"id"`
	Queue          string                 `json:"queue"`
	Payload        map[string]interface{} `json:"payload"`
	Headers        map[string]string      `json:"headers"`
	CreatedAt      time.Time              `json:"created_at"`
	DeliveredAt    *time.Time             `json:"delivered_at,omitempty"`
	AcknowledgedAt *time.Time             `json:"acknowledged_at,omitempty"`
	Status         DeliveryStatus         `json:"status"`
	Attempts       int                    `json:"attempts"`
	MaxAttempts    int                    `json:"max_attempts"`
	ConsumerID     string                 `json:"consumer_id,omitempty"`
}

func NewMessage(queue string, payload map[string]interface{}, headers map[string]string) *Message {
	id := generateMessageID()
	now := time.Now()

	return &Message{
		ID:          id,
		Queue:       queue,
		Payload:     payload,
		Headers:     headers,
		CreatedAt:   now,
		Status:      StatusPending,
		Attempts:    0,
		MaxAttempts: 3,
	}
}

func generateMessageID() string {
	id := atomic.AddInt64(&messageIDCounter, 1)

	return fmt.Sprintf("msg_%d_%d", time.Now().Unix(), id)
}

func (m *Message) MarkDelivered(consumerID string) {
	now := time.Now()

	m.DeliveredAt = &now
	m.Status = StatusDelivered
	m.ConsumerID = consumerID
	m.Attempts++
}

func (m *Message) MarkAcknowledged() {
	now := time.Now()

	m.AcknowledgedAt = &now
	m.Status = StatusAcknowledged
}

func (m *Message) MarkFailed() {
	m.Status = StatusFailed
}

func (m *Message) CanRetry() bool {
	return m.Attempts < m.MaxAttempts && m.Status != StatusAcknowledged
}