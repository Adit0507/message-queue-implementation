package broker

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type Queue struct {
	Name        string
	messages    *list.List
	subscribers map[string]*Consumer
	maxSize     int
	mutex       sync.RWMutex
	totalSent   int64
	totalAcked  int64
	created     time.Time
}

type Consumer struct {
	ID         string
	Connection interface{}
	Active     bool
	LastSeen   time.Time
}

func NewQueue(name string, maxSize int) *Queue {
	return &Queue{
		Name:        name,
		messages:    list.New(),
		subscribers: make(map[string]*Consumer),
		maxSize:     maxSize,
		created:     time.Now(),
	}
}

func (q *Queue) Publish(message *Message) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// checking if queue is full
	if q.messages.Len() >= q.maxSize {
		return fmt.Errorf("queue %s is full (max size: %d)", q.Name, q.maxSize)
	}

	q.messages.PushBack(message)

	return nil
}
func (q *Queue) Subscribe(consumerID string, connection interface{}) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.subscribers[consumerID] = &Consumer{
		ID:         consumerID,
		Connection: connection,
		Active:     true,
		LastSeen:   time.Now(),
	}
}

func (q *Queue) Unsubscribe(consumerID string) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	delete(q.subscribers, consumerID)
}

func (q *Queue) GetNextMessage() *Message {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.messages.Len() == 0 {
		return nil
	}

	element := q.messages.Front()
	msg := element.Value.(*Message)
	q.messages.Remove(element)

	return msg
}

func (q *Queue) GetSubscribers() []*Consumer {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	subscribers := make([]*Consumer, 0, len(q.subscribers))
	for _, consumer := range q.subscribers {
		if consumer.Active {
			subscribers = append(subscribers, consumer)
		}
	}

	return subscribers
}

type QueueStats struct {
	Name            string    `json:"name"`
	MessageCount    int       `json:"message_count"`
	SubscriberCount int       `json:"subscriber_count"`
	TotalSent       int64     `json:"total_sent"`
	TotalAcked      int64     `json:"total_acked"`
	Created         time.Time `json:"created"`
}

func (q *Queue) GetStats() QueueStats {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return QueueStats{
		Name:            q.Name,
		MessageCount:    q.messages.Len(),
		SubscriberCount: len(q.subscribers),
		TotalSent:       q.totalSent,
		TotalAcked:      q.totalAcked,
		Created:         q.created,
	}
}
