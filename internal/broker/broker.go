package broker

import (
	"sync"

	"github.com/Adit0507/message-queue-implementation/pkg/config"
	"github.com/gorilla/websocket"
)

type Broker struct {
	config            *config.BrokerConfig
	queues            map[string]Queue
	connections       map[string]*websocket.Conn
	pendingMessages   map[string]*Message
	mutex             sync.RWMutex
	upgrader          websocket.Upgrader
	messageDispatcher chan *MessageDispatch
}

type MessageDispatch struct {
	Queue   string
	Message *Message
}
