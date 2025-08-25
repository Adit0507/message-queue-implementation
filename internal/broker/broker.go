package broker

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/Adit0507/message-queue-implementation/pkg/config"
	"github.com/gorilla/mux"
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

func NewBroker(cfg *config.BrokerConfig) *Broker {
	return &Broker{
		config:            cfg,
		queues:            make(map[string]Queue),
		connections:       make(map[string]*websocket.Conn),
		pendingMessages:   make(map[string]*Message),
		messageDispatcher: make(chan *MessageDispatch, 1000),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true //allowing connections from any origin
			},
		},
	}
}

func (b *Broker) Start() error {
	go b.startMessageDispatcher()

	go b.startCleanupRoutine()

	router := mux.NewRouter()
	router.HandleFunc("/ws", b.handleWebsocket)
	router.HandleFunc("/queues", b.HandleGetQueues).Methods("GET")
	router.HandleFunc("/queues/{name}/stats", b.handleGetQueueStats).Methods("GET")
	router.HandleFunc("/health", b.handleHealth).Methods("GET")

	addr := fmt.Sprintf("%s:%d", b.config.Host, b.config.Port)
	log.Printf("Message Queue Broker starting on %s", addr)

	return http.ListenAndServe(addr, router)
}


func (b *Broker) handleWebsocket(w http.ResponseWriter, r* http.Request) {
	conn, err := b.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed: %v", err)
		return
	}
	


}