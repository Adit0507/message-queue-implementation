package broker

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Adit0507/message-queue-implementation/internal/protocol"
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

func (b *Broker) handleWebsocket(w http.ResponseWriter, r *http.Request) {
	conn, err := b.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed: %v", err)
		return
	}

	clientID := generateClientID()
	b.mutex.Lock()
	b.connections[clientID] = conn
	b.mutex.Unlock()

	defer func() {
		b.mutex.Lock()
		delete(b.connections, clientID)
		b.mutex.Unlock()

		conn.Close()
		log.Printf("Client %s disconnected", clientID)
	}()

	log.Printf("Client %s connected", clientID)

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("error reading message from %s: %v", clientID, err)
			break
		}

		if err = b.handleMessage(clientID, message, conn); err != nil {
			log.Printf("error handling message from %s: %v", clientID, err)
			b.sendErrorResponse(conn, "", fmt.Sprintf("Error processing message: %v", err))
		}
	}
}

func (b *Broker) sendErrorResponse(conn *websocket.Conn, messageID, errorMsg string) {
	response := &protocol.Response{
		Type:      protocol.TypeError,
		Success:   false,
		MessageID: messageID,
		Error:     errorMsg,
		Timestamp: time.Now(),
	}

	b.sendResponse(conn, response)
}

func (b *Broker) sendResponse(conn *websocket.Conn, response *protocol.Response) error {
	data, err := response.ToJSON()
	if err != nil {
		return err
	}

	return conn.WriteMessage(websocket.TextMessage, data)

}

func (b *Broker) handleMessage(clientID string, data []byte, conn *websocket.Conn) error {
	cmd, err := protocol.ParseCommand(data)
	if err != nil {
		return fmt.Errorf("failed to parse command %v", err)
	}

	switch cmd.Type {
	case protocol.TypePublish:
		return b.handlePublish(clientID, cmd, conn)

	default:
		return fmt.Errorf("unknown message type %s", cmd.Type)
	}
}

func (b *Broker) handlePublish(clientID string, cmd *protocol.Command, conn *websocket.Conn) error {
	if cmd.Queue == "" {
		return fmt.Errorf("queue name is required for publish")
	}

	queue := b.getOrCreateQueue(cmd.Queue)

	msg := NewMessage(cmd.Queue, cmd.Payload, cmd.Headers)

	if err := queue.Publish(msg); err != nil {
		return err
	}

	// dispatchin message to consumers
	b.messageDispatcher <- &MessageDispatch{
		Queue: cmd.Queue,
		Message: msg,
	}

	response := &protocol.Response{
		Type: protocol.TypeSuccess,
		Success: true,
		MessageID: msg.ID,
		Timestamp: time.Now(),
	}


	return b.sendResponse(conn, response)
}

func (b *Broker) getOrCreateQueue(name string) *Queue {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if queue, exists := b.queues[name]; exists {
		return  &queue
	}

}

func generateClientID() string {
	return fmt.Sprintf("client_%d_%d", time.Now().UnixNano(), len(fmt.Sprintf("%d", time.Now().UnixNano())))
}
