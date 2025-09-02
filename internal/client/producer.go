package client

import (
	"fmt"
	"log"
	"time"

	"github.com/Adit0507/message-queue-implementation/internal/protocol"
	"github.com/gorilla/websocket"
)

type Producer struct {
	conn      *websocket.Conn
	brokerURL string
	connected bool
}

func NewProducer(brokerURL string) *Producer {
	return &Producer{
		brokerURL: brokerURL,
		connected: false,
	}
}

func (p *Producer) Connect() error {
	var err error
	p.conn, _, err = websocket.DefaultDialer.Dial(p.brokerURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to broker %v", err)
	}

	p.connected = true
	log.Printf("producer connected to %s", p.brokerURL)

	return nil
}

func (p *Producer) Disconnect() error {
	if p.conn != nil {
		p.connected = false
		return  p.conn.Close()
	}
	
	return nil
}

func (p *Producer) Publish(queue string, payload map[string] interface{}, headers map[string]string) (*protocol.Response, error) {
	if !p.connected{
		return nil, fmt.Errorf("producer is not connected")
	}

	if headers == nil{
		headers = make(map[string]string)
	}

	cmd := &protocol.Command{
		Type: protocol.TypePublish,
		Queue: queue,
		Payload: payload,
		Headers: headers,
		Timestamp: time.Now(),
	}

	data, err := cmd.ToJSON()
	if err != nil {
		return  nil, fmt.Errorf("failed to serialize command %v", err)
	}

	if err := p.conn.WriteMessage(websocket.TextMessage, data); err != nil{
		return  nil, fmt.Errorf("failed to send message %v", err)
	}

	// read response
	_, responseData, err := p.conn.ReadMessage()
	if err != nil {
		return  nil, fmt.Errorf("failed to read response %v", err)
	}

	response, err := protocol.ParseResponse(responseData)
	if err != nil{
		return nil, fmt.Errorf("failed to parse response %v", err)
	}

	return response, nil
}