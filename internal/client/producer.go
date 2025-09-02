package client

import (
	"fmt"
	"log"

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
