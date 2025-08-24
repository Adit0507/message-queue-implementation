package protocol

import (
	"encoding/json"
	"time"
)

type MessageType string

const (
	TypePublish     MessageType = "PUBLISH"
	TypeSubscribe   MessageType = "SUBSCRIBE"
	TypeUnsubscribe MessageType = "UNSUBSCRIBE"
	TypeACK         MessageType = "ACK"
	TypeNACK        MessageType = "NACK"
	TypeHeartbeat   MessageType = "HEARTBEAT"
	TypeError       MessageType = "ERROR"
	TypeSuccess     MessageType = "SUCCESS"
)

type Command struct {
	Type      MessageType            `json:"type"`
	Queue     string                 `json:"queue,omitempty"`
	MessageID string                 `json:"message_id,omitempty"`
	Payload   map[string]interface{} `json:"payload,omitempty"`
	Headers   map[string]string      `json:"headers,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

type Response struct {
	Type      MessageType            `json:"type"`
	Success   bool                   `json:"success"`
	MessageID string                 `json:"message_id,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Error     string                 `json:"error,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

func (c *Command) ToJSON() ([]byte, error) {
	return json.Marshal(c)
}

func (r *Response) ToJSON() ([]byte, error) {
	return json.Marshal(r)
}

func ParseCommand(data []byte) (*Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)

	return &cmd, err
}

func ParseResponse(data []byte) (*Response, error) {
	var resp Response
	err := json.Unmarshal(data, &resp)

	return &resp, err
}
