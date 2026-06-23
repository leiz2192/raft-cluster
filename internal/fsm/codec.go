package fsm

import (
	"encoding/json"
	"fmt"
)

type Op string

const (
	OpPut    Op = "put"
	OpDelete Op = "delete"
)

type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

func EncodeCommand(c *Command) ([]byte, error) {
	return json.Marshal(c)
}

func DecodeCommand(data []byte) (*Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("decode command: %w", err)
	}
	return &c, nil
}
