package fsm

import (
	"encoding/json"
	"fmt"
)

type Op string

const (
	OpPut        Op = "put"
	OpDelete     Op = "delete"
	OpAddPeer    Op = "addPeer"    // record a runtime-added peer's HTTP addr
	OpRemovePeer Op = "removePeer" // drop a runtime-added peer by ID
)

// Command is the replicated log entry. KV ops use Key/Value; peer ops use
// PeerID / PeerAddr (raft) / PeerHTTP (HTTP business addr).
type Command struct {
	Op       Op     `json:"op"`
	Key      string `json:"key,omitempty"`
	Value    []byte `json:"value,omitempty"`
	PeerID   string `json:"peerId,omitempty"`
	PeerAddr string `json:"peerAddr,omitempty"`
	PeerHTTP string `json:"peerHttp,omitempty"`
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
