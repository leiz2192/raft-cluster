package store

import (
	"errors"
	"time"

	"raft-meta/internal/fsm"
	"raft-meta/internal/raftnode"
)

// ErrNotLeader is returned for writes on a non-leader. LeaderAddr holds the
// known leader's raft address (may be empty during election).
var ErrNotLeader = errors.New("not leader")

type Store struct {
	node         *raftnode.Node
	fsm          *fsm.FSM
	applyTimeout time.Duration
}

func New(n *raftnode.Node, f *fsm.FSM, applyTimeout time.Duration) *Store {
	return &Store{node: n, fsm: f, applyTimeout: applyTimeout}
}

func (s *Store) Put(key string, value []byte) error {
	if !s.node.IsLeader() {
		return ErrNotLeader
	}
	cmd, err := fsm.EncodeCommand(&fsm.Command{Op: fsm.OpPut, Key: key, Value: value})
	if err != nil {
		return err
	}
	return s.node.Apply(cmd, s.applyTimeout).Error()
}

func (s *Store) Delete(key string) error {
	if !s.node.IsLeader() {
		return ErrNotLeader
	}
	cmd, err := fsm.EncodeCommand(&fsm.Command{Op: fsm.OpDelete, Key: key})
	if err != nil {
		return err
	}
	return s.node.Apply(cmd, s.applyTimeout).Error()
}

// Get reads from local FSM. On a leader this is strongly consistent.
// On a follower it may be stale (脏读) — caller accepts this by default.
func (s *Store) Get(key string) ([]byte, bool) {
	return s.fsm.Get(key)
}

func (s *Store) List(prefix string) map[string][]byte {
	return s.fsm.List(prefix)
}

// LeaderAddr returns the known leader raft address for redirect purposes.
func (s *Store) LeaderAddr() string {
	return s.node.LeaderAddr()
}
