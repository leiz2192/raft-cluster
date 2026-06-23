package store

import (
	"errors"
	"time"

	"raft-meta/internal/fsm"
	"raft-meta/internal/metrics"
	"raft-meta/internal/raftnode"
)

// ErrNotLeader is returned for writes on a non-leader. LeaderAddr holds the
// known leader's address (may be empty during election).
var ErrNotLeader = errors.New("not leader")

type Store struct {
	node         *raftnode.Node
	fsm          *fsm.FSM
	applyTimeout time.Duration
	metrics      *metrics.Metrics // nil if not wired; instrumentation is nil-safe
}

func New(n *raftnode.Node, f *fsm.FSM, applyTimeout time.Duration) *Store {
	return &Store{node: n, fsm: f, applyTimeout: applyTimeout}
}

// SetMetrics wires a Metrics instance for KV op instrumentation. Optional —
// when unset, operations work but are not counted.
func (s *Store) SetMetrics(m *metrics.Metrics) { s.metrics = m }

func (s *Store) Put(key string, value []byte) error {
	start := time.Now()
	err := s.put(key, value)
	if s.metrics != nil {
		s.metrics.ObserveKVOp("put", err, time.Since(start))
	}
	return err
}

func (s *Store) put(key string, value []byte) error {
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
	start := time.Now()
	err := s.delete(key)
	if s.metrics != nil {
		s.metrics.ObserveKVOp("delete", err, time.Since(start))
	}
	return err
}

func (s *Store) delete(key string) error {
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
	start := time.Now()
	v, ok := s.fsm.Get(key)
	if s.metrics != nil {
		s.metrics.ObserveKVOp("get", nil, time.Since(start))
	}
	return v, ok
}

func (s *Store) List(prefix string) map[string][]byte {
	start := time.Now()
	out := s.fsm.List(prefix)
	if s.metrics != nil {
		s.metrics.ObserveKVOp("list", nil, time.Since(start))
	}
	return out
}

// LeaderAddr returns the known leader raft address for redirect purposes.
func (s *Store) LeaderAddr() string {
	return s.node.LeaderAddr()
}
