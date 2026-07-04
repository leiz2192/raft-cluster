package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"raft-meta/internal/config"
)

// FSM implements raft.FSM with an in-memory map protected by a RWMutex.
// It holds two logical stores: the KV data and the dynamic-peer map. Both are
// replicated via raft (Apply) and included in snapshots, so every node —
// including after failover — sees the same dynamic peers' HTTP addresses.
type FSM struct {
	mu     sync.RWMutex
	data   map[string][]byte
	peers  map[string]config.Peer // runtime-added peers (id -> peer), replicated
	logger hclog.Logger
}

// New creates an FSM with a silent (NullLogger) logger. Production wiring
// should use NewWithLogger to surface decode/restore errors to operators.
func New() *FSM {
	return NewWithLogger(hclog.NewNullLogger())
}

// NewWithLogger creates an FSM whose Apply/Restore error paths log to logger.
func NewWithLogger(logger hclog.Logger) *FSM {
	if logger == nil {
		logger = hclog.NewNullLogger()
	}
	return &FSM{data: make(map[string][]byte), peers: make(map[string]config.Peer), logger: logger}
}

// Peers returns a snapshot of the dynamic-peer map (runtime-added via
// /cluster/join). Order is unspecified.
func (f *FSM) Peers() []config.Peer {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]config.Peer, 0, len(f.peers))
	for _, p := range f.peers {
		out = append(out, p)
	}
	return out
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	cmd, err := DecodeCommand(log.Data)
	if err != nil {
		// 损坏的日志条目不应让集群卡死：记录后跳过。
		// 返回 error 只会进入 ApplyFuture.Response()，调用方通常只读
		// .Error()，会被静默吞掉；故显式记日志让运维可见。
		f.logger.Error("fsm: undecodable log entry, skipping", "index", log.Index, "err", err)
		return fmt.Errorf("decode log: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch cmd.Op {
	case OpPut:
		f.data[cmd.Key] = append([]byte(nil), cmd.Value...)
	case OpDelete:
		delete(f.data, cmd.Key)
	case OpAddPeer:
		if cmd.PeerID != "" {
			f.peers[cmd.PeerID] = config.Peer{ID: cmd.PeerID, Addr: cmd.PeerAddr, HTTPAddr: cmd.PeerHTTP}
		}
	case OpRemovePeer:
		delete(f.peers, cmd.PeerID)
	default:
		// 未知操作：幂等忽略。
	}
	return nil
}

func (f *FSM) Get(key string) ([]byte, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

func (f *FSM) List(prefix string) map[string][]byte {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string][]byte)
	for k, v := range f.data {
		if strings.HasPrefix(k, prefix) {
			out[k] = append([]byte(nil), v...)
		}
	}
	return out
}

// Len returns the number of keys in the FSM (for monitoring/metrics).
func (f *FSM) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.data)
}

// snapshotData is the serialized form of the FSM for snapshots.
type snapshotData struct {
	Data  map[string][]byte      `json:"data"`
	Peers map[string]config.Peer `json:"peers"`
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	// 拷贝一致视图，避免 Persist 期间被 Apply 改动。
	copied := make(map[string][]byte, len(f.data))
	for k, v := range f.data {
		copied[k] = append([]byte(nil), v...)
	}
	copiedPeers := make(map[string]config.Peer, len(f.peers))
	for id, p := range f.peers {
		copiedPeers[id] = p
	}
	return &fsmSnapshot{data: copied, peers: copiedPeers}, nil
}

type fsmSnapshot struct {
	data  map[string][]byte
	peers map[string]config.Peer
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	enc := json.NewEncoder(sink)
	if err := enc.Encode(snapshotData{Data: s.data, Peers: s.peers}); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var sd snapshotData
	if err := json.NewDecoder(rc).Decode(&sd); err != nil {
		return fmt.Errorf("restore decode: %w", err)
	}
	f.mu.Lock()
	f.data = sd.Data
	if f.data == nil {
		f.data = make(map[string][]byte)
	}
	f.peers = sd.Peers
	if f.peers == nil {
		f.peers = make(map[string]config.Peer)
	}
	f.mu.Unlock()
	return nil
}
