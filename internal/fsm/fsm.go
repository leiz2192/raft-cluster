package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
)

// FSM implements raft.FSM with an in-memory map protected by a RWMutex.
type FSM struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func New() *FSM {
	return &FSM{data: make(map[string][]byte)}
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	cmd, err := DecodeCommand(log.Data)
	if err != nil {
		// 损坏的日志条目不应让集群卡死；记录后跳过。
		return fmt.Errorf("decode log: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch cmd.Op {
	case OpPut:
		f.data[cmd.Key] = append([]byte(nil), cmd.Value...)
	case OpDelete:
		delete(f.data, cmd.Key)
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
	Data map[string][]byte `json:"data"`
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	// 拷贝一致视图，避免 Persist 期间被 Apply 改动。
	copied := make(map[string][]byte, len(f.data))
	for k, v := range f.data {
		copied[k] = append([]byte(nil), v...)
	}
	return &fsmSnapshot{data: copied}, nil
}

type fsmSnapshot struct {
	data map[string][]byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	enc := json.NewEncoder(sink)
	if err := enc.Encode(snapshotData{Data: s.data}); err != nil {
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
	f.mu.Unlock()
	return nil
}
