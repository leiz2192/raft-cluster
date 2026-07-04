package peers

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"raft-meta/internal/config"
)

// Store persists dynamically-added peers (added via /cluster/join) so they
// survive restart. The static peer list (cfg.Peers) is the bootstrap set;
// this store captures peers added at runtime, whose HTTP address is needed
// for /cluster/status?full=true fanout and leader redirects but is not held
// by raft's configuration (raft only knows raft addresses).
type Store struct {
	mu   sync.RWMutex
	data map[string]config.Peer // keyed by ID
	path string                 // "" → in-memory only (no persistence)
}

// New returns a Store backed by path. path == "" means in-memory only (no
// persistence); used by tests that don't want file I/O. Call Load to hydrate
// from disk.
func New(path string) *Store {
	return &Store{data: map[string]config.Peer{}, path: path}
}

// Load reads persisted peers from disk into memory. A missing file is not an
// error (fresh start). A corrupt file IS an error — operator can delete it.
func (s *Store) Load() error {
	if s.path == "" {
		return nil
	}
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read peers file: %w", err)
	}
	var pjs peerJSON
	if err := json.Unmarshal(b, &pjs); err != nil {
		return fmt.Errorf("decode peers file: %w", err)
	}
	s.mu.Lock()
	for _, p := range pjs.Peers {
		s.data[p.ID] = p
	}
	s.mu.Unlock()
	return nil
}

// Add upserts a peer by ID and persists.
func (s *Store) Add(p config.Peer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[p.ID] = p
	return s.persistLocked()
}

// Remove deletes a peer by ID (no-op if absent) and persists.
func (s *Store) Remove(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, id)
	return s.persistLocked()
}

// All returns a snapshot of the stored peers (order unspecified).
func (s *Store) All() []config.Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]config.Peer, 0, len(s.data))
	for _, p := range s.data {
		out = append(out, p)
	}
	return out
}

// peerJSON is the on-disk envelope.
type peerJSON struct {
	Peers []config.Peer `json:"peers"`
}

// persistLocked writes the current data atomically. Caller holds mu.
func (s *Store) persistLocked() error {
	if s.path == "" {
		return nil
	}
	snapshot := make([]config.Peer, 0, len(s.data))
	for _, p := range s.data {
		snapshot = append(snapshot, p)
	}
	b, err := json.MarshalIndent(peerJSON{Peers: snapshot}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode peers: %w", err)
	}
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("mkdir peers dir: %w", err)
		}
	}
	// Atomic: write temp file alongside the target, then rename.
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return fmt.Errorf("write peers file: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename peers file: %w", err)
	}
	return nil
}
