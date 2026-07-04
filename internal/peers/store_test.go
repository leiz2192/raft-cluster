package peers

import (
	"path/filepath"
	"sort"
	"testing"

	"raft-meta/internal/config"
)

func peer(id, raftAddr, httpAddr string) config.Peer {
	return config.Peer{ID: id, Addr: raftAddr, HTTPAddr: httpAddr}
}

func sortedIDs(ps []config.Peer) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.ID)
	}
	sort.Strings(out)
	return out
}

// TestStoreAddPersistAndReload verifies Add persists to disk and a fresh Store
// loading the same path sees the peer.
func TestStoreAddPersistAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	s := New(path)
	if err := s.Add(peer("n4", "127.0.0.1:7004", "127.0.0.1:8004")); err != nil {
		t.Fatalf("Add: %v", err)
	}

	s2 := New(path)
	if err := s2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := sortedIDs(s2.All())
	want := []string{"n4"}
	if len(got) != 1 || got[0] != "n4" {
		t.Fatalf("after reload All = %v, want %v", got, want)
	}
}

// TestStoreRemovePersists verifies Remove deletes from disk, not just memory.
func TestStoreRemovePersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	s := New(path)
	s.Add(peer("n4", "127.0.0.1:7004", "127.0.0.1:8004"))
	s.Add(peer("n5", "127.0.0.1:7005", "127.0.0.1:8005"))
	if err := s.Remove("n4"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	s2 := New(path)
	s2.Load()
	got := sortedIDs(s2.All())
	want := []string{"n5"}
	if len(got) != 1 || got[0] != "n5" {
		t.Fatalf("after remove+reload All = %v, want %v", got, want)
	}
}

// TestStoreLoadMissingFileOK verifies a missing peers file is not an error.
func TestStoreLoadMissingFileOK(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "absent", "peers.json"))
	if err := s.Load(); err != nil {
		t.Fatalf("Load missing file = %v, want nil", err)
	}
	if got := s.All(); len(got) != 0 {
		t.Fatalf("All = %v, want empty", got)
	}
}

// TestStoreInMemoryWhenNoPath verifies path=="" works without touching disk.
func TestStoreInMemoryWhenNoPath(t *testing.T) {
	s := New("")
	if err := s.Add(peer("n4", "127.0.0.1:7004", "127.0.0.1:8004")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if got := sortedIDs(s.All()); len(got) != 1 || got[0] != "n4" {
		t.Fatalf("All = %v, want [n4]", got)
	}
	if err := s.Remove("n4"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if got := s.All(); len(got) != 0 {
		t.Fatalf("after remove All = %v, want empty", got)
	}
}

// TestStoreRemoveMissingIsNoOp verifies removing an absent peer is not an error.
func TestStoreRemoveMissingIsNoOp(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "peers.json"))
	if err := s.Remove("ghost"); err != nil {
		t.Fatalf("Remove absent = %v, want nil", err)
	}
}

// TestStoreAddUpserts verifies a second Add for the same ID replaces, not appends.
func TestStoreAddUpserts(t *testing.T) {
	s := New("")
	s.Add(peer("n4", "127.0.0.1:7004", "127.0.0.1:8004"))
	s.Add(peer("n4", "127.0.0.1:7004", "127.0.0.1:9004")) // httpAddr changed
	got := s.All()
	if len(got) != 1 {
		t.Fatalf("All len = %d, want 1 (upsert)", len(got))
	}
	if got[0].HTTPAddr != "127.0.0.1:9004" {
		t.Fatalf("upserted HTTPAddr = %q, want 127.0.0.1:9004", got[0].HTTPAddr)
	}
}
