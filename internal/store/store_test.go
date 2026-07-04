package store

import (
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"raft-meta/internal/config"
	"raft-meta/internal/fsm"
	"raft-meta/internal/raftnode"
)

func newLeaderStore(t *testing.T) (*Store, *fsm.FSM) {
	t.Helper()
	log := hclog.NewNullLogger()
	f := fsm.New()
	cfg := &config.Config{
		NodeID:            "n1",
		RaftAddr:          "127.0.0.1:7101",
		Peers:             []config.Peer{{ID: "n1", Addr: "127.0.0.1:7101"}},
		Snapshot:          config.SnapshotConfig{Type: "inmem"},
		UseInmemTransport: true,
	}
	n, err := raftnode.New(cfg, f, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Shutdown() })
	if err := n.BootstrapCluster(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !n.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !n.IsLeader() {
		t.Fatal("not leader")
	}
	return New(n, f, 2*time.Second), f
}

func TestPutGetDeleteList(t *testing.T) {
	s, _ := newLeaderStore(t)
	if err := s.Put("k1", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put("/nodes/n1", []byte("a")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok := s.Get("k1")
	if !ok || string(got) != "v1" {
		t.Fatalf("Get(k1) = %q,%v", got, ok)
	}
	if len(s.List("/nodes/")) != 1 {
		t.Fatalf("List /nodes/ len = %d, want 1", len(s.List("/nodes/")))
	}
	if err := s.Delete("k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := s.Get("k1"); ok {
		t.Fatal("k1 should be deleted")
	}
}

// TestPutOnFollowerReturnsErrNotLeader verifies the spec rule that writes on a
// non-leader fail fast with ErrNotLeader instead of being forwarded. A second
// single-node cluster cannot elect a different leader, so we instead drive a
// node into a non-leader state by creating it without bootstrapping a leader
// (it stays a candidate/follower during the election window) and asserting the
// error path.
func TestPutOnFollowerReturnsErrNotLeader(t *testing.T) {
	log := hclog.NewNullLogger()
	f := fsm.New()
	cfg := &config.Config{
		NodeID:            "n2",
		RaftAddr:          "127.0.0.1:7102",
		Peers:             []config.Peer{{ID: "n2", Addr: "127.0.0.1:7102"}},
		Snapshot:          config.SnapshotConfig{Type: "inmem"},
		UseInmemTransport: true,
	}
	n, err := raftnode.New(cfg, f, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Shutdown() })
	// Deliberately do NOT bootstrap; a single node with no configuration cannot
	// become leader, so IsLeader() stays false.
	s := New(n, f, 2*time.Second)
	if err := s.Put("k", []byte("v")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Put on non-leader = %v, want ErrNotLeader", err)
	}
	if err := s.Delete("k"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Delete on non-leader = %v, want ErrNotLeader", err)
	}
}

// TestAddPeerOnLeaderApplies verifies AddPeer replicates an AddPeer command:
// after it returns, the leader's FSM has the peer.
func TestAddPeerOnLeaderApplies(t *testing.T) {
	s, f := newLeaderStore(t)
	if err := s.AddPeer("n4", "127.0.0.1:7104", "127.0.0.1:8104"); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	var found bool
	for _, p := range f.Peers() {
		if p.ID == "n4" && p.HTTPAddr == "127.0.0.1:8104" {
			found = true
		}
	}
	if !found {
		t.Fatalf("FSM.Peers = %v, want n4 with http 127.0.0.1:8104", f.Peers())
	}
}

// TestAddPeerOnFollowerReturnsErrNotLeader verifies AddPeer fails fast on a
// non-leader (peer ops, like writes, are leader-only).
func TestAddPeerOnFollowerReturnsErrNotLeader(t *testing.T) {
	log := hclog.NewNullLogger()
	f := fsm.New()
	cfg := &config.Config{
		NodeID:            "n2",
		RaftAddr:          "127.0.0.1:7102",
		Peers:             []config.Peer{{ID: "n2", Addr: "127.0.0.1:7102"}},
		Snapshot:          config.SnapshotConfig{Type: "inmem"},
		UseInmemTransport: true,
	}
	n, err := raftnode.New(cfg, f, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Shutdown() })
	s := New(n, f, 2*time.Second)
	if err := s.AddPeer("n4", "127.0.0.1:7104", "127.0.0.1:8104"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("AddPeer on non-leader = %v, want ErrNotLeader", err)
	}
}
