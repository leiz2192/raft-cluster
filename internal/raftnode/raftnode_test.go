package raftnode

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"raft-meta/internal/config"
	"raft-meta/internal/fsm"
)

func testConfig(id, raftAddr string) *config.Config {
	return &config.Config{
		NodeID:   id,
		RaftAddr: raftAddr,
		HTTPAddr: "127.0.0.1:0",
		DataDir:  "",
		Peers: []config.Peer{
			{ID: id, Addr: raftAddr},
		},
		Snapshot: config.SnapshotConfig{Type: "inmem"},
	}
}

func TestSingleNodeBootstrapAndApply(t *testing.T) {
	log := hclog.NewNullLogger()
	f := fsm.New()
	cfg := testConfig("n1", "127.0.0.1:7001")
	cfg.UseInmemTransport = true

	n, err := New(cfg, f, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer n.Shutdown()

	if err := n.BootstrapCluster(); err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}

	// 单节点引导后应在选举超时内成为 leader。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !n.IsLeader() {
		t.Fatal("node did not become leader")
	}

	cmd, _ := fsm.EncodeCommand(&fsm.Command{Op: fsm.OpPut, Key: "k", Value: []byte("v")})
	fut := n.Apply(cmd, 2*time.Second)
	if err := fut.Error(); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got, ok := f.Get("k")
	if !ok || string(got) != "v" {
		t.Fatalf("fsm Get(k) = %q,%v", got, ok)
	}
	if n.State() != raft.Leader {
		t.Errorf("State = %v, want Leader", n.State())
	}
	if n.LeaderAddr() == "" {
		t.Error("LeaderAddr empty")
	}
	// Focused assertion: Stats returns non-empty map (spec status query surface).
	if stats := n.Stats(); len(stats) == 0 {
		t.Error("Stats returned empty map")
	}
}

// TestBoltDBPersistenceRoundtrip verifies the spec's core DR guarantee:
// 已提交数据在重启后零丢失。用 inmem transport + 真实 BoltDB log/stable +
// 文件快照，写→强制快照→shutdown→用同数据目录重建→验证数据存活。
// 这覆盖 spec §1.4/§5.9 的"零丢失"成功标准。
func TestBoltDBPersistenceRoundtrip(t *testing.T) {
	log := hclog.NewNullLogger()
	dir := t.TempDir()
	mkcfg := func() *config.Config {
		return &config.Config{
			NodeID:   "p1",
			RaftAddr: "127.0.0.1:7501",
			DataDir:  dir,
			Peers:    []config.Peer{{ID: "p1", Addr: "127.0.0.1:7501"}},
			Snapshot:          config.SnapshotConfig{Type: "file", Path: filepath.Join(dir, "snaps"), Retain: 1},
			UseInmemTransport: true,
		}
	}

	// 第一次启动：写入并强制快照。
	f1 := fsm.New()
	n1, err := New(mkcfg(), f1, log)
	if err != nil {
		t.Fatalf("New(1): %v", err)
	}
	if err := n1.BootstrapCluster(); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !n1.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !n1.IsLeader() {
		t.Fatal("not leader")
	}
	cmd, _ := fsm.EncodeCommand(&fsm.Command{Op: fsm.OpPut, Key: "persisted", Value: []byte("yes")})
	if err := n1.Apply(cmd, 2*time.Second).Error(); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// 强制快照，验证快照路径也能持久化（不止日志）。
	if err := n1.Raft().Snapshot().Error(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := n1.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	// 第二次启动：同数据目录，不重新引导（已引导过）。
	f2 := fsm.New()
	n2, err := New(mkcfg(), f2, log)
	if err != nil {
		t.Fatalf("New(2): %v", err)
	}
	defer n2.Shutdown()
	if err := n2.BootstrapCluster(); err != nil {
		t.Fatalf("Bootstrap(2): %v", err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !n2.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	// 等待 Restore + 日志重放完成。
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := f2.Get("persisted"); ok && string(v) == "yes" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	v, _ := f2.Get("persisted")
	t.Fatalf("persisted data lost after restart: got %q", v)
}

// TestBootstrapClusterRejectsNodeIDNotInPeers verifies that bootstrapping a
// cluster when the local nodeID is not among the configured peers is rejected
// up front with a clear error. A bootstrap node must be a voter in the
// bootstrap configuration, otherwise it would seed a cluster it cannot elect
// itself into. (The self-in-peers check lives here, not in config.validate,
// so `start` nodes joining dynamically can legitimately omit themselves.)
func TestBootstrapClusterRejectsNodeIDNotInPeers(t *testing.T) {
	log := hclog.NewNullLogger()
	f := fsm.New()
	cfg := &config.Config{
		NodeID:   "lonely",
		RaftAddr: "127.0.0.1:7601",
		HTTPAddr: "127.0.0.1:0",
		DataDir:  "",
		// peers lists everyone except the local nodeID "lonely".
		Peers: []config.Peer{
			{ID: "n1", Addr: "127.0.0.1:7602"},
			{ID: "n2", Addr: "127.0.0.1:7603"},
		},
		Snapshot:          config.SnapshotConfig{Type: "inmem"},
		UseInmemTransport: true,
	}
	n, err := New(cfg, f, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer n.Shutdown()
	if err := n.BootstrapCluster(); err == nil {
		t.Fatal("BootstrapCluster expected error for nodeID not in peers, got nil")
	}
}
