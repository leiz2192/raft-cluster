package raftnode

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-hclog"
	"raft-meta/internal/config"
	"raft-meta/internal/fsm"
)

func TestResetClearsDataDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "raft.db"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := Reset(dir); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("data dir not empty after reset: %v", entries)
	}
}

// Focused assertion: Reset is a no-op when the data dir does not exist
// (spec: resetting a never-initialized node must not error).
func TestResetNoOpOnMissingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if err := Reset(missing); err != nil {
		t.Fatalf("Reset on missing dir should be no-op, got: %v", err)
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("Reset should not create the dir; stat err = %v", err)
	}
}

// Focused assertion: Reset removes nested subdirectories too (e.g. the
// snapshot subdir created by NewFileSnapshotStore), not just top-level files.
func TestResetRemovesNestedSnapshotSubdir(t *testing.T) {
	dir := t.TempDir()
	snaps := filepath.Join(dir, "snaps")
	if err := os.MkdirAll(snaps, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snaps, "meta.json"), []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "raft.db"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := Reset(dir); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("data dir not empty after reset: %v", entries)
	}
}

func TestRecoverClusterSingleOnFreshDir(t *testing.T) {
	// 在一个未引导过的空数据目录上强制恢复：应能把它变成单节点集群配置。
	dir := t.TempDir()
	log := hclog.NewNullLogger()
	cfg := &config.Config{
		NodeID:   "n1",
		RaftAddr: "127.0.0.1:7301",
		DataDir:  dir,
		Peers:    []config.Peer{{ID: "n1", Addr: "127.0.0.1:7301"}},
		Snapshot: config.SnapshotConfig{Type: "file", Path: filepath.Join(dir, "snaps"), Retain: 1},
	}
	if err := RecoverClusterSingle(cfg, log); err != nil {
		t.Fatalf("RecoverClusterSingle: %v", err)
	}
	// 恢复后应能用正常 New 启动并当选 leader。
	f := fsm.New()
	n, err := New(cfg, f, log)
	if err != nil {
		t.Fatalf("New after recover: %v", err)
	}
	defer n.Shutdown()
	// 不再 BootstrapCluster（已被 RecoverCluster 写入单节点配置）。
	// leader 选举在 TestSingleNodeBootstrapAndApply 已覆盖；此处仅验证启动不报错。
}
