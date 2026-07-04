package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node1.yaml")
	content := `
nodeID: node1
raftAddr: 127.0.0.1:7001
httpAddr: 127.0.0.1:8001
dataDir: ./data/node1
peers:
  - id: node1
    addr: 127.0.0.1:7001
  - id: node2
    addr: 127.0.0.1:7002
  - id: node3
    addr: 127.0.0.1:7003
snapshot:
  type: file
  path: ./data/node1/snapshots
  retain: 3
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.NodeID != "node1" {
		t.Errorf("NodeID = %q, want node1", cfg.NodeID)
	}
	if len(cfg.Peers) != 3 {
		t.Fatalf("Peers len = %d, want 3", len(cfg.Peers))
	}
	if cfg.Snapshot.Type != "file" {
		t.Errorf("Snapshot.Type = %q, want file", cfg.Snapshot.Type)
	}
	if cfg.Snapshot.Retain != 3 {
		t.Errorf("Snapshot.Retain = %d, want 3", cfg.Snapshot.Retain)
	}
}

func TestLoadRejectsEmptyPeers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	content := `
nodeID: node1
raftAddr: 127.0.0.1:7001
httpAddr: 127.0.0.1:8001
dataDir: ./data/node1
peers: []
snapshot:
  type: file
  path: ./data/node1/snapshots
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for 0 peers, got nil")
	}
}

// TestLoadAcceptsSingleNodePeers verifies the peers count is no longer
// hardcoded to 3: a 1-peer config (with nodeID matching) is valid, enabling
// single-node bootstrap / recover topologies.
func TestLoadAcceptsSingleNodePeers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "single.yaml")
	content := `
nodeID: node1
raftAddr: 127.0.0.1:7001
httpAddr: 127.0.0.1:8001
dataDir: ./data/node1
peers:
  - id: node1
    addr: 127.0.0.1:7001
snapshot:
  type: file
  path: ./data/node1/snapshots
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("expected 1-peer config to load, got: %v", err)
	}
	if len(cfg.Peers) != 1 {
		t.Fatalf("Peers len = %d, want 1", len(cfg.Peers))
	}
}

// TestLoadRejectsNodeIDNotInPeers verifies that a nodeID absent from the
// peers list is rejected at config load time, rather than failing later at
// redirect/status-fanout with a confusing "self not found".
func TestLoadRejectsNodeIDNotInPeers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	content := `
nodeID: nodeX
raftAddr: 127.0.0.1:7001
httpAddr: 127.0.0.1:8001
dataDir: ./data/node1
peers:
  - id: node1
    addr: 127.0.0.1:7001
  - id: node2
    addr: 127.0.0.1:7002
  - id: node3
    addr: 127.0.0.1:7003
snapshot:
  type: file
  path: ./data/node1/snapshots
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for nodeID not in peers, got nil")
	}
}
