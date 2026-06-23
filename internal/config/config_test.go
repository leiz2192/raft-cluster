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

func TestLoadRejectsIncompletePeers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
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
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for <3 peers, got nil")
	}
}
