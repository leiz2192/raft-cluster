package server

import (
	"testing"

	"raft-meta/internal/config"
)

// inmemConfig builds a single-node config that uses inmem transport and an
// inmem snapshot store with no on-disk persistence, so Init can run without
// touching the filesystem or binding real ports.
func inmemConfig() *config.Config {
	return &config.Config{
		NodeID:   "n1",
		RaftAddr: "127.0.0.1:7001",
		HTTPAddr: "127.0.0.1:8001",
		DataDir:  "",
		Peers: []config.Peer{
			{ID: "n1", Addr: "127.0.0.1:7001"},
		},
		Snapshot:          config.SnapshotConfig{Type: "inmem"},
		UseInmemTransport: true,
	}
}

// TestInit bootstraps a single-node inmem cluster and asserts Init succeeds.
// It does not assert leadership (that belongs to raftnode tests); it only
// verifies the server wiring constructs a node and calls BootstrapCluster
// without error.
func TestInit(t *testing.T) {
	cfg := inmemConfig()
	if err := Init(cfg); err != nil {
		t.Fatalf("Init: %v", err)
	}
}
