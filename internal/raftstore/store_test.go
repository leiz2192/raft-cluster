package raftstore

import (
	"io"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"raft-meta/internal/config"
)

func TestNewStoresBoltDB(t *testing.T) {
	log := hclog.NewNullLogger()
	cfg := config.LogStoreConfig{Type: "boltdb"}
	logs, stable, closer, err := NewStores(cfg, t.TempDir(), log)
	if err != nil {
		t.Fatalf("NewStores: %v", err)
	}
	if logs == nil || stable == nil {
		t.Fatal("nil store")
	}
	if closer == nil {
		t.Fatal("boltdb closer must be non-nil")
	}
	// Functional smoke: FirstIndex on an empty store is 0.
	idx, err := logs.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex: %v", err)
	}
	if idx != 0 {
		t.Fatalf("FirstIndex = %d, want 0", idx)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestNewStoresInmem(t *testing.T) {
	log := hclog.NewNullLogger()
	cfg := config.LogStoreConfig{Type: "inmem"}
	logs, stable, closer, err := NewStores(cfg, "", log)
	if err != nil {
		t.Fatalf("NewStores: %v", err)
	}
	if logs == nil || stable == nil {
		t.Fatal("nil store")
	}
	if closer != nil {
		t.Fatalf("inmem closer must be nil, got %T", closer)
	}
}

func TestNewStoresRocksdbNotImplemented(t *testing.T) {
	log := hclog.NewNullLogger()
	cfg := config.LogStoreConfig{Type: "rocksdb"}
	_, _, _, err := NewStores(cfg, t.TempDir(), log)
	if err == nil {
		t.Fatal("expected not-implemented error for rocksdb")
	}
}

func TestNewStoresUnsupported(t *testing.T) {
	log := hclog.NewNullLogger()
	cfg := config.LogStoreConfig{Type: "mysql"}
	if _, _, _, err := NewStores(cfg, "", log); err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

// TestNewStoresAutoDefault 验证空 type 按 dataDir 自动选择，保持旧配置行为。
func TestNewStoresAutoDefault(t *testing.T) {
	log := hclog.NewNullLogger()

	// dataDir != "" → boltdb（closer 非 nil）。
	_, _, closer, err := NewStores(config.LogStoreConfig{}, t.TempDir(), log)
	if err != nil {
		t.Fatalf("auto boltdb: %v", err)
	}
	if closer == nil {
		t.Fatal("auto with dataDir should pick boltdb (closer non-nil)")
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}

	// dataDir == "" → inmem（closer nil）。
	_, _, closer2, err := NewStores(config.LogStoreConfig{}, "", log)
	if err != nil {
		t.Fatalf("auto inmem: %v", err)
	}
	if closer2 != nil {
		t.Fatal("auto without dataDir should pick inmem (closer nil)")
	}
}

// 编译期保证返回类型满足 raft 接口。
var _ raft.LogStore = (raft.LogStore)(nil)
var _ raft.StableStore = (raft.StableStore)(nil)
var _ io.Closer = (io.Closer)(nil)
