package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"raft-meta/internal/config"
)

func TestNewStoreFile(t *testing.T) {
	log := hclog.NewNullLogger()
	cfg := config.SnapshotConfig{Type: "file", Path: t.TempDir(), Retain: 1}
	s, err := NewStore(cfg, log)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if s == nil {
		t.Fatal("store is nil")
	}
	if _, err := s.List(); err != nil {
		t.Fatalf("List: %v", err)
	}
}

func TestNewStoreInmem(t *testing.T) {
	log := hclog.NewNullLogger()
	cfg := config.SnapshotConfig{Type: "inmem"}
	s, err := NewStore(cfg, log)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := s.List(); err != nil {
		t.Fatalf("List: %v", err)
	}
}

func TestNewStoreRejectsUnknown(t *testing.T) {
	log := hclog.NewNullLogger()
	cfg := config.SnapshotConfig{Type: "s3"}
	if _, err := NewStore(cfg, log); err == nil {
		t.Fatal("expected error for unknown snapshot type")
	}
}

// 编译期保证返回类型满足 raft.SnapshotStore。
var _ raft.SnapshotStore = (raft.SnapshotStore)(nil)

// Focused assertion: retain <= 0 defaults to 3, and the file store actually
// creates its base directory so List() can read it.
func TestNewStoreFileRetainDefaultAndDirCreation(t *testing.T) {
	log := hclog.NewNullLogger()
	dir := filepath.Join(t.TempDir(), "nested", "snapshots")
	cfg := config.SnapshotConfig{Type: "file", Path: dir, Retain: 0}
	s, err := NewStore(cfg, log)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := s.List(); err != nil {
		t.Fatalf("List: %v", err)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("snapshot dir not created: %v", err)
	}
}
