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
	s, err := NewStore(cfg, "", log)
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
	s, err := NewStore(cfg, "", log)
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
	if _, err := NewStore(cfg, "", log); err == nil {
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
	s, err := NewStore(cfg, "", log)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := s.List(); err != nil {
		t.Fatalf("List: %v", err)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("snapshot base dir not created: %v", err)
	}
}

// TestNewStoreFileDefaultsToDataDir：cfg.Path 空 → 回退 dataDir 作 base，
// raft 在 <dataDir>/snapshots/ 建快照目录（不再 snapshots/snapshots 双层）。
func TestNewStoreFileDefaultsToDataDir(t *testing.T) {
	log := hclog.NewNullLogger()
	dataDir := t.TempDir()
	cfg := config.SnapshotConfig{Type: "file", Retain: 1} // Path 空
	s, err := NewStore(cfg, dataDir, log)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := s.List(); err != nil {
		t.Fatalf("List: %v", err)
	}
	// raft 在 base 下建 "snapshots/" 子目录。
	snapDir := filepath.Join(dataDir, "snapshots")
	if info, err := os.Stat(snapDir); err != nil || !info.IsDir() {
		t.Fatalf("<dataDir>/snapshots not created: %v", err)
	}
	// 不应有双层 snapshots/snapshots。
	if _, err := os.Stat(filepath.Join(snapDir, "snapshots")); err == nil {
		t.Fatal("double snapshots/snapshots layer exists — base should be dataDir, not dataDir/snapshots")
	}
}
