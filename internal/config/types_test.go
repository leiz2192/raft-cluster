package config

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestSizeUnmarshal(t *testing.T) {
	cases := map[string]int64{
		"100MB":   100 * (1 << 20),
		"100mb":   100 * (1 << 20), // 大小写不敏感
		"1GB":     1 << 30,
		"1GiB":    1 << 30,
		"512KiB":  512 * (1 << 10),
		"512K":    512 * (1 << 10),
		"1024":    1024, // 裸数字 = 字节
		"1.5MB":   1.5 * (1 << 20),
		"0":       0,
	}
	for in, want := range cases {
		var s Size
		if err := yaml.Unmarshal([]byte(in), &s); err != nil {
			t.Errorf("Unmarshal %q: %v", in, err)
			continue
		}
		if s.Bytes() != want {
			t.Errorf("Unmarshal %q = %d bytes, want %d", in, s.Bytes(), want)
		}
	}
}

func TestSizeRejectsBadSuffix(t *testing.T) {
	var s Size
	if err := yaml.Unmarshal([]byte("100XB"), &s); err == nil {
		t.Fatal("expected error for bad suffix")
	}
}

func TestSizeMegabytes(t *testing.T) {
	var s Size
	yaml.Unmarshal([]byte("100MB"), &s)
	if s.Megabytes() != 100 {
		t.Errorf("Megabytes = %d, want 100", s.Megabytes())
	}
}

func TestDurationUnmarshal(t *testing.T) {
	cases := map[string]time.Duration{
		"5s":    5 * time.Second,
		"10m":   10 * time.Minute,
		"1h":    time.Hour,
		"500ms": 500 * time.Millisecond,
		"1m30s": 90 * time.Second,
		"30":    30 * time.Second, // 裸数字 = 秒
	}
	for in, want := range cases {
		var d Duration
		if err := yaml.Unmarshal([]byte(in), &d); err != nil {
			t.Errorf("Unmarshal %q: %v", in, err)
			continue
		}
		if d.D() != want {
			t.Errorf("Unmarshal %q = %v, want %v", in, d.D(), want)
		}
	}
}

func TestDurationRejectsBad(t *testing.T) {
	var d Duration
	if err := yaml.Unmarshal([]byte("5xyz"), &d); err == nil {
		t.Fatal("expected error for bad duration")
	}
}

// TestFullConfigWithSizeAndDuration 验证带 Size/Duration 字段的完整配置能解析。
func TestFullConfigWithSizeAndDuration(t *testing.T) {
	yml := `
nodeID: node1
raftAddr: 127.0.0.1:7000
httpAddr: 127.0.0.1:8000
dataDir: ./data/node1
peers:
  - {id: node1, addr: 127.0.0.1:7000, httpAddr: 127.0.0.1:8000}
  - {id: node2, addr: 127.0.0.2:7000, httpAddr: 127.0.0.2:8000}
  - {id: node3, addr: 127.0.0.3:7000, httpAddr: 127.0.0.3:8000}
snapshot: {type: file, retain: 3}
raft:
  applyTimeout: 5s
  snapshotInterval: 10m
  snapshotThreshold: 2048
log:
  file: ./data/node1/raft-meta.log
  level: info
  json: false
  maxSize: 100MB
  maxBackups: 7
  maxAge: 30
`
	cfg, err := LoadFromBytes([]byte(yml))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Log.MaxSize.Megabytes() != 100 {
		t.Errorf("maxSize = %d MB, want 100", cfg.Log.MaxSize.Megabytes())
	}
	if cfg.Raft.ApplyTimeout.D() != 5*time.Second {
		t.Errorf("applyTimeout = %v, want 5s", cfg.Raft.ApplyTimeout.D())
	}
	if cfg.Raft.SnapshotInterval.D() != 10*time.Minute {
		t.Errorf("snapshotInterval = %v, want 10m", cfg.Raft.SnapshotInterval.D())
	}
	if cfg.Raft.SnapshotThreshold != 2048 {
		t.Errorf("snapshotThreshold = %d, want 2048", cfg.Raft.SnapshotThreshold)
	}
}
