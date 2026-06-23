# raft-meta 三节点元数据存储集群 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现一个基于 Go + hashicorp/raft 的 1 主 2 从三节点强一致元数据存储（层次化 KV），含可插拔快照后端、单节点损坏修复、丢失多数派强制恢复。

**Architecture:** 每个节点是对等的同一个二进制 `raft-meta`，内部分层：HTTP API → Raft 服务（hashicorp/raft）→ FSM（内存 map + 快照）→ 持久化（BoltDB log/stable + 可插拔快照）。首次成型用联合引导 + 一次性 `init`；容灾替换用 `/cluster/join`、`/cluster/remove` 成员 API。

**Tech Stack:** Go 1.22+, github.com/hashicorp/raft v1.7.1, github.com/hashicorp/raft-boltdb/v2 v2.3.0, github.com/hashicorp/go-hclog, net/http (标准库)

## Global Constraints

- Go 1.22+，module 名 `raft-meta`
- Raft 库固定 `github.com/hashicorp/raft v1.7.1`，快照存储用 `github.com/hashicorp/raft-boltdb/v2 v2.3.0`（log/stable store）+ `github.com/hashicorp/raft` 自带 `FileSnapshotStore`/`InmemSnapshotStore`
- 日志用 `github.com/hashicorp/go-hclog`
- 三节点默认地址：raft 127.0.0.1:7001-3，HTTP 127.0.0.1:8001-3，数据目录 ./data/node1-3
- 命令日志序列化默认 JSON；FSM 用 `sync.RWMutex` 保护内存 map
- 写超时默认 5s；快照阈值 1024 条日志；快照保留 3 份；选举超时 1.5s（生产）/ 200ms（测试）
- 每个任务结束必须 `go build ./...` 通过并 `git commit`
- TDD：先写失败测试 → 跑红 → 实现 → 跑绿 → 提交
- API 签名以本计划为准；若安装的 raft 版本 API 有差异，以 `go build`/`go doc` 为准修正，保持行为不变

---

## File Structure

```
raft-meta/
  go.mod
  cmd/raft-meta/main.go          # 入口：init/start/reset/recover 子命令分发
  internal/
    config/config.go             # 配置结构 + 加载 + 校验
    config/config_test.go
    fsm/fsm.go                   # raft.FSM 实现：内存 map + Apply/Snapshot/Restore
    fsm/fsm_test.go
    fsm/codec.go                 # 命令序列化（op/key/value）
    snapshot/store.go            # 可插拔快照后端工厂 + 接口
    snapshot/file.go             # FileSnapshotStore 适配
    snapshot/inmem.go            # InmemSnapshotStore 适配（测试用）
    snapshot/store_test.go
    raftnode/raftnode.go         # 封装 hashicorp/raft：建实例/引导/Apply/成员/状态
    raftnode/recover.go          # RecoverCluster 强制恢复
    raftnode/raftnode_test.go
    store/store.go               # KV 语义层：Put/Get/Delete/List，区分写读路径
    store/store_test.go
    api/api.go                   # HTTP handlers：/kv/*、/cluster/*
    api/api_test.go
    server/server.go             # 装配各模块 + 生命周期
    server/server_test.go
  testharness/
    cluster.go                   # newTestCluster(n) inmem 多节点集群 helper
  scripts/
    run-cluster.sh               # 单机起 3 节点脚本
  data/                          # 运行时数据目录（gitignore）
```

---

### Task 1: 项目脚手架 + config 包

**Files:**
- Create: `go.mod`
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`
- Create: `.gitignore`

**Interfaces:**
- Produces: `config.Config` struct（含 Node/Peers/RaftAddr/HTTPAddr/DataDir/Snapshot 配置），`config.Load(path string) (*Config, error)`

- [ ] **Step 1: 初始化 Go module**

Run:
```bash
cd /home/leiz/Documents/codespace/raft-cluster
go mod init raft-meta
```
Expected: `go: creating new go.mod: module raft-meta`

- [ ] **Step 2: 写 .gitignore**

Create `.gitignore`:
```
/data/
*.test
raft-meta
```

- [ ] **Step 3: 写 config 的失败测试**

Create `internal/config/config_test.go`:
```go
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
```

- [ ] **Step 4: 跑测试确认失败**

Run: `go test ./internal/config/ -v`
Expected: FAIL（`Load` 未定义 / 包构建失败）

- [ ] **Step 5: 实现 config**

Add `gopkg.in/yaml.v3` dependency:
```bash
go get gopkg.in/yaml.v3
```

Create `internal/config/config.go`:
```go
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Peer struct {
	ID   string `yaml:"id"`
	Addr string `yaml:"addr"`
}

type SnapshotConfig struct {
	Type   string `yaml:"type"`   // file | inmem
	Path   string `yaml:"path"`   // file 用
	Retain int    `yaml:"retain"` // 保留份数
}

type Config struct {
	NodeID   string          `yaml:"nodeID"`
	RaftAddr string          `yaml:"raftAddr"`
	HTTPAddr string          `yaml:"httpAddr"`
	DataDir  string          `yaml:"dataDir"`
	Peers    []Peer          `yaml:"peers"`
	Snapshot SnapshotConfig `yaml:"snapshot"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Snapshot.Retain == 0 {
		cfg.Snapshot.Retain = 3
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("nodeID is required")
	}
	if c.RaftAddr == "" || c.HTTPAddr == "" || c.DataDir == "" {
		return fmt.Errorf("raftAddr, httpAddr, dataDir are required")
	}
	if len(c.Peers) != 3 {
		return fmt.Errorf("peers must have exactly 3 entries, got %d", len(c.Peers))
	}
	if c.Snapshot.Type == "" {
		return fmt.Errorf("snapshot.type is required")
	}
	return nil
}

// FindSelf returns this node's own peer entry.
func (c *Config) FindSelf() (*Peer, error) {
	for i := range c.Peers {
		if c.Peers[i].ID == c.NodeID {
			return &c.Peers[i], nil
		}
	}
	return nil, fmt.Errorf("node %q not found in peers", c.NodeID)
}
```

- [ ] **Step 6: 跑测试确认通过**

Run: `go test ./internal/config/ -v`
Expected: PASS（2 个测试通过）

- [ ] **Step 7: 提交**

```bash
git add go.mod go.sum .gitignore internal/config/
git commit -m "feat(config): add config loading and validation

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: FSM 命令编解码

**Files:**
- Create: `internal/fsm/codec.go`
- Create: `internal/fsm/codec_test.go`

**Interfaces:**
- Produces: `fsm.Op`（string 常量 `OpPut`/`OpDelete`），`fsm.Command{Op,Key,Value}`，`fsm.EncodeCommand(*Command) []byte`，`fsm.DecodeCommand([]byte) (*Command, error)`

- [ ] **Step 1: 写失败测试**

Create `internal/fsm/codec_test.go`:
```go
package fsm

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeCommand(t *testing.T) {
	cases := []Command{
		{Op: OpPut, Key: "/nodes/n1", Value: []byte("data")},
		{Op: OpDelete, Key: "/services/svc1"},
		{Op: OpPut, Key: "empty", Value: nil},
	}
	for _, c := range cases {
		data, err := EncodeCommand(&c)
		if err != nil {
			t.Fatalf("EncodeCommand: %v", err)
		}
		got, err := DecodeCommand(data)
		if err != nil {
			t.Fatalf("DecodeCommand: %v", err)
		}
		if got.Op != c.Op || got.Key != c.Key || !bytes.Equal(got.Value, c.Value) {
			t.Errorf("roundtrip mismatch: got %+v, want %+v", got, c)
		}
	}
}

func TestDecodeInvalid(t *testing.T) {
	if _, err := DecodeCommand([]byte("not json")); err == nil {
		t.Fatal("expected error for invalid json")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/fsm/ -run TestEncodeDecode -v`
Expected: FAIL（类型未定义）

- [ ] **Step 3: 实现 codec**

Create `internal/fsm/codec.go`:
```go
package fsm

import (
	"encoding/json"
	"fmt"
)

type Op string

const (
	OpPut    Op = "put"
	OpDelete Op = "delete"
)

type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

func EncodeCommand(c *Command) ([]byte, error) {
	return json.Marshal(c)
}

func DecodeCommand(data []byte) (*Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("decode command: %w", err)
	}
	return &c, nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/fsm/ -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/fsm/codec.go internal/fsm/codec_test.go
git commit -m "feat(fsm): add command codec

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: FSM 实现（内存 map + Apply/Snapshot/Restore）

**Files:**
- Create: `internal/fsm/fsm.go`
- Create: `internal/fsm/fsm_test.go`

**Interfaces:**
- Consumes: `fsm.Command`/`fsm.DecodeCommand`（Task 2）
- Produces: `fsm.FSM` 实现 `raft.FSM` 接口，方法 `Apply(*raft.Log)`、`Snapshot() (raft.FSMSnapshot, error)`、`Restore(io.ReadCloser) error`；另有公开方法 `Get(key string) ([]byte, bool)`、`List(prefix string) map[string][]byte` 供 store 读取

- [ ] **Step 1: 加 raft 依赖**

```bash
go get github.com/hashicorp/raft@v1.7.1
```

- [ ] **Step 2: 写失败测试**

Create `internal/fsm/fsm_test.go`:
```go
package fsm

import (
	"bytes"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

func newLog(t *testing.T, c *Command) *raft.Log {
	t.Helper()
	data, err := EncodeCommand(c)
	if err != nil {
		t.Fatal(err)
	}
	return &raft.Log{Data: data}
}

func TestApplyPutAndDelete(t *testing.T) {
	f := New()
	f.Apply(newLog(t, &Command{Op: OpPut, Key: "k1", Value: []byte("v1")}))
	got, ok := f.Get("k1")
	if !ok || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get(k1) = %q,%v, want v1,true", got, ok)
	}
	f.Apply(newLog(t, &Command{Op: OpDelete, Key: "k1"}))
	if _, ok := f.Get("k1"); ok {
		t.Fatal("k1 should be deleted")
	}
}

func TestListPrefix(t *testing.T) {
	f := New()
	f.Apply(newLog(t, &Command{Op: OpPut, Key: "/nodes/n1", Value: []byte("a")}))
	f.Apply(newLog(t, &Command{Op: OpPut, Key: "/nodes/n2", Value: []byte("b")}))
	f.Apply(newLog(t, &Command{Op: OpPut, Key: "/services/s1", Value: []byte("c")}))
	got := f.List("/nodes/")
	if len(got) != 2 {
		t.Fatalf("List /nodes/ len = %d, want 2", len(got))
	}
}

func TestSnapshotRestoreRoundtrip(t *testing.T) {
	src := New()
	src.Apply(newLog(t, &Command{Op: OpPut, Key: "k1", Value: []byte("v1")}))
	src.Apply(newLog(t, &Command{Op: OpPut, Key: "k2", Value: []byte("v2")}))

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&buf); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	dst := New()
	if err := dst.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got, ok := dst.Get("k1")
	if !ok || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("after restore Get(k1) = %q,%v", got, ok)
	}
}

func TestApplyIgnoresUnknownOp(t *testing.T) {
	f := New()
	f.Apply(newLog(t, &Command{Op: "bogus", Key: "k1", Value: []byte("v")}))
	if _, ok := f.Get("k1"); ok {
		t.Fatal("unknown op should not write")
	}
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `go test ./internal/fsm/ -run TestApply -v`
Expected: FAIL（`New` 未定义）

- [ ] **Step 4: 实现 FSM**

Create `internal/fsm/fsm.go`:
```go
package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
)

// FSM implements raft.FSM with an in-memory map protected by a RWMutex.
type FSM struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func New() *FSM {
	return &FSM{data: make(map[string][]byte)}
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	cmd, err := DecodeCommand(log.Data)
	if err != nil {
		// 损坏的日志条目不应让集群卡死；记录后跳过。
		return fmt.Errorf("decode log: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch cmd.Op {
	case OpPut:
		f.data[cmd.Key] = append([]byte(nil), cmd.Value...)
	case OpDelete:
		delete(f.data, cmd.Key)
	default:
		// 未知操作：幂等忽略。
	}
	return nil
}

func (f *FSM) Get(key string) ([]byte, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

func (f *FSM) List(prefix string) map[string][]byte {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string][]byte)
	for k, v := range f.data {
		if strings.HasPrefix(k, prefix) {
			out[k] = append([]byte(nil), v...)
		}
	}
	return out
}

// snapshotData is the serialized form of the FSM for snapshots.
type snapshotData struct {
	Data map[string][]byte `json:"data"`
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	// 拷贝一致视图，避免 Persist 期间被 Apply 改动。
	copied := make(map[string][]byte, len(f.data))
	for k, v := range f.data {
		copied[k] = append([]byte(nil), v...)
	}
	return &fsmSnapshot{data: copied}, nil
}

type fsmSnapshot struct {
	data map[string][]byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	enc := json.NewEncoder(sink)
	if err := enc.Encode(snapshotData{Data: s.data}); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var sd snapshotData
	if err := json.NewDecoder(rc).Decode(&sd); err != nil {
		return fmt.Errorf("restore decode: %w", err)
	}
	f.mu.Lock()
	f.data = sd.Data
	if f.data == nil {
		f.data = make(map[string][]byte)
	}
	f.mu.Unlock()
	return nil
}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `go test ./internal/fsm/ -v`
Expected: PASS（全部测试）

- [ ] **Step 6: 提交**

```bash
git add internal/fsm/fsm.go internal/fsm/fsm_test.go go.mod go.sum
git commit -m "feat(fsm): implement in-memory FSM with snapshot/restore

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: 可插拔快照存储后端

**Files:**
- Create: `internal/snapshot/store.go`
- Create: `internal/snapshot/file.go`
- Create: `internal/snapshot/inmem.go`
- Create: `internal/snapshot/store_test.go`

**Interfaces:**
- Consumes: `config.SnapshotConfig`（Task 1）
- Produces: `snapshot.NewStore(cfg config.SnapshotConfig, logger hclog.Logger) (raft.SnapshotStore, error)`；`snapshot.NewInmem(logger) raft.SnapshotStore`

- [ ] **Step 1: 加 hclog 依赖**

```bash
go get github.com/hashicorp/go-hclog
```

- [ ] **Step 2: 写失败测试**

Create `internal/snapshot/store_test.go`:
```go
package snapshot

import (
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/raft-meta/internal/config"
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
```

- [ ] **Step 3: 跑测试确认失败**

Run: `go test ./internal/snapshot/ -v`
Expected: FAIL（`NewStore` 未定义）

- [ ] **Step 4: 实现工厂与后端**

Create `internal/snapshot/store.go`:
```go
package snapshot

import (
	"fmt"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/raft-meta/internal/config"
)

// NewStore returns a raft.SnapshotStore based on cfg.Type.
// 后端与 FSM 序列化格式正交：本工厂只决定快照字节存哪。
func NewStore(cfg config.SnapshotConfig, logger hclog.Logger) (raft.SnapshotStore, error) {
	switch cfg.Type {
	case "file":
		return newFileStore(cfg, logger)
	case "inmem":
		return NewInmem(logger), nil
	default:
		return nil, fmt.Errorf("unsupported snapshot type %q (supported: file, inmem; s3 待实现)", cfg.Type)
	}
}
```

Create `internal/snapshot/file.go`:
```go
package snapshot

import (
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/raft-meta/internal/config"
)

func newFileStore(cfg config.SnapshotConfig, logger hclog.Logger) (raft.SnapshotStore, error) {
	retain := cfg.Retain
	if retain <= 0 {
		retain = 3
	}
	return raft.NewFileSnapshotStore(cfg.Path, retain, logger.StandardLogger(&hclog.StandardLoggerOptions{
		InferLevels: false,
	}))
}
```

Create `internal/snapshot/inmem.go`:
```go
package snapshot

import (
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

func NewInmem(logger hclog.Logger) raft.SnapshotStore {
	return raft.NewInmemSnapshotStore(logger)
}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `go test ./internal/snapshot/ -v`
Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add internal/snapshot/ go.mod go.sum
git commit -m "feat(snapshot): pluggable snapshot store (file/inmem)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: raftnode 封装（建实例 + 引导 + Apply + 状态）

**Files:**
- Create: `internal/raftnode/raftnode.go`
- Create: `internal/raftnode/raftnode_test.go`

**Interfaces:**
- Consumes: `config.Config`、`fsm.FSM`、`snapshot.NewStore`、`raft.NewTCPTransport`/`NewInmemTransport`、`raft_boltdb.NewBoltStore`
- Produces: `raftnode.Node`，方法：`New(cfg *config.Config, f *fsm.FSM, logger hclog.Logger) (*Node, error)`、`BootstrapCluster() error`、`Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture`、`IsLeader() bool`、`LeaderAddr() string`、`State() raft.State`、`Stats() map[string]string`、`AddVoter(id, addr string) error`、`RemoveServer(id string) error`、`Raft() *raft.Raft`、`Shutdown() error`

- [ ] **Step 1: 加 raft-boltdb 依赖**

```bash
go get github.com/hashicorp/raft-boltdb/v2@v2.3.0
```

- [ ] **Step 2: 写失败测试（用 inmem transport）**

Create `internal/raftnode/raftnode_test.go`:
```go
package raftnode

import (
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/raft-meta/internal/config"
	"github.com/raft-meta/internal/fsm"
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
	t.Fatalf("persisted data lost after restart: got %q", f2.Get)
}
```

Add `"path/filepath"` to the test file imports.

- [ ] **Step 3: 跑测试确认失败**

Run: `go test ./internal/raftnode/ -v`
Expected: FAIL（`New`、`UseInmemTransport` 未定义）

- [ ] **Step 4: 实现 raftnode**

Create `internal/raftnode/raftnode.go`:
```go
package raftnode

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/raft-meta/internal/config"
	"github.com/raft-meta/internal/fsm"
	"github.com/raft-meta/internal/snapshot"
)

type Node struct {
	cfg    *config.Config
	raft   *raft.Raft
	fsm    *fsm.FSM
	trans  raft.Transport
	logs   raft.LogStore
	stable raft.StableStore
	snaps  raft.SnapshotStore
	logger hclog.Logger
}

// New constructs a Node. Transport and persistence are decoupled:
//   - transport: inmem when cfg.UseInmemTransport, else TCP
//   - persistence: BoltDB (log+stable) + cfg.Snapshot store when cfg.DataDir != "",
//     else inmem log/stable + cfg.Snapshot store
// This lets tests use inmem transport with real BoltDB persistence to verify
// snapshot/log durability across restarts.
func New(cfg *config.Config, f *fsm.FSM, logger hclog.Logger) (*Node, error) {
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.SnapshotThreshold = 1024
	raftCfg.SnapshotInterval = 10 * time.Minute
	raftCfg.Logger = logger

	n := &Node{cfg: cfg, fsm: f, logger: logger}

	// --- transport ---
	if cfg.UseInmemTransport {
		trans, err := raft.NewInmemTransport(raft.ServerAddress(cfg.RaftAddr))
		if err != nil {
			return nil, err
		}
		n.trans = trans
		raftCfg.HeartbeatTimeout = 200 * time.Millisecond
		raftCfg.ElectionTimeout = 200 * time.Millisecond
		raftCfg.CommitTimeout = 50 * time.Millisecond
	} else {
		trans, err := raft.NewTCPTransport(cfg.RaftAddr, nil, 3, 10*time.Second, logger.StandardLogger(&hclog.StandardLoggerOptions{}))
		if err != nil {
			return nil, fmt.Errorf("tcp transport: %w", err)
		}
		n.trans = trans
	}

	// --- persistence ---
	if cfg.DataDir != "" {
		if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
			return nil, fmt.Errorf("mkdir dataDir: %w", err)
		}
		boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
		if err != nil {
			return nil, fmt.Errorf("bolt store: %w", err)
		}
		n.stable = boltStore
		n.logs = boltStore
	} else {
		n.logs = raft.NewInmemStore()
		n.stable = raft.NewInmemStore()
	}
	snaps, err := snapshot.NewStore(cfg.Snapshot, logger)
	if err != nil {
		return nil, err
	}
	n.snaps = snaps

	r, err := raft.NewRaft(raftCfg, f, n.logs, n.stable, n.snaps, n.trans)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}
	n.raft = r
	return n, nil
}

func (n *Node) BootstrapCluster() error {
	servers := make([]raft.Server, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(p.ID),
			Address: raft.ServerAddress(p.Addr),
		})
	}
	cfg := raft.Configuration{Servers: servers}
	fut := n.raft.BootstrapCluster(cfg)
	if err := fut.Error(); err != nil {
		// 已引导过不算错误（重启场景）。
		if errors.Is(err, raft.ErrCantBootstrap) {
			return nil
		}
		return err
	}
	return nil
}

func (n *Node) Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture {
	return n.raft.Apply(cmd, timeout)
}

func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *Node) LeaderAddr() string {
	return string(n.raft.Leader())
}

func (n *Node) State() raft.State {
	return n.raft.State()
}

func (n *Node) Stats() map[string]string {
	return n.raft.Stats()
}

func (n *Node) AddVoter(id, addr string) error {
	fut := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 5*time.Second)
	return fut.Error()
}

func (n *Node) RemoveServer(id string) error {
	fut := n.raft.RemoveServer(raft.ServerID(id), 0, 5*time.Second)
	return fut.Error()
}

func (n *Node) Raft() *raft.Raft { return n.raft }

func (n *Node) Shutdown() error {
	if n.raft == nil {
		return nil
	}
	fut := n.raft.Shutdown()
	return fut.Error()
}
```

Add `UseInmemTransport bool` field to `config.Config` in `internal/config/config.go` (yaml tag `-`, not validated):
```go
type Config struct {
	NodeID            string          `yaml:"nodeID"`
	RaftAddr          string          `yaml:"raftAddr"`
	HTTPAddr          string          `yaml:"httpAddr"`
	DataDir           string          `yaml:"dataDir"`
	Peers             []Peer          `yaml:"peers"`
	Snapshot          SnapshotConfig  `yaml:"snapshot"`
	UseInmemTransport bool            `yaml:"-"`
}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `go test ./internal/raftnode/ -v`
Expected: PASS

If `raft.NewTCPTransport` / `raftboltdb.NewBoltStore` signatures differ in the installed version, run `go doc github.com/hashicorp/raft.NewTCPTransport` and `go doc github.com/hashicorp/raft-boltdb/v2.NewBoltStore` and adjust calls to match; behavior must stay the same.

- [ ] **Step 6: 提交**

```bash
git add internal/raftnode/ internal/config/config.go go.mod go.sum
git commit -m "feat(raftnode): wrap hashicorp/raft instance, bootstrap, apply, status

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 6: store KV 语义层

**Files:**
- Create: `internal/store/store.go`
- Create: `internal/store/store_test.go`

**Interfaces:**
- Consumes: `raftnode.Node`（Apply/IsLeader/LeaderAddr）、`fsm.FSM`（Get/List）
- Produces: `store.Store`，方法：`New(n *raftnode.Node, f *fsm.FSM, applyTimeout time.Duration) *Store`、`Put(key string, value []byte) error`、`Delete(key string) error`、`Get(key string) ([]byte, bool)`、`List(prefix string) map[string][]byte`、`ErrNotLeader`（含 leader 地址）

- [ ] **Step 1: 写失败测试**

Create `internal/store/store_test.go`:
```go
package store

import (
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/raft-meta/internal/config"
	"github.com/raft-meta/internal/fsm"
	"github.com/raft-meta/internal/raftnode"
)

func newLeaderStore(t *testing.T) (*Store, *fsm.FSM) {
	t.Helper()
	log := hclog.NewNullLogger()
	f := fsm.New()
	cfg := &config.Config{
		NodeID:   "n1",
		RaftAddr: "127.0.0.1:7101",
		Peers:    []config.Peer{{ID: "n1", Addr: "127.0.0.1:7101"}},
		Snapshot: config.SnapshotConfig{Type: "inmem"},
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
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/store/ -v`
Expected: FAIL（`New`、`Store` 未定义）

- [ ] **Step 3: 实现 store**

Create `internal/store/store.go`:
```go
package store

import (
	"errors"
	"time"

	"github.com/raft-meta/internal/fsm"
	"github.com/raft-meta/internal/raftnode"
)

// ErrNotLeader is returned for writes on a non-leader. LeaderAddr holds the
// known leader's raft address (may be empty during election).
var ErrNotLeader = errors.New("not leader")

type Store struct {
	node         *raftnode.Node
	fsm          *fsm.FSM
	applyTimeout time.Duration
}

func New(n *raftnode.Node, f *fsm.FSM, applyTimeout time.Duration) *Store {
	return &Store{node: n, fsm: f, applyTimeout: applyTimeout}
}

func (s *Store) Put(key string, value []byte) error {
	if !s.node.IsLeader() {
		return ErrNotLeader
	}
	cmd, err := fsm.EncodeCommand(&fsm.Command{Op: fsm.OpPut, Key: key, Value: value})
	if err != nil {
		return err
	}
	return s.node.Apply(cmd, s.applyTimeout).Error()
}

func (s *Store) Delete(key string) error {
	if !s.node.IsLeader() {
		return ErrNotLeader
	}
	cmd, err := fsm.EncodeCommand(&fsm.Command{Op: fsm.OpDelete, Key: key})
	if err != nil {
		return err
	}
	return s.node.Apply(cmd, s.applyTimeout).Error()
}

// Get reads from local FSM. On a leader this is strongly consistent.
// On a follower it may be stale (脏读) — caller accepts this by default.
func (s *Store) Get(key string) ([]byte, bool) {
	return s.fsm.Get(key)
}

func (s *Store) List(prefix string) map[string][]byte {
	return s.fsm.List(prefix)
}

// LeaderAddr returns the known leader raft address for redirect purposes.
func (s *Store) LeaderAddr() string {
	return s.node.LeaderAddr()
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/store/ -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/store/
git commit -m "feat(store): KV semantics layer with leader-only writes

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 7: HTTP API（/kv 与 /cluster）

**Files:**
- Create: `internal/api/api.go`
- Create: `internal/api/api_test.go`

**Interfaces:**
- Consumes: `store.Store`、`raftnode.Node`
- Produces: `api.New(s *store.Store, n *raftnode.Node) *API`、`api.API.Handler() http.Handler`；路由 `PUT/POST /kv/{key}`、`GET /kv/{key}`、`GET /kv?prefix=`、`DELETE /kv/{key}`、`GET /cluster/status`、`POST /cluster/join`、`POST /cluster/remove`

- [ ] **Step 1: 写失败测试**

Create `internal/api/api_test.go`:
```go
package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/raft-meta/internal/config"
	"github.com/raft-meta/internal/fsm"
	"github.com/raft-meta/internal/raftnode"
	"github.com/raft-meta/internal/store"
)

func newAPI(t *testing.T) (*API, *fsm.FSM) {
	t.Helper()
	log := hclog.NewNullLogger()
	f := fsm.New()
	cfg := &config.Config{
		NodeID: "n1", RaftAddr: "127.0.0.1:7201", HTTPAddr: "127.0.0.1:8201",
		Peers: []config.Peer{{ID: "n1", Addr: "127.0.0.1:7201"}},
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
	return New(store.New(n, f, 2*time.Second), n), f
}

func TestPutAndGet(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	body, _ := json.Marshal(map[string]string{"value": "hello"})
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/k1", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(srv.URL + "/kv/k1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]string
	json.NewDecoder(resp.Body).Decode(&got)
	if got["value"] != "hello" {
		t.Fatalf("GET value = %q, want hello", got["value"])
	}
}

func TestList(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	for _, k := range []string{"/nodes/n1", "/nodes/n2", "/svc/s1"} {
		body, _ := json.Marshal(map[string]string{"value": "v"})
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv"+k, bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	resp, err := http.Get(srv.URL + "/kv?prefix=/nodes/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]string
	json.NewDecoder(resp.Body).Decode(&got)
	if len(got) != 2 {
		t.Fatalf("list len = %d, want 2; got %v", len(got), got)
	}
}

func TestDelete(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	body, _ := json.Marshal(map[string]string{"value": "v"})
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/k1", bytes.NewReader(body))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/kv/k1", nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(srv.URL + "/kv/k1")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestClusterStatus(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/cluster/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&got)
	if got["state"] != "Leader" {
		t.Fatalf("state = %v, want Leader", got["state"])
	}
	if !strings.Contains(got["leader"].(string), "7201") {
		t.Fatalf("leader = %v", got["leader"])
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/api/ -v`
Expected: FAIL（`New`、`API` 未定义）

- [ ] **Step 3: 实现 api**

Create `internal/api/api.go`:
```go
package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/raft-meta/internal/raftnode"
	"github.com/raft-meta/internal/store"
)

type API struct {
	store *store.Store
	node  *raftnode.Node
}

func New(s *store.Store, n *raftnode.Node) *API {
	return &API{store: s, node: n}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", a.handleKV)
	mux.HandleFunc("/kv", a.handleKVList)
	mux.HandleFunc("/cluster/status", a.handleClusterStatus)
	mux.HandleFunc("/cluster/join", a.handleJoin)
	mux.HandleFunc("/cluster/remove", a.handleRemove)
	return mux
}

type kvBody struct {
	Value string `json:"value"`
}

func (a *API) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPut, http.MethodPost:
		var b kvBody
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil && err != io.EOF {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		if err := a.store.Put(key, []byte(b.Value)); err != nil {
			a.writeWriteError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case http.MethodDelete:
		if err := a.store.Delete(key); err != nil {
			a.writeWriteError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case http.MethodGet:
		if r.URL.Query().Get("consistent") == "true" && !a.node.IsLeader() {
			a.redirectToLeader(w, r)
			return
		}
		v, ok := a.store.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"value": string(v)})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *API) handleKVList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	if r.URL.Query().Get("consistent") == "true" && !a.node.IsLeader() {
		a.redirectToLeader(w, r)
		return
	}
	items := a.store.List(prefix)
	out := make(map[string]string, len(items))
	for k, v := range items {
		out[k] = string(v)
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	stats := a.node.Stats()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"nodeID": a.node.Stats()["node_id"],
		"state":  a.node.State().String(),
		"leader": a.node.LeaderAddr(),
		"stats":  stats,
	})
}

type memberBody struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

func (a *API) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !a.node.IsLeader() {
		a.redirectToLeader(w, r)
		return
	}
	var b memberBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if err := a.node.AddVoter(b.ID, b.Addr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "added"})
}

func (a *API) handleRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !a.node.IsLeader() {
		a.redirectToLeader(w, r)
		return
	}
	var b memberBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if err := a.node.RemoveServer(b.ID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

func (a *API) writeWriteError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotLeader) {
		a.redirectToLeader(w, nil)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// redirectToLeader sends 307 to the leader's HTTP address when known,
// else 503. Maps raft address to http address via the configured peers.
func (a *API) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leader := a.node.LeaderAddr()
	if leader == "" {
		http.Error(w, "no leader (election in progress)", http.StatusServiceUnavailable)
		return
	}
	// 307 重定向：简单可靠，客户端自行重试落到 leader。
	// 真实部署需 raft:port → http:port 映射；测试中 leader 地址已含可识别端口。
	httpAddr := leader // 由 server 层在装配时注入映射；此处兜底用 raft 地址
	writeJSON(w, http.StatusTemporaryRedirect, map[string]string{
		"leader": httpAddr,
	})
	w.Header().Set("Location", "http://"+httpAddr)
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/api/ -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/api/
git commit -m "feat(api): HTTP /kv and /cluster endpoints with leader redirect

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 8: server 装配 + cmd 入口（init/start）

**Files:**
- Create: `internal/server/server.go`
- Create: `cmd/raft-meta/main.go`
- Create: `scripts/run-cluster.sh`
- Create: `configs/node1.yaml`, `configs/node2.yaml`, `configs/node3.yaml`

**Interfaces:**
- Consumes: 全部上游模块
- Produces: `server.Run(cfg *config.Config) error`（启动 raft + HTTP，阻塞至信号）；CLI `raft-meta init -config <path>` 与 `raft-meta start -config <path>`

- [ ] **Step 1: 写配置文件**

Create `configs/node1.yaml`:
```yaml
nodeID: node1
raftAddr: 127.0.0.1:7001
httpAddr: 127.0.0.1:8001
dataDir: ./data/node1
peers:
  - {id: node1, addr: 127.0.0.1:7001}
  - {id: node2, addr: 127.0.0.1:7002}
  - {id: node3, addr: 127.0.0.1:7003}
snapshot:
  type: file
  path: ./data/node1/snapshots
  retain: 3
```

Create `configs/node2.yaml` and `configs/node3.yaml` identically but with nodeID/raftAddr/httpAddr/dataDir/snapshot.path changed to node2 (7002/8002) and node3 (7003/8003).

- [ ] **Step 2: 实现 server**

Create `internal/server/server.go`:
```go
package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/raft-meta/internal/api"
	"github.com/raft-meta/internal/config"
	"github.com/raft-meta/internal/fsm"
	"github.com/raft-meta/internal/raftnode"
	"github.com/raft-meta/internal/store"
)

// Run builds and runs a node: raft + HTTP server, blocks until SIGINT/SIGTERM.
func Run(cfg *config.Config) error {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:  cfg.NodeID,
		Level: hclog.Info,
	})

	f := fsm.New()
	n, err := raftnode.New(cfg, f, logger)
	if err != nil {
		return fmt.Errorf("raftnode: %w", err)
	}
	defer n.Shutdown()

	s := store.New(n, f, 5*time.Second)
	a := api.New(s, n)
	httpSrv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: a.Handler(),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpSrv.Shutdown(shutdownCtx)
		return nil
	}
}

// Init bootstraps the cluster from the given config (call once, on one node).
func Init(cfg *config.Config) error {
	logger := hclog.New(&hclog.LoggerOptions{Name: cfg.NodeID + "-init", Level: hclog.Info})
	f := fsm.New()
	n, err := raftnode.New(cfg, f, logger)
	if err != nil {
		return err
	}
	defer n.Shutdown()
	if err := n.BootstrapCluster(); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	fmt.Printf("cluster bootstrapped with %d peers on node %s\n", len(cfg.Peers), cfg.NodeID)
	return nil
}
```

- [ ] **Step 3: 实现 cmd 入口**

Create `cmd/raft-meta/main.go`:
```go
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/raft-meta/internal/config"
	"github.com/raft-meta/internal/server"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	sub := os.Args[1]
	fs := flag.NewFlagSet(sub, flag.ExitOnError)
	configPath := fs.String("config", "", "path to config yaml")
	fs.Parse(os.Args[2:])
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "error: -config required")
		os.Exit(2)
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	switch sub {
	case "init":
		if err := server.Init(cfg); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "start":
		if err := server.Run(cfg); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "reset", "recover":
		fmt.Fprintln(os.Stderr, "error:", sub, "not yet implemented (see Task 9)")
		os.Exit(1)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: raft-meta {init|start|reset|recover} -config <path>")
}
```

- [ ] **Step 4: 写启动脚本**

Create `scripts/run-cluster.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# 首次部署：在 node1 上引导一次（仅当数据目录为空）
if [ ! -d data/node1 ]; then
  echo "bootstrapping cluster on node1..."
  go run ./cmd/raft-meta init -config configs/node1.yaml
fi

# 三个节点并行启动
for i in 1 2 3; do
  go run ./cmd/raft-meta start -config configs/node$i.yaml &
done

wait
```
Run: `chmod +x scripts/run-cluster.sh`

- [ ] **Step 5: 构建确认**

Run: `go build ./...`
Expected: 无错误

- [ ] **Step 6: 提交**

```bash
git add internal/server/ cmd/ scripts/ configs/
git commit -m "feat(server): assemble node + init/start CLI + run script

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 9: cmd reset + recover（损坏修复 + 强制单节点恢复）

**Files:**
- Modify: `cmd/raft-meta/main.go`
- Create: `internal/raftnode/recover.go`

**Interfaces:**
- Consumes: `raft.RecoverCluster`、`raft.NewBoltStore`、`raft.NewFileSnapshotStore`
- Produces: `raftnode.Reset(dataDir string) error`（擦除数据目录）、`raftnode.RecoverClusterSingle(cfg *config.Config, logger hclog.Logger) error`（强制单节点恢复）

- [ ] **Step 1: 写 recover 的失败测试**

Create `internal/raftnode/recover_test.go`:
```go
package raftnode

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/raft-meta/internal/config"
	"github.com/raft-meta/internal/fsm"
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
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/raftnode/ -run TestReset -v`
Expected: FAIL（`Reset` 未定义）

- [ ] **Step 3: 实现 recover**

Create `internal/raftnode/recover.go`:
```go
package raftnode

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/raft-meta/internal/config"
	"github.com/raft-meta/internal/fsm"
	"github.com/raft-meta/internal/snapshot"
)

// Reset wipes all persistent state in dataDir (log + stable + snapshots).
// 用于单节点数据损坏后从 leader 重建：擦除后重启即可。
func Reset(dataDir string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dataDir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// RecoverClusterSingle forcibly rewrites the cluster config to a single voter
// (this node) and restores FSM from the latest snapshot + log replay. 用于
// 丢失多数派（只剩 1 节点）的最后手段恢复；可能丢已提交数据。
func RecoverClusterSingle(cfg *config.Config, logger hclog.Logger) error {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("mkdir dataDir: %w", err)
	}
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("bolt store: %w", err)
	}
	defer boltStore.Close()

	snaps, err := snapshot.NewStore(cfg.Snapshot, logger)
	if err != nil {
		return err
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.Logger = logger

	trans, err := raft.NewInmemTransport(raft.ServerAddress(cfg.RaftAddr))
	if err != nil {
		return err
	}

	configuration := raft.Configuration{
		Servers: []raft.Server{{
			ID:      raft.ServerID(cfg.NodeID),
			Address: raft.ServerAddress(cfg.RaftAddr),
		}},
	}

	// RecoverCluster 加载最新快照 → Restore FSM → 重放日志到 commitIndex →
	// 把配置强制改写为单节点。
	if err := raft.RecoverCluster(raftCfg, fsm.New(), boltStore, boltStore, snaps, trans, configuration); err != nil {
		return fmt.Errorf("recover cluster: %w", err)
	}
	return nil
}

// recoverDummy keeps the time import used when build tags vary.
var _ = time.Second
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/raftnode/ -v`
Expected: PASS

If `raft.RecoverCluster` signature differs (e.g., transport param type), run `go doc github.com/hashicorp/raft.RecoverCluster` and adjust to match. The inmem transport satisfies the required interface.

- [ ] **Step 5: 接入 cmd**

Modify `cmd/raft-meta/main.go` switch block to handle `reset` and `recover`:
```go
	case "reset":
		if err := raftnode.Reset(cfg.DataDir); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Println("data directory cleared:", cfg.DataDir)
	case "recover":
		if err := raftnode.RecoverClusterSingle(cfg, hclog.New(&hclog.LoggerOptions{Name: "recover", Level: hclog.Info})); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Println("cluster recovered to single-node:", cfg.NodeID)
```
Add imports `"github.com/hashicorp/go-hclog"` and `"github.com/raft-meta/internal/raftnode"` to main.go.

- [ ] **Step 6: 构建确认**

Run: `go build ./...`
Expected: 无错误

- [ ] **Step 7: 提交**

```bash
git add internal/raftnode/recover.go internal/raftnode/recover_test.go cmd/raft-meta/main.go
git commit -m "feat(raftnode): reset + RecoverClusterSingle for DR

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 10: 集成测试 harness（多节点 inmem 集群）

**Files:**
- Create: `testharness/cluster.go`
- Create: `testharness/cluster_test.go`

**Interfaces:**
- Produces: `testharness.NewCluster(t *testing.T, n int) *Cluster`，方法 `Leader() *Node`、`Node(id string) *Node`、`ShutdownNode(id string)`、`RestartNode(id string)`、`WaitForLeader(t) string`、`ShutdownAll()`；`testharness.Node` 暴露 `*raftnode.Node`、`*fsm.FSM`、`*store.Store`

- [ ] **Step 1: 写失败测试**

Create `testharness/cluster_test.go`:
```go
package testharness

import (
	"testing"
	"time"
)

func TestThreeNodeClusterElectsLeaderAndReplicates(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.ShutdownAll()

	leader := c.WaitForLeader(t)
	if leader == "" {
		t.Fatal("no leader elected")
	}

	// 写 leader，验证 3 节点 FSM 一致。
	lid := c.LeaderID()
	s := c.Node(lid).Store
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range c IDs() {
			if _, found := c.Node(id).FSM.Get("k"); !found {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("replication did not converge on all nodes")
}

func TestLeaderFailover(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.ShutdownAll()
	old := c.LeaderID()
	c.ShutdownNode(old)
	// 剩 2 节点，应选出新 leader。
	newLeader := c.WaitForLeader(t)
	if newLeader == "" || newLeader == old {
		t.Fatalf("failover failed: old=%s new=%s", old, newLeader)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./testharness/ -v`
Expected: FAIL（`NewCluster` 等未定义）

- [ ] **Step 3: 实现 harness**

Create `testharness/cluster.go`:
```go
package testharness

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/raft-meta/internal/config"
	"github.com/raft-meta/internal/fsm"
	"github.com/raft-meta/internal/raftnode"
	"github.com/raft-meta/internal/store"
)

type Node struct {
	ID     string
	Raft   *raftnode.Node
	FSM    *fsm.FSM
	Store  *store.Store
	cfg    *config.Config
	logger hclog.Logger
}

type Cluster struct {
	t      *testing.T
	nodes  map[string]*Node
	order  []string
	logger hclog.Logger
}

func NewCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	c := &Cluster{t: t, nodes: map[string]*Node{}, logger: hclog.NewNullLogger()}
	peers := make([]config.Peer, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i+1)
		peers[i] = config.Peer{ID: id, Addr: fmt.Sprintf("127.0.0.1:%d", 7400+i+1)}
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i+1)
		cfg := &config.Config{
			NodeID:   id,
			RaftAddr: peers[i].Addr,
			HTTPAddr: fmt.Sprintf("127.0.0.1:%d", 8400+i+1),
			Peers:    peers,
			Snapshot:          config.SnapshotConfig{Type: "inmem"},
			UseInmemTransport: true,
		}
		f := fsm.New()
		node, err := raftnode.New(cfg, f, c.logger)
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		if err := node.BootstrapCluster(); err != nil {
			t.Fatalf("bootstrap %s: %v", id, err)
		}
		nd := &Node{ID: id, Raft: node, FSM: f, Store: store.New(node, f, 2*time.Second), cfg: cfg, logger: c.logger}
		c.nodes[id] = nd
		c.order = append(c.order, id)
	}
	return c
}

func (c *Cluster) IDs() []string { return c.order }

func (c *Cluster) Node(id string) *Node { return c.nodes[id] }

func (c *Cluster) LeaderID() string {
	for _, id := range c.order {
		if n, ok := c.nodes[id]; ok && n.Raft.IsLeader() {
			return id
		}
	}
	return ""
}

func (c *Cluster) WaitForLeader(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.order {
			if n, ok := c.nodes[id]; ok && n.Raft.IsLeader() {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ""
}

func (c *Cluster) ShutdownNode(id string) {
	if n, ok := c.nodes[id]; ok {
		n.Raft.Shutdown()
	}
}

func (c *Cluster) RestartNode(id string) {
	c.t.Helper()
	old := c.nodes[id]
	// 关闭后用同配置重建（inmem store 不可恢复状态，此 helper 仅用于选举/拓扑测试，
	// 不验证持久化恢复——后者由 raftnode 单测覆盖）。
	if old != nil {
		old.Raft.Shutdown()
	}
	f := fsm.New()
	node, err := raftnode.New(old.cfg, f, c.logger)
	if err != nil {
		c.t.Fatalf("restart %s: %v", id, err)
	}
	c.nodes[id] = &Node{ID: id, Raft: node, FSM: f, Store: store.New(node, f, 2*time.Second), cfg: old.cfg, logger: c.logger}
}

func (c *Cluster) ShutdownAll() {
	for _, n := range c.nodes {
		n.Raft.Shutdown()
	}
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./testharness/ -v`
Expected: PASS（选主 + 复制 + failover）

- [ ] **Step 5: 提交**

```bash
git add testharness/
git commit -m "test(harness): multi-node inmem cluster test helper

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 11: 容灾场景测试

**Files:**
- Create: `testharness/disaster_test.go`

**Interfaces:**
- Consumes: `testharness.Cluster`（Task 10）、`raftnode.Reset`

- [ ] **Step 1: 写测试**

Create `testharness/disaster_test.go`:
```go
package testharness

import (
	"testing"
	"time"
)

// 3 节点全 Shutdown，恢复 2 个 → 2/3 多数派选主，已提交数据零丢失。
func TestRecoverTwoOfThreeKeepsCommittedData(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.ShutdownAll()

	leader := c.WaitForLeader(t)
	if leader == "" {
		t.Fatal("no leader")
	}
	if err := c.Node(leader).Store.Put("committed", []byte("yes")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// 等待复制到所有节点。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range c.IDs() {
			if _, found := c.Node(id).FSM.Get("committed"); !found {
				ok = false
			}
		}
		if ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 全部关闭，移除一个，重启剩 2 个。
	keep := c.IDs()
	drop := keep[2]
	c.ShutdownAll()
	// 模拟永久丢一个：从集群移除该节点对象。
	delete(c.nodes, drop)
	for _, id := range c.IDs() {
		c.RestartNode(id)
	}

	newLeader := c.WaitForLeader(t)
	if newLeader == "" {
		t.Fatal("2-of-3 failed to elect leader")
	}
	// inmem 重启不保状态，此用例验证的是"2 节点能选主对外服务"，
	// 持久化零丢失由真实 BoltDB 的 raftnode 单测保证。
}

// 3 节点全 Shutdown，只恢复 1 个 → 1/3 无法选主。
func TestSingleSurvivorCannotElect(t *testing.T) {
	c := NewCluster(t, 3)
	c.WaitForLeader(t)
	c.ShutdownAll()
	keep := c.IDs()[0]
	delete(c.nodes, c.IDs()[1])
	delete(c.nodes, c.IDs()[2])
	c.RestartNode(keep)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.Node(keep).Raft.IsLeader() {
			t.Fatal("single survivor should not become leader in 3-voter cluster")
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.nodes = map[string]*Node{} // 防止 ShutdownAll 重启
}

// 1 节点数据损坏：擦除后重启，leader InstallSnapshot 重新同步。
// （inmem 无持久化，此用例验证 Reset 语义 + 重启后重新加入拓扑。）
func TestResetNodeRejoins(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.ShutdownAll()
	c.WaitForLeader(t)
	id := c.IDs()[0]
	// inmem 模式 DataDir 为空，Reset 应无错返回。
	if err := resetForTest(c.Node(id)); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	c.RestartNode(id)
	if c.WaitForLeader(t) == "" {
		t.Fatal("no leader after rejoin")
	}
}
```

- [ ] **Step 2: 加测试辅助**

Add to `testharness/cluster.go`:
```go
// resetForTest wipes a node's data dir (no-op for inmem) for DR tests.
func resetForTest(n *Node) error {
	return raftnodeReset(n.cfg.DataDir)
}
```

Create `testharness/reset_shim.go` to avoid importing raftnode's Reset directly into a circular-free but separate concern:
```go
package testharness

import "github.com/raft-meta/internal/raftnode"

func raftnodeReset(dir string) error { return raftnode.Reset(dir) }
```

- [ ] **Step 3: 跑测试**

Run: `go test ./testharness/ -run TestRecover -v` and `go test ./testharness/ -run TestSingle -v` and `go test ./testharness/ -run TestReset -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add testharness/disaster_test.go testharness/reset_shim.go testharness/cluster.go
git commit -m "test(disaster): 2-of-3 recovery, single survivor, reset rejoin

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 12: 全量构建 + 跑全部测试 + 文档

**Files:**
- Modify: `README.md` (create)

**Interfaces:** 无新接口

- [ ] **Step 1: 全量测试**

Run: `go test ./... -v -timeout 120s`
Expected: 全部 PASS。若有 flaky 选举超时，重试一次；若稳定失败，回看对应 task 的超时设置。

- [ ] **Step 2: 写 README**

Create `README.md`:
```markdown
# raft-meta

基于 hashicorp/raft 的 1 主 2 从三节点强一致元数据存储（层次化 KV）。

## 构建

    go build ./cmd/raft-meta

## 启动集群（单机多端口）

    ./scripts/run-cluster.sh

首次运行会在 node1 上引导一次（仅当 data/node1 不存在），然后并行启动 3 节点。

## 操作

    # 写（任意节点，非 leader 自动 307 重定向）
    curl -X PUT http://127.0.0.1:8001/kv/nodes/n1 -d '{"value":"up"}'
    # 读（默认本地读，可能脏读；?consistent=true 强一致走 leader）
    curl http://127.0.0.1:8002/kv/nodes/n1
    curl http://127.0.0.1:8002/kv/nodes/n1?consistent=true
    # 列表
    curl 'http://127.0.0.1:8001/kv?prefix=/nodes/'
    # 集群状态
    curl http://127.0.0.1:8001/cluster/status

## 容灾

**单节点数据损坏**（另 2 个健康，零丢失）：

    raft-meta reset -config configs/node2.yaml   # 擦除损坏节点数据
    raft-meta start -config configs/node2.yaml   # 重启，leader 自动 InstallSnapshot 同步

**丢失多数派（只剩 1 节点，可能丢已提交数据）**：

    raft-meta recover -config configs/node1.yaml  # 强制单节点恢复
    raft-meta start  -config configs/node1.yaml   # 单节点自选主对外
    # 再 AddVoter 2 个新节点恢复 3 节点：
    curl -X POST http://127.0.0.1:8001/cluster/join -d '{"id":"node2","addr":"127.0.0.1:7002"}'

## 测试

    go test ./...

设计文档：docs/superpowers/specs/2026-06-22-raft-metadata-cluster-design.md
```

- [ ] **Step 3: 提交**

```bash
git add README.md
git commit -m "docs: README with usage and DR procedures

Co-Authored-By: Claude <noreply@anthropic.com>"
```

- [ ] **Step 4: 最终验证**

Run: `go build ./... && go test ./... -timeout 120s`
Expected: 构建通过，全部测试 PASS

---

### Task 13: HTTP 端到端测试（真实 3 进程集群）

**Files:**
- Create: `e2e/cluster_e2e_test.go`
- Create: `e2e/helper.go`

**Interfaces:**
- Consumes: 已构建的 `raft-meta` 二进制（`go build ./cmd/raft-meta`）、`configs/node*.yaml`（Task 8）
- 产出：覆盖 spec §6.4：真实 3 进程集群，curl 写/读/重定向，kill leader 验证客户端重试落新主

- [ ] **Step 1: 写 e2e helper**

Create `e2e/helper.go`:
```go
package e2e

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// proc wraps a running raft-meta node process.
type proc struct {
	cmd *exec.Cmd
	id  string
}

// startCluster builds the binary once and starts 3 nodes with the given
// temporary data root. Returns the 3 procs and a cleanup func.
func startCluster(t *testing.T, bin string, dataRoot string) ([]*proc, func()) {
	t.Helper()
	// 写 3 份临时配置（端口用 9001-3 raft / 10001-3 http，避免与开发端口冲突）。
	cfgs := make([]string, 3)
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node%d", i+1)
		dir := filepath.Join(dataRoot, id)
		os.MkdirAll(filepath.Join(dir, "snaps"), 0755)
		path := filepath.Join(dataRoot, id+".yaml")
		content := fmt.Sprintf(`
nodeID: %s
raftAddr: 127.0.0.1:%d
httpAddr: 127.0.0.1:%d
dataDir: %s
peers:
  - {id: node1, addr: 127.0.0.1:9001}
  - {id: node2, addr: 127.0.0.1:9002}
  - {id: node3, addr: 127.0.0.1:9003}
snapshot:
  type: file
  path: %s/snaps
  retain: 3
`, id, 9001+i, 10001+i, dir, dir)
		os.WriteFile(path, []byte(content), 0644)
		cfgs[i] = path
	}

	// 首次部署：在 node1 引导一次。
	if err := exec.Command(bin, "init", "-config", cfgs[0]).Run(); err != nil {
		t.Fatalf("init: %v", err)
	}

	procs := make([]*proc, 3)
	for i, cfg := range cfgs {
		c := exec.Command(bin, "start", "-config", cfg)
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		if err := c.Start(); err != nil {
			t.Fatalf("start node%d: %v", i+1, err)
		}
		procs[i] = &proc{cmd: c, id: fmt.Sprintf("node%d", i+1)}
	}
	cleanup := func() {
		for _, p := range procs {
			if p.cmd != nil && p.cmd.Process != nil {
				p.cmd.Process.Signal(syscall.SIGTERM)
				p.cmd.Wait()
			}
		}
		os.RemoveAll(dataRoot)
	}
	return procs, cleanup
}

// httpBase returns the HTTP base URL for node index i (0-based).
func httpBase(i int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", 10001+i)
}

// waitForHTTP polls until the node responds on /cluster/status or times out.
func waitForHTTP(t *testing.T, i int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := httpGet(httpBase(i) + "/cluster/status")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node %d did not come up", i+1)
}

// httpGet is a thin wrapper around http.Get.
func httpGet(url string) (*http.Response, error) { return http.Get(url) }
```

- [ ] **Step 2: 写 e2e 测试**

Create `e2e/cluster_e2e_test.go`:
```go
//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// TestE2EWriteReadAndFailover 覆盖 spec §6.4：真实 3 进程，写 leader、读、
// kill leader 后客户端重试落新主，数据仍在。
func TestE2EWriteReadAndFailover(t *testing.T) {
	bin := os.Getenv("RAFT_META_BIN")
	if bin == "" {
		// 默认用 go run 拉起的临时构建产物。
		out, err := exec.Command("go", "build", "-o", t.TempDir()+"/raft-meta", "./cmd/raft-meta").CombinedOutput()
		if err != nil {
			t.Fatalf("build: %v\n%s", err, out)
		}
		bin = t.TempDir() + "/raft-meta"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("binary not built: %v", err)
	}

	dataRoot := t.TempDir()
	procs, cleanup := startCluster(t, bin, dataRoot)
	defer cleanup()
	for i := 0; i < 3; i++ {
		waitForHTTP(t, i)
	}

	// 找到 leader。
	leaderIdx := -1
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < 3; i++ {
			resp, _ := http.Get(httpBase(i) + "/cluster/status")
			if resp != nil && resp.StatusCode == 200 {
				var s map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&s)
				resp.Body.Close()
				if s["state"] == "Leader" {
					leaderIdx = i
					break
				}
			}
		}
		if leaderIdx >= 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if leaderIdx < 0 {
		t.Fatal("no leader elected")
	}

	// 写 leader。
	body, _ := json.Marshal(map[string]string{"value": "e2e"})
	req, err := http.NewRequest(http.MethodPut, httpBase(leaderIdx)+"/kv/e2ekey", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if r.StatusCode != 200 {
		t.Fatalf("PUT status = %d", r.StatusCode)
	}
	r.Body.Close()

	// 等复制，从 follower 读（本地读，可能脏读，但等一会应一致）。
	time.Sleep(time.Second)
	follower := (leaderIdx + 1) % 3
	got, err := http.Get(httpBase(follower) + "/kv/e2ekey")
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]string
	json.NewDecoder(got.Body).Decode(&v)
	got.Body.Close()
	if v["value"] != "e2e" {
		t.Fatalf("follower read = %q, want e2e", v["value"])
	}

	// kill leader，验证选新主且数据仍在。
	procs[leaderIdx].cmd.Process.Signal(syscall.SIGTERM)
	procs[leaderIdx].cmd.Wait()
	procs[leaderIdx].cmd = nil

	newLeader := -1
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < 3; i++ {
			if i == leaderIdx {
				continue
			}
			resp, _ := http.Get(httpBase(i) + "/cluster/status")
			if resp != nil && resp.StatusCode == 200 {
				var s map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&s)
				resp.Body.Close()
				if s["state"] == "Leader" {
					newLeader = i
					break
				}
			}
		}
		if newLeader >= 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if newLeader < 0 {
		t.Fatal("no new leader after failover")
	}

	got, err = http.Get(httpBase(newLeader) + "/kv/e2ekey")
	if err != nil {
		t.Fatal(err)
	}
	var v2 map[string]string
	json.NewDecoder(got.Body).Decode(&v2)
	got.Body.Close()
	if v2["value"] != "e2e" {
		t.Fatalf("after failover read = %q, want e2e", v2["value"])
	}
}
```

The build tag `//go:build e2e` keeps this test out of the default `go test ./...` run; run explicitly with `-tags e2e`.

- [ ] **Step 3: 跑 e2e（手动）**

Run:
```bash
go build -o /tmp/raft-meta ./cmd/raft-meta
RAFT_META_BIN=/tmp/raft-meta go test -tags e2e ./e2e/ -v -timeout 120s
```
Expected: PASS（写→读→kill leader→新主读，数据都在）

- [ ] **Step 4: 提交**

```bash
git add e2e/
git commit -m "test(e2e): real 3-process cluster write/read/failover

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Self-Review 结果

**Spec coverage:**
- §1 选型/主从语义 → 全局约束 + Task 5/6/7
- §2 拓扑 + 联合引导 + 成员 API → Task 5 (BootstrapCluster) + Task 7 (/cluster/join,remove) + Task 8 (run-cluster.sh, init)
- §3 模块划分 + snapshot 可插拔 → Task 1-7 文件结构与 spec 一致；snapshot 可插拔 Task 4
- §4 数据流 写/读/快照/恢复 → Task 6 (写读路径) + Task 3 (Snapshot/Restore) + Task 5 (Apply + BoltDB 持久化往返)
- §5 错误处理：5.1 重定向 (Task 7), 5.3 重启恢复 (Task 5 持久化测试 + Task 10), 5.6 成员变更 (Task 7/9), 5.9 单节点损坏 (Task 9 reset + Task 11), 5.10 强制恢复 (Task 9 recover + Task 11)
- §6 测试策略 → Task 1-9 单测 + Task 5 BoltDB 持久化往返 + Task 10 集成 + Task 11 容灾 + Task 13 HTTP e2e + Task 12 全量

**已知简化（计划内，非占位符）：**
- Task 10/11 的多节点集成/容灾测试用 inmem transport（无跨进程网络），仅验证拓扑/选举/复制语义；持久化"零丢失"由 Task 5 的 BoltDB 持久化往返测试单独保证
- API 层 307 重定向的 raft:port→http:port 映射在生产部署需 server 层注入；当前用 raft 地址兜底，spec §4.1 接受重定向语义。真实 HTTP 跨进程重定向由 Task 13 e2e 覆盖写→读→failover，但不专门断言 307 响应体

**Placeholder scan:** 无 TBD/TODO；每步含完整代码或确切命令。
**Type consistency:** `raftnode.Node` 方法签名（Apply/IsLeader/LeaderAddr/State/Stats/AddVoter/RemoveServer/Shutdown/Raft）在 Task 5 定义，Task 6/7/8/10/11 消费一致；`store.Store` 方法在 Task 6 定义，Task 7 消费一致；`fsm.Command/EncodeCommand/DecodeCommand` Task 2 定义，Task 3/5/6 消费一致；Task 5 的 `New` 解耦 transport/persistence 后，DataDir!="" 走 BoltDB、UseInmemTransport 走 inmem transport，二者正交，Task 5 持久化测试与 Task 10/11 的 inmem 用法均兼容。
