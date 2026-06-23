package raftstore

import (
	"fmt"
	"io"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"raft-meta/internal/config"
)

// NewStores returns the raft LogStore and StableStore for the configured
// backend, plus an io.Closer to release resources on Shutdown (nil when the
// backend holds no resources, e.g. inmem).
//
// log store 与 stable store 通常共用同一后端（BoltDB 的一个 BoltStore 同时
// 实现两个接口），故工厂一次返回两者。后端与 FSM 序列化格式正交，与 snapshot
// 后端也正交——各自独立演进。
//
// 空 type 按 dataDir 自动选择：dataDir != "" → boltdb（生产），否则 inmem
// （测试）。这让不带 logStore 字段的旧配置行为不变。
func NewStores(cfg config.LogStoreConfig, dataDir string, logger hclog.Logger) (raft.LogStore, raft.StableStore, io.Closer, error) {
	t := cfg.Type
	if t == "" {
		if dataDir != "" {
			t = "boltdb"
		} else {
			t = "inmem"
		}
	}
	switch t {
	case "boltdb":
		return newBoltStores(dataDir, logger)
	case "inmem":
		s := raft.NewInmemStore()
		return s, s, nil, nil
	case "rocksdb":
		return nil, nil, nil, fmt.Errorf("logStore type %q not yet implemented (extension point, see internal/raftstore/rocksdb.go)", t)
	default:
		return nil, nil, nil, fmt.Errorf("unsupported logStore type %q (supported: inmem, boltdb; rocksdb 待实现)", t)
	}
}
