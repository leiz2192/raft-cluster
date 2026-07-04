package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Peer struct {
	ID       string `yaml:"id"`
	Addr     string `yaml:"addr"`     // raft 地址
	HTTPAddr string `yaml:"httpAddr"` // HTTP 业务地址（集群状态扇出/重定向用）
}

type SnapshotConfig struct {
	Type   string `yaml:"type"`   // file | inmem
	Path   string `yaml:"path"`   // file 用
	Retain int    `yaml:"retain"` // 保留份数
}

// LogStoreConfig selects the raft log/stable store backend.
type LogStoreConfig struct {
	Type string `yaml:"type"` // inmem | boltdb | rocksdb (future); 空 → 按 dataDir 自动选
}

// LogConfig configures business logging (hclog). File empty → stderr (旧行为).
type LogConfig struct {
	File       string `yaml:"file"`       // 日志文件路径；空则写 stderr
	Level      string `yaml:"level"`      // trace/debug/info/warn/error；空=info
	JSON       bool   `yaml:"json"`       // JSON 格式
	MaxSize    Size   `yaml:"maxSize"`    // 轮转阈值，可写 "100MB"；空=100MB
	MaxBackups int    `yaml:"maxBackups"` // 保留份数；空=7
	MaxAge     int    `yaml:"maxAge"`     // 保留天数；空=30
}

// RaftConfig configures raft timing/limits (otherwise hardcoded).
type RaftConfig struct {
	ApplyTimeout      Duration `yaml:"applyTimeout"`      // 写 raft.Apply 超时；空=5s
	SnapshotInterval  Duration `yaml:"snapshotInterval"`  // 快照检测间隔；空=10m
	SnapshotThreshold uint64   `yaml:"snapshotThreshold"` // 快照日志条数阈值；空=1024
}

// DebugConfig configures the isolated pprof debug server. Addr empty → pprof off.
type DebugConfig struct {
	Addr string `yaml:"addr"` // 调试端口监听地址，如 127.0.0.1:6061；空=不开
}

type Config struct {
	NodeID            string          `yaml:"nodeID"`
	RaftAddr          string          `yaml:"raftAddr"`
	HTTPAddr          string          `yaml:"httpAddr"`
	DataDir           string          `yaml:"dataDir"`
	Peers             []Peer          `yaml:"peers"`
	Snapshot          SnapshotConfig  `yaml:"snapshot"`
	LogStore          LogStoreConfig  `yaml:"logStore"`
	Log               LogConfig       `yaml:"log"`
	Raft              RaftConfig      `yaml:"raft"`
	Debug             DebugConfig     `yaml:"debug"`
	UseInmemTransport bool            `yaml:"-"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return LoadFromBytes(data)
}

// LoadFromBytes parses config YAML (shared by Load and tests).
func LoadFromBytes(data []byte) (*Config, error) {
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
	if cfg.Log.MaxSize <= 0 {
		cfg.Log.MaxSize = Size(100 * (1 << 20)) // 100MB
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
	// peers 数量不硬编码到 3：允许 1（单节点 bootstrap/recover）/3/5… 任意非空
	// 拓扑，联合引导本身面向 3 节点但不在此强制。
	//
	// 注意：不在此要求 NodeID ∈ Peers。`start` 路径（含 /cluster/join 动态加入的
	// 新节点）的 peers 只是 HTTP 重定向/状态扇出的对端地图，可不包含自身；强制
	// 要求会让动态加节点无法启动。bootstrap 路径的「自身必须是 voter」不变量由
	// raft.BootstrapCluster 自身校验（返回 "node is not a voter"），无需重复。
	if len(c.Peers) < 1 {
		return fmt.Errorf("peers must have at least 1 entry, got %d", len(c.Peers))
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
