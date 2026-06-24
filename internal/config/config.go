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
	MaxSize    int    `yaml:"maxSize"`    // MB，轮转阈值；空=100
	MaxBackups int    `yaml:"maxBackups"` // 保留份数；空=7
	MaxAge     int    `yaml:"maxAge"`     // 保留天数；空=30
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
	Debug             DebugConfig     `yaml:"debug"`
	UseInmemTransport bool            `yaml:"-"`
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
