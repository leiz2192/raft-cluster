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
	NodeID            string         `yaml:"nodeID"`
	RaftAddr          string         `yaml:"raftAddr"`
	HTTPAddr          string         `yaml:"httpAddr"`
	DataDir           string         `yaml:"dataDir"`
	Peers             []Peer         `yaml:"peers"`
	Snapshot          SnapshotConfig `yaml:"snapshot"`
	UseInmemTransport bool           `yaml:"-"`
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
