package logging

import (
	"os"
	"path/filepath"

	"github.com/hashicorp/go-hclog"
	"gopkg.in/natefinch/lumberjack.v2"
	"raft-meta/internal/config"
)

// NewLogger builds an hclog.Logger per cfg:
//   - cfg.File set  → file output via lumberjack rotation (maxSize MB / 份数 / 天)
//   - cfg.File empty → hclog default (stderr), 保持旧行为
//   - cfg.JSON       → JSON 格式（便于 ELK/Loki 解析）
//   - cfg.Level      → trace/debug/info/warn/error；空或非法则 info
//
// raft.Config.Logger 的类型是 hclog.Logger，所以这个 logger 同时承载 raft
// 自身的日志（选举/复制/快照/成员变更）和应用日志——一条管道、同格式、同输出。
func NewLogger(cfg config.LogConfig, name string) hclog.Logger {
	level := hclog.Info
	if l := hclog.LevelFromString(cfg.Level); l != hclog.NoLevel {
		level = l
	}
	opts := &hclog.LoggerOptions{
		Name:       name,
		Level:      level,
		JSONFormat: cfg.JSON,
	}
	if cfg.File != "" {
		// lumberjack 不创建父目录，先建。
		if dir := filepath.Dir(cfg.File); dir != "" {
			_ = os.MkdirAll(dir, 0755)
		}
		// lumberjack MaxSize 单位是 MB（二进制 1024²）。cfg.MaxSize 是 Size
		//（可写 "100MB"）；空/<=0 默认 100MB。
		maxSize := cfg.MaxSize
		if maxSize <= 0 {
			maxSize = config.Size(100 * (1 << 20))
		}
		opts.Output = &lumberjack.Logger{
			Filename:   cfg.File,
			MaxSize:    maxSize.Megabytes(),
			MaxBackups: orDefault(cfg.MaxBackups, 7),
			MaxAge:     orDefault(cfg.MaxAge, 30), // 天
			LocalTime:  true,
			Compress:   true,
		}
	}
	return hclog.New(opts)
}

func orDefault(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}
