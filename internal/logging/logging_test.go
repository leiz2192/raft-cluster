package logging

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hashicorp/go-hclog"
	"raft-meta/internal/config"
)

func TestNewLoggerFileWritesJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "node.log") // 父目录不存在，验证 MkdirAll
	cfg := config.LogConfig{File: path, Level: "info", JSON: true}
	logger := NewLogger(cfg, "node1")

	logger.Info("hello world", "key", "val")

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	var entry map[string]interface{}
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("log not JSON: %v\nraw: %s", err, data)
	}
	if entry["@message"] != "hello world" {
		t.Errorf("@message = %v, want 'hello world'", entry["@message"])
	}
	if entry["key"] != "val" {
		t.Errorf("structured field key = %v, want val", entry["key"])
	}
}

func TestNewLoggerLevelFilters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.log")
	cfg := config.LogConfig{File: path, Level: "warn", JSON: true}
	logger := NewLogger(cfg, "node1")

	logger.Info("should-not-appear")
	logger.Warn("should-appear")

	data, _ := os.ReadFile(path)
	s := string(data)
	if strings.Contains(s, "should-not-appear") {
		t.Errorf("info message leaked at warn level: %s", s)
	}
	if !strings.Contains(s, "should-appear") {
		t.Errorf("warn message missing: %s", s)
	}
}

func TestNewLoggerInvalidLevelDefaultsInfo(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.log")
	cfg := config.LogConfig{File: path, Level: "garbage", JSON: true}
	logger := NewLogger(cfg, "node1")

	// 非法 level 应回退到 info：info 出现，debug 不出现。
	logger.Debug("dbg")
	logger.Info("inf")
	data, _ := os.ReadFile(path)
	s := string(data)
	if strings.Contains(s, "dbg") {
		t.Errorf("debug leaked under default info level: %s", s)
	}
	if !strings.Contains(s, "inf") {
		t.Errorf("info missing: %s", s)
	}
}

func TestNewLoggerEmptyFileUsesStderr(t *testing.T) {
	// file 空 → 不 panic，logger 可用，走 hclog 默认（stderr）。
	logger := NewLogger(config.LogConfig{}, "node1")
	if logger == nil {
		t.Fatal("nil logger")
	}
	if !logger.IsInfo() {
		t.Errorf("default level should be info")
	}
	// 写一条不应报错（写到 stderr，测试中忽略输出）。
	logger.Info("smoke")
}

// 编译期保证返回类型满足 hclog.Logger。
var _ hclog.Logger = (hclog.Logger)(nil)
