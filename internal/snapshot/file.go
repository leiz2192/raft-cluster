package snapshot

import (
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"raft-meta/internal/config"
)

func newFileStore(cfg config.SnapshotConfig, logger hclog.Logger) (raft.SnapshotStore, error) {
	retain := cfg.Retain
	if retain <= 0 {
		retain = 3
	}
	// raft v1.7.1's NewFileSnapshotStore takes an io.Writer logOutput.
	// StandardWriter returns an io.Writer bridging hclog; StandardLogger
	// returns *log.Logger which does not satisfy io.Writer.
	return raft.NewFileSnapshotStore(cfg.Path, retain, logger.StandardWriter(&hclog.StandardLoggerOptions{
		InferLevels: false,
	}))
}
