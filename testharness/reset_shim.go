package testharness

import "raft-meta/internal/raftnode"

// raftnodeReset wraps raftnode.Reset to keep the disaster tests' import
// surface narrow (the Reset symbol is a DR-specific concern).
func raftnodeReset(dir string) error { return raftnode.Reset(dir) }
