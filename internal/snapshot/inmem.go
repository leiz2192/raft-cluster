package snapshot

import (
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// NewInmem returns an in-memory raft.SnapshotStore.
// Note: raft v1.7.1's NewInmemSnapshotStore() takes no logger argument; the
// logger parameter is retained on this factory's signature for API stability
// and future backends.
func NewInmem(logger hclog.Logger) raft.SnapshotStore {
	_ = logger
	return raft.NewInmemSnapshotStore()
}
