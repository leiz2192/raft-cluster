package raftstore

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// newBoltStores returns a single BoltStore used as BOTH log and stable store
// (BoltStore implements both interfaces, using separate buckets), persisted at
// dataDir/raft.db. The returned io.Closer is the BoltStore itself — close it
// on Shutdown to release the file lock so a restart can reopen the same dir.
func newBoltStores(dataDir string, logger hclog.Logger) (raft.LogStore, raft.StableStore, io.Closer, error) {
	if dataDir == "" {
		return nil, nil, nil, fmt.Errorf("boltdb logStore requires dataDir")
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, nil, nil, fmt.Errorf("mkdir dataDir: %w", err)
	}
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("bolt store: %w", err)
	}
	return boltStore, boltStore, boltStore, nil
}
