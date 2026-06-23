package raftstore

// rocksdb.go — extension point for a RocksDB-backed log/stable store.
//
// Not yet implemented. The factory (store.go) returns a "not yet implemented"
// error for type "rocksdb". To add it:
//
//  1. Add a CGO dependency (github.com/linxGnu/grocksdb or
//     github.com/tecbot/gorocksdb) and ensure a RocksDB C++ library is
//     installed on the build/host system.
//  2. Implement a rocksdbStore type satisfying BOTH raft.LogStore and
//     raft.StableStore:
//       LogStore:    FirstIndex / LastIndex / GetLog / StoreLog / StoreLogs / DeleteRange
//       StableStore: Set / Get / SetUint64 / GetUint64
//     (github.com/hashicorp/raft-boltdb/v2 BoltStore is a complete reference
//     implementation — mirror its bucket design using two RocksDB column
//     families, one for logs keyed by index, one for stable kv.)
//  3. Add newRocksdbStores(dataDir, logger) (raft.LogStore, raft.StableStore,
//     io.Closer, error) returning the store as both interfaces plus a Closer
//     that closes the RocksDB handle, and wire case "rocksdb" in NewStores.
//
// Why a stub, not an impl now: RocksDB needs a native C++ library + CGO,
// which is a heavy dependency for this project. The pluggable factory means
// it can be added later without touching raftnode or config.
