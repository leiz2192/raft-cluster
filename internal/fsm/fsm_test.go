package fsm

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"raft-meta/internal/config"
)

// memSink adapts a *bytes.Buffer to raft.SnapshotSink for testing Persist.
type memSink struct {
	buf *bytes.Buffer
}

func (m *memSink) Write(p []byte) (int, error) { return m.buf.Write(p) }
func (m *memSink) Close() error                 { return nil }
func (m *memSink) Cancel() error                { return nil }
func (m *memSink) ID() string                   { return "test" }

func newLog(t *testing.T, c *Command) *raft.Log {
	t.Helper()
	data, err := EncodeCommand(c)
	if err != nil {
		t.Fatal(err)
	}
	return &raft.Log{Data: data}
}

func TestApplyPutAndDelete(t *testing.T) {
	f := New()
	f.Apply(newLog(t, &Command{Op: OpPut, Key: "k1", Value: []byte("v1")}))
	got, ok := f.Get("k1")
	if !ok || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get(k1) = %q,%v, want v1,true", got, ok)
	}
	f.Apply(newLog(t, &Command{Op: OpDelete, Key: "k1"}))
	if _, ok := f.Get("k1"); ok {
		t.Fatal("k1 should be deleted")
	}
}

func TestListPrefix(t *testing.T) {
	f := New()
	f.Apply(newLog(t, &Command{Op: OpPut, Key: "/nodes/n1", Value: []byte("a")}))
	f.Apply(newLog(t, &Command{Op: OpPut, Key: "/nodes/n2", Value: []byte("b")}))
	f.Apply(newLog(t, &Command{Op: OpPut, Key: "/services/s1", Value: []byte("c")}))
	got := f.List("/nodes/")
	if len(got) != 2 {
		t.Fatalf("List /nodes/ len = %d, want 2", len(got))
	}
}

func TestSnapshotRestoreRoundtrip(t *testing.T) {
	src := New()
	src.Apply(newLog(t, &Command{Op: OpPut, Key: "k1", Value: []byte("v1")}))
	src.Apply(newLog(t, &Command{Op: OpPut, Key: "k2", Value: []byte("v2")}))

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&memSink{buf: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	dst := New()
	if err := dst.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got, ok := dst.Get("k1")
	if !ok || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("after restore Get(k1) = %q,%v", got, ok)
	}
}

func TestApplyIgnoresUnknownOp(t *testing.T) {
	f := New()
	f.Apply(newLog(t, &Command{Op: "bogus", Key: "k1", Value: []byte("v")}))
	if _, ok := f.Get("k1"); ok {
		t.Fatal("unknown op should not write")
	}
}

// findPeer returns the peer with the given ID from ps, or false.
func findPeer(ps []config.Peer, id string) (config.Peer, bool) {
	for _, p := range ps {
		if p.ID == id {
			return p, true
		}
	}
	return config.Peer{}, false
}

// TestApplyAddRemovePeer verifies AddPeer upserts a peer into the FSM's
// dynamic-peer map and RemovePeer drops it. These ops are replicated via raft,
// so the map is cluster-wide consistent.
func TestApplyAddRemovePeer(t *testing.T) {
	f := New()
	f.Apply(newLog(t, &Command{Op: OpAddPeer, PeerID: "n4", PeerAddr: "127.0.0.1:7004", PeerHTTP: "127.0.0.1:8004"}))
	p, ok := findPeer(f.Peers(), "n4")
	if !ok || p.Addr != "127.0.0.1:7004" || p.HTTPAddr != "127.0.0.1:8004" {
		t.Fatalf("Peers = %v, want n4{addr=127.0.0.1:7004 http=127.0.0.1:8004}", f.Peers())
	}
	// Upsert: second Add with same ID replaces, not appends.
	f.Apply(newLog(t, &Command{Op: OpAddPeer, PeerID: "n4", PeerAddr: "127.0.0.1:7004", PeerHTTP: "127.0.0.1:9004"}))
	if len(f.Peers()) != 1 {
		t.Fatalf("after upsert Peers len = %d, want 1", len(f.Peers()))
	}
	f.Apply(newLog(t, &Command{Op: OpRemovePeer, PeerID: "n4"}))
	if len(f.Peers()) != 0 {
		t.Fatalf("after remove Peers = %v, want empty", f.Peers())
	}
}

// TestSnapshotRestoreIncludesPeers verifies the dynamic-peer map is captured in
// snapshots and restored — so peers survive snapshot+restart on every node.
func TestSnapshotRestoreIncludesPeers(t *testing.T) {
	src := New()
	src.Apply(newLog(t, &Command{Op: OpAddPeer, PeerID: "n4", PeerAddr: "127.0.0.1:7004", PeerHTTP: "127.0.0.1:8004"}))
	src.Apply(newLog(t, &Command{Op: OpPut, Key: "k1", Value: []byte("v1")}))

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&memSink{buf: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	dst := New()
	if err := dst.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, ok := findPeer(dst.Peers(), "n4"); !ok {
		t.Errorf("after restore Peers = %v, want n4 present", dst.Peers())
	}
	if got, ok := dst.Get("k1"); !ok || string(got) != "v1" {
		t.Errorf("after restore Get(k1) = %q,%v, want v1,true", got, ok)
	}
}

// TestApplyDecodeFailureLogsAndSkips verifies that an undecodable log entry
// does not stall the cluster: the FSM logs the corruption (so operators can
// see it) and skips the entry (no FSM mutation). Previously the error was
// returned as ApplyFuture.Response() but never read by callers that only
// inspect .Error(), so it was silently swallowed — the log is now the
// authoritative operator-visible signal.
func TestApplyDecodeFailureLogsAndSkips(t *testing.T) {
	var buf bytes.Buffer
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "fsm-test",
		Level:  hclog.Error,
		Output: &buf,
	})
	f := NewWithLogger(logger)

	resp := f.Apply(&raft.Log{Index: 42, Data: []byte("!!!not-json!!!")})
	// The error is still returned as ApplyFuture.Response() (preserved
	// behavior); the fix is that it is now ALSO logged, so it is not
	// silently swallowed.
	err, ok := resp.(error)
	if !ok || err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("Apply corrupt log response = %v, want an error mentioning decode", resp)
	}
	out := buf.String()
	if !strings.Contains(out, "decode") {
		t.Errorf("log output missing decode mention; got: %q", out)
	}
	if !strings.Contains(out, "42") {
		t.Errorf("log output missing log index 42; got: %q", out)
	}
	if f.Len() != 0 {
		t.Errorf("FSM mutated on corrupt entry; keys=%d", f.Len())
	}
}

// Spec-rule guards (allowed additions within scope).

func TestApplyPutOverwrites(t *testing.T) {
	f := New()
	f.Apply(newLog(t, &Command{Op: OpPut, Key: "k", Value: []byte("v1")}))
	f.Apply(newLog(t, &Command{Op: OpPut, Key: "k", Value: []byte("v2")}))
	got, ok := f.Get("k")
	if !ok || !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("Get(k) = %q,%v, want v2,true", got, ok)
	}
}

func TestApplyDeleteMissingIsNoOp(t *testing.T) {
	f := New()
	f.Apply(newLog(t, &Command{Op: OpPut, Key: "keep", Value: []byte("v")}))
	// Deleting a key that does not exist must not error or disturb others.
	f.Apply(newLog(t, &Command{Op: OpDelete, Key: "absent"}))
	if got, ok := f.Get("keep"); !ok || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get(keep) = %q,%v, want v,true", got, ok)
	}
}

func TestRestoreReplacesNotMerges(t *testing.T) {
	dst := New()
	dst.Apply(newLog(t, &Command{Op: OpPut, Key: "old", Value: []byte("v")}))

	src := New()
	src.Apply(newLog(t, &Command{Op: OpPut, Key: "k1", Value: []byte("v1")}))
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&memSink{buf: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if err := dst.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, ok := dst.Get("old"); ok {
		t.Fatal("Restore should replace existing map, not merge")
	}
	if got, ok := dst.Get("k1"); !ok || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("after restore Get(k1) = %q,%v, want v1,true", got, ok)
	}
}
