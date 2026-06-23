package fsm

import (
	"bytes"
	"io"
	"testing"

	"github.com/hashicorp/raft"
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
