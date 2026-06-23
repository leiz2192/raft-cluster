package fsm

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeCommand(t *testing.T) {
	cases := []Command{
		{Op: OpPut, Key: "/nodes/n1", Value: []byte("data")},
		{Op: OpDelete, Key: "/services/svc1"},
		{Op: OpPut, Key: "empty", Value: nil},
	}
	for _, c := range cases {
		data, err := EncodeCommand(&c)
		if err != nil {
			t.Fatalf("EncodeCommand: %v", err)
		}
		got, err := DecodeCommand(data)
		if err != nil {
			t.Fatalf("DecodeCommand: %v", err)
		}
		if got.Op != c.Op || got.Key != c.Key || !bytes.Equal(got.Value, c.Value) {
			t.Errorf("roundtrip mismatch: got %+v, want %+v", got, c)
		}
	}
}

func TestDecodeInvalid(t *testing.T) {
	if _, err := DecodeCommand([]byte("not json")); err == nil {
		t.Fatal("expected error for invalid json")
	}
}
