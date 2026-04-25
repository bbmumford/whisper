/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"bytes"
	"testing"
)

// TestReedSolomonErasureRoundTrip validates the default helper's
// EncodeErasure / DecodeErasure round-trip for k=2 data + 1 parity.
// Drops one data shard before decode to verify parity reconstruction.
func TestReedSolomonErasureRoundTrip(t *testing.T) {
	rs := ReedSolomonErasure{}
	dataShards := [][][]byte{
		{[]byte("alpha"), []byte("beta"), []byte("gamma")},
		{[]byte("delta"), []byte("epsilon"), []byte("zeta")},
	}

	encoded, err := rs.EncodeErasure(dataShards, 1)
	if err != nil {
		t.Fatalf("EncodeErasure failed: %v", err)
	}
	if len(encoded) != 3 {
		t.Fatalf("expected 3 shards (2 data + 1 parity), got %d", len(encoded))
	}

	// Drop the first data shard — the parity should reconstruct it.
	corrupted := make([][][]byte, 3)
	corrupted[0] = nil
	corrupted[1] = encoded[1]
	corrupted[2] = encoded[2]

	decoded, err := rs.DecodeErasure(corrupted, 2)
	if err != nil {
		t.Fatalf("DecodeErasure failed: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("expected 2 data shards out, got %d", len(decoded))
	}

	// First reconstructed shard should match original.
	if len(decoded[0]) != 3 {
		t.Fatalf("expected 3 records in shard 0, got %d", len(decoded[0]))
	}
	if !bytes.Equal(decoded[0][0], []byte("alpha")) ||
		!bytes.Equal(decoded[0][1], []byte("beta")) ||
		!bytes.Equal(decoded[0][2], []byte("gamma")) {
		t.Errorf("shard 0 reconstruction mismatch: got %v", decoded[0])
	}
	// Second should be the same as input.
	if !bytes.Equal(decoded[1][0], []byte("delta")) ||
		!bytes.Equal(decoded[1][1], []byte("epsilon")) ||
		!bytes.Equal(decoded[1][2], []byte("zeta")) {
		t.Errorf("shard 1 mismatch: got %v", decoded[1])
	}
}

// TestReedSolomonNoParity validates the n=k path (no parity shards).
// Used on cellular paths where parallel transfer is too expensive.
func TestReedSolomonNoParity(t *testing.T) {
	rs := ReedSolomonErasure{}
	dataShards := [][][]byte{
		{[]byte("only-shard-record-1"), []byte("record-2")},
	}
	encoded, err := rs.EncodeErasure(dataShards, 0)
	if err != nil {
		t.Fatalf("EncodeErasure failed: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("expected 1 shard out, got %d", len(encoded))
	}
	decoded, err := rs.DecodeErasure(encoded, 1)
	if err != nil {
		t.Fatalf("DecodeErasure failed: %v", err)
	}
	if len(decoded) != 1 || len(decoded[0]) != 2 {
		t.Fatalf("expected 1 shard with 2 records, got %d shards", len(decoded))
	}
}
