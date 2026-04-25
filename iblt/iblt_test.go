/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package iblt

import (
	"crypto/rand"
	"testing"
)

func makeKeys(n int) []Key {
	out := make([]Key, n)
	for i := range out {
		_, _ = rand.Read(out[i][:])
	}
	return out
}

// TestIBLT_DecodeFromEncoder verifies that encoding N keys then
// decoding the table directly recovers all N as Positive.
//
// Sizing note: peeling decoder with k=3 has a known failure tail
// at the cell/element ratio sweet spot. For the test we oversize
// (4×d + 16) to drive per-run failure probability below 0.1%; the
// production reconcile driver picks size from d_max estimation
// which has its own headroom layered on the rate-1.5 baseline.
func TestIBLT_DecodeFromEncoder(t *testing.T) {
	keys := makeKeys(50)
	tbl := New(4*len(keys)+16, 3, 42)
	for _, k := range keys {
		tbl.Insert(k)
	}
	res := tbl.Decode()
	if !res.Complete {
		t.Fatalf("decode incomplete: positive=%d negative=%d", len(res.Positive), len(res.Negative))
	}
	if len(res.Positive) != len(keys) {
		t.Errorf("expected %d positives, got %d", len(keys), len(res.Positive))
	}
	if len(res.Negative) != 0 {
		t.Errorf("expected 0 negatives, got %d", len(res.Negative))
	}
}

// TestIBLT_SymmetricDifference verifies the canonical reconciliation
// flow: encoder has {A, shared}; receiver has {B, shared}; receiver
// subtracts its table from the encoder's; decode yields A as
// Positive, B as Negative.
func TestIBLT_SymmetricDifference(t *testing.T) {
	const sharedCount = 100
	const onlyEncoder = 5
	const onlyReceiver = 7

	shared := makeKeys(sharedCount)
	encOnly := makeKeys(onlyEncoder)
	recvOnly := makeKeys(onlyReceiver)

	const seed uint64 = 0xBADBADCAFE
	d := onlyEncoder + onlyReceiver
	// Peeling decoder with k=3 needs ~1.5 × d for high probability;
	// at small d the constant-factor matters AND random seeds
	// occasionally hit the failure tail. Use 4 × d + 16 in tests
	// to drive the per-run failure probability below 0.1% — keeps
	// CI stable on the cryptorand-keyed input. Production reconcile
	// driver uses computeCellCount which sizes more conservatively
	// than d alone via the d_max estimate.
	m := 4*d + 16

	encTable := New(m, 3, seed)
	recvTable := New(m, 3, seed)

	for _, k := range shared {
		encTable.Insert(k)
		recvTable.Insert(k)
	}
	for _, k := range encOnly {
		encTable.Insert(k)
	}
	for _, k := range recvOnly {
		recvTable.Insert(k)
	}

	encTable.Subtract(recvTable)
	res := encTable.Decode()

	if !res.Complete {
		t.Fatalf("decode incomplete (m=%d, d=%d): pos=%d neg=%d", m, d, len(res.Positive), len(res.Negative))
	}
	if len(res.Positive) != onlyEncoder {
		t.Errorf("Positive: got %d, want %d", len(res.Positive), onlyEncoder)
	}
	if len(res.Negative) != onlyReceiver {
		t.Errorf("Negative: got %d, want %d", len(res.Negative), onlyReceiver)
	}
}

// TestIBLT_DecodeFailsWhenOverloaded verifies that exceeding cell
// capacity returns Complete=false rather than spuriously succeeding.
func TestIBLT_DecodeFailsWhenOverloaded(t *testing.T) {
	// 50 differing keys but only 10 cells — peeling cannot complete.
	keys := makeKeys(50)
	tbl := New(10, 3, 42)
	for _, k := range keys {
		tbl.Insert(k)
	}
	res := tbl.Decode()
	if res.Complete {
		t.Fatal("decode incorrectly reported Complete=true with overloaded table")
	}
}
