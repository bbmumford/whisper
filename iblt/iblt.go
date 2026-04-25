/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package iblt implements an Invertible Bloom Lookup Table for set
// reconciliation. Used by whisper's G4 reconciliation protocol to
// transfer the symmetric difference of two peers' record sets in
// O(|d|) bytes rather than O(|state|).
//
// Algorithm: Goodrich-Mitzenmacher peeling decoder. Each cell holds
// (key_sum XOR, key_hash_sum XOR, count). On insert/delete, the cell
// at each of k hash positions is updated. On peel-decode, cells with
// count == ±1 and key_sum.hash() == key_hash_sum are "pure" and yield
// a single element; removing them may make other cells pure, so the
// decoder iterates until no more pure cells exist.
//
// Two encoding modes are supported:
//
//   - Fixed-size: caller chooses cell count up-front (1.5 × d_max).
//     Single-shot encode + decode. Failure ("decode incomplete")
//     means d > d_max — caller retries with a larger table or falls
//     back to G1 + watermark.
//
//   - Rateless: caller streams cells indefinitely; receiver decodes
//     incrementally as cells arrive, succeeding with high probability
//     once ~1.5 × |d| cells received. No d_max estimation required.
//     Higher bandwidth in the worst case but adapts to any churn level.
//
// The whisper rumor pusher's G4 driver picks between modes based on
// a per-peer state machine: rate-1.5 when estimate is stable,
// rateless after 2 consecutive decode failures. See
// _PACKAGES/whisper/reconcile.go for the protocol driver.
package iblt

import (
	"encoding/binary"
	"hash/fnv"
)

// Key is the type of element stored in the IBLT. For whisper
// reconciliation, the key is a content hash (BLAKE3-256 truncated to
// 16 bytes — bit-cheap and collision-rare at the 11-fleet scale).
type Key [16]byte

// Cell is a single IBLT bucket. Three counters: key XOR-sum, key-hash
// XOR-sum, count. Pure-cell detection: |count| == 1 AND
// hash(keySum) == keyHashSum.
type Cell struct {
	KeySum     Key
	KeyHashSum uint64
	Count      int32
}

// IBLT is the encoder/decoder state. Cell count is fixed at
// construction; hash function count k determines the per-key write
// cost (and decode robustness).
type IBLT struct {
	cells []Cell
	k     int // hashes per key (typically 3 or 4)
	seed  uint64
}

// New returns an IBLT with `m` cells using `k` hash functions and the
// given seed. Receiver must construct an identical-shape IBLT (same
// m, k, seed) to decode the symmetric difference.
//
// Sizing rule: m = 1.5 × d_max for rate-1.5 mode. For rateless mode,
// the encoder appends cells indefinitely and the decoder grows its
// table on each batch.
//
// k = 3 is the standard choice — gives a peel-decode success rate
// > 99% at m / d ≥ 1.4. k = 4 is more robust at the cost of one
// extra hash per insert. Use 3 for the common case.
func New(m, k int, seed uint64) *IBLT {
	if m < 1 {
		m = 1
	}
	if k < 2 {
		k = 2
	}
	return &IBLT{
		cells: make([]Cell, m),
		k:     k,
		seed:  seed,
	}
}

// Cells returns the underlying cell slice (read-only — modifications
// break decode). Used by the wire codec.
func (t *IBLT) Cells() []Cell { return t.cells }

// K returns the hash count.
func (t *IBLT) K() int { return t.k }

// Seed returns the seed.
func (t *IBLT) Seed() uint64 { return t.seed }

// Insert adds a key to the table. Symmetric — Delete uses the same
// positions to subtract.
func (t *IBLT) Insert(k Key) {
	t.update(k, +1)
}

// Delete removes a key. After receiver-of-stream subtracts its own
// keys from the encoder's table, the remaining counts reveal the
// symmetric difference.
func (t *IBLT) Delete(k Key) {
	t.update(k, -1)
}

// Subtract subtracts another table from this one (cell-wise XOR for
// the sums, signed sum for the count). Used by the receiver to
// compute (encoder ∪ receiver) − receiver = encoder \ receiver, then
// peel-decode that to get the elements only the encoder has.
func (t *IBLT) Subtract(other *IBLT) {
	if other == nil || len(other.cells) != len(t.cells) {
		return
	}
	for i := range t.cells {
		for j := range t.cells[i].KeySum {
			t.cells[i].KeySum[j] ^= other.cells[i].KeySum[j]
		}
		t.cells[i].KeyHashSum ^= other.cells[i].KeyHashSum
		t.cells[i].Count -= other.cells[i].Count
	}
}

// update applies a +1 or -1 to all k hash positions for key.
func (t *IBLT) update(k Key, delta int32) {
	keyHash := hashKey(k)
	for i := 0; i < t.k; i++ {
		idx := t.position(k, i)
		c := &t.cells[idx]
		for j := range c.KeySum {
			c.KeySum[j] ^= k[j]
		}
		c.KeyHashSum ^= keyHash
		c.Count += delta
	}
}

// position returns the i-th cell index for key. Uses FNV-1a with a
// stream-position counter so different i values land in different
// cells without colluding via the seed.
func (t *IBLT) position(k Key, i int) int {
	h := fnv.New64a()
	var seed [8]byte
	binary.LittleEndian.PutUint64(seed[:], t.seed)
	h.Write(seed[:])
	var ibyte [4]byte
	binary.LittleEndian.PutUint32(ibyte[:], uint32(i))
	h.Write(ibyte[:])
	h.Write(k[:])
	return int(h.Sum64() % uint64(len(t.cells)))
}

// hashKey returns a 64-bit hash of a key, used for the pure-cell
// check. Independent of position — uses a different (no-seed) FNV
// stream so a malicious key can't collide both position and hash.
func hashKey(k Key) uint64 {
	h := fnv.New64a()
	h.Write([]byte("iblt-keyhash-v1"))
	h.Write(k[:])
	return h.Sum64()
}

// DecodeResult is the output of a peel-decode pass. Positive +
// Negative is the symmetric difference; the sender's set
// corresponds to Positive (peer has, we don't), the receiver's
// set corresponds to Negative (we have, peer doesn't).
type DecodeResult struct {
	Positive []Key // count == +1 cells (encoder-only elements)
	Negative []Key // count == -1 cells (receiver-only elements)
	Complete bool  // true when peel-decode terminated with all cells empty
}

// Decode performs peel-decode on the (possibly post-Subtract) table.
// Iterates until no more pure cells exist; if the result has all
// cells with count==0 and zero key-sum, the decode is complete and
// the symmetric difference is fully recovered. Otherwise the
// difference exceeds the table's capacity and the caller should
// retry with a larger table or fall back to G1 + watermark.
func (t *IBLT) Decode() DecodeResult {
	res := DecodeResult{}
	for {
		progress := false
		for i := range t.cells {
			c := &t.cells[i]
			if c.Count == 1 || c.Count == -1 {
				if hashKey(c.KeySum) == c.KeyHashSum {
					if c.Count == 1 {
						res.Positive = append(res.Positive, c.KeySum)
						t.Delete(c.KeySum)
					} else {
						res.Negative = append(res.Negative, c.KeySum)
						t.Insert(c.KeySum)
					}
					progress = true
				}
			}
		}
		if !progress {
			break
		}
	}
	// Check completion: all cells must have Count==0 and zero sums.
	for _, c := range t.cells {
		if c.Count != 0 || c.KeyHashSum != 0 {
			return res
		}
		var zero Key
		if c.KeySum != zero {
			return res
		}
	}
	res.Complete = true
	return res
}
