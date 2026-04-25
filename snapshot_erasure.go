/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"encoding/binary"
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// ReedSolomonErasure is a default Reed-Solomon implementation of the
// erasure-coding portion of SnapshotCodec. Codec implementations
// embed it (or call directly) to satisfy EncodeErasure / DecodeErasure
// without re-implementing the byte-slice marshalling that pads
// variable-length record shards into the equal-length buffers
// reedsolomon.Encode requires.
//
// The wire format inside each shard is:
//
//	[count uint32][len uint32][record_bytes][len uint32][record_bytes]...
//
// Padded with zero bytes to the shard buffer length. The first 4
// bytes (count) on a parity shard are not record bytes — parity
// shards are opaque to the consumer; only the n-of-n decode path
// reads them.
type ReedSolomonErasure struct{}

// EncodeErasure flattens k data shards (each carrying its own
// records) into equal-length buffers, runs reed-solomon over them to
// produce parityCount parity shards, and returns all k+parityCount
// shards in a stable order: data first, parity after.
//
// Output shape: [][][]byte where each outer entry is one shard's
// flattened bytes split into a single inner [][]byte (the codec
// keeps a slice-of-slices interface for symmetry with the input).
// Concretely each output shard's inner slice has length 1 — the
// flattened buffer.
func (ReedSolomonErasure) EncodeErasure(dataShards [][][]byte, parityCount int) ([][][]byte, error) {
	k := len(dataShards)
	if k == 0 {
		return nil, fmt.Errorf("erasure: zero data shards")
	}
	if parityCount < 0 {
		return nil, fmt.Errorf("erasure: negative parity")
	}

	// Step 1: pack each shard's records into a length-prefixed
	// buffer.
	packed := make([][]byte, k)
	maxLen := 0
	for i, recs := range dataShards {
		buf := packShardRecords(recs)
		packed[i] = buf
		if len(buf) > maxLen {
			maxLen = len(buf)
		}
	}

	// Step 2: pad every data shard to maxLen so reedsolomon's
	// equal-length-shard contract is satisfied. The 4-byte length
	// prefix at the start of each shard tells the decoder how much
	// to trust on the way back; the rest is zero padding.
	for i := range packed {
		if len(packed[i]) < maxLen {
			pad := make([]byte, maxLen)
			copy(pad, packed[i])
			packed[i] = pad
		}
	}

	// Parity-only path: no parity requested → return just the data
	// shards as-is.
	if parityCount == 0 {
		out := make([][][]byte, k)
		for i := range packed {
			out[i] = [][]byte{packed[i]}
		}
		return out, nil
	}

	// Step 3: build the n-shard buffer (k data + parityCount
	// parity), each entry sized to maxLen. reedsolomon.Encode fills
	// in the parity entries.
	n := k + parityCount
	full := make([][]byte, n)
	for i := 0; i < k; i++ {
		full[i] = packed[i]
	}
	for i := k; i < n; i++ {
		full[i] = make([]byte, maxLen)
	}

	enc, err := reedsolomon.New(k, parityCount)
	if err != nil {
		return nil, fmt.Errorf("erasure: new encoder (k=%d,p=%d): %w", k, parityCount, err)
	}
	if err := enc.Encode(full); err != nil {
		return nil, fmt.Errorf("erasure: encode: %w", err)
	}

	// Wrap each flat shard in its own inner slice for the [][][]byte
	// interface symmetry.
	out := make([][][]byte, n)
	for i := range full {
		out[i] = [][]byte{full[i]}
	}
	return out, nil
}

// DecodeErasure reconstructs the original k data shards from any
// k-of-n shards. Missing shards in the input MUST be nil entries
// (their inner slice empty or the outer entry nil). The output is
// the first k reconstructed shards as record-byte-slice slices.
func (ReedSolomonErasure) DecodeErasure(shards [][][]byte, k int) ([][][]byte, error) {
	n := len(shards)
	if n == 0 {
		return nil, fmt.Errorf("erasure: empty shard set")
	}
	if k <= 0 || k > n {
		return nil, fmt.Errorf("erasure: invalid k=%d for n=%d", k, n)
	}
	parity := n - k

	// Flatten the input into the [][]byte form reedsolomon expects;
	// missing shards stay nil (the package treats nil as "needs
	// reconstruction").
	flat := make([][]byte, n)
	maxLen := 0
	for i, s := range shards {
		if len(s) == 0 || s[0] == nil || len(s[0]) == 0 {
			flat[i] = nil
			continue
		}
		flat[i] = s[0]
		if len(s[0]) > maxLen {
			maxLen = len(s[0])
		}
	}
	if maxLen == 0 {
		return nil, fmt.Errorf("erasure: no shards have data")
	}

	// Reedsolomon needs missing shards to be a slice of the right
	// length so the reconstruction can fill them in-place. Allocate
	// zero-filled buffers for any nil entries.
	for i := range flat {
		if flat[i] == nil {
			flat[i] = make([]byte, maxLen)
			// Mark as missing for Reconstruct by setting len(0)
			flat[i] = flat[i][:0]
		}
	}

	if parity > 0 {
		dec, err := reedsolomon.New(k, parity)
		if err != nil {
			return nil, fmt.Errorf("erasure: new decoder (k=%d,p=%d): %w", k, parity, err)
		}
		if err := dec.Reconstruct(flat); err != nil {
			return nil, fmt.Errorf("erasure: reconstruct: %w", err)
		}
	}

	// Unpack each data shard back into its constituent records.
	out := make([][][]byte, k)
	for i := 0; i < k; i++ {
		out[i] = unpackShardRecords(flat[i])
	}
	return out, nil
}

// packShardRecords serialises records into a length-prefixed buffer.
//
//	[count uint32][len uint32][record][len uint32][record]...
func packShardRecords(records [][]byte) []byte {
	totalLen := 4
	for _, r := range records {
		totalLen += 4 + len(r)
	}
	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(records)))
	off := 4
	for _, r := range records {
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(r)))
		off += 4
		copy(buf[off:], r)
		off += len(r)
	}
	return buf
}

// unpackShardRecords deserialises a packed shard buffer back into
// its records, ignoring trailing zero padding past the records'
// declared lengths.
func unpackShardRecords(buf []byte) [][]byte {
	if len(buf) < 4 {
		return nil
	}
	count := binary.BigEndian.Uint32(buf[0:4])
	out := make([][]byte, 0, count)
	off := 4
	for i := uint32(0); i < count; i++ {
		if off+4 > len(buf) {
			return out
		}
		ln := binary.BigEndian.Uint32(buf[off : off+4])
		off += 4
		if off+int(ln) > len(buf) {
			return out
		}
		rec := make([]byte, ln)
		copy(rec, buf[off:off+int(ln)])
		out = append(out, rec)
		off += int(ln)
	}
	return out
}
