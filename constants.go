/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import "sync"

// maxGossipPayload caps the size of a single gossip message (1 MB).
// With 11 nodes, typical exchanges are 15-50KB. 1MB provides generous headroom
// while catching corrupted length headers.
const maxGossipPayload = 1 << 20

// GossipMagic is the 2-byte prefix on all G1 gossip frames for corruption detection.
// Wire format: [2-byte magic 0x4731][4-byte big-endian length][payload]
const GossipMagic uint16 = 0x4731 // "G1"
const gossipHeaderSize = 6        // 2 (magic) + 4 (length)

// gossipBufPoolMaxCap is the maximum buffer capacity retained in the pool.
// Buffers larger than this are discarded to prevent pool inflation from
// occasional large payloads (e.g., full sync). Typical gossip payloads are
// ~100KB; 128KB cap retains buffers for reuse without holding oversized ones.
const gossipBufPoolMaxCap = 128 * 1024

// gossipBufPool reuses byte buffers for marshal/unmarshal to reduce
// per-exchange heap allocations that contribute to OOM under high gossip load.
var gossipBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 32*1024) // 32 KB initial capacity
		return &b
	},
}

// returnBufToPool returns a buffer to the pool, discarding oversized buffers
// to prevent pool inflation from occasional large payloads.
func returnBufToPool(bufPtr *[]byte, buf []byte) {
	if cap(buf) > gossipBufPoolMaxCap {
		b := make([]byte, 0, 32*1024)
		bufPtr = &b
	} else {
		*bufPtr = buf[:0]
	}
	gossipBufPool.Put(bufPtr)
}
