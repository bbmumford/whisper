/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"hash/fnv"
	"math"
	"sort"
	"sync"
)

// hashKey computes an FNV-1a 64-bit hash for order-independent fingerprinting.
func hashKey(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// Hypercube implements a virtual hypercube overlay for structured rumor routing.
//
// Each node is assigned a position based on its index in the sorted LAD member
// list. Neighbors are computed by flipping one bit at a time (one per dimension).
// Dimension-ordered routing ensures each node receives a rumor exactly once:
//
//   - Origin sends to ALL dimensions (fromDimension = 0xFF → -1)
//   - Each receiver forwards only on dimensions GREATER than the one it received on
//   - Total messages = N-1 (optimal, zero redundancy)
//
// Example: 4 nodes (2D), positions 00, 01, 10, 11
//
//	Node 00 originates: → dim0→01, dim1→10
//	Node 01 received on dim0: → dim1→11 (only dims > 0)
//	Node 10 received on dim1: → stop (no dims > 1)
//	Node 11 received on dim1: → stop
//	Total: 3 messages for 4 nodes. Optimal.
type Hypercube struct {
	mu        sync.RWMutex
	selfID    string
	selfPos   int      // position in sorted member list (-1 if not found)
	dimension int      // ceil(log2(N))
	members   []string // sorted node IDs (index = hypercube position)
	version   uint64   // fingerprint of member list for staleness detection
}

// NewHypercube creates a hypercube for the given local node ID.
// Call Rebuild() with the current member list to initialize.
func NewHypercube(selfID string) *Hypercube {
	return &Hypercube{selfID: selfID, selfPos: -1}
}

// Rebuild recomputes the hypercube from the current member list.
// All nodes produce the same topology from the same sorted member list.
func (h *Hypercube) Rebuild(memberIDs []string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	sorted := make([]string, len(memberIDs))
	copy(sorted, memberIDs)
	sort.Strings(sorted)

	h.members = sorted
	h.dimension = 0
	if len(sorted) > 1 {
		h.dimension = int(math.Ceil(math.Log2(float64(len(sorted)))))
	}
	if h.dimension == 0 && len(sorted) == 1 {
		h.dimension = 0 // single node, no dimensions
	}

	h.selfPos = -1
	for i, id := range sorted {
		if id == h.selfID {
			h.selfPos = i
			break
		}
	}

	// Version = XOR of all member hashes (order-independent)
	var v uint64
	for _, id := range sorted {
		v ^= hashKey(id)
	}
	h.version = v

	dbgHypercube.Printf("Rebuilt: %d members, %d dimensions, selfPos=%d, version=%016x",
		len(sorted), h.dimension, h.selfPos, v)
}

// Neighbors returns all neighbor node IDs (one per dimension).
// Returns nil if this node is not in the hypercube.
func (h *Hypercube) Neighbors() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.selfPos < 0 || h.dimension == 0 {
		return nil
	}
	var neighbors []string
	for d := 0; d < h.dimension; d++ {
		pos := h.selfPos ^ (1 << d)
		if pos < len(h.members) {
			neighbors = append(neighbors, h.members[pos])
		}
	}
	return neighbors
}

// DimensionNeighbor returns the neighbor in a specific dimension, or "" if none.
func (h *Hypercube) DimensionNeighbor(d int) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.selfPos < 0 || d >= h.dimension || d < 0 {
		return ""
	}
	pos := h.selfPos ^ (1 << d)
	if pos >= len(h.members) {
		return "" // non-power-of-2: this dimension has no neighbor
	}
	return h.members[pos]
}

// RouteRumor returns the dimensions to forward on based on dimension-ordered routing.
// fromDimension = -1 (or 0xFF as uint8) means origin — forward on ALL dimensions.
// Otherwise, forward on dimensions strictly greater than fromDimension.
func (h *Hypercube) RouteRumor(fromDimension int) []int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.selfPos < 0 || h.dimension == 0 {
		return nil
	}

	var dims []int
	start := 0
	if fromDimension >= 0 && fromDimension < 255 {
		start = fromDimension + 1
	}
	for d := start; d < h.dimension; d++ {
		pos := h.selfPos ^ (1 << d)
		if pos < len(h.members) {
			dims = append(dims, d)
		}
	}
	return dims
}

// Dimension returns the cube dimension count.
func (h *Hypercube) Dimension() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.dimension
}

// Position returns this node's hypercube address (-1 if not in cube).
func (h *Hypercube) Position() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.selfPos
}

// MemberCount returns the number of members in the hypercube.
func (h *Hypercube) MemberCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.members)
}

// Version returns the fingerprint of the member list.
func (h *Hypercube) Version() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.version
}
