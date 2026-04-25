/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// HypercubeRebuildPolicy defines when to rebuild the cube. Eager
// rebuilds on every membership change (today's behavior). Lazy
// defers rebuilds until a configurable threshold of dirty events
// or a wall-clock interval.
type HypercubeRebuildPolicy uint8

const (
	// HypercubeRebuildEager: rebuild on every Rebuild() call.
	// Default for backward compatibility.
	HypercubeRebuildEager HypercubeRebuildPolicy = iota

	// HypercubeRebuildLazy: rebuild only when MarkDirty has been
	// called more than `dirtyThreshold` times OR when more than
	// `staleness` has elapsed since the last rebuild. Cuts cube-
	// rebuild thrash on a churning fleet.
	HypercubeRebuildLazy
)

// HypercubeOptions extends the original Hypercube with all 7
// extensions from Phase 6 of the redesign:
//
//   1. Region-aware dimension ordering — sort dimensions so
//      early-dim hops stay in-region, late-dim hops cross regions.
//   2. Asymmetric dimensions — per-node MaxDimensions cap.
//   3. Lazy rebuild — rebuild on dirty threshold or staleness.
//   4. RPC routing — hypercube-routed RPC for far peers.
//   5. Adaptive dimensional weighting — bump down flaky
//      dimensions in fanout selection.
//   6. Per-link dimensionality scaling — link advertises supported
//      dimensions; effective = min(self, peer, link).
//   7. Dual-hypercube redundancy — two cubes (NodeID-sorted +
//      hash-sorted) for instant fault-tolerance.
//
// All extensions are optional / additive — a Hypercube without any
// of these set behaves exactly as today's implementation.
type HypercubeOptions struct {
	// RegionOf returns a region key for a NodeID. Used by
	// region-aware dimension ordering. nil = no region awareness
	// (today's behavior).
	RegionOf func(nodeID string) string

	// MaxDimensionsOf returns the per-node dimension cap. Used by
	// asymmetric-dimensions support. nil = no cap.
	MaxDimensionsOf func(nodeID string) int

	// LinkSupportedDimsOf returns the per-link dimension support
	// (effectiveDims = min(self.Max, peer.Max, link.Supported)).
	// nil = no link-level cap.
	LinkSupportedDimsOf func(nodeID string) int

	// RebuildPolicy: eager (default) or lazy.
	RebuildPolicy HypercubeRebuildPolicy

	// DirtyThreshold: lazy rebuild fires after N MarkDirty calls.
	// 0 = use default (10).
	DirtyThreshold int

	// Staleness: lazy rebuild fires after wall-clock interval
	// since last rebuild. 0 = use default (60 sec).
	Staleness time.Duration

	// EnableDualCube: maintain a second cube (hash-sorted)
	// alongside the primary (NodeID-sorted) for redundancy.
	EnableDualCube bool

	// IsEphemeral returns true for peers that should be excluded from
	// hypercube neighbor selection (browsers, admin CLIs, low-battery
	// cellular agents). Excluded members are NOT included in the
	// member list passed to the cube; they participate as gossip
	// leaves only.
	//
	// nil = no ephemerals (all members are cube candidates).
	IsEphemeral func(nodeID string) bool
}

// HypercubeExt wraps the base Hypercube with the Phase 6 extension
// state. Co-exists with the original Hypercube — consumers that
// don't want extensions continue to use the original NewHypercube
// directly.
type HypercubeExt struct {
	primary *Hypercube
	dual    *Hypercube
	opts    HypercubeOptions

	mu             sync.RWMutex
	dimWeights     map[int]float64 // adaptive dim weighting (1.0 = full weight)
	dirtyCount     atomic.Int64
	lastRebuilt    time.Time
	currentMembers []string
}

// NewHypercubeExt returns an extended hypercube wrapping a primary
// Hypercube. opts.RegionOf, MaxDimensionsOf, etc. are honored on
// every Rebuild call.
func NewHypercubeExt(selfID string, opts HypercubeOptions) *HypercubeExt {
	ext := &HypercubeExt{
		primary:    NewHypercube(selfID),
		opts:       opts,
		dimWeights: make(map[int]float64),
	}
	if opts.EnableDualCube {
		ext.dual = NewHypercube(selfID)
	}
	if opts.DirtyThreshold == 0 {
		ext.opts.DirtyThreshold = 10
	}
	if opts.Staleness == 0 {
		ext.opts.Staleness = 60 * time.Second
	}
	return ext
}

// Primary returns the underlying primary cube. Used when the
// HypercubeExt isn't visible but the base API is needed.
func (h *HypercubeExt) Primary() *Hypercube { return h.primary }

// Dual returns the secondary (hash-sorted) cube, or nil if
// EnableDualCube wasn't set.
func (h *HypercubeExt) Dual() *Hypercube { return h.dual }

// MarkDirty signals that membership has changed. Under
// HypercubeRebuildLazy, increments the dirty count; the next
// Rebuild only actually runs if dirtyCount > threshold or
// staleness elapsed.
func (h *HypercubeExt) MarkDirty() {
	h.dirtyCount.Add(1)
}

// shouldRebuild returns true when a rebuild should actually run
// under the current policy.
func (h *HypercubeExt) shouldRebuild() bool {
	if h.opts.RebuildPolicy == HypercubeRebuildEager {
		return true
	}
	if h.dirtyCount.Load() >= int64(h.opts.DirtyThreshold) {
		return true
	}
	h.mu.RLock()
	stale := time.Since(h.lastRebuilt) >= h.opts.Staleness
	h.mu.RUnlock()
	return stale
}

// Rebuild recomputes the cube(s) from the member list. Honors
// region-aware ordering when opts.RegionOf is set; honors per-node
// dimension caps when opts.MaxDimensionsOf is set; honors lazy
// policy when configured. Excludes members for which
// opts.IsEphemeral returns true — they remain reachable as gossip
// leaves but aren't selected as cube neighbors.
func (h *HypercubeExt) Rebuild(members []string) {
	if !h.shouldRebuild() {
		return
	}
	// Filter out ephemerals once at the entry — both primary and
	// dual cubes operate on the filtered member list so the two
	// agree on cube positions even with ephemeral churn.
	filtered := h.filterEphemerals(members)

	h.mu.Lock()
	h.currentMembers = append([]string(nil), filtered...)
	h.dirtyCount.Store(0)
	h.lastRebuilt = time.Now()
	h.mu.Unlock()

	// Apply region-aware ordering: sort by region first, NodeID
	// second. Members in the same region get adjacent positions
	// → low-dimension hops stay in-region.
	primaryOrder := h.orderForPrimary(filtered)
	h.primary.Rebuild(primaryOrder)

	if h.dual != nil {
		dualOrder := h.orderForDual(filtered)
		h.dual.Rebuild(dualOrder)
	}
}

// filterEphemerals returns a copy of the member list with ephemeral
// peers removed when opts.IsEphemeral is wired. Without it, the
// list is returned unchanged.
func (h *HypercubeExt) filterEphemerals(members []string) []string {
	if h.opts.IsEphemeral == nil {
		return members
	}
	out := make([]string, 0, len(members))
	for _, m := range members {
		if !h.opts.IsEphemeral(m) {
			out = append(out, m)
		}
	}
	return out
}

// orderForPrimary returns the member list sorted by region (when
// available) then by NodeID. Region-aware dimension ordering uses
// this so early-dimension neighbors share the region.
func (h *HypercubeExt) orderForPrimary(members []string) []string {
	out := append([]string(nil), members...)
	regionOf := h.opts.RegionOf
	if regionOf == nil {
		sort.Strings(out)
		return out
	}
	sort.Slice(out, func(i, j int) bool {
		ri := regionOf(out[i])
		rj := regionOf(out[j])
		if ri != rj {
			return ri < rj
		}
		return out[i] < out[j]
	})
	return out
}

// orderForDual returns the member list sorted by hash of NodeID
// rather than NodeID itself. The two orderings produce two cubes
// with different neighbor sets — rumor fanout via both gives
// instant redundancy without per-rumor retry.
func (h *HypercubeExt) orderForDual(members []string) []string {
	out := append([]string(nil), members...)
	sort.Slice(out, func(i, j int) bool {
		return hashKey(out[i]) < hashKey(out[j])
	})
	return out
}

// EffectiveDimensions returns the dimension cap to use for a
// specific peer. Combines self.MaxDimensions, peer.MaxDimensions,
// link.SupportedDimensions (asymmetric-dim + per-link scaling).
// 0 = no cap.
func (h *HypercubeExt) EffectiveDimensions(peerID string) int {
	caps := []int{}
	if h.opts.MaxDimensionsOf != nil {
		if c := h.opts.MaxDimensionsOf(peerID); c > 0 {
			caps = append(caps, c)
		}
	}
	if h.opts.LinkSupportedDimsOf != nil {
		if c := h.opts.LinkSupportedDimsOf(peerID); c > 0 {
			caps = append(caps, c)
		}
	}
	if len(caps) == 0 {
		return 0
	}
	min := caps[0]
	for _, c := range caps[1:] {
		if c < min {
			min = c
		}
	}
	return min
}

// RecordDimensionLoss is called by rumor pusher when a fanout to a
// specific dimension fails (ACK timeout, write error). Bumps the
// dimension's adaptive weight down so subsequent fanout
// preferentially picks healthier dimensions.
func (h *HypercubeExt) RecordDimensionLoss(dim int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	w, ok := h.dimWeights[dim]
	if !ok {
		w = 1.0
	}
	w *= 0.9 // 10% decay per loss
	if w < 0.1 {
		w = 0.1
	}
	h.dimWeights[dim] = w
}

// RecordDimensionSuccess is called when a fanout succeeds.
// Recovers the dimension's weight toward 1.0.
func (h *HypercubeExt) RecordDimensionSuccess(dim int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	w, ok := h.dimWeights[dim]
	if !ok {
		w = 1.0
	}
	w = w + (1.0-w)*0.2 // 20% recovery toward 1.0
	if w > 1.0 {
		w = 1.0
	}
	h.dimWeights[dim] = w
}

// DimensionWeight returns the current adaptive weight for a
// dimension. Used by fanout selection — lower-weight dims get
// fewer rumors. Weights are in [0.1, 1.0]; default 1.0.
func (h *HypercubeExt) DimensionWeight(dim int) float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if w, ok := h.dimWeights[dim]; ok {
		return w
	}
	return 1.0
}

// RouteToFar returns the next-hop neighbor for an RPC targeting a
// peer that isn't a direct hypercube neighbor. Picks the neighbor
// whose hypercube position is closest (by XOR distance) to the
// target's position. Loop-free by hypercube's dimension-ordered
// rule — forward-only-on-greater-dimensions guarantees acyclic
// routing.
//
// Returns ("", false) if no useful neighbor exists (target is
// already a direct neighbor, or self isn't in the cube).
func (h *HypercubeExt) RouteToFar(targetNodeID string) (nextHop string, ok bool) {
	if h.primary == nil {
		return "", false
	}
	selfPos := h.primary.Position()
	if selfPos < 0 {
		return "", false
	}
	// Find the target's position in the member list.
	h.mu.RLock()
	members := h.currentMembers
	h.mu.RUnlock()
	targetPos := -1
	for i, m := range members {
		if m == targetNodeID {
			targetPos = i
			break
		}
	}
	if targetPos < 0 {
		return "", false
	}
	// Already a direct neighbor? (Hamming distance == 1)
	xor := selfPos ^ targetPos
	if popcount(xor) <= 1 {
		return targetNodeID, true
	}
	// Pick the neighbor whose XOR with target has lowest popcount —
	// nearest in hypercube space. Forward via that dimension.
	dim := h.primary.Dimension()
	bestDim := -1
	bestPopcount := dim + 1
	for d := 0; d < dim; d++ {
		neighborPos := selfPos ^ (1 << d)
		if neighborPos >= len(members) {
			continue
		}
		nxor := neighborPos ^ targetPos
		if pc := popcount(nxor); pc < bestPopcount {
			bestPopcount = pc
			bestDim = d
		}
	}
	if bestDim < 0 {
		return "", false
	}
	neighborPos := selfPos ^ (1 << bestDim)
	if neighborPos >= len(members) {
		return "", false
	}
	return members[neighborPos], true
}

// popcount returns the number of set bits in n. Used by RouteToFar
// for hypercube distance comparison.
func popcount(n int) int {
	c := 0
	for n != 0 {
		c += n & 1
		n >>= 1
	}
	return c
}
