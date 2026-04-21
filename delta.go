/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"sync"
	"time"
)

// deltaMaxPeerAge is the maximum time a peer can remain in the watermark/exchange
// maps without being updated before it is evicted.
const deltaMaxPeerAge = 1 * time.Hour

// DeltaTracker maintains per-peer sync watermarks for delta gossip.
// Each peer connection tracks the highest record timestamp successfully
// exchanged. On each gossip cycle, only records newer than the watermark
// are sent. Every Nth exchange triggers a full sync for consistency.
//
// The full-sync cadence is per-peer and adaptive: noisy (lossy / high-
// disagreement) peers get full syncs more often; quiet (consistent)
// peers get them less often. The baseline is fullSyncInterval; the
// actual threshold is scaled per peer via peerScale ∈ [0.5, 2.0] based
// on how many full-syncs have actually APPLIED records recently (a
// signal that the delta path alone is missing state).
type DeltaTracker struct {
	mu               sync.Mutex
	watermarks       map[string]time.Time // peerNodeID -> last synced timestamp
	exchangeCounts   map[string]int       // peerNodeID -> exchanges since last full sync
	lastActivity     map[string]time.Time // peerNodeID -> last activity time (for eviction)
	// appliedOnFull: count of full-syncs that actually applied > 0 records
	// per peer. Adaptive: many applied-on-full means the delta path keeps
	// missing state on this peer, so full-sync more often. Few applied-on-
	// full means delta is keeping up, so full-sync less often.
	appliedOnFull    map[string]int
	fullSyncCount    map[string]int
	fullSyncInterval int // baseline: force full sync every N exchanges (default: 10)
}

// NewDeltaTracker creates a tracker with the default full sync interval (10).
func NewDeltaTracker() *DeltaTracker {
	return &DeltaTracker{
		watermarks:       make(map[string]time.Time),
		exchangeCounts:   make(map[string]int),
		lastActivity:     make(map[string]time.Time),
		appliedOnFull:    make(map[string]int),
		fullSyncCount:    make(map[string]int),
		fullSyncInterval: 10,
	}
}

// NewDeltaTrackerWithInterval creates a tracker with a custom full sync interval.
func NewDeltaTrackerWithInterval(fullSyncInterval int) *DeltaTracker {
	if fullSyncInterval <= 0 {
		fullSyncInterval = 10
	}
	return &DeltaTracker{
		watermarks:       make(map[string]time.Time),
		exchangeCounts:   make(map[string]int),
		lastActivity:     make(map[string]time.Time),
		appliedOnFull:    make(map[string]int),
		fullSyncCount:    make(map[string]int),
		fullSyncInterval: fullSyncInterval,
	}
}

// RecordWatermark returns the last successful sync timestamp for a peer.
// Returns zero time if no prior exchange exists (triggers full sync).
func (dt *DeltaTracker) RecordWatermark(peerID string) time.Time {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	return dt.watermarks[peerID]
}

// UpdateWatermark sets the watermark for a peer after a successful exchange.
func (dt *DeltaTracker) UpdateWatermark(peerID string, timestamp time.Time) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.watermarks[peerID] = timestamp
	dt.exchangeCounts[peerID]++
	dt.lastActivity[peerID] = time.Now()
	dt.evictStalePeers()
}

// ShouldFullSync returns true if this peer should receive a full cache dump.
// True when:
//   - The peer has no watermark (first exchange)
//   - The exchange count is a multiple of the peer's ADAPTIVE interval
//
// Adaptive interval = baseline × peerScale, where peerScale is derived
// from the ratio of applied-on-full to total full-syncs for this peer:
//   - high ratio (≥0.5): delta is missing state on this peer — scale 0.5
//     (full-sync at half the baseline interval, i.e. more often)
//   - mid ratio (0.1-0.5): baseline scale 1.0
//   - low ratio (<0.1): delta is keeping up fine — scale 2.0 (less often)
//
// Scale factors are clamped to [0.5, 2.0] so the adaptive interval
// stays in [5, 20] with the default 10-baseline — never far from the
// static default but responsive to per-peer reality.
func (dt *DeltaTracker) ShouldFullSync(peerID string) bool {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	wm, exists := dt.watermarks[peerID]
	if !exists || wm.IsZero() {
		return true
	}
	interval := dt.peerFullSyncIntervalLocked(peerID)
	count := dt.exchangeCounts[peerID]
	return count%interval == 0
}

// peerFullSyncIntervalLocked returns the adaptive full-sync interval for
// a peer. Caller must hold dt.mu.
func (dt *DeltaTracker) peerFullSyncIntervalLocked(peerID string) int {
	base := dt.fullSyncInterval
	fullCount := dt.fullSyncCount[peerID]
	if fullCount < 3 {
		// Not enough samples to adapt — use baseline.
		return base
	}
	ratio := float64(dt.appliedOnFull[peerID]) / float64(fullCount)
	switch {
	case ratio >= 0.5:
		v := base / 2
		if v < 1 {
			v = 1
		}
		return v
	case ratio < 0.1:
		return base * 2
	default:
		return base
	}
}

// RecordFullSyncResult is called by the engine after a full-sync exchange
// completes. appliedRecords is the number of records the peer's cache
// accepted from our full dump. Drives the adaptive scale in
// peerFullSyncIntervalLocked.
func (dt *DeltaTracker) RecordFullSyncResult(peerID string, appliedRecords int) {
	dt.mu.Lock()
	dt.fullSyncCount[peerID]++
	if appliedRecords > 0 {
		dt.appliedOnFull[peerID]++
	}
	// Cap the counters so a very long-lived peer doesn't starve the ratio
	// from changing. Halving both preserves the ratio while making recent
	// behaviour weigh more.
	if dt.fullSyncCount[peerID] > 100 {
		dt.fullSyncCount[peerID] /= 2
		dt.appliedOnFull[peerID] /= 2
	}
	dt.mu.Unlock()
}

// evictStalePeers removes entries for peers not seen in >1 hour.
// Must be called with mu held.
func (dt *DeltaTracker) evictStalePeers() {
	now := time.Now()
	evicted := 0
	for peerID, lastSeen := range dt.lastActivity {
		if now.Sub(lastSeen) > deltaMaxPeerAge {
			delete(dt.watermarks, peerID)
			delete(dt.exchangeCounts, peerID)
			delete(dt.lastActivity, peerID)
			evicted++
		}
	}
	// Also clean up any watermark entries that have no lastActivity tracking
	// (pre-existing entries from before this fix was added)
	for peerID := range dt.watermarks {
		if _, ok := dt.lastActivity[peerID]; !ok {
			delete(dt.watermarks, peerID)
			delete(dt.exchangeCounts, peerID)
			evicted++
		}
	}
	if evicted > 0 {
		dbgGossip.Printf("Delta evicted %d stale peer entries (threshold=%v)", evicted, deltaMaxPeerAge)
	}
}

// Reset clears the watermark and exchange count for a peer (e.g., after disconnect/reconnect).
func (dt *DeltaTracker) Reset(peerID string) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	delete(dt.watermarks, peerID)
	delete(dt.exchangeCounts, peerID)
	delete(dt.lastActivity, peerID)
	delete(dt.appliedOnFull, peerID)
	delete(dt.fullSyncCount, peerID)
}

// Stats returns debug information about delta tracking for all peers.
func (dt *DeltaTracker) Stats() map[string]DeltaPeerStats {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	stats := make(map[string]DeltaPeerStats, len(dt.watermarks))
	for peer, wm := range dt.watermarks {
		interval := dt.peerFullSyncIntervalLocked(peer)
		stats[peer] = DeltaPeerStats{
			Watermark:        wm,
			ExchangeCount:    dt.exchangeCounts[peer],
			NextFullSync:     interval - (dt.exchangeCounts[peer] % interval),
			AdaptiveInterval: interval,
			FullSyncCount:    dt.fullSyncCount[peer],
			AppliedOnFull:    dt.appliedOnFull[peer],
		}
	}
	return stats
}

// DeltaPeerStats is debug info for a single peer's delta state.
type DeltaPeerStats struct {
	Watermark        time.Time
	ExchangeCount    int
	NextFullSync     int // exchanges until next forced full sync (per-peer adaptive)
	AdaptiveInterval int // current full-sync interval for this peer
	FullSyncCount    int // total full-syncs performed for this peer
	AppliedOnFull    int // full-syncs that applied > 0 records
}

// DeltaSyncMeta carries delta sync metadata in the gossip envelope.
type DeltaSyncMeta struct {
	IsFullSync  bool      `json:"full"`         // true if this is a full dump
	Watermark   time.Time `json:"wm,omitempty"` // sender's watermark for this peer
	RecordCount int       `json:"count"`        // number of records in this exchange
}
