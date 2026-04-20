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
type DeltaTracker struct {
	mu               sync.Mutex
	watermarks       map[string]time.Time // peerNodeID -> last synced timestamp
	exchangeCounts   map[string]int       // peerNodeID -> exchanges since last full sync
	lastActivity     map[string]time.Time // peerNodeID -> last activity time (for eviction)
	fullSyncInterval int                  // force full sync every N exchanges (default: 10)
}

// NewDeltaTracker creates a tracker with the default full sync interval (10).
func NewDeltaTracker() *DeltaTracker {
	return &DeltaTracker{
		watermarks:       make(map[string]time.Time),
		exchangeCounts:   make(map[string]int),
		lastActivity:     make(map[string]time.Time),
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
// This is true when:
//   - The peer has no watermark (first exchange)
//   - The exchange count is a multiple of fullSyncInterval (periodic consistency)
func (dt *DeltaTracker) ShouldFullSync(peerID string) bool {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	wm, exists := dt.watermarks[peerID]
	if !exists || wm.IsZero() {
		return true
	}
	count := dt.exchangeCounts[peerID]
	return count%dt.fullSyncInterval == 0
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
}

// Stats returns debug information about delta tracking for all peers.
func (dt *DeltaTracker) Stats() map[string]DeltaPeerStats {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	stats := make(map[string]DeltaPeerStats, len(dt.watermarks))
	for peer, wm := range dt.watermarks {
		stats[peer] = DeltaPeerStats{
			Watermark:     wm,
			ExchangeCount: dt.exchangeCounts[peer],
			NextFullSync:  dt.fullSyncInterval - (dt.exchangeCounts[peer] % dt.fullSyncInterval),
		}
	}
	return stats
}

// DeltaPeerStats is debug info for a single peer's delta state.
type DeltaPeerStats struct {
	Watermark     time.Time
	ExchangeCount int
	NextFullSync  int // exchanges until next forced full sync
}

// DeltaSyncMeta carries delta sync metadata in the gossip envelope.
type DeltaSyncMeta struct {
	IsFullSync  bool      `json:"full"`         // true if this is a full dump
	Watermark   time.Time `json:"wm,omitempty"` // sender's watermark for this peer
	RecordCount int       `json:"count"`        // number of records in this exchange
}
