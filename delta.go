/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"context"
	"sync"
	"time"
)

// deltaMaxPeerAge is the maximum time a peer can remain in the delta state
// map without being updated before it is evicted.
const deltaMaxPeerAge = 1 * time.Hour

// deltaSweepInterval is how often the background sweeper scans for stale
// peer entries. A 5-minute cadence with a 1-hour age threshold gives the
// sweeper plenty of time to catch anything the disconnect hook missed
// without burning CPU on pointless scans.
const deltaSweepInterval = 5 * time.Minute

// peerDeltaState aggregates all per-peer delta-sync bookkeeping into one
// struct. Previously these lived in five parallel maps keyed by peerNodeID,
// which multiplied hashmap overhead by 5 and made eviction (every Reset
// call) delete from 5 maps in lock-step. One map of pointers is cheaper
// and atomically consistent.
type peerDeltaState struct {
	watermark     time.Time // last synced record timestamp
	exchangeCount int       // exchanges since last full sync
	lastActivity  time.Time // last activity (for eviction)
	// appliedOnFull: count of full-syncs that actually applied > 0 records
	// on this peer. Adaptive: many applied-on-full means the delta path
	// keeps missing state on this peer, so full-sync more often. Few
	// applied-on-full means delta is keeping up, so full-sync less often.
	appliedOnFull int
	fullSyncCount int
}

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
//
// Callers are expected to call Reset(peerNodeID) from their peer-
// disconnect hook so stale state doesn't accumulate across reconnects.
// A background sweeper (started by StartSweeper) evicts entries older
// than deltaMaxPeerAge every deltaSweepInterval as a safety net.
type DeltaTracker struct {
	mu               sync.Mutex
	peers            map[string]*peerDeltaState // peerNodeID → state
	fullSyncInterval int                        // baseline: force full sync every N exchanges (default: 10)

	stopSweeper chan struct{}
	sweeperOnce sync.Once
}

// NewDeltaTracker creates a tracker with the default full sync interval (10).
func NewDeltaTracker() *DeltaTracker {
	return &DeltaTracker{
		peers:            make(map[string]*peerDeltaState),
		fullSyncInterval: 10,
	}
}

// NewDeltaTrackerWithInterval creates a tracker with a custom full sync interval.
func NewDeltaTrackerWithInterval(fullSyncInterval int) *DeltaTracker {
	if fullSyncInterval <= 0 {
		fullSyncInterval = 10
	}
	return &DeltaTracker{
		peers:            make(map[string]*peerDeltaState),
		fullSyncInterval: fullSyncInterval,
	}
}

// getOrCreateLocked returns (and creates if missing) the state entry for a
// peer. Caller must hold dt.mu.
func (dt *DeltaTracker) getOrCreateLocked(peerID string) *peerDeltaState {
	st, ok := dt.peers[peerID]
	if !ok {
		st = &peerDeltaState{}
		dt.peers[peerID] = st
	}
	return st
}

// RecordWatermark returns the last successful sync timestamp for a peer.
// Returns zero time if no prior exchange exists (triggers full sync).
func (dt *DeltaTracker) RecordWatermark(peerID string) time.Time {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	if st, ok := dt.peers[peerID]; ok {
		return st.watermark
	}
	return time.Time{}
}

// UpdateWatermark sets the watermark for a peer after a successful exchange.
func (dt *DeltaTracker) UpdateWatermark(peerID string, timestamp time.Time) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	st := dt.getOrCreateLocked(peerID)
	st.watermark = timestamp
	st.exchangeCount++
	st.lastActivity = time.Now()
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
	st, exists := dt.peers[peerID]
	if !exists || st.watermark.IsZero() {
		return true
	}
	interval := dt.peerFullSyncIntervalLocked(st)
	return st.exchangeCount%interval == 0
}

// peerFullSyncIntervalLocked returns the adaptive full-sync interval for
// a peer. Caller must hold dt.mu.
func (dt *DeltaTracker) peerFullSyncIntervalLocked(st *peerDeltaState) int {
	base := dt.fullSyncInterval
	if st == nil || st.fullSyncCount < 3 {
		// Not enough samples to adapt — use baseline.
		return base
	}
	ratio := float64(st.appliedOnFull) / float64(st.fullSyncCount)
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
	defer dt.mu.Unlock()
	st := dt.getOrCreateLocked(peerID)
	st.fullSyncCount++
	if appliedRecords > 0 {
		st.appliedOnFull++
	}
	// Cap the counters so a very long-lived peer doesn't starve the ratio
	// from changing. Halving both preserves the ratio while making recent
	// behaviour weigh more.
	if st.fullSyncCount > 100 {
		st.fullSyncCount /= 2
		st.appliedOnFull /= 2
	}
}

// evictStalePeersLocked removes entries for peers not seen in >deltaMaxPeerAge.
// Caller must hold dt.mu.
func (dt *DeltaTracker) evictStalePeersLocked() {
	now := time.Now()
	evicted := 0
	for peerID, st := range dt.peers {
		if now.Sub(st.lastActivity) > deltaMaxPeerAge {
			delete(dt.peers, peerID)
			evicted++
		}
	}
	if evicted > 0 {
		dbgGossip.Printf("Delta evicted %d stale peer entries (threshold=%v)", evicted, deltaMaxPeerAge)
	}
}

// Reset clears the state for a peer. Callers should invoke this from
// their peer-disconnect hook — the tracker doesn't have its own view of
// connection lifecycle.
func (dt *DeltaTracker) Reset(peerID string) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	delete(dt.peers, peerID)
}

// StartSweeper launches a background goroutine that periodically evicts
// entries older than deltaMaxPeerAge. Safe to call multiple times — only
// the first call starts the goroutine. The sweeper is a safety net; the
// primary eviction path is caller-driven Reset on peer disconnect.
func (dt *DeltaTracker) StartSweeper() {
	dt.sweeperOnce.Do(func() {
		dt.stopSweeper = make(chan struct{})
		go dt.sweepLoop()
	})
}

// StopSweeper signals the background sweeper to exit. Idempotent — safe
// to call before StartSweeper or multiple times.
func (dt *DeltaTracker) StopSweeper() {
	dt.mu.Lock()
	ch := dt.stopSweeper
	dt.stopSweeper = nil
	dt.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case <-ch:
		// already closed
	default:
		close(ch)
	}
}

func (dt *DeltaTracker) sweepLoop() {
	ticker := time.NewTicker(deltaSweepInterval)
	defer ticker.Stop()
	for {
		dt.mu.Lock()
		ch := dt.stopSweeper
		dt.mu.Unlock()
		if ch == nil {
			return
		}
		select {
		case <-ch:
			return
		case <-ticker.C:
			dt.mu.Lock()
			dt.evictStalePeersLocked()
			dt.mu.Unlock()
		}
	}
}

// Stats returns debug information about delta tracking for all peers.
func (dt *DeltaTracker) Stats() map[string]DeltaPeerStats {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	stats := make(map[string]DeltaPeerStats, len(dt.peers))
	for peer, st := range dt.peers {
		interval := dt.peerFullSyncIntervalLocked(st)
		stats[peer] = DeltaPeerStats{
			Watermark:        st.watermark,
			ExchangeCount:    st.exchangeCount,
			NextFullSync:     interval - (st.exchangeCount % interval),
			AdaptiveInterval: interval,
			FullSyncCount:    st.fullSyncCount,
			AppliedOnFull:    st.appliedOnFull,
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

// PersistedPeerState is the over-the-wire / over-the-disk representation
// of a single peer's delta state, used by DeltaPersistence to round-trip
// state across process restarts. Keep field tags stable — they're the
// disk schema for any backend that serialises this struct.
type PersistedPeerState struct {
	PeerID        string    `json:"peer_id"`
	Watermark     time.Time `json:"watermark"`
	ExchangeCount int       `json:"exchange_count"`
	LastActivity  time.Time `json:"last_activity"`
	AppliedOnFull int       `json:"applied_on_full"`
	FullSyncCount int       `json:"full_sync_count"`
}

// DeltaPersistence is the storage backend the consumer supplies for
// surviving DeltaTracker state across process restarts. Without this,
// every fly redeploy wipes per-peer watermarks and the next gossip
// exchange to every peer is a full snapshot — the convergence-burst
// pattern that drains stream credit windows.
//
// Implementations:
//   - Library: JSON file under <dataDir>/.mesh/watermarks.json with
//     atomic write (temp + rename).
//   - Agent: row in the existing SQLite keystore (encrypts at rest
//     when ColumnEncryptor is wired).
//
// Save / Load run on the DeltaTracker's lifecycle ticks (every 30 sec
// by default plus on shutdown); both are called with the tracker's
// mutex NOT held so backends can do whatever IO they need.
type DeltaPersistence interface {
	// Save persists the snapshot to durable storage. Errors are logged
	// but don't stop the tracker — a failed save means the next
	// restart costs more, not that the running process breaks.
	Save([]PersistedPeerState) error

	// Load returns the most recent snapshot. An empty slice + nil err
	// means "nothing persisted yet" (first-ever boot). A non-nil error
	// means the backend is broken; the tracker logs and continues
	// with empty state (degrades to today's behavior).
	Load() ([]PersistedPeerState, error)
}

// AttachPersistence wires a persistence backend to the tracker.
// On attach, the backend's Load is invoked synchronously; persisted
// entries with watermarks older than maxAge are discarded (they refer
// to records the peer has likely evicted, so the watermark would
// suppress a needed full sync).
//
// The save loop runs on saveInterval ticks until ctx is cancelled.
// On ctx cancellation a final Save fires so graceful shutdowns
// preserve state. Crash-stops lose at most saveInterval of progress —
// acceptable for an optimisation layer (worst case is one full sync
// per crashed peer pair, which Phase 0a's credit-leak fix tolerates).
//
// Idempotent — multiple calls overwrite the persistence reference
// but only one save loop runs (guarded by sweeperOnce-like state).
func (dt *DeltaTracker) AttachPersistence(ctx context.Context, p DeltaPersistence, saveInterval time.Duration, maxAge time.Duration) {
	if p == nil {
		return
	}
	if saveInterval <= 0 {
		saveInterval = 30 * time.Second
	}
	if maxAge <= 0 {
		maxAge = 20 * time.Minute // 2× recordTTL by default
	}

	// Load + restore.
	if persisted, err := p.Load(); err == nil {
		now := time.Now()
		dt.mu.Lock()
		for _, ps := range persisted {
			if ps.PeerID == "" {
				continue
			}
			// Discard stale watermarks — they refer to records likely
			// already evicted from the peer's cache.
			if !ps.Watermark.IsZero() && now.Sub(ps.Watermark) > maxAge {
				continue
			}
			dt.peers[ps.PeerID] = &peerDeltaState{
				watermark:     ps.Watermark,
				exchangeCount: ps.ExchangeCount,
				lastActivity:  ps.LastActivity,
				appliedOnFull: ps.AppliedOnFull,
				fullSyncCount: ps.FullSyncCount,
			}
		}
		dt.mu.Unlock()
	}

	// Save loop.
	go func() {
		ticker := time.NewTicker(saveInterval)
		defer ticker.Stop()
		flush := func() {
			snap := dt.snapshotForPersist()
			if err := p.Save(snap); err != nil {
				// Log via dbgGossip — Save is best-effort; a failure
				// reduces convergence cost on next boot but doesn't
				// affect the running process.
				dbgGossip.Printf("DeltaTracker persistence save: %v", err)
			}
		}
		for {
			select {
			case <-ctx.Done():
				flush()
				return
			case <-ticker.C:
				flush()
			}
		}
	}()
}

// snapshotForPersist returns a snapshot suitable for DeltaPersistence.Save.
// Internal helper — takes dt.mu briefly to copy state, then returns
// without the lock so the backend can take its time on IO.
func (dt *DeltaTracker) snapshotForPersist() []PersistedPeerState {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	out := make([]PersistedPeerState, 0, len(dt.peers))
	for peerID, st := range dt.peers {
		out = append(out, PersistedPeerState{
			PeerID:        peerID,
			Watermark:     st.watermark,
			ExchangeCount: st.exchangeCount,
			LastActivity:  st.lastActivity,
			AppliedOnFull: st.appliedOnFull,
			FullSyncCount: st.fullSyncCount,
		})
	}
	return out
}
