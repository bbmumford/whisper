/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"sync"
	"time"
)

// NetworkSignals is the multi-signal input vector that drives a
// NetworkProfile. Consumers update signals asynchronously; the policy
// synthesizer reads them whenever a profile is requested.
//
// Not every signal is meaningful for every consumer:
//   - Library mesh nodes on fly run on stable wired links, so
//     LinkType is always "ethernet-equivalent" and battery fields
//     are zero / N/A. The fixed-profile policy ignores most signals.
//   - Agents on roaming devices populate every field — LinkType
//     transitions, RTT histograms, loss-rate, battery state all
//     drive profile transitions.
type NetworkSignals struct {
	LinkType        int           // 0=unknown, 1=ethernet, 2=wifi, 3=cellular, 4=vpn
	AvgRTT          time.Duration // exponential moving average of observed RTT
	LossRate        float64       // recent rumor-ACK miss rate, 0.0-1.0
	EventBurstRate  float64       // events/min on the address-change bus
	BatteryLevel    float64       // 0.0-1.0; -1.0 = AC power / unknown
	BatteryDraining bool          // explicit signal independent of level
	TimeOfDay       int           // 0-23 hour, local; 0 = unknown
	LinkStableSince time.Duration // how long current LinkType has held
	PeerCount       int           // active sessions
}

// NetworkProfile is the synthesised configuration consumers read for
// every cadence / pacing / sizing decision. All durations and byte
// counts are absolute values — consumers don't need to know which
// signals produced them.
//
// Defaults (zero values) are chosen so a consumer that never updates
// the policy still gets sensible behavior (matches the existing
// AdaptiveHWPInterval defaults from 2026-04-09 Task 10a + the
// 2026-04-23 freshness 30-sec emit + RumorPusher defaults).
type NetworkProfile struct {
	GossipMin           time.Duration // 2s — convergence floor
	GossipBase          time.Duration // 10s — base interval
	GossipMax           time.Duration // 60s — idle ceiling (wifi default)
	FreshnessProbeMin   time.Duration // 30s — between probes per peer
	FreshnessProbeMax   time.Duration // 5min — coalesce repeated stale
	RumorRetryInitial   time.Duration // 200ms — first retry delay
	RumorRetryMax       time.Duration // 5s — final retry cap
	RumorFanout         int           // 1 — hypercube dimensions
	ReconcilePacing     int           // 0 = unlimited; cellular: 8 KB/s
	SnapshotChunkMax    int           // 65536 — bytes per snapshot chunk
	SnapshotShardCount  int           // 2 — k-of-N for cold-start
	HypercubeMaxDims    int           // 0 = unlimited
}

// DefaultProfile returns a NetworkProfile suitable for stable wired
// connections (fly nodes, ethernet agents). Consumers without a
// signal source can use this directly.
func DefaultProfile() NetworkProfile {
	return NetworkProfile{
		GossipMin:          2 * time.Second,
		GossipBase:         10 * time.Second,
		GossipMax:          60 * time.Second,
		FreshnessProbeMin:  30 * time.Second,
		FreshnessProbeMax:  5 * time.Minute,
		RumorRetryInitial:  200 * time.Millisecond,
		RumorRetryMax:      5 * time.Second,
		RumorFanout:        1,
		ReconcilePacing:    0,
		SnapshotChunkMax:   64 * 1024,
		SnapshotShardCount: 2,
		HypercubeMaxDims:   0,
	}
}

// CellularProfile returns a NetworkProfile suitable for metered
// cellular links — wider intervals, paced reconciliation, smaller
// snapshot chunks. Used by the multi-signal synthesizer when
// LinkType=cellular and one of the secondary signals (battery low,
// RTT high) confirms the transition.
func CellularProfile() NetworkProfile {
	return NetworkProfile{
		GossipMin:          15 * time.Second,
		GossipBase:         60 * time.Second,
		GossipMax:          30 * time.Minute,
		FreshnessProbeMin:  60 * time.Second,
		FreshnessProbeMax:  10 * time.Minute,
		RumorRetryInitial:  1 * time.Second,
		RumorRetryMax:      60 * time.Second,
		RumorFanout:        1,
		ReconcilePacing:    8 * 1024, // 8 KB/s
		SnapshotChunkMax:   8 * 1024,
		SnapshotShardCount: 1, // serial transfer; parallel costs more on cellular
		HypercubeMaxDims:   3, // reduced cube footprint
	}
}

// NetworkPolicy is the consumer-facing surface every cadence/pacing
// decision reads. Implementations may be static (library mesh-node)
// or signal-driven (agent). Subscribe is non-blocking — slow
// subscribers miss profile transitions, which is fine because every
// caller re-reads Profile() before each interval/pacing decision.
type NetworkPolicy interface {
	Profile() NetworkProfile
	Signals() NetworkSignals
	PeerOverride(peer string) (NetworkProfile, bool)
	Subscribe(func(NetworkProfile))
}

// FixedProfilePolicy is a NetworkPolicy that always returns the same
// profile. Used by library mesh nodes on stable wired links and by
// any consumer that wants explicit control over cadence without
// signal-driven adaptation. Thread-safe.
type FixedProfilePolicy struct {
	mu        sync.RWMutex
	profile   NetworkProfile
	overrides map[string]NetworkProfile
	subs      []func(NetworkProfile)
}

// NewFixedProfilePolicy returns a policy backed by the given profile.
// Pass DefaultProfile() for the standard wired-network behavior.
func NewFixedProfilePolicy(p NetworkProfile) *FixedProfilePolicy {
	return &FixedProfilePolicy{profile: p, overrides: map[string]NetworkProfile{}}
}

// Profile returns the current default profile.
func (p *FixedProfilePolicy) Profile() NetworkProfile {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.profile
}

// Signals returns an empty signal set — fixed-profile policies don't
// observe network conditions.
func (p *FixedProfilePolicy) Signals() NetworkSignals {
	return NetworkSignals{}
}

// PeerOverride returns the override for a specific peer, or false if
// none is configured. Allows individual peers to deviate from the
// default profile (e.g. tighter retry timings for a known-flaky
// peer).
func (p *FixedProfilePolicy) PeerOverride(peer string) (NetworkProfile, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pr, ok := p.overrides[peer]
	return pr, ok
}

// SetPeerOverride installs a per-peer profile override. Empty peer
// string clears all overrides.
func (p *FixedProfilePolicy) SetPeerOverride(peer string, profile NetworkProfile) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer == "" {
		p.overrides = map[string]NetworkProfile{}
		return
	}
	p.overrides[peer] = profile
}

// SetProfile updates the default profile and notifies subscribers.
// Used by signal-driven adapters layered on top of a FixedProfilePolicy
// (the agent's adapter to wire.NetworkMonitor).
func (p *FixedProfilePolicy) SetProfile(profile NetworkProfile) {
	p.mu.Lock()
	p.profile = profile
	subs := make([]func(NetworkProfile), len(p.subs))
	copy(subs, p.subs)
	p.mu.Unlock()
	// Fire callbacks outside the lock so a slow subscriber can't
	// block other Subscribe / SetProfile calls.
	for _, fn := range subs {
		fn(profile)
	}
}

// Subscribe registers a callback fired on every profile change.
// Callbacks run synchronously in SetProfile's caller goroutine —
// slow callbacks delay subsequent profile transitions.
func (p *FixedProfilePolicy) Subscribe(fn func(NetworkProfile)) {
	if fn == nil {
		return
	}
	p.mu.Lock()
	p.subs = append(p.subs, fn)
	p.mu.Unlock()
}

// Compile-time interface check.
var _ NetworkPolicy = (*FixedProfilePolicy)(nil)
