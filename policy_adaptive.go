/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"fmt"
	"sync"
	"time"
)

// AdaptivePolicy is a NetworkPolicy that synthesises a profile from
// multiple input signals — LinkType, RTT trends, loss rate, battery
// state, time-of-day. Used by agents on roaming devices where a
// fixed-profile policy would be wrong (constant cellular profile
// even on wifi, or vice versa).
//
// Profile transitions are smooth gradients for bandwidth values
// (30-sec ramp on direction changes) and immediate for cadence
// values (next gossip in 10 min vs 30 min has no meaningful
// gradient). Battery-low and predictive transitions short-circuit
// directly to conservative profiles regardless of LinkType.
type AdaptivePolicy struct {
	mu       sync.RWMutex
	signals  NetworkSignals
	current  NetworkProfile
	target   NetworkProfile
	transitionStart time.Time
	transitionDur   time.Duration

	// History trackers for predictive transitions.
	rttHistory  *rttRing
	lossHistory *rttRing // reuses ring for float64 EMA-like trend tracking

	// Hysteresis state.
	linkSettledSince time.Time
	pendingProfile   NetworkProfile
	pendingValid     bool      // true while pendingProfile carries a real candidate
	pendingSince     time.Time // when pendingProfile was first proposed (NOT when last updated)

	// Per-peer overrides.
	overrides map[string]NetworkProfile

	// Subscribers for profile-change callbacks.
	subs []func(NetworkProfile)

	// Event emitter — when wired, transitions emit
	// EventNetworkPolicyTransition with the new profile signature in
	// the Body so subscribers (mesh-debug, OTLP) can correlate
	// behavioral changes with link/battery shifts.
	emitFn func(GossipEvent)
}

// NewAdaptivePolicy returns an AdaptivePolicy seeded with the
// default profile. The synthesizer recomputes whenever UpdateSignals
// is called; consumers wire UpdateSignals to platform monitors
// (NetworkMonitor.onChanged, battery API, RTT histogram).
func NewAdaptivePolicy() *AdaptivePolicy {
	def := DefaultProfile()
	return &AdaptivePolicy{
		current:        def,
		target:         def,
		transitionDur:  30 * time.Second,
		rttHistory:     newRTTRing(8),
		lossHistory:    newRTTRing(8),
		overrides:      map[string]NetworkProfile{},
		linkSettledSince: time.Now(),
	}
}

// UpdateSignals re-runs the synthesizer with new signals. Triggers
// a profile transition if any change crosses a hysteresis threshold.
// Safe for concurrent use; subscribers are called outside the lock.
func (p *AdaptivePolicy) UpdateSignals(s NetworkSignals) {
	p.mu.Lock()

	prev := p.signals
	p.signals = s

	// Track RTT history for trend analysis.
	if s.AvgRTT > 0 {
		p.rttHistory.push(float64(s.AvgRTT.Milliseconds()))
	}
	if s.LossRate > 0 {
		p.lossHistory.push(s.LossRate)
	}

	// Reset hysteresis dwell when LinkType changes.
	if prev.LinkType != s.LinkType {
		p.linkSettledSince = time.Now()
		// Discard any pending more-restricted profile — the link
		// flipped before the dwell window completed, so the
		// previous candidate is stale.
		p.pendingValid = false
	}

	target := p.synthesize(s)

	// Hysteresis on transitions to a more-restricted profile —
	// require 60-sec dwell on the new link before flipping. Not
	// applied to "more-restricted-due-to-battery" or "predictive"
	// transitions, those are safety-relevant and fire immediately.
	moreRestricted := isMoreRestricted(target, p.current)
	predictive := p.predictiveTrigger(s)
	batteryOverride := s.BatteryLevel > 0 && s.BatteryLevel < 0.2 && s.BatteryDraining

	if moreRestricted && !predictive && !batteryOverride {
		const dwell = 60 * time.Second
		// Stage the target. If we already have a valid pending
		// candidate that matches this synthesized one, preserve
		// pendingSince so the dwell timer keeps counting from the
		// FIRST observation. If the candidate has changed, reset.
		if p.pendingValid && profilesEqual(p.pendingProfile, target) {
			// Same candidate — preserve pendingSince.
			if time.Since(p.pendingSince) < dwell {
				p.mu.Unlock()
				return
			}
			// Dwell elapsed — fall through to commit.
		} else {
			// New candidate — start fresh.
			p.pendingProfile = target
			p.pendingSince = time.Now()
			p.pendingValid = true
			if time.Since(p.linkSettledSince) < dwell {
				p.mu.Unlock()
				return
			}
		}
	} else {
		// Less-restricted, predictive, or battery transition —
		// any pending more-restricted candidate is no longer
		// relevant; clear it.
		p.pendingValid = false
	}

	// Commit transition.
	p.startTransition(target)
	subs := make([]func(NetworkProfile), len(p.subs))
	copy(subs, p.subs)
	emitFn := p.emitFn
	p.mu.Unlock()

	for _, fn := range subs {
		fn(target)
	}
	if emitFn != nil {
		// Pack the new profile's most-distinctive cadence values
		// into the event's existing fields. Topic carries a
		// human-readable signature; Records/DurationMs/Bytes carry
		// the structured cadence so subscribers can compare across
		// transitions without parsing strings.
		emitFn(GossipEvent{
			Kind:       EventNetworkPolicyTransition,
			Time:       time.Now(),
			Topic:      profileSignatureString(target),
			Records:    target.RumorFanout,
			DurationMs: target.GossipMax.Milliseconds(),
			Bytes:      int64(target.SnapshotChunkMax),
		})
	}
}

// SetEventEmitter wires a callback that fires on every committed
// profile transition. Pass nil to disable. Used by consumers to
// surface link/battery-driven behavior changes in mesh-debug, OTLP,
// or operator dashboards without polling Profile() on a clock.
func (p *AdaptivePolicy) SetEventEmitter(fn func(GossipEvent)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.emitFn = fn
}

// profileSignatureString returns a compact human-readable signature
// of the profile for the EventNetworkPolicyTransition Topic. Stable
// across transitions of the same profile so subscribers can de-dup
// or render a "kind: cellular(min=5s,fanout=2)" label.
func profileSignatureString(pr NetworkProfile) string {
	return fmt.Sprintf("min=%s/max=%s/retry=%s/fanout=%d/shards=%d/chunk=%d/pacing=%d/dims=%d",
		pr.GossipMin, pr.GossipMax, pr.RumorRetryInitial,
		pr.RumorFanout, pr.SnapshotShardCount, pr.SnapshotChunkMax,
		pr.ReconcilePacing, pr.HypercubeMaxDims)
}

// profilesEqual returns true when two profiles match on every field
// the synthesizer drives. Used by the hysteresis logic to decide if
// a new pending candidate is the same as the existing one.
func profilesEqual(a, b NetworkProfile) bool {
	return a.GossipMin == b.GossipMin &&
		a.GossipBase == b.GossipBase &&
		a.GossipMax == b.GossipMax &&
		a.RumorRetryInitial == b.RumorRetryInitial &&
		a.RumorRetryMax == b.RumorRetryMax &&
		a.RumorFanout == b.RumorFanout &&
		a.FreshnessProbeMin == b.FreshnessProbeMin &&
		a.FreshnessProbeMax == b.FreshnessProbeMax &&
		a.SnapshotShardCount == b.SnapshotShardCount &&
		a.SnapshotChunkMax == b.SnapshotChunkMax &&
		a.ReconcilePacing == b.ReconcilePacing &&
		a.HypercubeMaxDims == b.HypercubeMaxDims
}

// startTransition initiates a profile transition. Caller holds
// p.mu. Bandwidth-related fields ramp over transitionDur; cadence
// fields jump immediately.
func (p *AdaptivePolicy) startTransition(target NetworkProfile) {
	p.target = target
	p.transitionStart = time.Now()
	// Cadence fields apply immediately — meaningless to gradient
	// "next gossip in 30 min vs 10 min".
	p.current.GossipMin = target.GossipMin
	p.current.GossipBase = target.GossipBase
	p.current.GossipMax = target.GossipMax
	p.current.FreshnessProbeMin = target.FreshnessProbeMin
	p.current.FreshnessProbeMax = target.FreshnessProbeMax
	p.current.RumorRetryInitial = target.RumorRetryInitial
	p.current.RumorRetryMax = target.RumorRetryMax
	p.current.RumorFanout = target.RumorFanout
	p.current.SnapshotShardCount = target.SnapshotShardCount
	p.current.HypercubeMaxDims = target.HypercubeMaxDims
	// Bandwidth/pacing fields — keep at current; Profile() ramps
	// them on read.
}

// Profile returns the synthesised profile. Bandwidth/pacing fields
// are ramped between current and target on transition.
func (p *AdaptivePolicy) Profile() NetworkProfile {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := p.current
	if p.transitionDur > 0 && !p.transitionStart.IsZero() {
		elapsed := time.Since(p.transitionStart)
		if elapsed < p.transitionDur {
			// Linear ramp on bandwidth/pacing.
			alpha := float64(elapsed) / float64(p.transitionDur)
			out.ReconcilePacing = lerpInt(p.current.ReconcilePacing, p.target.ReconcilePacing, alpha)
			out.SnapshotChunkMax = lerpInt(p.current.SnapshotChunkMax, p.target.SnapshotChunkMax, alpha)
		} else {
			out.ReconcilePacing = p.target.ReconcilePacing
			out.SnapshotChunkMax = p.target.SnapshotChunkMax
		}
	}
	return out
}

// Signals returns the most recent signal vector.
func (p *AdaptivePolicy) Signals() NetworkSignals {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.signals
}

// PeerOverride returns a per-peer override or false.
func (p *AdaptivePolicy) PeerOverride(peer string) (NetworkProfile, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pr, ok := p.overrides[peer]
	return pr, ok
}

// SetPeerOverride installs a per-peer override.
func (p *AdaptivePolicy) SetPeerOverride(peer string, profile NetworkProfile) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer == "" {
		p.overrides = map[string]NetworkProfile{}
		return
	}
	p.overrides[peer] = profile
}

// Subscribe registers a callback fired on every profile change.
func (p *AdaptivePolicy) Subscribe(fn func(NetworkProfile)) {
	if fn == nil {
		return
	}
	p.mu.Lock()
	p.subs = append(p.subs, fn)
	p.mu.Unlock()
}

// synthesize is the policy synthesizer. Maps the signal vector
// to a NetworkProfile via the priority-ordered rules:
//
//  1. Battery low + draining → conservative profile regardless
//     of LinkType.
//  2. Predictive: RTT trending up + loss trending up + battery
//     draining → conservative one step early.
//  3. LinkType-driven: cellular → CellularProfile; wifi/ethernet/
//     vpn → DefaultProfile.
//  4. RTT-driven: even on wifi, RTT > 200 ms widens GossipMin to
//     10 sec.
func (p *AdaptivePolicy) synthesize(s NetworkSignals) NetworkProfile {
	if s.BatteryLevel > 0 && s.BatteryLevel < 0.2 && s.BatteryDraining {
		return CellularProfile() // conservative — battery rescue
	}
	if p.predictiveTrigger(s) {
		return CellularProfile()
	}
	switch s.LinkType {
	case 3: // cellular
		return CellularProfile()
	default: // ethernet, wifi, vpn, unknown
		base := DefaultProfile()
		if s.AvgRTT > 200*time.Millisecond {
			// High latency on wifi — widen the convergence floor.
			base.GossipMin = 10 * time.Second
			base.RumorRetryInitial = 500 * time.Millisecond
		}
		return base
	}
}

// predictiveTrigger checks for the "anticipated handoff to
// constrained network" pattern: RTT trending up + loss trending up
// + battery draining. Fires the conservative profile before the
// LinkType change actually happens, so we don't pay the cost of
// the transition's worst moment.
func (p *AdaptivePolicy) predictiveTrigger(s NetworkSignals) bool {
	if !s.BatteryDraining {
		return false
	}
	rttTrend := p.rttHistory.trend()
	lossTrend := p.lossHistory.trend()
	return rttTrend > 0.20 && lossTrend > 0.05 // 20% RTT up + 5% loss up
}

// lerpInt linearly interpolates between two ints. alpha ∈ [0, 1].
func lerpInt(from, to int, alpha float64) int {
	return from + int(float64(to-from)*alpha)
}

// rttRing is a small ring buffer for trend computation. push appends
// a value; trend returns the relative change (newest avg - oldest
// avg) / oldest avg.
type rttRing struct {
	mu    sync.Mutex
	vals  []float64
	idx   int
	cap   int
	count int
}

func newRTTRing(cap int) *rttRing {
	return &rttRing{vals: make([]float64, cap), cap: cap}
}

func (r *rttRing) push(v float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.vals[r.idx] = v
	r.idx = (r.idx + 1) % r.cap
	if r.count < r.cap {
		r.count++
	}
}

// trend returns the relative change between the first half and
// second half of the ring. Returns 0 if not enough samples.
func (r *rttRing) trend() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count < 4 {
		return 0
	}
	// Reconstruct chronological order.
	ordered := make([]float64, r.count)
	for i := 0; i < r.count; i++ {
		j := (r.idx - r.count + i + r.cap) % r.cap
		ordered[i] = r.vals[j]
	}
	half := r.count / 2
	var first, second float64
	for i := 0; i < half; i++ {
		first += ordered[i]
	}
	for i := half; i < r.count; i++ {
		second += ordered[i]
	}
	first /= float64(half)
	second /= float64(r.count - half)
	if first == 0 {
		return 0
	}
	return (second - first) / first
}

// isMoreRestricted returns true when target is more constrained
// than current — used for hysteresis-on-transition-down.
func isMoreRestricted(target, current NetworkProfile) bool {
	// Heuristic: a profile is "more restricted" when it has a
	// non-zero ReconcilePacing where the previous didn't, or when
	// GossipMax has widened significantly.
	if target.ReconcilePacing > 0 && current.ReconcilePacing == 0 {
		return true
	}
	if target.GossipMax > current.GossipMax*2 {
		return true
	}
	return false
}

// Compile-time interface check.
var _ NetworkPolicy = (*AdaptivePolicy)(nil)
