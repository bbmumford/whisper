/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"math/rand/v2"
	"time"
)

// JitterFraction is the ±fraction applied to each computed gossip
// interval to desynchronise peers that came up together. Without this a
// fleet-restart produces synchronised gossip cycles every 10 s, which
// spikes cross-region egress at the same wall-clock boundary for every
// peer. ±15% spreads the spike over ~1.5 s without changing the
// effective rate.
//
// Applied in Next() after the base/idle/convergence arithmetic picks the
// unjittered value; also applied in ApplyBackpressure() so an overloaded
// fleet doesn't all back off in lockstep. Uses math/rand/v2 so the
// randomness is goroutine-safe without an explicit seed.
const JitterFraction = 0.15

// AdaptiveInterval adjusts gossip exchange timing based on exchange results.
// When records are applied (convergence), interval snaps to min (2s).
// When caches match (idle), interval backs off exponentially to max (60s).
// When records are sent but not applied (peer already had them), interval
// returns to base (10s).
//
// Stateless — reads only GossipResult fields, no direct state store dependency.
type AdaptiveInterval struct {
	base    time.Duration // default interval (e.g. 10s)
	min     time.Duration // fastest (e.g. 2s during convergence)
	max     time.Duration // slowest (e.g. 60s when idle)
	current time.Duration
	// idleExchanges is incremented each time the peer fingerprint matches
	// ours (both caches in sync) and reset whenever we see records applied
	// or records sent. It drives the exponential backoff — the "nothing
	// has changed on either side for N cycles" signal. Formerly named
	// `consecutive`; renamed for clarity since "consecutive what?" wasn't
	// obvious from the field name alone.
	idleExchanges int
	fingerprint   uint64 // last known cache fingerprint
}

// NewAdaptiveInterval creates an adaptive interval with the given bounds.
// base is the normal interval, min is the fastest (during convergence),
// max is the slowest (when idle).
func NewAdaptiveInterval(base, min, max time.Duration) *AdaptiveInterval {
	if base <= 0 {
		base = 10 * time.Second
	}
	if min <= 0 {
		min = 2 * time.Second
	}
	if max <= 0 {
		max = 60 * time.Second
	}
	if min > base {
		min = base
	}
	if max < base {
		max = base
	}
	return &AdaptiveInterval{
		base:    base,
		min:     min,
		max:     max,
		current: base,
	}
}

// Next returns the interval for the next exchange based on the last result.
// The returned interval includes ±JitterFraction randomisation so peers
// that came up together don't produce synchronised gossip bursts. The
// stored a.current is the UNJITTERED value; jitter is applied per-call
// so every call to Next() returns a fresh jittered sample around it.
func (a *AdaptiveInterval) Next(result GossipResult) time.Duration {
	if result.RecordsApplied > 0 {
		// Active convergence — accelerate to min
		a.idleExchanges = 0
		a.current = a.min
	} else if result.PeerMeta != nil && result.PeerMeta.CacheFingerprint != 0 &&
		result.PeerMeta.CacheFingerprint == a.fingerprint {
		// Caches match, nothing changed — back off exponentially
		a.idleExchanges++
		a.current = a.current * 2
		if a.current > a.max {
			a.current = a.max
		}
	} else {
		// Records sent but none applied (peer already had them) — moderate
		a.idleExchanges = 0
		a.current = a.base
	}

	if result.PeerMeta != nil && result.PeerMeta.CacheFingerprint != 0 {
		a.fingerprint = result.PeerMeta.CacheFingerprint
	}

	return jitter(a.current)
}

// jitter returns d scaled by a uniform random factor in
// [1-JitterFraction, 1+JitterFraction]. Separated out for testability —
// 100 000 calls should have mean ≈ d and stddev proportional to
// JitterFraction. Always returns ≥ 1 (avoids ticker panics on 0 or
// negative durations).
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	// rand.Float64 is in [0, 1); map to [-JitterFraction, +JitterFraction]
	scale := 1.0 + JitterFraction*(2*rand.Float64()-1)
	out := time.Duration(float64(d) * scale)
	if out < 1 {
		return 1
	}
	return out
}

// ApplyBackpressure grows the adaptive interval when global backpressure is
// active. Called by the engine between exchanges so over-subscribed nodes
// slow their G1 cycles until load clears. Returns the jittered sample
// around the backpressured base for the same reason Next() does: prevent
// an overloaded fleet from all backing off in lockstep.
func (a *AdaptiveInterval) ApplyBackpressure(overloaded bool) time.Duration {
	if !overloaded {
		return jitter(a.current)
	}
	next := a.current * 2
	if next < a.base*2 {
		next = a.base * 2
	}
	if next > a.max {
		next = a.max
	}
	a.current = next
	return jitter(a.current)
}

// Current returns the most recently computed interval.
func (a *AdaptiveInterval) Current() time.Duration {
	return a.current
}

// AdaptiveStats is a point-in-time snapshot of adaptive-interval state.
type AdaptiveStats struct {
	Current       time.Duration // most recently computed interval
	Base          time.Duration
	Min           time.Duration
	Max           time.Duration
	IdleExchanges int    // consecutive exchanges with matching fingerprint
	Fingerprint   uint64 // last peer-cache fingerprint seen
}

// Stats returns a snapshot for metrics / diagnostics.
func (a *AdaptiveInterval) Stats() AdaptiveStats {
	return AdaptiveStats{
		Current:       a.current,
		Base:          a.base,
		Min:           a.min,
		Max:           a.max,
		IdleExchanges: a.idleExchanges,
		Fingerprint:   a.fingerprint,
	}
}
