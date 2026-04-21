/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"time"
)

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

	return a.current
}

// ApplyBackpressure grows the adaptive interval when global backpressure is
// active. Called by the engine between exchanges so over-subscribed nodes
// slow their G1 cycles until load clears.
func (a *AdaptiveInterval) ApplyBackpressure(overloaded bool) time.Duration {
	if !overloaded {
		return a.current
	}
	next := a.current * 2
	if next < a.base*2 {
		next = a.base * 2
	}
	if next > a.max {
		next = a.max
	}
	a.current = next
	return a.current
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
