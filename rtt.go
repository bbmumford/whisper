/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"sync"
	"time"
)

// NetworkRTTMeasurer tracks per-connection network RTT using header exchange timing.
// During a gossip exchange, both sides simultaneously write their length-prefixed
// header (4 bytes). The time from our write to receiving the peer's header
// approximates one network round-trip.
//
// Samples live in a fixed-size ring buffer indexed by idx%len(ring). No
// slice copy on roll-over, and the backing array never grows — matches
// cpu_gate.go's rolling-window pattern.
type NetworkRTTMeasurer struct {
	mu     sync.Mutex
	ring   []time.Duration
	idx    int  // next write position; (idx - 1) mod len = most-recent
	filled bool // true once the ring has wrapped once
}

// NewNetworkRTTMeasurer creates a measurer that keeps the last N samples.
func NewNetworkRTTMeasurer(maxSamples int) *NetworkRTTMeasurer {
	if maxSamples <= 0 {
		maxSamples = 10
	}
	return &NetworkRTTMeasurer{
		ring: make([]time.Duration, maxSamples),
	}
}

// Record adds a new RTT sample.
func (m *NetworkRTTMeasurer) Record(rtt time.Duration) {
	if rtt <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ring[m.idx%len(m.ring)] = rtt
	m.idx++
	if m.idx >= len(m.ring) {
		m.filled = true
	}
}

// liveCountLocked returns the number of populated ring slots. Caller must hold mu.
func (m *NetworkRTTMeasurer) liveCountLocked() int {
	if m.filled {
		return len(m.ring)
	}
	return m.idx
}

// AvgRTT returns the average of recent RTT samples. Returns 0 if no samples.
func (m *NetworkRTTMeasurer) AvgRTT() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.liveCountLocked()
	if n == 0 {
		return 0
	}
	var total time.Duration
	for i := 0; i < n; i++ {
		total += m.ring[i]
	}
	return total / time.Duration(n)
}

// LastRTT returns the most recent RTT sample. Returns 0 if no samples.
func (m *NetworkRTTMeasurer) LastRTT() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.idx == 0 {
		return 0
	}
	// (idx - 1) mod len is the most-recent slot.
	prev := (m.idx - 1) % len(m.ring)
	if prev < 0 {
		prev += len(m.ring)
	}
	return m.ring[prev]
}

// SampleCount returns how many RTT samples are currently held in the window.
// Caps at the ring capacity once the ring has wrapped.
func (m *NetworkRTTMeasurer) SampleCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.liveCountLocked()
}

