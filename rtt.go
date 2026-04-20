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
type NetworkRTTMeasurer struct {
	mu         sync.Mutex
	samples    []time.Duration
	maxSamples int
}

// NewNetworkRTTMeasurer creates a measurer that keeps the last N samples.
func NewNetworkRTTMeasurer(maxSamples int) *NetworkRTTMeasurer {
	if maxSamples <= 0 {
		maxSamples = 10
	}
	return &NetworkRTTMeasurer{
		maxSamples: maxSamples,
	}
}

// Record adds a new RTT sample.
func (m *NetworkRTTMeasurer) Record(rtt time.Duration) {
	if rtt <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.samples = append(m.samples, rtt)
	if len(m.samples) > m.maxSamples {
		m.samples = m.samples[1:]
	}
}

// AvgRTT returns the average of recent RTT samples. Returns 0 if no samples.
func (m *NetworkRTTMeasurer) AvgRTT() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.samples) == 0 {
		return 0
	}
	var total time.Duration
	for _, s := range m.samples {
		total += s
	}
	return total / time.Duration(len(m.samples))
}

// LastRTT returns the most recent RTT sample. Returns 0 if no samples.
func (m *NetworkRTTMeasurer) LastRTT() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.samples) == 0 {
		return 0
	}
	return m.samples[len(m.samples)-1]
}

// SampleCount returns how many RTT samples have been recorded.
func (m *NetworkRTTMeasurer) SampleCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.samples)
}

