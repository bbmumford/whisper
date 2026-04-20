/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"sync"
	"time"
)

// DefaultBackpressureThreshold is the number of concurrent pending gossip
// exchanges before a node signals backpressure to its peers.
const DefaultBackpressureThreshold = 5

// DefaultBackpressureMaxBackoff is the maximum backoff duration a sender
// will apply when a peer repeatedly signals backpressure.
const DefaultBackpressureMaxBackoff = 120 * time.Second

// DefaultBackpressureInitialBackoff is the starting backoff when a peer
// first signals backpressure.
const DefaultBackpressureInitialBackoff = 30 * time.Second

// BackpressureMonitor tracks gossip processing load for a node.
// When the number of concurrent (pending) gossip exchanges exceeds the
// threshold, the node sets BackpressureSignal=true in its outbound
// ExchangeMeta, telling peers to slow down.
type BackpressureMonitor struct {
	mu               sync.Mutex
	pendingExchanges int
	maxPending       int // threshold to trigger backpressure
}

// NewBackpressureMonitor creates a monitor with the default threshold (5).
func NewBackpressureMonitor() *BackpressureMonitor {
	return &BackpressureMonitor{maxPending: DefaultBackpressureThreshold}
}

// NewBackpressureMonitorWithThreshold creates a monitor with a custom threshold.
func NewBackpressureMonitorWithThreshold(threshold int) *BackpressureMonitor {
	if threshold <= 0 {
		threshold = DefaultBackpressureThreshold
	}
	return &BackpressureMonitor{maxPending: threshold}
}

// Enter increments the pending exchange count. Returns true if backpressure
// should be signaled (pending count exceeds threshold).
func (bp *BackpressureMonitor) Enter() bool {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.pendingExchanges++
	return bp.pendingExchanges > bp.maxPending
}

// Exit decrements the pending exchange count.
func (bp *BackpressureMonitor) Exit() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.pendingExchanges > 0 {
		bp.pendingExchanges--
	}
}

// IsOverloaded returns true if the node should signal backpressure.
func (bp *BackpressureMonitor) IsOverloaded() bool {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.pendingExchanges > bp.maxPending
}

// Pending returns the current number of pending exchanges.
func (bp *BackpressureMonitor) Pending() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.pendingExchanges
}

// PeerBackoff tracks exponential backoff state for a single peer that has
// signaled backpressure. Used by the sender side of the gossip loop.
type PeerBackoff struct {
	mu           sync.Mutex
	backoff      time.Duration // current backoff duration
	backoffUntil time.Time     // don't gossip until this time
	maxBackoff   time.Duration
}

// NewPeerBackoff creates a new per-peer backoff tracker.
func NewPeerBackoff() *PeerBackoff {
	return &PeerBackoff{maxBackoff: DefaultBackpressureMaxBackoff}
}

// RecordSignal processes a backpressure signal from a peer exchange result.
// If the peer signaled backpressure, the backoff doubles (exponential).
// If the peer did NOT signal backpressure, the backoff resets to zero.
func (pb *PeerBackoff) RecordSignal(peerSignaledBackpressure bool) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if peerSignaledBackpressure {
		if pb.backoff == 0 {
			pb.backoff = DefaultBackpressureInitialBackoff
		} else {
			pb.backoff *= 2
		}
		if pb.backoff > pb.maxBackoff {
			pb.backoff = pb.maxBackoff
		}
		pb.backoffUntil = time.Now().Add(pb.backoff)
	} else {
		// Peer is healthy -- reset backoff.
		pb.backoff = 0
		pb.backoffUntil = time.Time{}
	}
}

// ShouldSkip returns true if the peer is currently in backoff and gossip
// should be skipped for this tick.
func (pb *PeerBackoff) ShouldSkip() bool {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.backoffUntil.IsZero() {
		return false
	}
	return time.Now().Before(pb.backoffUntil)
}

// CurrentBackoff returns the current backoff duration (0 if not backing off).
func (pb *PeerBackoff) CurrentBackoff() time.Duration {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	return pb.backoff
}
