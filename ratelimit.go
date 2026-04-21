/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"errors"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ErrBackpressure is returned by Publish when the engine is overloaded and
// cannot accept more outbound messages. Callers decide whether to retry,
// drop, or bubble up to their caller.
var ErrBackpressure = errors.New("whisper: publish rejected — engine overloaded")

// ErrPeerThrottled is returned (internally, via dropped sends) when a single
// peer has exceeded its rate limit. The publish for that peer is skipped but
// other peers continue to receive the message.
var ErrPeerThrottled = errors.New("whisper: peer rate limit exceeded")

// Default rate-limiting parameters. All are overridable via engine options.
const (
	// DefaultPeerMessagesPerSec is the steady-state per-peer message allowance.
	DefaultPeerMessagesPerSec = 100.0

	// DefaultPeerBurst is the per-peer burst capacity.
	DefaultPeerBurst = 500

	// DefaultGlobalEgressBytesPerSec is the total outbound throughput cap.
	DefaultGlobalEgressBytesPerSec = 10 * 1024 * 1024 // 10 MB/s

	// DefaultGlobalEgressBurst is the burst capacity for the egress limiter.
	DefaultGlobalEgressBurst = 10 * 1024 * 1024 // 10 MB burst

	// DefaultOverloadQueueThreshold is the outbound queue size above which
	// Publish returns ErrBackpressure. 0 disables the check.
	DefaultOverloadQueueThreshold = 1024

	// violationWindow is the window within which per-peer violations are
	// counted toward the cooldown trigger.
	violationWindow = 5 * time.Minute

	// violationThreshold is the number of violations in the window that
	// triggers a temporary cooldown for a peer.
	violationThreshold = 1000

	// cooldownDuration is how long a peer is skipped after sustained violations.
	cooldownDuration = 5 * time.Minute
)

// PeerRateLimiter tracks token-bucket state plus violation accounting for a
// single peer. When violations exceed the threshold in the window, the peer
// is placed in cooldown and skipped by the fanout.
type PeerRateLimiter struct {
	mu           sync.Mutex
	bucket       *rate.Limiter
	violations   int
	windowStart  time.Time
	cooldownEnds time.Time
}

// newPeerRateLimiter constructs a limiter with the given per-second rate
// and burst.
func newPeerRateLimiter(per float64, burst int) *PeerRateLimiter {
	if per <= 0 {
		per = DefaultPeerMessagesPerSec
	}
	if burst <= 0 {
		burst = DefaultPeerBurst
	}
	return &PeerRateLimiter{
		bucket:      rate.NewLimiter(rate.Limit(per), burst),
		windowStart: time.Now(),
	}
}

// Allow returns true if a single message is allowed for this peer right now.
// A false return indicates a violation (either throttled or in cooldown) and
// increments the violation counter. When the counter crosses the threshold
// within violationWindow, the peer is put in cooldown.
func (p *PeerRateLimiter) Allow() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()

	// Cooldown active?
	if !p.cooldownEnds.IsZero() && now.Before(p.cooldownEnds) {
		return false
	}
	// Cooldown expired — clear state.
	if !p.cooldownEnds.IsZero() && !now.Before(p.cooldownEnds) {
		p.cooldownEnds = time.Time{}
		p.violations = 0
		p.windowStart = now
	}

	// Roll the violation window.
	if now.Sub(p.windowStart) > violationWindow {
		p.windowStart = now
		p.violations = 0
	}

	if p.bucket.AllowN(now, 1) {
		return true
	}

	p.violations++
	if p.violations >= violationThreshold {
		p.cooldownEnds = now.Add(cooldownDuration)
	}
	return false
}

// InCooldown reports whether the peer is currently cooling down.
func (p *PeerRateLimiter) InCooldown() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.cooldownEnds.IsZero() && time.Now().Before(p.cooldownEnds)
}

// Violations returns the current violation count in the rolling window.
func (p *PeerRateLimiter) Violations() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.violations
}

// RateLimiter is the engine-level coordinator: per-peer buckets plus a
// global egress (bytes/sec) limiter plus overload queue tracking.
type RateLimiter struct {
	mu    sync.RWMutex
	peers map[string]*PeerRateLimiter

	// per-peer parameters (applied on first Allow for each peer).
	peerPerSec float64
	peerBurst  int

	// global egress bytes/sec limiter.
	egress *rate.Limiter

	// outbound queue depth counter (bumped and decremented by the engine to
	// signal overload through Publish).
	queueDepth     int
	queueThreshold int
}

// NewRateLimiter builds an engine rate limiter using the provided settings.
// Zero values fall back to package defaults.
func NewRateLimiter(peerPerSec float64, peerBurst int, egressBytesPerSec int, queueThreshold int) *RateLimiter {
	if peerPerSec <= 0 {
		peerPerSec = DefaultPeerMessagesPerSec
	}
	if peerBurst <= 0 {
		peerBurst = DefaultPeerBurst
	}
	if egressBytesPerSec <= 0 {
		egressBytesPerSec = DefaultGlobalEgressBytesPerSec
	}
	if queueThreshold <= 0 {
		queueThreshold = DefaultOverloadQueueThreshold
	}
	return &RateLimiter{
		peers:          make(map[string]*PeerRateLimiter),
		peerPerSec:     peerPerSec,
		peerBurst:      peerBurst,
		egress:         rate.NewLimiter(rate.Limit(egressBytesPerSec), DefaultGlobalEgressBurst),
		queueThreshold: queueThreshold,
	}
}

// AllowPeer returns true if a publish to peerID is currently allowed. A false
// return means the message should be dropped for that peer (other peers still
// receive it). Unknown peers are lazily registered.
func (r *RateLimiter) AllowPeer(peerID string) bool {
	if r == nil {
		return true
	}
	r.mu.RLock()
	p, ok := r.peers[peerID]
	r.mu.RUnlock()
	if !ok {
		r.mu.Lock()
		if p, ok = r.peers[peerID]; !ok {
			p = newPeerRateLimiter(r.peerPerSec, r.peerBurst)
			r.peers[peerID] = p
		}
		r.mu.Unlock()
	}
	return p.Allow()
}

// RemovePeer drops per-peer rate-limit state. Called when a peer disconnects.
func (r *RateLimiter) RemovePeer(peerID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.peers, peerID)
	r.mu.Unlock()
}

// PeerInCooldown reports whether the peer is currently cooling down after
// sustained violations.
func (r *RateLimiter) PeerInCooldown(peerID string) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	p, ok := r.peers[peerID]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	return p.InCooldown()
}

// ReserveEgress attempts to consume n bytes from the global egress bucket.
// Returns true if allowed immediately; false means egress is saturated and
// the caller should defer or drop (priority-aware callers pick by priority).
func (r *RateLimiter) ReserveEgress(n int) bool {
	if r == nil || n <= 0 {
		return true
	}
	return r.egress.AllowN(time.Now(), n)
}

// EgressSaturated returns true if the global egress bucket has no tokens
// available for the given payload size. Used by the engine to decide whether
// to hold low-priority traffic in favor of high-priority topics.
func (r *RateLimiter) EgressSaturated(n int) bool {
	if r == nil || n <= 0 {
		return false
	}
	// Inspect without consuming: peek via Reserve and cancel if too expensive.
	res := r.egress.ReserveN(time.Now(), n)
	if !res.OK() {
		return true
	}
	delay := res.Delay()
	res.Cancel()
	return delay > 0
}

// IncQueue increments the outbound queue counter. Used by the engine to
// signal that a publish is in flight.
func (r *RateLimiter) IncQueue() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.queueDepth++
	r.mu.Unlock()
}

// DecQueue decrements the outbound queue counter.
func (r *RateLimiter) DecQueue() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.queueDepth > 0 {
		r.queueDepth--
	}
	r.mu.Unlock()
}

// Overloaded returns true if the outbound queue has exceeded the threshold.
// When Overloaded is true, Publish returns ErrBackpressure.
func (r *RateLimiter) Overloaded() bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.queueDepth >= r.queueThreshold
}

// QueueDepth returns the current outbound queue depth (test observability).
func (r *RateLimiter) QueueDepth() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.queueDepth
}

// QueueThreshold returns the current overload threshold. Useful for
// dashboards showing headroom (depth vs threshold) and for operator
// validation after startup.
func (r *RateLimiter) QueueThreshold() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.queueThreshold
}

// SetQueueThreshold adjusts the overload threshold at runtime. Values ≤ 0
// are ignored (keeps previous value). Intended for operator tuning via a
// diagnostics endpoint — e.g. temporarily raise the threshold during a
// known convergence storm without bouncing the process.
func (r *RateLimiter) SetQueueThreshold(n int) {
	if r == nil || n <= 0 {
		return
	}
	r.mu.Lock()
	r.queueThreshold = n
	r.mu.Unlock()
}

// PeerCount returns the number of peers currently tracked.
func (r *RateLimiter) PeerCount() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.peers)
}
