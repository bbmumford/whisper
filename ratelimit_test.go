/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// TestPeerRateLimiterAllowsWithinBudget verifies that a single peer can
// consume up to its burst capacity without being throttled.
func TestPeerRateLimiterAllowsWithinBudget(t *testing.T) {
	p := newPeerRateLimiter(100, 10)
	allowed := 0
	for i := 0; i < 10; i++ {
		if p.Allow() {
			allowed++
		}
	}
	if allowed != 10 {
		t.Fatalf("expected 10 allowed within burst, got %d", allowed)
	}
}

// TestPeerRateLimiterThrottlesExcess verifies that a single peer is throttled
// when it exceeds its burst + steady rate.
func TestPeerRateLimiterThrottlesExcess(t *testing.T) {
	p := newPeerRateLimiter(10, 5) // 10/s, burst 5
	allowed, denied := 0, 0
	for i := 0; i < 50; i++ {
		if p.Allow() {
			allowed++
		} else {
			denied++
		}
	}
	if denied == 0 {
		t.Fatalf("expected some denied messages, got %d allowed / %d denied", allowed, denied)
	}
	if allowed > 10 { // burst + a tiny amount of refill in test runtime
		t.Fatalf("expected <=10 allowed in a tight loop, got %d", allowed)
	}
}

// TestPerPeerIsolation verifies that throttling one peer does not affect
// other peers' rate budgets.
func TestPerPeerIsolation(t *testing.T) {
	rl := NewRateLimiter(10, 5, 1<<30, 0) // generous egress, tight per-peer
	// Burn peerA's budget.
	for i := 0; i < 100; i++ {
		rl.AllowPeer("peerA")
	}
	// peerB should still be clean.
	allowed := 0
	for i := 0; i < 5; i++ {
		if rl.AllowPeer("peerB") {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("peerB was affected by peerA's throttling: %d allowed", allowed)
	}
}

// TestOverloadTriggersBackpressure verifies that Publish returns
// ErrBackpressure for low-priority topics once the queue threshold is crossed.
func TestOverloadTriggersBackpressure(t *testing.T) {
	e := NewEngine(WithOverloadQueueThreshold(2))
	_ = e.RegisterTopic("low", TopicConfig{Mode: BroadcastOnly, Priority: 0})
	_ = e.RegisterTopic("high", TopicConfig{Mode: BroadcastOnly, Priority: 10})

	// Simulate outstanding in-flight publishes.
	e.rateLimiter.IncQueue()
	e.rateLimiter.IncQueue()

	if err := e.Publish("low", []byte("x")); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("expected ErrBackpressure for low-priority, got %v", err)
	}
	if err := e.Publish("high", []byte("x")); err != nil {
		t.Fatalf("expected high-priority to bypass queue threshold, got %v", err)
	}
}

// TestEgressSaturationPrioritises verifies that low-priority publishes are
// rejected when the global egress bucket is saturated while high-priority
// publishes still go through.
func TestEgressSaturationPrioritises(t *testing.T) {
	e := NewEngine(WithGlobalEgressLimit(1)) // 1 byte/sec → immediately saturated on any payload
	// Force the egress bucket empty.
	e.rateLimiter.egress = rate.NewLimiter(1, 1)
	// Drain the burst.
	_ = e.rateLimiter.egress.AllowN(time.Now(), 1)

	_ = e.RegisterTopic("low", TopicConfig{Mode: BroadcastOnly, Priority: 0})
	_ = e.RegisterTopic("high", TopicConfig{Mode: BroadcastOnly, Priority: 10})

	payload := make([]byte, 1024)
	if err := e.Publish("low", payload); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("expected ErrBackpressure for low-priority under egress pressure, got %v", err)
	}
	if err := e.Publish("high", payload); err != nil {
		t.Fatalf("expected high-priority to bypass egress saturation, got %v", err)
	}
}

// TestAdaptiveIntervalGrowsUnderBackpressure verifies the adaptive interval
// backs off when the engine reports overload.
func TestAdaptiveIntervalGrowsUnderBackpressure(t *testing.T) {
	a := NewAdaptiveInterval(10*time.Second, 2*time.Second, 60*time.Second)
	start := a.Current()
	next := a.ApplyBackpressure(true)
	if next <= start {
		t.Fatalf("expected interval to grow under backpressure, start=%v next=%v", start, next)
	}
	// Repeated calls keep growing until capped.
	for i := 0; i < 10; i++ {
		a.ApplyBackpressure(true)
	}
	if a.Current() != 60*time.Second {
		t.Fatalf("expected interval to saturate at max=60s, got %v", a.Current())
	}
	// No backpressure → unchanged.
	a = NewAdaptiveInterval(10*time.Second, 2*time.Second, 60*time.Second)
	before := a.Current()
	after := a.ApplyBackpressure(false)
	if before != after {
		t.Fatalf("expected no change without backpressure, %v → %v", before, after)
	}
}

// TestCooldownAfterSustainedViolations verifies a peer enters cooldown after
// crossing the violation threshold and is blocked for the cooldown window.
func TestCooldownAfterSustainedViolations(t *testing.T) {
	p := newPeerRateLimiter(1, 1) // extremely tight so every extra call violates
	// Drain burst.
	p.Allow()
	// Hammer violations above threshold.
	for i := 0; i < violationThreshold+10; i++ {
		p.Allow()
	}
	if !p.InCooldown() {
		t.Fatalf("expected peer to be in cooldown after %d violations", violationThreshold)
	}
	// New calls should be denied while in cooldown.
	if p.Allow() {
		t.Fatalf("expected Allow to return false while in cooldown")
	}
}

// TestRemovePeerClearsState verifies that RemovePeer drops the limiter so
// reconnects get a fresh bucket.
func TestRemovePeerClearsState(t *testing.T) {
	rl := NewRateLimiter(10, 2, 1<<30, 0)
	for i := 0; i < 20; i++ {
		rl.AllowPeer("x")
	}
	if rl.PeerCount() != 1 {
		t.Fatalf("expected 1 peer tracked, got %d", rl.PeerCount())
	}
	rl.RemovePeer("x")
	if rl.PeerCount() != 0 {
		t.Fatalf("expected 0 peers after RemovePeer, got %d", rl.PeerCount())
	}
}

// TestQueueDepthIncDec verifies queue depth bookkeeping used by Publish.
func TestQueueDepthIncDec(t *testing.T) {
	rl := NewRateLimiter(0, 0, 0, 2)
	if rl.Overloaded() {
		t.Fatalf("expected not overloaded initially")
	}
	rl.IncQueue()
	rl.IncQueue()
	if !rl.Overloaded() {
		t.Fatalf("expected overloaded at threshold")
	}
	rl.DecQueue()
	if rl.Overloaded() {
		t.Fatalf("expected not overloaded after DecQueue")
	}
}

// TestPublishSucceedsWithoutLimiter verifies that a bare engine (default
// rate limiter) still accepts typical publish volume.
func TestPublishSucceedsWithoutLimiter(t *testing.T) {
	e := NewEngine()
	_ = e.RegisterTopic("t", TopicConfig{Mode: BroadcastOnly})
	for i := 0; i < 100; i++ {
		if err := e.Publish("t", []byte("hello")); err != nil {
			t.Fatalf("unexpected error at i=%d: %v", i, err)
		}
	}
}

// BenchmarkPublish measures throughput of the Publish path including rate
// limiter bookkeeping, so before/after numbers can be compared.
func BenchmarkPublish(b *testing.B) {
	e := NewEngine()
	_ = e.RegisterTopic("bench", TopicConfig{Mode: BroadcastOnly, Priority: 10})
	payload := make([]byte, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = e.Publish("bench", payload)
	}
}

// BenchmarkAllowPeer measures per-peer rate limiter overhead under
// contention (simulating the rumor fanout path).
func BenchmarkAllowPeer(b *testing.B) {
	rl := NewRateLimiter(1e9, 1<<30, 1<<30, 0) // very generous so calls always allow
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rl.AllowPeer("peer-a")
	}
}
