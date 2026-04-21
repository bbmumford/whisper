/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"math"
	"testing"
	"time"
)

// Jitter must keep the mean close to d and spread samples within
// [1-JitterFraction, 1+JitterFraction]×d. 10k samples are enough to
// statistically verify both properties.
func TestJitter_DistributionWithinBounds(t *testing.T) {
	const (
		base    = 10 * time.Second
		samples = 10000
	)
	min := time.Duration(float64(base) * (1 - JitterFraction))
	max := time.Duration(float64(base) * (1 + JitterFraction))

	var sum time.Duration
	for i := 0; i < samples; i++ {
		s := jitter(base)
		if s < min || s > max {
			t.Fatalf("sample %d out of bounds: got %v, want [%v, %v]", i, s, min, max)
		}
		sum += s
	}
	mean := sum / samples
	meanDiff := math.Abs(float64(mean - base))
	// Tolerance: < 1% of base over 10k samples.
	if meanDiff > float64(base)/100 {
		t.Errorf("mean drift: got mean %v, base %v, diff %v (>1%% of base)", mean, base, meanDiff)
	}
}

// Independent peers must produce different jittered samples so a fleet
// doesn't synchronise. We simulate 100 "peers" calling Next() from a
// fresh AdaptiveInterval — the returned durations should have non-zero
// stddev, proving the randomisation isn't deterministic per-process.
func TestJitter_DesynchronisesPeers(t *testing.T) {
	const peers = 100
	samples := make([]time.Duration, peers)
	for i := range samples {
		a := NewAdaptiveInterval(10*time.Second, 2*time.Second, 60*time.Second)
		// No records applied, no fingerprint match → "base" branch.
		samples[i] = a.Next(GossipResult{})
	}

	// All identical → jitter broken.
	allSame := true
	for i := 1; i < len(samples); i++ {
		if samples[i] != samples[0] {
			allSame = false
			break
		}
	}
	if allSame {
		t.Fatal("all 100 peer samples are identical — jitter did not desynchronise")
	}
}

// Edge case: jitter on 0 returns 0 (no panic, no underflow).
func TestJitter_ZeroDurationNoPanic(t *testing.T) {
	if got := jitter(0); got != 0 {
		t.Errorf("jitter(0) = %v, want 0", got)
	}
	if got := jitter(-1); got != -1 {
		t.Errorf("jitter(-1) = %v, want -1 (preserved)", got)
	}
}
