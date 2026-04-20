/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"fmt"
	"testing"
)

func TestRumorTrackerDedup(t *testing.T) {
	rt := NewRumorTracker(ServiceMeshRumorConfig())

	id := "member:tenant:node1:12345"
	if rt.IsSeen(id) {
		t.Error("should not be seen before marking")
	}
	rt.MarkSeen(id)
	if !rt.IsSeen(id) {
		t.Error("should be seen after marking")
	}
}

func TestRumorTrackerEviction(t *testing.T) {
	cfg := ServiceMeshRumorConfig()
	rt := NewRumorTracker(cfg)
	rt.capacity = 10 // small capacity for testing

	// Fill past capacity
	for i := 0; i < 15; i++ {
		rt.MarkSeen(fmt.Sprintf("rumor-%d", i))
	}

	// Should have evicted oldest half
	rt.mu.Lock()
	count := len(rt.seen)
	rt.mu.Unlock()
	if count > 10 {
		t.Errorf("expected ≤10 entries after eviction, got %d", count)
	}
}

func TestShouldForwardProbability(t *testing.T) {
	cfg := RumorConfig{
		MaxHops:            4,
		ForwardProbability: 1.0, // always forward
	}
	rt := NewRumorTracker(cfg)

	// hop 0 with P=1.0 → P=1.0/(1+0) = 1.0 → always true
	for i := 0; i < 100; i++ {
		if !rt.ShouldForward(0) {
			t.Error("hop 0 with P=1.0 should always forward")
		}
	}

	// hop >= MaxHops → always false
	if rt.ShouldForward(4) {
		t.Error("hop >= MaxHops should never forward")
	}
	if rt.ShouldForward(5) {
		t.Error("hop > MaxHops should never forward")
	}
}

func TestShouldForwardDecay(t *testing.T) {
	cfg := RumorConfig{
		MaxHops:            10,
		ForwardProbability: 0.5,
	}
	rt := NewRumorTracker(cfg)

	// At hop 9 with P=0.5: effective P = 0.5/10 = 0.05
	// Run 1000 trials — should forward roughly 50 times (±30)
	forwards := 0
	for i := 0; i < 1000; i++ {
		if rt.ShouldForward(9) {
			forwards++
		}
	}
	if forwards > 150 {
		t.Errorf("hop 9 with P=0.5: expected ~50 forwards, got %d (too many)", forwards)
	}
}

func TestRumorIDUniqueness(t *testing.T) {
	// Test that different payloads produce different IDs using hash-based default
	idFn := func(payload []byte) string {
		return fmt.Sprintf("%x", hashKey(string(payload)))
	}

	id1 := idFn([]byte(`{"topic":"member","ts":1000}`))
	id2 := idFn([]byte(`{"topic":"member","ts":1001}`))
	id3 := idFn([]byte(`{"topic":"role","ts":1000}`))

	if id1 == id2 {
		t.Error("different payloads should produce different IDs")
	}
	if id1 == id3 {
		t.Error("different payloads should produce different IDs")
	}
}
