/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"testing"
	"time"
)

// An idle process (this test binary during normal ops) must have load
// below the default threshold → Allow returns true.
func TestCPUGate_IdleAllows(t *testing.T) {
	g := NewCPUGate(CPUGateConfig{
		Threshold:    0.8,
		TickInterval: 20 * time.Millisecond,
		WindowSize:   5,
	})
	defer g.Close()

	// Give the tick loop a few samples before querying.
	time.Sleep(150 * time.Millisecond)

	if !g.Allow() {
		t.Errorf("idle process reported overloaded: load=%f threshold=%f",
			g.Load(), g.Threshold())
	}
}

// Pluggable reader max-merges with tick-skew: a reader returning 1.0
// forces Allow to fail regardless of the tick-skew signal.
func TestCPUGate_ReaderForcesDeny(t *testing.T) {
	g := NewCPUGate(CPUGateConfig{Threshold: 0.5})
	defer g.Close()

	g.SetReader(func() float64 { return 1.0 })
	if g.Allow() {
		t.Errorf("reader returned 1.0 but Allow is true (load=%f threshold=%f)",
			g.Load(), g.Threshold())
	}

	// Clearing the reader allows again (tick-skew is idle).
	g.SetReader(nil)
	time.Sleep(100 * time.Millisecond)
	if !g.Allow() {
		t.Errorf("after clearing reader, Allow still false (load=%f)", g.Load())
	}
}

// Threshold=0 means any positive load denies; Threshold=1 means nothing
// can ever deny.
func TestCPUGate_ThresholdBoundaries(t *testing.T) {
	g := NewCPUGate(CPUGateConfig{})
	defer g.Close()

	g.SetReader(func() float64 { return 0.5 })

	g.SetThreshold(0) // any positive load gates
	if g.Allow() {
		t.Error("Threshold=0 with load=0.5 should deny")
	}

	g.SetThreshold(1) // nothing gates
	if !g.Allow() {
		t.Error("Threshold=1 should always allow")
	}
}

// Close is idempotent; subsequent SetReader / Allow calls don't panic.
func TestCPUGate_CloseIdempotent(t *testing.T) {
	g := NewCPUGate(CPUGateConfig{})
	g.Close()
	g.Close() // must not panic
	// Allow and SetReader on a closed gate don't panic.
	_ = g.Allow()
	g.SetReader(func() float64 { return 0 })
	g.SetThreshold(0.5)
}

// Reader returning out-of-range values is clamped to [0, 1].
func TestCPUGate_ReaderClamped(t *testing.T) {
	g := NewCPUGate(CPUGateConfig{Threshold: 0.99})
	defer g.Close()

	g.SetReader(func() float64 { return -5 })
	if l := g.Load(); l < 0 {
		t.Errorf("negative reader not clamped: Load=%f", l)
	}

	g.SetReader(func() float64 { return 100 })
	if l := g.Load(); l > 1 {
		t.Errorf("oversized reader not clamped: Load=%f", l)
	}
}
