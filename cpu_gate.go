/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// CPUGate provides a defer-or-drop decision based on process CPU load.
// It is queried before a gossip publish or response is enqueued; when
// load is above the configured threshold, the caller receives
// ErrBackpressure (or similar) and can shed work instead of piling up
// queue depth.
//
// Two signal sources, max-merged so either one can trip the gate:
//
//  1. Tick-skew (zero-dep, always-on). A background goroutine wakes on
//     a 50 ms ticker and records the actual elapsed time since the last
//     wake. When the Go runtime scheduler is stressed (CPU saturation,
//     GC thrash, huge goroutine count), elapsed balloons well above the
//     nominal interval. Observed skew / expected interval gives a ratio;
//     max over a rolling window is the load estimate.
//
//  2. Pluggable reader (optional). A consumer-supplied readFn returning
//     a load ratio in [0, 1] — typically from a gopsutil-based probe
//     or a /sys/fs/cgroup/cpu.stat reader. Added via SetReader so
//     callers can plug in higher-fidelity signals when they have them,
//     without making the whisper package itself depend on gopsutil.
//
// The gate's reported load is max(tickSkewLoad, readerLoad). Allow()
// returns true when load < threshold, false otherwise.
type CPUGate struct {
	threshold atomic.Uint64 // float64 bits; [0, 1]. 0 = always allow, 1 = always gate.

	// Tick-skew state (background goroutine). Loaded via atomic from
	// Allow(); written only by tickLoop.
	tickSkewLoad atomic.Uint64 // float64 bits

	// Pluggable reader slot. Read under mu to swap pointers.
	mu      sync.RWMutex
	readFn  func() float64
	stopCh  chan struct{}
	stopped atomic.Bool
}

// CPUGateConfig controls construction. Zero-value → sensible defaults
// (threshold 0.8, 50 ms tick, 500 ms rolling window).
type CPUGateConfig struct {
	Threshold     float64       // 0..1, fraction above which Allow returns false. Default 0.8.
	TickInterval  time.Duration // how often the tick-skew probe fires. Default 50 ms.
	WindowSize    int           // rolling window of skew samples. Default 10 (→ 500 ms at 50 ms ticks).
}

// NewCPUGate starts a gate with the given config and spawns the tick
// goroutine. Call Close to stop it.
func NewCPUGate(cfg CPUGateConfig) *CPUGate {
	if cfg.Threshold <= 0 {
		cfg.Threshold = 0.8
	}
	if cfg.Threshold > 1 {
		cfg.Threshold = 1
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 50 * time.Millisecond
	}
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = 10
	}
	g := &CPUGate{
		stopCh: make(chan struct{}),
	}
	g.threshold.Store(math.Float64bits(cfg.Threshold))
	go g.tickLoop(cfg.TickInterval, cfg.WindowSize)
	return g
}

// SetReader installs a pluggable load-reader. Pass nil to clear. The
// reader runs on Allow()'s goroutine — keep it cheap; a slow reader
// blocks the caller.
func (g *CPUGate) SetReader(fn func() float64) {
	g.mu.Lock()
	g.readFn = fn
	g.mu.Unlock()
}

// SetThreshold adjusts the gate threshold at runtime. Useful for
// operator tuning via a diagnostics endpoint. Values are clamped to
// [0, 1]; 0 disables the gate (always allow); 1 forces it on (never
// allow above zero load).
func (g *CPUGate) SetThreshold(t float64) {
	if t < 0 {
		t = 0
	} else if t > 1 {
		t = 1
	}
	g.threshold.Store(math.Float64bits(t))
}

// Threshold returns the current threshold.
func (g *CPUGate) Threshold() float64 {
	return math.Float64frombits(g.threshold.Load())
}

// Load returns the current max-merged load estimate in [0, 1].
func (g *CPUGate) Load() float64 {
	tickLoad := math.Float64frombits(g.tickSkewLoad.Load())
	g.mu.RLock()
	fn := g.readFn
	g.mu.RUnlock()
	var readerLoad float64
	if fn != nil {
		readerLoad = fn()
		if readerLoad < 0 {
			readerLoad = 0
		} else if readerLoad > 1 {
			readerLoad = 1
		}
	}
	if readerLoad > tickLoad {
		return readerLoad
	}
	return tickLoad
}

// Allow reports whether new work should be admitted. Returns true when
// current Load() is below the threshold.
func (g *CPUGate) Allow() bool {
	return g.Load() < g.Threshold()
}

// Close stops the tick goroutine. Idempotent.
func (g *CPUGate) Close() {
	if g.stopped.CompareAndSwap(false, true) {
		close(g.stopCh)
	}
}

// tickLoop drives the zero-dep tick-skew signal. Every tickInterval
// it records (elapsed - tickInterval) / tickInterval as "this wake's
// skew ratio" and tracks the rolling max over windowSize samples.
func (g *CPUGate) tickLoop(tickInterval time.Duration, windowSize int) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	ring := make([]float64, windowSize)
	idx := 0
	last := time.Now()
	expectedNs := float64(tickInterval.Nanoseconds())

	for {
		select {
		case <-g.stopCh:
			return
		case now := <-ticker.C:
			actualNs := float64(now.Sub(last).Nanoseconds())
			last = now
			skew := (actualNs - expectedNs) / expectedNs
			if skew < 0 {
				skew = 0
			}
			if skew > 1 {
				skew = 1
			}
			// Boost by goroutine pressure. The Go runtime doesn't expose
			// scheduler load directly; NumGoroutine is a coarse proxy for
			// "how many things might be competing for a P". At very high
			// goroutine counts a 50 ms tick gets delayed even when
			// wall-clock CPU isn't saturated, because the scheduler
			// spends time just walking run queues. Blend at a small
			// weight so goroutine count alone doesn't dominate.
			ng := runtime.NumGoroutine()
			ngLoad := float64(ng) / 10000.0
			if ngLoad > 1 {
				ngLoad = 1
			}
			blended := skew*0.8 + ngLoad*0.2
			ring[idx%windowSize] = blended
			idx++
			// Rolling max over the window.
			var maxLoad float64
			for _, v := range ring {
				if v > maxLoad {
					maxLoad = v
				}
			}
			g.tickSkewLoad.Store(math.Float64bits(maxLoad))
		}
	}
}
