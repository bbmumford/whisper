/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Engine is the central gossip orchestrator. It manages topic registration,
// coordinates exchange timing via AdaptiveInterval, routes rumors via
// Hypercube, and provides the Publish/Subscribe API for consumers.
//
// The exchange loop (G1/G2/G3) runs over a net.Conn provided by the consumer.
// Consumers wire the Engine to their transport layer (e.g., Aether streams
// via adapter.StreamConn).
type Engine struct {
	registry     *TopicRegistry
	delta        *DeltaTracker
	hypercube    *Hypercube
	pex          *PEXManager
	adaptive     *AdaptiveInterval
	rtt          *NetworkRTTMeasurer
	backpressure *BackpressureMonitor
	rumor        *RumorPusher
	rateLimiter  *RateLimiter
	policy       NetworkPolicy
	reconcile    *ReconcileDriver
	snapshot     *SnapshotDriver

	mu          sync.RWMutex
	subscribers map[string][]Subscriber
	stopped     chan struct{}

	// ExchangeFunc is called by the engine to perform a single G1 exchange.
	// Consumers provide this — it encapsulates the wire-format-specific
	// serialization (protobuf, JSON, etc.) and the actual send/receive over conn.
	// Returns the exchange result for adaptive interval tuning.
	ExchangeFunc func(ctx context.Context, conn net.Conn, meta *ExchangeMeta) (GossipResult, error)

	// resp holds the responder-loop configuration populated by
	// With*-option setters. RunResponder consults it at call time;
	// the zero value is usable (defaults apply) so simple initiator-
	// only consumers never notice the field.
	resp responderConfig

	// g1 holds the G1 exchange wiring (StateStore, codec, observer).
	// Empty until WithG1Store + WithG1Codec are supplied — the native
	// G1 handler installs itself only once both are present so
	// consumers that still ship a custom G1 handler via
	// RegisterFrameKind are free to keep doing so.
	g1 g1Package
}

// EngineOption configures the engine at creation time.
type EngineOption func(*Engine)

// WithDelta enables delta sync with per-peer watermarks.
func WithDelta(dt *DeltaTracker) EngineOption {
	return func(e *Engine) { e.delta = dt }
}

// WithHypercube enables structured rumor routing.
func WithHypercube(h *Hypercube) EngineOption {
	return func(e *Engine) { e.hypercube = h }
}

// WithPEX enables peer exchange.
func WithPEX(pex *PEXManager) EngineOption {
	return func(e *Engine) { e.pex = pex }
}

// PEXManager returns the PEX manager attached to this engine, or nil if PEX
// was not enabled. Consumers (e.g. Minerva's discovery layer) use this to
// register PEXDiscoverer hooks without reaching into engine internals.
func (e *Engine) PEXManager() *PEXManager { return e.pex }

// WithAdaptive sets the adaptive interval tuner.
func WithAdaptive(a *AdaptiveInterval) EngineOption {
	return func(e *Engine) { e.adaptive = a }
}

// WithRTT enables RTT measurement.
func WithRTT(rtt *NetworkRTTMeasurer) EngineOption {
	return func(e *Engine) { e.rtt = rtt }
}

// WithBackpressure enables backpressure signaling.
func WithBackpressure(bp *BackpressureMonitor) EngineOption {
	return func(e *Engine) { e.backpressure = bp }
}

// WithRumor enables rumor-mongering (G3 immediate push).
func WithRumor(r *RumorPusher) EngineOption {
	return func(e *Engine) { e.rumor = r }
}

// WithNetworkPolicy wires the engine's policy-aware components
// (rumor retry timing, gossip cadence ceiling, reconciliation
// pacing) to consult a shared NetworkPolicy. Without this option
// every component falls back to its hard-coded defaults — same
// behavior as today.
func WithNetworkPolicy(p NetworkPolicy) EngineOption {
	return func(e *Engine) { e.policy = p }
}

// WithReconcile wires a ReconcileDriver into the engine so the
// G4 frame handler dispatches inbound TableFrames into the driver's
// RunResponderRound. Without this option the engine's responder
// loop returns "unknown frame magic" on G4 frames — peers must
// fall back to G1 + DeltaTracker.
func WithReconcile(d *ReconcileDriver) EngineOption {
	return func(e *Engine) { e.reconcile = d }
}

// WithSnapshot wires a SnapshotDriver into the engine so the G5
// frame handler dispatches inbound ManifestRequest / ShardRequest
// frames into the driver. Without this option G5 frames return
// "unknown magic" and cold-start falls back to G1 full sync.
func WithSnapshot(d *SnapshotDriver) EngineOption {
	return func(e *Engine) { e.snapshot = d }
}

// WithExchangeFunc sets the wire-format-specific exchange function.
func WithExchangeFunc(fn func(ctx context.Context, conn net.Conn, meta *ExchangeMeta) (GossipResult, error)) EngineOption {
	return func(e *Engine) { e.ExchangeFunc = fn }
}

// WithRateLimiter installs a fully-configured engine rate limiter. For most
// use cases, prefer WithPeerRateLimit and WithGlobalEgressLimit.
func WithRateLimiter(rl *RateLimiter) EngineOption {
	return func(e *Engine) { e.rateLimiter = rl }
}

// WithPeerRateLimit configures the per-peer token bucket. per is messages per
// second (steady state), burst is the burst capacity. If the engine already
// has a rate limiter, these values are applied on the next NewEngine boot
// only — create the limiter via NewRateLimiter to reuse across restarts.
func WithPeerRateLimit(per float64, burst int) EngineOption {
	return func(e *Engine) {
		if e.rateLimiter == nil {
			e.rateLimiter = NewRateLimiter(per, burst, 0, 0)
			return
		}
		e.rateLimiter.peerPerSec = per
		e.rateLimiter.peerBurst = burst
	}
}

// WithGlobalEgressLimit sets the total outbound gossip throughput cap
// (bytes/sec). Exceeded traffic is prioritised by TopicConfig.Priority.
func WithGlobalEgressLimit(bytesPerSec int) EngineOption {
	return func(e *Engine) {
		if e.rateLimiter == nil {
			e.rateLimiter = NewRateLimiter(0, 0, bytesPerSec, 0)
			return
		}
		if bytesPerSec > 0 {
			e.rateLimiter.egress.SetLimit(rate.Limit(bytesPerSec))
		}
	}
}

// WithOverloadQueueThreshold sets the outbound queue depth above which
// Publish returns ErrBackpressure.
func WithOverloadQueueThreshold(n int) EngineOption {
	return func(e *Engine) {
		if e.rateLimiter == nil {
			e.rateLimiter = NewRateLimiter(0, 0, 0, n)
			return
		}
		if n > 0 {
			e.rateLimiter.queueThreshold = n
		}
	}
}

// NewEngine creates a gossip engine with the given options.
func NewEngine(opts ...EngineOption) *Engine {
	e := &Engine{
		registry:    NewTopicRegistry(),
		subscribers: make(map[string][]Subscriber),
		stopped:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(e)
	}
	// Apply defaults for components not explicitly set
	if e.delta == nil {
		e.delta = NewDeltaTracker()
	}
	if e.adaptive == nil {
		e.adaptive = NewAdaptiveInterval(10*time.Second, 2*time.Second, 60*time.Second)
	}
	if e.rtt == nil {
		e.rtt = NewNetworkRTTMeasurer(10)
	}
	if e.backpressure == nil {
		e.backpressure = NewBackpressureMonitor()
	}
	if e.rateLimiter == nil {
		e.rateLimiter = NewRateLimiter(0, 0, 0, 0)
	}
	return e
}

// RegisterTopic registers a topic with the engine's registry.
func (e *Engine) RegisterTopic(name string, config TopicConfig) error {
	return e.registry.Register(name, config)
}

// Publish sends a payload to all subscribers of a BroadcastOnly topic.
// For StatefulMerge topics, use the StateStore.Apply path instead.
//
// Returns ErrBackpressure if the engine is overloaded. Low-priority topics
// (Priority <= 0) are rejected first when the global egress bucket is
// saturated; high-priority topics are allowed through. Callers decide
// whether to retry, drop, or bubble up.
func (e *Engine) Publish(topic string, payload []byte) error {
	cfg, ok := e.registry.Get(topic)
	if !ok {
		return fmt.Errorf("whisper: topic %q not registered", topic)
	}
	if cfg.Mode != BroadcastOnly {
		return fmt.Errorf("whisper: Publish only works on BroadcastOnly topics (topic %q is %s)", topic, cfg.Mode)
	}

	// Engine-level overload check. High-priority traffic bypasses the queue
	// threshold; everything else is rejected so the caller can shed load.
	if e.rateLimiter != nil && e.rateLimiter.Overloaded() && cfg.Priority <= 0 {
		return ErrBackpressure
	}

	// Global egress prioritisation: if the egress bucket is saturated and the
	// topic is low priority, reject. High-priority topics still attempt to
	// reserve and pay through.
	if e.rateLimiter != nil && cfg.Priority <= 0 && e.rateLimiter.EgressSaturated(len(payload)) {
		return ErrBackpressure
	}

	e.rateLimiter.IncQueue()
	defer e.rateLimiter.DecQueue()

	// Reserve egress bytes for the local fanout portion. High-priority topics
	// consume from the bucket without blocking.
	_ = e.rateLimiter.ReserveEgress(len(payload))

	// Notify local subscribers
	e.mu.RLock()
	subs := e.subscribers[topic]
	e.mu.RUnlock()
	for _, sub := range subs {
		sub.OnBroadcast(topic, "local", payload)
	}

	// Push via rumor (G3) if configured. Rumor fanout consults per-peer rate
	// limits via the RateLimiter; throttled peers are skipped but others
	// still receive the message.
	if e.rumor != nil {
		e.rumor.NotifyNewPayload(payload)
	}

	return nil
}

// RateLimiter returns the engine rate limiter. Peers and fanout loops use
// this to enforce per-peer token buckets before writing messages.
func (e *Engine) RateLimiter() *RateLimiter { return e.rateLimiter }

// AllowPeer is a convenience shortcut that returns true if the engine's
// rate limiter permits a message to the named peer right now. Fanout paths
// call this before writing to skip throttled peers without tearing down the
// publish to other peers.
func (e *Engine) AllowPeer(peerID string) bool {
	if e.rateLimiter == nil {
		return true
	}
	return e.rateLimiter.AllowPeer(peerID)
}

// Query sends a request to matching peers and collects responses.
// For RequestResponse topics only.
func (e *Engine) Query(ctx context.Context, topic string, payload []byte) ([][]byte, error) {
	cfg, ok := e.registry.Get(topic)
	if !ok {
		return nil, fmt.Errorf("whisper: topic %q not registered", topic)
	}
	if cfg.Mode != RequestResponse {
		return nil, fmt.Errorf("whisper: Query only works on RequestResponse topics")
	}
	if cfg.Handler == nil {
		return nil, fmt.Errorf("whisper: topic %q has no handler", topic)
	}

	// For local queries, invoke the handler directly
	resp, err := cfg.Handler.OnMessage(ctx, "local", topic, payload)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		return [][]byte{resp}, nil
	}
	return nil, nil
}

// Subscribe registers a subscriber for broadcast messages on a topic.
func (e *Engine) Subscribe(topic string, sub Subscriber) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.subscribers[topic] = append(e.subscribers[topic], sub)
}

// Run starts the gossip exchange loop on the given connection.
// Blocks until ctx is cancelled, the connection closes, or Stop is called.
// The ExchangeFunc must be set before calling Run.
func (e *Engine) Run(ctx context.Context, conn net.Conn, peerNodeID string, onExchange func(GossipResult)) error {
	if e.ExchangeFunc == nil {
		return fmt.Errorf("whisper: ExchangeFunc not set — call WithExchangeFunc")
	}

	interval := e.adaptive.Current()
	if interval <= 0 {
		interval = 10 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.stopped:
			return nil
		case <-time.After(interval):
		}

		// Each iteration runs in its own scope so `defer e.backpressure.Exit()`
		// fires at the end of the iteration instead of accumulating on the
		// goroutine's defer stack until Run returns. The previous shape
		// leaked ~96B per cycle and — worse — never decremented
		// pendingExchanges, so backpressure permanently tripped after
		// maxPending cycles. A fatal error bubbles out via the fatal flag.
		var (
			result    GossipResult
			fatal     error
			gotResult bool
		)
		func() {
			isOverloaded := e.backpressure.Enter()
			defer e.backpressure.Exit()

			meta := &ExchangeMeta{
				BackpressureSignal: isOverloaded,
			}
			if e.pex != nil {
				meta.PEXEntries = e.pex.BuildPEXEntries(peerNodeID)
			}

			res, err := e.ExchangeFunc(ctx, conn, meta)
			if err != nil {
				if isFatalError(err) {
					fatal = fmt.Errorf("whisper: exchange fatal: %w", err)
				}
				return
			}
			result = res
			gotResult = true
		}()
		if fatal != nil {
			return fatal
		}
		if !gotResult {
			continue
		}

		// Record RTT
		if result.NetworkRTT > 0 && e.rtt != nil {
			e.rtt.Record(result.NetworkRTT)
		}

		// Process PEX entries from peer
		if e.pex != nil && result.PeerMeta != nil && len(result.PeerMeta.PEXEntries) > 0 {
			e.pex.ProcessPEXEntries(result.PeerMeta.PEXEntries)
		}

		// Update delta tracker
		if e.delta != nil && peerNodeID != "" {
			e.delta.UpdateWatermark(peerNodeID, time.Now())
		}

		// Adapt interval based on result
		interval = e.adaptive.Next(result)

		// If the engine is currently overloaded (outbound queue beyond
		// threshold), grow the interval so gossip cycles slow down while
		// load clears.
		if e.rateLimiter != nil && e.rateLimiter.Overloaded() {
			interval = e.adaptive.ApplyBackpressure(true)
		}

		// Notify callback
		if onExchange != nil {
			onExchange(result)
		}
	}
}

// Stop signals the engine to stop all exchange loops.
func (e *Engine) Stop() {
	select {
	case <-e.stopped:
	default:
		close(e.stopped)
	}
}

// Registry returns the topic registry for external access.
func (e *Engine) Registry() *TopicRegistry { return e.registry }

// Delta returns the delta tracker.
func (e *Engine) Delta() *DeltaTracker { return e.delta }

// Hypercube returns the hypercube router (may be nil).
func (e *Engine) Hypercube() *Hypercube { return e.hypercube }

// Backpressure returns the backpressure monitor.
func (e *Engine) Backpressure() *BackpressureMonitor { return e.backpressure }

// RTT returns the RTT measurer.
func (e *Engine) RTT() *NetworkRTTMeasurer { return e.rtt }

// EngineStats aggregates observable state from all attached subsystems.
// Nil fields correspond to subsystems not wired on this engine.
type EngineStats struct {
	Topics          int             // registered topic count
	Subscribers     int             // total subscribers across topics
	Adaptive        *AdaptiveStats  // current interval + idle counter
	Backpressure    bool            // currently overloaded?
	PendingExchange int             // current pendingExchanges on backpressure monitor
	Rumor           *RumorStats     // rumor-push effectiveness
	RateLimiter     *RateLimitStats // queue depth, threshold, peer count
	RTT             time.Duration   // recent avg RTT from NetworkRTTMeasurer (0 if unmeasured)
	HypercubeValid  bool            // hypercube has usable state
	HypercubeDim    int             // current hypercube dimension
}

// RateLimitStats is an inlined view of RateLimiter state for EngineStats.
type RateLimitStats struct {
	QueueDepth     int
	QueueThreshold int
	Peers          int
	Overloaded     bool
}

// Stats returns a snapshot of aggregate engine state. Cheap — one atomic
// read per subsystem + a few locks. Intended for diagnostics endpoints
// (e.g. /api/monitoring/mesh-debug) that want a single call instead of
// pulling from each subsystem individually.
func (e *Engine) Stats() EngineStats {
	out := EngineStats{}

	e.mu.RLock()
	out.Topics = 0
	if e.registry != nil {
		out.Topics = e.registry.Count()
	}
	for _, subs := range e.subscribers {
		out.Subscribers += len(subs)
	}
	e.mu.RUnlock()

	if e.adaptive != nil {
		s := e.adaptive.Stats()
		out.Adaptive = &s
	}
	if e.backpressure != nil {
		out.Backpressure = e.backpressure.IsOverloaded()
		out.PendingExchange = e.backpressure.Pending()
	}
	if e.rumor != nil {
		s := e.rumor.Stats()
		out.Rumor = &s
	}
	if e.rateLimiter != nil {
		out.RateLimiter = &RateLimitStats{
			QueueDepth:     e.rateLimiter.QueueDepth(),
			QueueThreshold: e.rateLimiter.QueueThreshold(),
			Peers:          e.rateLimiter.PeerCount(),
			Overloaded:     e.rateLimiter.Overloaded(),
		}
	}
	if e.rtt != nil {
		out.RTT = e.rtt.AvgRTT()
	}
	if e.hypercube != nil {
		out.HypercubeValid = e.hypercube.Valid()
		out.HypercubeDim = e.hypercube.Dimension()
	}
	return out
}

// isFatalError returns true for errors that mean the connection is dead.
func isFatalError(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF {
		return true
	}
	s := err.Error()
	return s == "EOF" ||
		s == "connection reset by peer" ||
		s == "broken pipe" ||
		s == "use of closed network connection" ||
		s == "session closed"
}
