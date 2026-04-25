/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Rumor-mongering gossip (G3). When a genuinely new record is applied to the
// local cache, it's pushed immediately to random peers instead of waiting for
// the next gossip tick. Each receiving peer may forward the rumor with
// probability decay to prevent storms at scale (probabilistic broadcast).
//
// Wire format: [2-byte magic 0x4733][4-byte length][1-byte hop][1-byte fromDim][payload]

const RumorMagic uint16 = 0x4733 // "G3"
const rumorHeaderSize = 8        // 2 (magic) + 4 (length) + 1 (hop) + 1 (fromDimension)
const rumorSeenCapacity = 4096

// MaxRumorPayloadBytes caps the size of a payload admitted into the rumor
// inbound queue. Oversized payloads are dropped with a warning — they still
// propagate through the delta (G1) path; rumor fast-push is best-effort and
// must not pin large slices alive in the bounded inbound channel.
const MaxRumorPayloadBytes = 256 * 1024

// rumorInboundCapacity bounds the NotifyNewPayload queue. Senders are
// already fanout-limited, so a small queue is sufficient and keeps the
// retained-payload worst-case small (capacity * MaxRumorPayloadBytes).
const rumorInboundCapacity = 16

// RumorConfig configures rumor-mongering behavior.
type RumorConfig struct {
	MaxHops            int     // max hop count before rumor dies (default 4)
	Fanout             int     // number of peers to forward to (default 3)
	ForwardProbability float64 // base probability of forwarding (1.0 = always)
	Enabled            bool    // master switch
}

// ServiceMeshRumorConfig returns config optimized for small meshes (11 nodes).
func ServiceMeshRumorConfig() RumorConfig {
	return RumorConfig{
		MaxHops:            3,
		Fanout:             3,
		ForwardProbability: 1.0,
		Enabled:            true,
	}
}

// AgentMeshRumorConfig returns config optimized for large meshes (hundreds of devices).
func AgentMeshRumorConfig() RumorConfig {
	return RumorConfig{
		MaxHops:            4,
		Fanout:             3,
		ForwardProbability: 0.6,
		Enabled:            true,
	}
}

// RumorIDFunc generates a dedup key from a rumor payload. Consumers provide
// their own implementation based on their record structure. The returned string
// must be deterministic for the same payload.
type RumorIDFunc func(payload []byte) string

// RumorTracker deduplicates rumors using an O(1) LRU. The doubly-linked
// list orders entries by recency (front = newest, back = oldest); the map
// provides O(1) lookup into that list. MarkSeen moves an existing entry
// to the front or pushes a new one; eviction pops from the back.
type RumorTracker struct {
	mu       sync.Mutex
	seen     map[string]*list.Element
	order    *list.List // values are seenEntry
	config   RumorConfig
	capacity int
}

// seenEntry is the list-element value. key is stored so eviction (which
// pops from the list tail) can remove the corresponding map entry.
type seenEntry struct {
	key string
	ts  int64
}

// NewRumorTracker creates a tracker with the given config.
func NewRumorTracker(cfg RumorConfig) *RumorTracker {
	return &RumorTracker{
		seen:     make(map[string]*list.Element, rumorSeenCapacity),
		order:    list.New(),
		config:   cfg,
		capacity: rumorSeenCapacity,
	}
}

// IsSeen returns true if this rumor has been processed before.
func (rt *RumorTracker) IsSeen(rumorID string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	_, ok := rt.seen[rumorID]
	return ok
}

// MarkSeen records that this rumor has been processed. Existing keys are
// moved to the front (most-recent); new keys are pushed and the tail is
// evicted if capacity is exceeded.
func (rt *RumorTracker) MarkSeen(rumorID string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	now := time.Now().UnixNano()
	if el, ok := rt.seen[rumorID]; ok {
		el.Value.(*seenEntry).ts = now
		rt.order.MoveToFront(el)
		return
	}
	el := rt.order.PushFront(&seenEntry{key: rumorID, ts: now})
	rt.seen[rumorID] = el
	if rt.order.Len() > rt.capacity {
		rt.evictOldest()
	}
}

// evictOldest removes the single least-recently-used entry. O(1).
// Must hold mu.
func (rt *RumorTracker) evictOldest() {
	tail := rt.order.Back()
	if tail == nil {
		return
	}
	entry := tail.Value.(*seenEntry)
	rt.order.Remove(tail)
	delete(rt.seen, entry.key)
}

// ShouldForward returns true based on probability decay: P / (1 + hopCount).
func (rt *RumorTracker) ShouldForward(hopCount uint8) bool {
	if int(hopCount) >= rt.config.MaxHops {
		return false
	}
	p := rt.config.ForwardProbability / (1.0 + float64(hopCount))
	return rand.Float64() < p
}

// WriteRumor writes a G3 rumor frame to conn. The payload is opaque bytes
// that the consumer has already serialised. Uses net.Buffers to hand the
// header and payload to the OS writev path (where supported), avoiding
// the per-send copy into a merged buffer.
func WriteRumor(conn net.Conn, payload []byte, hopCount uint8, fromDimension uint8) error {
	var hdr [rumorHeaderSize]byte
	binary.BigEndian.PutUint16(hdr[0:2], RumorMagic)
	binary.BigEndian.PutUint32(hdr[2:6], uint32(len(payload)))
	hdr[6] = hopCount
	hdr[7] = fromDimension
	bufs := net.Buffers{hdr[:], payload}
	_, err := bufs.WriteTo(conn)
	return err
}

// ReadRumorBody reads the remaining bytes of a G3 frame after the 2-byte magic
// has been consumed by the multiplexer. Returns the opaque payload bytes.
func ReadRumorBody(r io.Reader) (payload []byte, hopCount uint8, fromDimension uint8, err error) {
	var hdr [6]byte // 4 (length) + 1 (hop) + 1 (fromDim)
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, 0, 0, fmt.Errorf("rumor: read header: %w", err)
	}
	payloadLen := binary.BigEndian.Uint32(hdr[0:4])
	hopCount = hdr[4]
	fromDimension = hdr[5]

	if payloadLen > maxGossipPayload {
		return nil, 0, 0, fmt.Errorf("rumor: payload too large (%d bytes)", payloadLen)
	}
	payload = make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, 0, fmt.Errorf("rumor: read payload: %w", err)
	}
	return payload, hopCount, fromDimension, nil
}

// RumorPeerConn is the interface for sending rumors to a peer.
type RumorPeerConn interface {
	PeerNodeID() string
	WriteRumor(payload []byte, hopCount uint8, fromDimension uint8) error
}

// RumorPusher pushes new records to peers immediately via hypercube or random.
type RumorPusher struct {
	mu       sync.RWMutex
	tracker  *RumorTracker
	peers    map[string]RumorPeerConn // nodeID → peer connection
	cube     *Hypercube               // structured overlay (nil until initialized)
	cubeExt  *HypercubeExt            // Phase 6 extended cube (optional; supersedes cube when set)
	inbound  chan RumorMessage
	rumorIDFn RumorIDFunc // consumer-provided dedup key generator

	// ackTracker tracks pending ACKs per (rumorID, peerID). Active only
	// when ACKEnabled — peers that don't advertise the RumorACK
	// capability bypass the tracker entirely. The retry sweeper goroutine
	// runs only when the tracker is active.
	ackTracker  *rumorACKTracker
	lossTracker *peerLossTracker
	ackEnabled  bool
	ackTimeout  time.Duration
	ackMaxRetry int

	// policy is the optional NetworkPolicy override. When set, ACK
	// retry timing and fanout decisions read from the policy
	// profile (RumorRetryInitial / RumorFanout) instead of the
	// fixed values supplied to EnableACKs. Profile transitions
	// take effect on the next push without restarting the sweeper.
	policy NetworkPolicy

	// emitFn is the optional MeshEvent producer. When set, rumor
	// lifecycle decisions emit EventRumorSent / Acked / Retry /
	// Dropped through it. Function-typed (rather than *eventBus)
	// so consumers can multiplex events into their own subscribers
	// (connection-history, mesh-debug, OTLP) without depending on
	// whisper's internal event-bus type.
	emitFn func(GossipEvent)

	// peerSupportsACKFn returns true when a peer has advertised the
	// RumorACK capability. nil = treat all peers as capable (today's
	// default; fine when ACKs are enabled fleet-wide). When set, the
	// ACK tracker only registers entries for capable peers — saves
	// retry budget on mixed-version meshes that include legacy
	// peers without RumorACK.
	peerSupportsACKFn func(peerID string) bool

	// Observability counters — lock-free. Call sites increment them via
	// atomic ops; Stats() reads them for a snapshot.
	//   - notified: total NotifyNewPayload calls (includes duplicates)
	//   - deduped:  rumors skipped by the tracker (already seen)
	//   - queueFull:NotifyNewPayload drops (inbound channel saturated)
	//   - pushesHypercube: rumor writes that went via the hypercube overlay
	//   - pushesRandom:    rumor writes that went via random-peer fallback
	//   - writeErrors:     RumorPeerConn.WriteRumor failures
	//   - acksReceived:    G3-ACK frames matched to a tracked rumor
	//   - rumorRetries:    rumors re-pushed after ACK timeout
	//   - rumorDropped:    rumors that exhausted retry budget without ACK
	notified         atomic.Uint64
	deduped          atomic.Uint64
	queueFull        atomic.Uint64
	pushesHypercube  atomic.Uint64
	pushesRandom     atomic.Uint64
	writeErrors      atomic.Uint64
	acksReceived     atomic.Uint64
	rumorRetries     atomic.Uint64
	rumorDropped     atomic.Uint64
}

// RumorStats is a point-in-time snapshot of rumor-push effectiveness.
type RumorStats struct {
	Notified        uint64 // NotifyNewPayload calls
	Deduped         uint64 // skipped because already-seen
	QueueFull       uint64 // notifies dropped because inbound channel full
	PushesHypercube uint64 // frame writes via hypercube overlay
	PushesRandom    uint64 // frame writes via random-peer fallback
	WriteErrors     uint64 // peer.WriteRumor failures
	AcksReceived    uint64 // G3-ACK confirmations matched to outstanding rumors
	RumorRetries    uint64 // rumor re-pushes after ACK timeout
	RumorDropped    uint64 // rumors that exhausted retry budget
	PendingAcks     int    // outstanding (rumor, peer) pairs awaiting ACK
	Peers           int    // current peer count (RegisterPeer − UnregisterPeer)
	SeenCache       int    // current size of the tracker's seen map
}

// Stats returns a snapshot of rumor-push effectiveness. Cheap:
// atomic reads for counters + one RLock for peer/cache sizes.
func (rp *RumorPusher) Stats() RumorStats {
	rp.mu.RLock()
	peerCount := len(rp.peers)
	rp.mu.RUnlock()
	rp.tracker.mu.Lock()
	seenCount := len(rp.tracker.seen)
	rp.tracker.mu.Unlock()
	pendingAcks := 0
	if rp.ackTracker != nil {
		pendingAcks = rp.ackTracker.PendingCount()
	}
	return RumorStats{
		Notified:        rp.notified.Load(),
		Deduped:         rp.deduped.Load(),
		QueueFull:       rp.queueFull.Load(),
		PushesHypercube: rp.pushesHypercube.Load(),
		PushesRandom:    rp.pushesRandom.Load(),
		WriteErrors:     rp.writeErrors.Load(),
		AcksReceived:    rp.acksReceived.Load(),
		RumorRetries:    rp.rumorRetries.Load(),
		RumorDropped:    rp.rumorDropped.Load(),
		PendingAcks:     pendingAcks,
		Peers:           peerCount,
		SeenCache:       seenCount,
	}
}

// RumorMessage is a payload with its dedup key, queued for push.
type RumorMessage struct {
	ID      string // dedup key (generated by consumer's RumorIDFunc)
	Payload []byte // opaque serialised record
}

// NewRumorPusher creates a pusher with the given config.
// rumorIDFn generates dedup keys from payloads; if nil, a hash-based default is used.
func NewRumorPusher(cfg RumorConfig, rumorIDFn RumorIDFunc) *RumorPusher {
	if rumorIDFn == nil {
		rumorIDFn = func(payload []byte) string {
			return fmt.Sprintf("%x", hashKey(string(payload)))
		}
	}
	return &RumorPusher{
		tracker:   NewRumorTracker(cfg),
		peers:     make(map[string]RumorPeerConn),
		inbound:   make(chan RumorMessage, rumorInboundCapacity),
		rumorIDFn: rumorIDFn,
	}
}

// Tracker returns the underlying RumorTracker (for responder G3 handling).
func (rp *RumorPusher) Tracker() *RumorTracker { return rp.tracker }

// SetNetworkPolicy wires the rumor pusher to consult a NetworkPolicy
// for retry timing and fanout overrides. Subsequent pushes read
// `Profile().RumorRetryInitial` and `Profile().RumorFanout` to
// override the fixed values supplied to EnableACKs / RumorConfig.
// Without this, fixed defaults apply.
func (rp *RumorPusher) SetNetworkPolicy(p NetworkPolicy) {
	rp.mu.Lock()
	rp.policy = p
	rp.mu.Unlock()
}

// SetEventEmitter wires the rumor pusher to a MeshEvent producer
// callback. Lifecycle events (RumorSent, RumorAcked, RumorRetry,
// RumorDropped) emit through the callback when set; without it the
// pusher updates only its atomic counters. Function-typed so
// consumers can fan events into their own subscribers
// (connection-history, mesh-debug, OTLP) without depending on
// whisper's internal event-bus type.
func (rp *RumorPusher) SetEventEmitter(fn func(GossipEvent)) {
	rp.mu.Lock()
	rp.emitFn = fn
	rp.mu.Unlock()
}

// effectiveRumorTimeout returns the retry timeout, preferring the
// network policy when configured.
func (rp *RumorPusher) effectiveRumorTimeout() time.Duration {
	rp.mu.RLock()
	policy := rp.policy
	timeout := rp.ackTimeout
	rp.mu.RUnlock()
	if policy != nil {
		if pt := policy.Profile().RumorRetryInitial; pt > 0 {
			return pt
		}
	}
	return timeout
}

// effectiveFanout returns the per-peer fanout factor, layered as:
//
//   1. Static config baseline (RumorConfig.Fanout)
//   2. Policy override when configured (Profile().RumorFanout)
//   3. Adaptive bump-up when the peer's loss rate > 10%
//
// Returns the fanout to use for a specific peer's push.
func (rp *RumorPusher) effectiveFanout(peerID string) int {
	rp.mu.RLock()
	policy := rp.policy
	loss := rp.lossTracker
	rp.mu.RUnlock()

	base := rp.tracker.config.Fanout
	if policy != nil {
		if pf := policy.Profile().RumorFanout; pf > 0 {
			base = pf
		}
	}
	// Adaptive bump on detected loss — the per-peer LossTracker
	// has accumulated rumor-ACK miss data over the recent window.
	if loss != nil && loss.LossRate(peerID) > 0.10 {
		return base + 1
	}
	return base
}

// emitEvent publishes a GossipEvent via the optional callback.
// No-op when no callback is wired.
func (rp *RumorPusher) emitEvent(kind GossipEventKind, peerID string) {
	rp.mu.RLock()
	fn := rp.emitFn
	rp.mu.RUnlock()
	if fn != nil {
		fn(GossipEvent{Kind: kind, PeerNodeID: peerID, Time: time.Now()})
	}
}

// EnableACKs activates the rumor ACK protocol on this pusher. Without
// it, PushRumor / PushRumorExcluding behave exactly as before (best-
// effort, no delivery confirmation). With it active:
//
//   - every per-peer rumor send registers an entry in the ACK tracker
//   - the receiver-of-ACK loop on the rumor stream calls HandleACK to
//     dispatch incoming G3-ACK frames into the tracker
//   - a sweeper goroutine polls for ACK timeouts on the configured
//     interval; timed-out rumors retry via an alternate hypercube edge
//     until the retry budget is exhausted
//
// Capability gating MUST happen at the registration boundary: only
// peers that advertise RumorACK should be passed to RegisterPeer
// when ACKs are enabled, otherwise their rumors will time out
// every cycle and waste retry budget. Mixed-version meshes register
// non-ACK peers separately or run with EnableACKs disabled per-peer.
//
// Idempotent — multiple calls update timeout/retry parameters but
// only one sweeper runs.
func (rp *RumorPusher) EnableACKs(timeout time.Duration, maxRetries int) {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	if maxRetries < 0 {
		maxRetries = 3
	}
	rp.mu.Lock()
	rp.ackEnabled = true
	rp.ackTimeout = timeout
	rp.ackMaxRetry = maxRetries
	if rp.ackTracker == nil {
		rp.ackTracker = newRumorACKTracker()
		rp.lossTracker = newPeerLossTracker(100)
		go rp.ackSweepLoop()
	}
	rp.mu.Unlock()
}

// HandleACK is called by the ACK reader loop on each peer's rumor
// stream when a G3-ACK frame arrives. Looks up the (rumorID, peerID)
// pair in the tracker; if matched, clears the entry, bumps the
// acksReceived counter, and emits EventRumorAcked. Unknown ACKs
// are silently ignored — duplicates or late arrivals after
// retry-budget exhaustion.
func (rp *RumorPusher) HandleACK(rumorID, peerID string) {
	rp.mu.RLock()
	tracker := rp.ackTracker
	rp.mu.RUnlock()
	if tracker == nil {
		return
	}
	if tracker.Acked(rumorID, peerID) {
		rp.acksReceived.Add(1)
		rp.emitEvent(EventRumorAcked, peerID)
	}
}

// ackSweepLoop polls the ACK tracker for timed-out entries and
// retries each via an alternate hypercube edge or drops on retry-
// budget exhaustion. Runs until ackTracker.Stop is called.
func (rp *RumorPusher) ackSweepLoop() {
	rp.mu.RLock()
	tracker := rp.ackTracker
	rp.mu.RUnlock()
	if tracker == nil {
		return
	}
	// Sweep at half the ACK timeout so we catch timeouts within one
	// timeout-window of when they actually elapsed.
	rp.mu.RLock()
	period := rp.ackTimeout / 2
	rp.mu.RUnlock()
	if period < 100*time.Millisecond {
		period = 100 * time.Millisecond
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-tracker.stopCh:
			return
		case <-ticker.C:
			expired := tracker.timedOutEntries()
			for _, pr := range expired {
				rp.handleAckTimeout(pr)
			}
		}
	}
}

// handleAckTimeout retries a rumor against an alternate hypercube
// edge or drops it if the retry budget is exhausted. Records the
// loss for adaptive-fanout purposes.
func (rp *RumorPusher) handleAckTimeout(pr *pendingRumor) {
	if pr.retries >= pr.maxRetries {
		rp.rumorDropped.Add(1)
		if rp.lossTracker != nil {
			rp.lossTracker.RecordMiss(pr.peerID)
		}
		rp.emitEvent(EventRumorDropped, pr.peerID)
		dbgRumor.Printf("Rumor %s to %s dropped after %d retries", pr.rumorID, pr.peerID, pr.retries)
		return
	}
	rp.rumorRetries.Add(1)
	rp.emitEvent(EventRumorRetry, pr.peerID)

	// Pick an alternate dimension if hypercube available; else random
	// fallback to the same peer (best we can do without an overlay).
	rp.mu.RLock()
	cube := rp.cube
	conn, ok := rp.peers[pr.peerID]
	rp.mu.RUnlock()
	if !ok {
		// Peer disconnected during the wait — drop and let the next
		// rumor's normal fanout cover it.
		return
	}

	// Try a different dimension than the original (pr.dimension).
	// Cube.RouteRumor(pr.dimension) returns dimensions > pr.dimension;
	// we want any other dimension we haven't tried.
	var altDim uint8 = pr.dimension
	if cube != nil {
		for _, d := range cube.RouteRumor(0) {
			if uint8(d) != pr.dimension {
				altDim = uint8(d)
				break
			}
		}
	}
	pr.retries++
	pr.sentAt = time.Now()
	if err := conn.WriteRumor(pr.payload, pr.hopCount+1, altDim); err != nil {
		dbgRumor.Printf("Rumor %s retry to %s failed: %v", pr.rumorID, pr.peerID, err)
		return
	}
	// Re-track for the next ACK window.
	rp.ackTracker.Track(pr.rumorID, pr.peerID, pr.payload, pr.hopCount+1, altDim, pr.timeout, pr.maxRetries-pr.retries)
}

// RegisterPeer adds a peer connection for rumor delivery.
func (rp *RumorPusher) RegisterPeer(nodeID string, conn RumorPeerConn) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	rp.peers[nodeID] = conn
}

// UnregisterPeer removes a peer connection.
func (rp *RumorPusher) UnregisterPeer(nodeID string) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	delete(rp.peers, nodeID)
}

// SetHypercube sets the structured overlay for dimension-ordered routing.
func (rp *RumorPusher) SetHypercube(cube *Hypercube) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	rp.cube = cube
}

// SetHypercubeExt sets the extended hypercube. When set, it supersedes
// any plain Hypercube wired via SetHypercube — the pusher uses the
// primary cube for the canonical routing decision and (when the
// extended options enable it) the dual cube for redundant fanout.
//
// Adaptive dimension weighting, RouteToFar dispatch, region-aware
// ordering and ephemeral exclusion all read from the extended cube;
// the plain Hypercube path retains today's behavior for callers
// that haven't migrated.
func (rp *RumorPusher) SetHypercubeExt(ext *HypercubeExt) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	rp.cubeExt = ext
	if ext != nil {
		rp.cube = ext.Primary()
	}
}

// HypercubeExt returns the configured extended cube, or nil if only
// a plain Hypercube has been wired.
func (rp *RumorPusher) HypercubeExt() *HypercubeExt {
	rp.mu.RLock()
	defer rp.mu.RUnlock()
	return rp.cubeExt
}

// SetPeerCapabilityCheck wires a per-peer capability lookup. The
// pusher only registers ACK tracking for peers that the callback
// returns true for. Pass nil to disable gating (every peer is
// treated as capable; fleet-wide ACK semantics).
func (rp *RumorPusher) SetPeerCapabilityCheck(fn func(peerID string) bool) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	rp.peerSupportsACKFn = fn
}

// NotifyNewPayload queues an opaque payload for rumor push (non-blocking).
// Payloads larger than MaxRumorPayloadBytes are dropped; they still reach
// peers via the G1 delta path, but holding them in the bounded inbound
// queue would pin (capacity × payload-size) bytes resident in the worst
// case. queueFull covers both overflow and oversized-drop cases.
func (rp *RumorPusher) NotifyNewPayload(payload []byte) {
	rp.notified.Add(1)
	if len(payload) > MaxRumorPayloadBytes {
		dbgRumor.Printf("NotifyNewPayload: dropping oversized rumor payload (%d > %d bytes)", len(payload), MaxRumorPayloadBytes)
		rp.queueFull.Add(1)
		return
	}
	id := rp.rumorIDFn(payload)
	select {
	case rp.inbound <- RumorMessage{ID: id, Payload: payload}:
	default:
		// Channel full — drop, gossip tick catches it
		rp.queueFull.Add(1)
	}
}

// Run processes inbound new-record notifications and pushes rumors.
func (rp *RumorPusher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-rp.inbound:
			if rp.tracker.IsSeen(msg.ID) {
				rp.deduped.Add(1)
				continue
			}
			rp.tracker.MarkSeen(msg.ID)
			rp.PushRumor(msg.Payload, 0, 0xFF) // origin: hop=0, fromDim=0xFF (all dimensions)
		}
	}
}

// PushRumor sends a payload to selected peers via hypercube or random fallback.
func (rp *RumorPusher) PushRumor(payload []byte, hopCount uint8, fromDimension uint8) {
	rp.pushRumorInternal(payload, hopCount, fromDimension, "")
}

// PushRumorExcluding is like PushRumor but excludes a specific peer (the sender)
// to prevent back-propagation of rumors to the node that sent them.
func (rp *RumorPusher) PushRumorExcluding(payload []byte, hopCount uint8, fromDimension uint8, excludeNodeID string) {
	rp.pushRumorInternal(payload, hopCount, fromDimension, excludeNodeID)
}

func (rp *RumorPusher) pushRumorInternal(payload []byte, hopCount uint8, fromDimension uint8, excludeNodeID string) {
	rp.mu.RLock()
	cube := rp.cube
	cubeExt := rp.cubeExt
	peers := make(map[string]RumorPeerConn, len(rp.peers))
	for k, v := range rp.peers {
		if k != excludeNodeID {
			peers[k] = v
		}
	}
	ackEnabled := rp.ackEnabled
	ackTimeout := rp.ackTimeout
	ackMaxRetry := rp.ackMaxRetry
	rp.mu.RUnlock()

	if len(peers) == 0 {
		return
	}

	// Compute the rumor ID once for ACK tracking; reused for every
	// peer the rumor reaches in this push.
	rumorID := rp.rumorIDFn(payload)

	// Effective ACK timeout reads from NetworkPolicy when set,
	// falling back to the value configured at EnableACKs time.
	if pt := rp.effectiveRumorTimeout(); pt > 0 {
		ackTimeout = pt
	}

	rp.mu.RLock()
	supports := rp.peerSupportsACKFn
	rp.mu.RUnlock()
	trackACK := func(peerID string, dim uint8) {
		if !ackEnabled {
			return
		}
		// Per-peer capability gate: skip ACK tracking for peers
		// that haven't advertised RumorACK. Without this, every
		// rumor to a legacy peer wastes retry budget timing out.
		if supports != nil && !supports(peerID) {
			return
		}
		rp.ackTracker.Track(rumorID, peerID, payload, hopCount+1, dim, ackTimeout, ackMaxRetry)
		if rp.lossTracker != nil {
			rp.lossTracker.RecordSent(peerID)
		}
		rp.emitEvent(EventRumorSent, peerID)
	}

	// Try hypercube routing first.
	// RouteRumor(255) returns all dimensions (origin push), RouteRumor(d) returns dims > d.
	if cube != nil {
		dims := cube.RouteRumor(int(fromDimension))
		sent := 0
		for _, d := range dims {
			neighbor := cube.DimensionNeighbor(d)
			if neighbor == "" {
				continue
			}
			if peer, ok := peers[neighbor]; ok {
				if err := peer.WriteRumor(payload, hopCount+1, uint8(d)); err != nil {
					dbgRumor.Printf("Hypercube send to %s dim %d failed: %v", neighbor, d, err)
					rp.writeErrors.Add(1)
					if cubeExt != nil {
						cubeExt.RecordDimensionLoss(d)
					}
				} else {
					sent++
					rp.pushesHypercube.Add(1)
					trackACK(neighbor, uint8(d))
					if cubeExt != nil {
						cubeExt.RecordDimensionSuccess(d)
					}
				}
				delete(peers, neighbor) // don't double-send via random
			}
		}

		// Dual-cube fanout — when the extended cube has the dual
		// cube enabled, also fan out via the dual cube's neighbors.
		// The two cubes use different orderings (NodeID-sorted vs
		// hash-sorted) so their neighbor sets differ; fanning out via
		// both gives instant redundancy at the cost of a small fanout
		// multiplier (~2× neighbors per origin push).
		if cubeExt != nil && cubeExt.Dual() != nil {
			dual := cubeExt.Dual()
			dualDims := dual.RouteRumor(int(fromDimension))
			for _, d := range dualDims {
				neighbor := dual.DimensionNeighbor(d)
				if neighbor == "" {
					continue
				}
				peer, ok := peers[neighbor]
				if !ok {
					continue
				}
				if err := peer.WriteRumor(payload, hopCount+1, uint8(d)); err != nil {
					dbgRumor.Printf("Dual-cube send to %s dim %d failed: %v", neighbor, d, err)
					rp.writeErrors.Add(1)
				} else {
					sent++
					rp.pushesHypercube.Add(1)
					trackACK(neighbor, uint8(d))
				}
				delete(peers, neighbor)
			}
		}

		if sent > 0 {
			dbgRumor.Printf("Pushed rumor via hypercube: %d dimensions, hop=%d", sent, hopCount)
			return // hypercube handled forwarding
		}
		// All hypercube neighbors dead — fall through to random
	}

	// Random fallback (origin push or degraded hypercube). Fanout scales
	// with peer count (2B — adaptiveFanout = max(log2(N), configured base))
	// so large fleets keep probabilistic coverage without fanning out to
	// every peer. The base fanout reads from NetworkPolicy when set,
	// and bumps up for peers with elevated loss rate (>10% over the
	// recent window) so flaky peers get extra fanout coverage.
	//
	// We pick a representative peer ID for loss-rate lookup — when
	// the random selection covers multiple peers, the per-peer
	// adaptive bump applies once at the global fanout level rather
	// than per-target. Acceptable simplification because random
	// fallback is the degraded path; hypercube routing is the
	// primary and already directs to specific neighbors.
	var sampleID string
	for k := range peers {
		sampleID = k
		break
	}
	targets := selectRandomPeers(peers, adaptiveFanout(len(peers), rp.effectiveFanout(sampleID)))
	for _, peer := range targets {
		if err := peer.WriteRumor(payload, hopCount+1, 0xFF); err != nil {
			dbgRumor.Printf("Random send failed: %v", err)
			rp.writeErrors.Add(1)
		} else {
			rp.pushesRandom.Add(1)
			trackACK(peer.PeerNodeID(), 0xFF)
		}
	}
	if len(targets) > 0 {
		dbgRumor.Printf("Pushed rumor to %d random peers, hop=%d", len(targets), hopCount)
	}
}

func selectRandomPeers(peers map[string]RumorPeerConn, n int) []RumorPeerConn {
	if len(peers) == 0 {
		return nil
	}
	if len(peers) <= n {
		result := make([]RumorPeerConn, 0, len(peers))
		for _, p := range peers {
			result = append(result, p)
		}
		return result
	}
	keys := make([]string, 0, len(peers))
	for k := range peers {
		keys = append(keys, k)
	}
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	result := make([]RumorPeerConn, n)
	for i := 0; i < n; i++ {
		result[i] = peers[keys[i]]
	}
	return result
}

// adaptiveFanout returns the random-peer fanout for the current peer
// count (2B). Scales with log2(N) above the configured base so a large
// fleet still gets probabilistic coverage without spamming every peer,
// while a small fleet keeps the configured minimum. Examples with
// baseFanout=3:
//   - 4 peers:    max(log2(4)=2,  3) = 3
//   - 16 peers:   max(log2(16)=4, 3) = 4
//   - 100 peers:  max(log2(100)=6, 3) = 6
//   - 1000 peers: max(log2(1000)=9, 3) = 9
//
// Hypercube routing already gives structured coverage and is naturally
// logarithmic in dimensions — this applies to the random-fallback path
// only, which used to cap at the static baseFanout regardless of fleet
// size.
func adaptiveFanout(peerCount, baseFanout int) int {
	if peerCount <= 0 {
		return 0
	}
	f := int(math.Log2(float64(peerCount)))
	if f < baseFanout {
		f = baseFanout
	}
	if f > peerCount {
		f = peerCount
	}
	return f
}

// HandleRumorFrame processes an incoming G3 rumor frame in the responder.
// Called from the responder multiplexer after the 2-byte magic is consumed.
// senderNodeID is excluded from forwarding to prevent back-propagation.
// applyFn is the consumer's function to apply the payload to local state.
func HandleRumorFrame(r io.Reader, tracker *RumorTracker, pusher *RumorPusher, senderNodeID string, rumorIDFn RumorIDFunc, applyFn func([]byte) error) {
	payload, hopCount, fromDim, err := ReadRumorBody(r)
	if err != nil {
		dbgRumor.Printf("Responder: rumor read error: %v", err)
		return
	}

	rumorID := rumorIDFn(payload)
	if tracker.IsSeen(rumorID) {
		dbgRumor.Printf("Responder: rumor duplicate, dropping")
		return
	}
	tracker.MarkSeen(rumorID)

	// Apply to local state via consumer callback
	if applyFn != nil {
		if err := applyFn(payload); err != nil {
			dbgRumor.Printf("Responder: rumor apply error: %v", err)
		}
	}

	// Forward synchronously — the responder runs in its own goroutine already.
	// No `go` keyword: avoids unbounded goroutine accumulation under gossip churn.
	if tracker.ShouldForward(hopCount) && pusher != nil {
		pusher.PushRumorExcluding(payload, hopCount, fromDim, senderNodeID)
	}

	dbgRumor.Printf("Responder: rumor applied and forwarded (hop=%d)", hopCount)
}
