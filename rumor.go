/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
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
const rumorSeenCapacity = 256

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

// RumorTracker deduplicates rumors using a generation-based LRU.
type RumorTracker struct {
	mu       sync.Mutex
	seen     map[string]int64 // rumorID → timestamp (for eviction)
	config   RumorConfig
	capacity int
}

// NewRumorTracker creates a tracker with the given config.
func NewRumorTracker(cfg RumorConfig) *RumorTracker {
	return &RumorTracker{
		seen:     make(map[string]int64, rumorSeenCapacity),
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

// MarkSeen records that this rumor has been processed.
func (rt *RumorTracker) MarkSeen(rumorID string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.seen[rumorID] = time.Now().UnixNano()
	if len(rt.seen) > rt.capacity {
		rt.evictOldest()
	}
}

// evictOldest removes the oldest half of entries. Must hold mu.
func (rt *RumorTracker) evictOldest() {
	type entry struct {
		key string
		ts  int64
	}
	entries := make([]entry, 0, len(rt.seen))
	for k, v := range rt.seen {
		entries = append(entries, entry{k, v})
	}
	half := len(entries) / 2
	for i := 0; i < half; i++ {
		minIdx := i
		for j := i + 1; j < len(entries); j++ {
			if entries[j].ts < entries[minIdx].ts {
				minIdx = j
			}
		}
		entries[i], entries[minIdx] = entries[minIdx], entries[i]
		delete(rt.seen, entries[i].key)
	}
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
// that the consumer has already serialised.
func WriteRumor(conn net.Conn, payload []byte, hopCount uint8, fromDimension uint8) error {
	buf := make([]byte, rumorHeaderSize+len(payload))
	binary.BigEndian.PutUint16(buf[0:2], RumorMagic)
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(payload)))
	buf[6] = hopCount
	buf[7] = fromDimension
	copy(buf[8:], payload)
	_, err := conn.Write(buf)
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
	inbound  chan RumorMessage
	rumorIDFn RumorIDFunc // consumer-provided dedup key generator

	// Observability counters — lock-free. Call sites increment them via
	// atomic ops; Stats() reads them for a snapshot.
	//   - notified: total NotifyNewPayload calls (includes duplicates)
	//   - deduped:  rumors skipped by the tracker (already seen)
	//   - queueFull:NotifyNewPayload drops (inbound channel saturated)
	//   - pushesHypercube: rumor writes that went via the hypercube overlay
	//   - pushesRandom:    rumor writes that went via random-peer fallback
	//   - writeErrors:     RumorPeerConn.WriteRumor failures
	notified         atomic.Uint64
	deduped          atomic.Uint64
	queueFull        atomic.Uint64
	pushesHypercube  atomic.Uint64
	pushesRandom     atomic.Uint64
	writeErrors      atomic.Uint64
}

// RumorStats is a point-in-time snapshot of rumor-push effectiveness.
type RumorStats struct {
	Notified        uint64 // NotifyNewPayload calls
	Deduped         uint64 // skipped because already-seen
	QueueFull       uint64 // notifies dropped because inbound channel full
	PushesHypercube uint64 // frame writes via hypercube overlay
	PushesRandom    uint64 // frame writes via random-peer fallback
	WriteErrors     uint64 // peer.WriteRumor failures
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
	return RumorStats{
		Notified:        rp.notified.Load(),
		Deduped:         rp.deduped.Load(),
		QueueFull:       rp.queueFull.Load(),
		PushesHypercube: rp.pushesHypercube.Load(),
		PushesRandom:    rp.pushesRandom.Load(),
		WriteErrors:     rp.writeErrors.Load(),
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
		inbound:   make(chan RumorMessage, 64),
		rumorIDFn: rumorIDFn,
	}
}

// Tracker returns the underlying RumorTracker (for responder G3 handling).
func (rp *RumorPusher) Tracker() *RumorTracker { return rp.tracker }

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

// NotifyNewPayload queues an opaque payload for rumor push (non-blocking).
func (rp *RumorPusher) NotifyNewPayload(payload []byte) {
	rp.notified.Add(1)
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
	peers := make(map[string]RumorPeerConn, len(rp.peers))
	for k, v := range rp.peers {
		if k != excludeNodeID {
			peers[k] = v
		}
	}
	rp.mu.RUnlock()

	if len(peers) == 0 {
		return
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
				} else {
					sent++
					rp.pushesHypercube.Add(1)
				}
				delete(peers, neighbor) // don't double-send via random
			}
		}
		if sent > 0 {
			dbgRumor.Printf("Pushed rumor via hypercube: %d dimensions, hop=%d", sent, hopCount)
			return // hypercube handled forwarding
		}
		// All hypercube neighbors dead — fall through to random
	}

	// Random fallback (origin push or degraded hypercube)
	targets := selectRandomPeers(peers, rp.tracker.config.Fanout)
	for _, peer := range targets {
		if err := peer.WriteRumor(payload, hopCount+1, 0xFF); err != nil {
			dbgRumor.Printf("Random send failed: %v", err)
			rp.writeErrors.Add(1)
		} else {
			rp.pushesRandom.Add(1)
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
