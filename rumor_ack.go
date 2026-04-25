/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// RumorACKMagic is the 2-byte prefix on G3-ACK frames sent back from
// rumor receiver to rumor sender to confirm delivery + apply. Lives
// on the same bidirectional rumor stream as G3 rumors themselves —
// receiver-side handlers write an ACK after a successful apply;
// sender-side reader loop dispatches ACKs into the pusher's
// pending-ACK tracker.
//
// Frame format: [magic 2][rumor_id 32] = 34 bytes total.
// Rumor IDs are content hashes (BLAKE3 / SHA-256 truncated) — the
// same value the rumor pusher's dedup tracker uses, so receiver
// and sender share key space.
const RumorACKMagic uint16 = 0x4735 // "G5" pun on the 5th gossip frame

// rumorACKHeaderSize is the fixed size of a G3-ACK frame: 2-byte
// magic + 32-byte rumor ID.
const rumorACKHeaderSize = 34

// rumorIDLen is the length of a rumor ID on the wire. Matches the
// hash output size used by the dedup tracker.
const rumorIDLen = 32

// WriteRumorACK serialises a G3-ACK frame to conn. Called by the
// rumor receiver after a successful apply to confirm delivery to
// the originator.
//
// rumorID must be exactly rumorIDLen bytes. Shorter / longer IDs
// are right-padded / truncated to keep the frame size constant.
func WriteRumorACK(conn net.Conn, rumorID []byte) error {
	var buf [rumorACKHeaderSize]byte
	binary.BigEndian.PutUint16(buf[0:2], RumorACKMagic)
	n := copy(buf[2:], rumorID)
	for i := n; i < rumorIDLen; i++ {
		buf[2+i] = 0
	}
	_, err := conn.Write(buf[:])
	return err
}

// ReadRumorACKBody reads the rumor ID following an already-consumed
// G3-ACK magic. Mirrors ReadRumorBody's contract.
func ReadRumorACKBody(r io.Reader) ([]byte, error) {
	var buf [rumorIDLen]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, fmt.Errorf("rumor-ack: read body: %w", err)
	}
	out := make([]byte, rumorIDLen)
	copy(out, buf[:])
	return out, nil
}

// pendingRumor tracks a single (rumorID, peerID) pair awaiting ACK.
// Held in the pusher's pending map until ACK arrives or retry budget
// is exhausted.
type pendingRumor struct {
	rumorID    string
	peerID     string
	payload    []byte
	hopCount   uint8
	dimension  uint8
	sentAt     time.Time
	retries    int
	maxRetries int
	timeout    time.Duration
}

// rumorACKTracker is the sender-side state for outstanding rumor
// deliveries. Keyed by (rumorID + peerID) so the same rumor sent to
// multiple peers tracks each delivery independently.
//
// Lifecycle:
//   - Track called from the rumor pusher's send path.
//   - Acked called from the receiver-of-ACK loop.
//   - sweepTimeouts called periodically; retries via the
//     pusher-supplied retry callback or drops on budget exhaustion.
type rumorACKTracker struct {
	mu       sync.Mutex
	pending  map[string]*pendingRumor
	stopCh   chan struct{}
	stopOnce sync.Once
}

// rumorACKKey combines rumor ID + peer ID into the tracker map key.
func rumorACKKey(rumorID, peerID string) string {
	return rumorID + "|" + peerID
}

// newRumorACKTracker returns a fresh tracker. Caller is responsible
// for invoking Stop on shutdown to release the sweeper goroutine.
func newRumorACKTracker() *rumorACKTracker {
	return &rumorACKTracker{
		pending: make(map[string]*pendingRumor),
		stopCh:  make(chan struct{}),
	}
}

// Track registers a rumor as awaiting ACK from a specific peer.
// timeout is the ACK deadline; on expiry, retry hooks fire. Returns
// false if the (rumorID, peerID) pair is already tracked — duplicate
// tracks are no-ops to keep retry budgets honest.
func (t *rumorACKTracker) Track(rumorID, peerID string, payload []byte,
	hopCount, dimension uint8, timeout time.Duration, maxRetries int) bool {
	if rumorID == "" || peerID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	key := rumorACKKey(rumorID, peerID)
	if _, exists := t.pending[key]; exists {
		return false
	}
	t.pending[key] = &pendingRumor{
		rumorID:    rumorID,
		peerID:     peerID,
		payload:    append([]byte(nil), payload...),
		hopCount:   hopCount,
		dimension:  dimension,
		sentAt:     time.Now(),
		maxRetries: maxRetries,
		timeout:    timeout,
	}
	return true
}

// Acked clears a pending rumor from the tracker. Called when the
// G3-ACK arrives. No-op for unknown keys (duplicate ACKs, late
// ACKs after retry-budget-exhaustion drop).
//
// Returns true when an entry was actually removed — useful for
// loss-rate accounting on the caller side.
func (t *rumorACKTracker) Acked(rumorID, peerID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := rumorACKKey(rumorID, peerID)
	if _, exists := t.pending[key]; !exists {
		return false
	}
	delete(t.pending, key)
	return true
}

// timedOutEntries returns pending rumors whose ACK deadline has
// passed. Caller decides whether to retry or drop based on retry
// budget. Returned entries are removed from the pending map — the
// caller must re-Track if they retry (with bumped retries count).
func (t *rumorACKTracker) timedOutEntries() []*pendingRumor {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	var out []*pendingRumor
	for key, pr := range t.pending {
		if now.Sub(pr.sentAt) > pr.timeout {
			out = append(out, pr)
			delete(t.pending, key)
		}
	}
	return out
}

// PendingCount is the number of outstanding-ACK rumors. Exposed for
// observability via Stats.
func (t *rumorACKTracker) PendingCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

// Stop signals the tracker's sweeper to exit. Idempotent.
func (t *rumorACKTracker) Stop() {
	t.stopOnce.Do(func() {
		close(t.stopCh)
	})
}

// peerLossTracker accumulates per-peer rumor-ACK miss rate over a
// sliding window so the pusher can adapt fanout. ~100-rumor window
// — large enough to be statistically meaningful, small enough to
// react within seconds on a churning peer.
type peerLossTracker struct {
	mu     sync.Mutex
	stats  map[string]*peerLossStats
	window int
}

// peerLossStats holds the rolling counters for a single peer.
type peerLossStats struct {
	sent   int
	missed int // rumors that exhausted retry budget before ACK
}

// newPeerLossTracker returns a tracker with the given sliding window.
func newPeerLossTracker(window int) *peerLossTracker {
	if window <= 0 {
		window = 100
	}
	return &peerLossTracker{
		stats:  map[string]*peerLossStats{},
		window: window,
	}
}

// RecordSent increments the sent counter for a peer.
func (l *peerLossTracker) RecordSent(peerID string) {
	if peerID == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.stats[peerID]
	if st == nil {
		st = &peerLossStats{}
		l.stats[peerID] = st
	}
	st.sent++
	if st.sent >= l.window*2 {
		// Halve to keep the window bounded while preserving ratio.
		st.sent /= 2
		st.missed /= 2
	}
}

// RecordMiss increments the miss counter for a peer (fired when a
// rumor exhausts retry budget without an ACK).
func (l *peerLossTracker) RecordMiss(peerID string) {
	if peerID == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.stats[peerID]
	if st == nil {
		st = &peerLossStats{}
		l.stats[peerID] = st
	}
	st.missed++
}

// LossRate returns the recent miss / sent ratio for a peer. Returns
// 0 if the peer has fewer than 10 sent rumors (not enough samples).
func (l *peerLossTracker) LossRate(peerID string) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.stats[peerID]
	if st == nil || st.sent < 10 {
		return 0
	}
	return float64(st.missed) / float64(st.sent)
}
