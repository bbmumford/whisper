/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestHypercube_ImmutableSelfID verifies that the hypercube constructor
// captures the local NodeID at build time and Rebuild() uses it to
// compute the local position. The previous mutable SetSelfID pattern
// shipped an intermediate state where selfPos was -1 until the setter
// ran, causing Neighbors() to return nil silently.
func TestHypercube_ImmutableSelfID(t *testing.T) {
	hc := NewHypercube("node-002")
	members := []string{"node-001", "node-002", "node-003", "node-004"}
	hc.Rebuild(members)

	if hc.Position() != 1 {
		t.Errorf("expected position 1, got %d", hc.Position())
	}
	if n := hc.Neighbors(); len(n) == 0 {
		t.Error("Neighbors() returned nil on a valid cube")
	}
	if !hc.Valid() {
		t.Error("Valid() returned false on a populated cube")
	}
}

// TestHypercube_EmptySelfIDSilentlyDisabled verifies that when the
// local NodeID isn't in the member list, Neighbors() returns nil.
// The agent's old startup path constructed the cube with selfID="",
// which caused rumor fanout to silently fall back to random-peer
// selection — the bug A-1 fixed by making selfID immutable and
// mandatory at construction time.
func TestHypercube_EmptySelfIDSilentlyDisabled(t *testing.T) {
	hc := NewHypercube("")
	hc.Rebuild([]string{"node-1", "node-2", "node-3"})
	if neighbors := hc.Neighbors(); neighbors != nil {
		t.Errorf("empty selfID must produce nil Neighbors, got %v", neighbors)
	}
}

// TestG1_NoG1CodecKeepsLegacyRegistrationOpen verifies that when a
// consumer wires a StateStore but NOT a G1Codec, the native G1
// handler does NOT install itself — leaving GossipMagic free for
// consumer-provided FrameHandlers. Protects the migration path where
// legacy consumers still ship a hand-rolled G1 handler.
func TestG1_NoG1CodecKeepsLegacyRegistrationOpen(t *testing.T) {
	store := newMemStateStore()
	engine := NewEngine(WithG1Store(store))

	// A consumer handler for GossipMagic should register cleanly —
	// without a G1Codec the native handler is NOT installed, so
	// GossipMagic is still free for consumer registration.
	err := engine.RegisterFrameKind(GossipMagic, FrameHandlerFunc(
		func(_ context.Context, _ net.Conn, _ string) FrameAction { return FrameReturn }))
	if err != nil {
		t.Fatalf("expected GossipMagic to be free when G1Codec not wired, got error: %v", err)
	}
}

// TestG1_StoreFingerprintFeedsG2 verifies that when a StateStore is
// wired without an explicit FingerprintProvider, the G2 digest
// handler still answers with the store's fingerprint. Protects the
// "StateStore IS the authoritative fingerprint" invariant.
func TestG1_StoreFingerprintFeedsG2(t *testing.T) {
	store := newMemStateStore()
	_ = store.Apply([]byte("record-alpha"))
	expected := store.Fingerprint()

	engine := NewEngine(
		WithG1Store(store),
		WithG1Codec(jsonG1Codec{}),
	)

	a, b := newPipeConns()
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _ = engine.RunResponder(ctx, b, "peer-fp") }()

	if err := WriteDigestProbe(a, 0, 0); err != nil {
		t.Fatalf("write probe: %v", err)
	}
	fp, _, err := readDigestReply(a)
	if err != nil {
		t.Fatalf("read probe reply: %v", err)
	}
	if fp != expected {
		t.Errorf("expected fingerprint %d, got %d", expected, fp)
	}
}

// TestG1_SeenIDTrackingPassesThroughCodec verifies that when
// WithSeenIDTracking is enabled, the codec-reported SeenNodeIDs
// flow into GossipResult for the observer. Protects the liveness
// tracking path.
func TestG1_SeenIDTrackingPassesThroughCodec(t *testing.T) {
	store := newMemStateStore()

	var obsSeen []string
	var obsMu sync.Mutex
	engine := NewEngine(
		WithG1Store(store),
		WithG1Codec(jsonG1Codec{}),
		WithSeenIDTracking(true),
		WithG1ExchangeObserver(func(res GossipResult, _ string) {
			obsMu.Lock()
			obsSeen = append(obsSeen, res.SeenNodeIDs...)
			obsMu.Unlock()
		}),
	)

	a, b := newPipeConns()
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _ = engine.RunResponder(ctx, b, "peer-seen") }()

	body, err := jsonG1Codec{}.EncodeExchange(
		[][]byte{[]byte("rec")},
		nil,
	)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Append SeenNodeIDs directly via raw JSON so our test codec
	// populates DecodedExchange.SeenNodeIDs.
	// (jsonG1Codec doesn't embed SeenNodeIDs on Encode; we patch the
	// body by decoding, enriching, and re-encoding.)
	enriched, _ := patchJSONExchangeNodeIDs(body, []string{"seen-1", "seen-2"})

	writeG1Frame(t, a, enriched)
	_ = readG1Frame(t, a)

	for i := 0; i < 50; i++ {
		obsMu.Lock()
		got := len(obsSeen)
		obsMu.Unlock()
		if got >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	obsMu.Lock()
	defer obsMu.Unlock()
	if len(obsSeen) != 2 {
		t.Errorf("expected 2 seen node IDs, got %d: %v", len(obsSeen), obsSeen)
	}
}

// TestG1_SeenIDTrackingDisabledDropsIDs verifies that without the
// WithSeenIDTracking flag the seen-IDs field stays empty — keeps the
// default zero-overhead path for consumers that don't care about
// liveness.
func TestG1_SeenIDTrackingDisabledDropsIDs(t *testing.T) {
	store := newMemStateStore()

	var obsSeen atomic.Int32
	engine := NewEngine(
		WithG1Store(store),
		WithG1Codec(jsonG1Codec{}),
		// No WithSeenIDTracking - default false
		WithG1ExchangeObserver(func(res GossipResult, _ string) {
			obsSeen.Store(int32(len(res.SeenNodeIDs)))
		}),
	)

	a, b := newPipeConns()
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _ = engine.RunResponder(ctx, b, "peer-nosee") }()

	body, _ := jsonG1Codec{}.EncodeExchange([][]byte{[]byte("rec")}, nil)
	enriched, _ := patchJSONExchangeNodeIDs(body, []string{"x", "y"})
	writeG1Frame(t, a, enriched)
	_ = readG1Frame(t, a)

	time.Sleep(50 * time.Millisecond)
	if got := obsSeen.Load(); got != 0 {
		t.Errorf("expected 0 seen node IDs without tracking, got %d", got)
	}
}

// patchJSONExchangeNodeIDs rewrites a jsonExchange body to add
// NodeIDs — test-only helper so we don't have to special-case the
// test codec's Encode signature.
func patchJSONExchangeNodeIDs(body []byte, ids []string) ([]byte, error) {
	decoded, err := jsonG1Codec{}.DecodeExchange(body)
	if err != nil {
		return nil, err
	}
	enriched := jsonExchange{Records: decoded.Records, Meta: decoded.Meta, NodeIDs: ids}
	return json.Marshal(enriched)
}

// TestG1_KeepaliveZeroLengthIgnored verifies a zero-length G1 body
// is accepted as a keepalive and doesn't disturb the store. Matches
// the legacy respondToG1Exchange behaviour the native handler
// replaced.
func TestG1_KeepaliveZeroLengthIgnored(t *testing.T) {
	store := newMemStateStore()
	_ = store.Apply([]byte("rec"))
	applies := store.applies

	engine := NewEngine(
		WithG1Store(store),
		WithG1Codec(jsonG1Codec{}),
	)

	a, b := newPipeConns()
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	go func() { _ = engine.RunResponder(ctx, b, "peer-ka") }()

	var hdr [6]byte
	binary.BigEndian.PutUint16(hdr[0:2], GossipMagic)
	binary.BigEndian.PutUint32(hdr[2:6], 0)
	if _, err := a.Write(hdr[:]); err != nil {
		t.Fatalf("write keepalive: %v", err)
	}

	// Give it a moment to process
	time.Sleep(100 * time.Millisecond)

	if store.applies != applies {
		t.Errorf("keepalive must not trigger Apply calls, applies went %d → %d", applies, store.applies)
	}
}
