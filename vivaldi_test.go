// Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
// Queries: licensing@hstles.com

package whisper

import (
	"testing"
	"time"
)

func TestVivaldiNewSeed(t *testing.T) {
	v := NewVivaldiCoords()
	c, e := v.Self()
	if c.Height != MinHeight {
		t.Fatalf("seed height = %g, want %g", c.Height, MinHeight)
	}
	if e != CeDefault {
		t.Fatalf("seed error = %g, want %g", e, CeDefault)
	}
}

func TestVivaldiObserveRecordsPeer(t *testing.T) {
	v := NewVivaldiCoords()
	peer := Coord{X: 30, Height: MinHeight}
	v.Observe("peerA", 30*time.Millisecond, peer, 0.4)

	got, ok := v.PeerCoord("peerA")
	if !ok || got != peer {
		t.Fatalf("PeerCoord = (%+v,%v), want (%+v,true)", got, ok, peer)
	}
	if _, ok := v.PredictRTT("peerA"); !ok {
		t.Fatal("PredictRTT should be known after Observe")
	}
	if _, ok := v.PredictRTT("unknown"); ok {
		t.Fatal("PredictRTT for an unobserved peer must be ok=false")
	}
}

func TestVivaldiEmptyPeerIDStillUpdatesSelf(t *testing.T) {
	v := NewVivaldiCoords()
	before, _ := v.Self()
	v.Observe("", 50*time.Millisecond, Coord{X: 200, Height: MinHeight}, 0.5)
	after, _ := v.Self()
	if before == after {
		t.Fatal("Observe with empty peerID should still move the self coordinate")
	}
	if len(v.peers) != 0 {
		t.Fatal("empty peerID must not record a peer entry")
	}
}

func TestVivaldiForget(t *testing.T) {
	v := NewVivaldiCoords()
	v.Observe("peerA", 10*time.Millisecond, Coord{X: 10, Height: MinHeight}, 0.5)
	v.Forget("peerA")
	if _, ok := v.PeerCoord("peerA"); ok {
		t.Fatal("PeerCoord should be gone after Forget")
	}
	if _, ok := v.PredictRTT("peerA"); ok {
		t.Fatal("PredictRTT should be unknown after Forget")
	}
}

// TestVivaldiConverges drives two wrappers observing each other at a fixed RTT
// and asserts the predicted RTT settles near the true value.
func TestVivaldiConverges(t *testing.T) {
	a := NewVivaldiCoords()
	b := NewVivaldiCoords()
	const rtt = 40 * time.Millisecond
	for range 300 {
		bc, be := b.Self()
		a.Observe("b", rtt, bc, be)
		ac, ae := a.Self()
		b.Observe("a", rtt, ac, ae)
	}
	pred, ok := a.PredictRTT("b")
	if !ok {
		t.Fatal("a should have a prediction for b")
	}
	if pred < 30 || pred > 50 {
		t.Fatalf("predicted RTT %g should converge near 40ms", pred)
	}
}

// TestVivaldiGossipRendezvous covers the decode->sampler handoff: RecordGossip
// stores a peer's coordinate, PeerGossip retrieves it, empty peerID is a no-op.
func TestVivaldiGossipRendezvous(t *testing.T) {
	v := NewVivaldiCoords()
	if _, _, ok := v.PeerGossip("p"); ok {
		t.Fatal("unknown peer gossip must be ok=false")
	}
	coord := Coord{X: 12, Y: -3, Height: MinHeight}
	v.RecordGossip("p", coord, 0.4)
	gc, ge, ok := v.PeerGossip("p")
	if !ok || gc != coord || ge != 0.4 {
		t.Fatalf("gossip round-trip mismatch: (%+v,%g,%v)", gc, ge, ok)
	}
	v.RecordGossip("", coord, 0.4) // empty peerID is a no-op
	if len(v.peers) != 1 {
		t.Fatal("empty peerID must not record a peer")
	}
}

// TestRouteToFarCoordTiebreak proves the stage-d coord tie-break: with members
// A,B,C,D (positions 0..3), routing from A to D leaves B(1) and C(2) both one
// XOR-bit from D — a tie. The coord tie-break forwards to whichever is closer in
// latency space; without coords it falls to the first (lowest) dimension.
func TestRouteToFarCoordTiebreak(t *testing.T) {
	members := []string{"A", "B", "C", "D"}
	coords := map[string]Coord{
		"A": {Height: MinHeight},
		"B": {X: 100, Height: MinHeight}, // far from D
		"C": {X: 5, Height: MinHeight},   // near D
		"D": {X: 6, Height: MinHeight},
	}

	h := NewHypercubeExt("A", HypercubeOptions{
		CoordOf: func(id string) (Coord, bool) { c, ok := coords[id]; return c, ok },
	})
	h.Rebuild(members)
	if hop, ok := h.RouteToFar("D"); !ok || hop != "C" {
		t.Fatalf("coord tie-break should forward to C (latency-closest to D), got %q ok=%v", hop, ok)
	}

	bare := NewHypercubeExt("A", HypercubeOptions{})
	bare.Rebuild(members)
	if hop, ok := bare.RouteToFar("D"); !ok || hop != "B" {
		t.Fatalf("without coords the tie falls to the first dimension (B), got %q ok=%v", hop, ok)
	}
}
