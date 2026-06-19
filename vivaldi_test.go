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
