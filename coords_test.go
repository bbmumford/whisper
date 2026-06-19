// Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
// Queries: licensing@hstles.com
//
// Coverage ported from pollen pkg/coords/coords_test.go, converted to stdlib
// assertions to match the whisper package's test style.

package whisper

import (
	"math"
	"testing"
	"time"
)

func inDelta(t *testing.T, want, got, delta float64, msg string) {
	t.Helper()
	if math.Abs(want-got) > delta {
		t.Fatalf("%s: want %g, got %g (delta %g)", msg, want, got, delta)
	}
}

func TestDistance(t *testing.T) {
	a := Coord{X: 1, Y: 2, Height: 0.5}
	b := Coord{X: 4, Y: 6, Height: 0.3}
	inDelta(t, Distance(a, b), Distance(b, a), 1e-12, "distance is symmetric")

	// Same X/Y, height-only separation: distance == sum of heights.
	inDelta(t, 3.0, Distance(Coord{Height: 1.0}, Coord{Height: 2.0}), 1e-12, "distance includes heights")
	inDelta(t, 0.0, Distance(Coord{}, Coord{}), 1e-12, "zero coords -> zero distance")
}

func TestMovementDistance(t *testing.T) {
	c := Coord{X: 1.2, Y: -3.4, Height: 5.6}
	inDelta(t, 0.0, MovementDistance(c, c), 1e-12, "equal coords -> zero movement")
	inDelta(t, 3.0, MovementDistance(Coord{X: 10, Y: -2, Height: 1}, Coord{X: 10, Y: -2, Height: 4}), 1e-12, "movement includes height delta")
}

func TestRandomCoord(t *testing.T) {
	c1 := RandomCoord()
	if c1.Height != MinHeight {
		t.Fatalf("random coord height = %g, want %g", c1.Height, MinHeight)
	}
	if c1.X*c1.X+c1.Y*c1.Y > initRadius*initRadius+1e-9 {
		t.Fatal("random coord outside init radius")
	}
	if c2 := RandomCoord(); c1.X == c2.X && c1.Y == c2.Y {
		t.Fatal("two random coords should differ")
	}
}

func TestUpdateZeroAndNegativeRTTNoop(t *testing.T) {
	local := Coord{X: 1, Y: 2, Height: 0.5}
	localErr := 0.5
	for _, rtt := range []time.Duration{0, -5 * time.Millisecond} {
		s := Sample{RTT: rtt, PeerCoord: Coord{X: 5, Y: 5, Height: 0.1}, PeerErr: 1.0}
		c, e := Update(local, localErr, s)
		if c != local || e != localErr {
			t.Fatalf("rtt=%v must be a no-op; got coord=%+v err=%g", rtt, c, e)
		}
	}
}

func TestUpdateZeroMagnitudeRepulsion(t *testing.T) {
	local := Coord{X: 1, Y: 1, Height: MinHeight}
	s := Sample{RTT: 10 * time.Millisecond, PeerCoord: local, PeerErr: 0.5}
	c, _ := Update(local, 0.5, s)
	if c.X == local.X && c.Y == local.Y {
		t.Fatal("co-located nodes with RTT>0 should randomly repel")
	}
}

func TestUpdatePeerErrZeroFallback(t *testing.T) {
	local := Coord{Height: MinHeight}
	peer := Coord{X: 10, Height: MinHeight}
	c1, e1 := Update(local, 0.5, Sample{RTT: 20 * time.Millisecond, PeerCoord: peer, PeerErr: 0})
	c2, e2 := Update(local, 0.5, Sample{RTT: 20 * time.Millisecond, PeerCoord: peer, PeerErr: CeDefault})
	if c1 != c2 || e1 != e2 {
		t.Fatal("PeerErr=0 should behave identically to PeerErr=CeDefault")
	}
}

func TestConvergenceTwoNodes(t *testing.T) {
	a := Coord{}
	b := Coord{X: 100, Height: MinHeight}
	aErr, bErr := 1.0, 1.0
	rtt := 50 * time.Millisecond
	for range 200 {
		a, aErr = Update(a, aErr, Sample{RTT: rtt, PeerCoord: b, PeerErr: bErr})
		b, bErr = Update(b, bErr, Sample{RTT: rtt, PeerCoord: a, PeerErr: aErr})
	}
	inDelta(t, 50.0, Distance(a, b), 5.0, "two-node distance converges near 50ms RTT")
}

func TestConvergenceThreeNodes(t *testing.T) {
	a, b, c := Coord{}, Coord{X: 50}, Coord{Y: 50}
	aErr, bErr, cErr := 1.0, 1.0, 1.0
	for range 300 {
		a, aErr = Update(a, aErr, Sample{RTT: 10 * time.Millisecond, PeerCoord: b, PeerErr: bErr})
		a, aErr = Update(a, aErr, Sample{RTT: 25 * time.Millisecond, PeerCoord: c, PeerErr: cErr})
		b, bErr = Update(b, bErr, Sample{RTT: 10 * time.Millisecond, PeerCoord: a, PeerErr: aErr})
		b, bErr = Update(b, bErr, Sample{RTT: 20 * time.Millisecond, PeerCoord: c, PeerErr: cErr})
		c, cErr = Update(c, cErr, Sample{RTT: 25 * time.Millisecond, PeerCoord: a, PeerErr: aErr})
		c, cErr = Update(c, cErr, Sample{RTT: 20 * time.Millisecond, PeerCoord: b, PeerErr: bErr})
	}
	inDelta(t, 10.0, Distance(a, b), 5.0, "a-b near 10ms")
	inDelta(t, 20.0, Distance(b, c), 5.0, "b-c near 20ms")
	inDelta(t, 25.0, Distance(a, c), 5.0, "a-c near 25ms")
}

func TestClampingPreventsRunaway(t *testing.T) {
	local := Coord{X: MaxCoord - 1, Y: MaxCoord - 1, Height: 1}
	s := Sample{RTT: 999 * time.Second, PeerCoord: Coord{X: -MaxCoord, Y: -MaxCoord, Height: 1}, PeerErr: 1.0}
	c, _ := Update(local, 1.0, s)
	if math.Abs(c.X) > MaxCoord || math.Abs(c.Y) > MaxCoord || c.Height > MaxCoord {
		t.Fatalf("coordinate escaped clamp bounds: %+v", c)
	}
}

func TestHeightFloor(t *testing.T) {
	local := Coord{Height: MinHeight}
	s := Sample{RTT: time.Microsecond, PeerCoord: Coord{X: 0.001, Height: MinHeight}, PeerErr: 1.0}
	c, _ := Update(local, 1.0, s)
	if c.Height < MinHeight {
		t.Fatalf("height %g dropped below floor %g", c.Height, MinHeight)
	}
}

func TestErrorEstimateDecreases(t *testing.T) {
	local := Coord{}
	localErr := 1.0
	peer := Coord{X: 50, Height: MinHeight}
	for range 100 {
		local, localErr = Update(local, localErr, Sample{RTT: 50 * time.Millisecond, PeerCoord: peer, PeerErr: 0.5})
	}
	if localErr >= 0.5 {
		t.Fatalf("error estimate %g should decrease below 0.5 with consistent samples", localErr)
	}
}

func TestRandomInitLANConvergence(t *testing.T) {
	// Regression: random-init coords spread over a 10ms radius produce predicted
	// distances far exceeding LAN RTTs; the max(rtt,dist,MinRTTFloor) denominator
	// keeps per-sample error < 1.0 so the estimate still converges.
	a := Coord{X: -7, Y: 5, Height: MinHeight}
	b := Coord{X: 6, Y: -4, Height: MinHeight}
	aErr, bErr := 1.0, 1.0
	rtt := 2 * time.Millisecond
	for range 60 {
		a, aErr = Update(a, aErr, Sample{RTT: rtt, PeerCoord: b, PeerErr: bErr})
		b, bErr = Update(b, bErr, Sample{RTT: rtt, PeerCoord: a, PeerErr: aErr})
	}
	if aErr >= 0.9 || bErr >= 0.9 {
		t.Fatalf("LAN error should drop below 0.9 within 60 ticks; got a=%g b=%g", aErr, bErr)
	}
}
