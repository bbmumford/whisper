/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"
)

// TestHandleRumorFrame_ReturnsTheReadError: an oversize frame and a truncated
// one each return their read error to the caller, which ends the stream on it,
// and nothing is marked seen, applied or forwarded for them; a whole frame
// returns nil and is applied.
func TestHandleRumorFrame_ReturnsTheReadError(t *testing.T) {
	tracker := NewRumorTracker(ServiceMeshRumorConfig())
	applied := 0
	apply := func([]byte) error { applied++; return nil }
	id := func(p []byte) string { return string(p) }

	err := HandleRumorFrame(bytes.NewReader(rumorBody(MaxRumorPayloadBytes+1, nil)), tracker, nil, "peer-1", id, apply)
	if !errors.Is(err, ErrRumorPayloadTooLarge) {
		t.Errorf("an oversize frame returned %v, want ErrRumorPayloadTooLarge", err)
	}
	if err := HandleRumorFrame(bytes.NewReader([]byte{0, 0}), tracker, nil, "peer-1", id, apply); err == nil {
		t.Error("a truncated header returned nil")
	}
	if applied != 0 {
		t.Fatalf("a refused frame was applied %d time(s)", applied)
	}

	if err := HandleRumorFrame(bytes.NewReader(rumorBody(5, []byte("hello"))), tracker, nil, "peer-1", id, apply); err != nil {
		t.Fatalf("a whole frame returned %v", err)
	}
	if applied != 1 || !tracker.IsSeen("hello") {
		t.Errorf("a whole frame: applied %d, seen %v; want applied once and seen", applied, tracker.IsSeen("hello"))
	}
}

// TestWriteRumor_RefusesPastTheBoundWritingNothing: WriteRumor refuses an oversize
// payload as ErrRumorPayloadTooLarge having written no byte, since every reader
// refuses the frame.
func TestWriteRumor_RefusesPastTheBoundWritingNothing(t *testing.T) {
	a, b := newPipeConns()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	_ = a.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	if err := WriteRumor(a, make([]byte, MaxRumorPayloadBytes+1), 0, 0); !errors.Is(err, ErrRumorPayloadTooLarge) {
		t.Errorf("WriteRumor of an oversize payload: %v, want ErrRumorPayloadTooLarge", err)
	}
	_ = b.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if n, _ := b.Read(make([]byte, 8)); n != 0 {
		t.Errorf("WriteRumor wrote %d bytes of a refused payload", n)
	}
}

// TestReadRumorBody_NamesTheBoundItRefused: the refusal of an oversize length is
// ErrRumorPayloadTooLarge, the error the responder's rumor handler and
// HandleRumorFrame's caller end the stream on.
func TestReadRumorBody_NamesTheBoundItRefused(t *testing.T) {
	for _, stated := range []uint32{MaxRumorPayloadBytes + 1, math.MaxUint32} {
		if _, _, _, err := ReadRumorBody(bytes.NewReader(rumorBody(stated, nil))); !errors.Is(err, ErrRumorPayloadTooLarge) {
			t.Errorf("stated length %d: err=%v, want ErrRumorPayloadTooLarge", stated, err)
		}
	}
}
