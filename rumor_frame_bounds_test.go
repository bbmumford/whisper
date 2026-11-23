/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package whisper

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// rumorBody is a G3 frame after its magic: the stated length, hop and dimension,
// then body.
func rumorBody(stated uint32, body []byte) []byte {
	var hdr [6]byte
	binary.BigEndian.PutUint32(hdr[0:4], stated)
	hdr[4], hdr[5] = 1, 0
	return append(hdr[:], body...)
}

// TestReadRumorBody_RefusesAnOversizeLengthAfterTheHeader: the one bound is
// MaxRumorPayloadBytes. A frame stating exactly that length is read whole; one
// stating a byte more, or the largest length the header can hold, is refused
// having read only the 6-byte header, so no body is allocated or read for it.
func TestReadRumorBody_RefusesAnOversizeLengthAfterTheHeader(t *testing.T) {
	at := bytes.Repeat([]byte{0x5a}, MaxRumorPayloadBytes)
	payload, _, _, err := ReadRumorBody(bytes.NewReader(rumorBody(uint32(len(at)), at)))
	if err != nil || len(payload) != MaxRumorPayloadBytes {
		t.Fatalf("a payload of exactly MaxRumorPayloadBytes: %d bytes, %v", len(payload), err)
	}
	for _, stated := range []uint32{MaxRumorPayloadBytes + 1, math.MaxUint32} {
		cr := &countingReader{r: bytes.NewReader(rumorBody(stated, bytes.Repeat([]byte{0x5a}, 64)))}
		if _, _, _, err := ReadRumorBody(cr); err == nil {
			t.Errorf("stated length %d was read as a rumor", stated)
		}
		if cr.n != 6 {
			t.Errorf("stated length %d: read %d bytes, want the 6-byte header only", stated, cr.n)
		}
	}
}

// TestRumorHandler_AnOversizeFrameEndsTheStream: the responder's G3 handler fails
// the stream on an oversize length even under a classifier that calls every
// error transient, because the refused body's bytes are next on the stream. A
// frame hidden in those bytes is never read as a frame of its own.
func TestRumorHandler_AnOversizeFrameEndsTheStream(t *testing.T) {
	var applied atomic.Int32
	engine := NewEngine(
		WithRumor(NewRumorPusher(ServiceMeshRumorConfig(), func(p []byte) string { return string(p) })),
		WithRumorApplyFn(func([]byte) error { applied.Add(1); return nil }),
		WithFatalErrorClassifier(func(error) bool { return false }),
	)
	a, b := newPipeConns()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	var smuggled bytes.Buffer
	_ = binary.Write(&smuggled, binary.BigEndian, RumorMagic)
	smuggled.Write(rumorBody(8, []byte("smuggled")))
	var frame bytes.Buffer
	_ = binary.Write(&frame, binary.BigEndian, RumorMagic)
	frame.Write(rumorBody(MaxRumorPayloadBytes+1, smuggled.Bytes()))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = a.Write(frame.Bytes())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = b.SetDeadline(time.Now().Add(2 * time.Second))
	err := engine.RunResponder(ctx, b, "peer-1")
	_ = a.Close()
	wg.Wait()
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the responder kept the stream after an oversize frame (err=%v)", err)
	}
	if n := applied.Load(); n != 0 {
		t.Fatalf("a frame read out of a refused body was applied %d time(s)", n)
	}
}

// recordingPeer is a RumorPeerConn that records each write.
type recordingPeer struct {
	id     string
	writes atomic.Int32
}

func (p *recordingPeer) PeerNodeID() string { return p.id }

func (p *recordingPeer) WriteRumor([]byte, uint8, uint8) error {
	p.writes.Add(1)
	return nil
}

// TestRumorPusher_SendsNothingPastTheBound: both pusher entry points drop a
// payload past MaxRumorPayloadBytes before any peer is written or any send is
// counted, since every reader refuses it; a payload at the bound is still written.
func TestRumorPusher_SendsNothingPastTheBound(t *testing.T) {
	over := make([]byte, MaxRumorPayloadBytes+1)
	rp := NewRumorPusher(ServiceMeshRumorConfig(), func(p []byte) string { return string(p[:1]) })
	peer := &recordingPeer{id: "peer-1"}
	rp.RegisterPeer(peer.id, peer)
	rp.PushRumor(over, 0, 0)
	rp.PushRumorExcluding(over, 0, 0, "peer-2")
	if n := peer.writes.Load(); n != 0 {
		t.Fatalf("the pusher wrote an oversize rumor to a peer %d time(s)", n)
	}
	rp.PushRumor(make([]byte, MaxRumorPayloadBytes), 0, 0)
	if n := peer.writes.Load(); n != 1 {
		t.Errorf("a payload at the bound was written %d time(s), want 1", n)
	}
}
