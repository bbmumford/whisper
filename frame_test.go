/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// pipeConn wraps a net.Pipe pair as bidirectional net.Conn for the
// responder tests. One side writes frames, the other reads them.
func newPipeConns() (net.Conn, net.Conn) { return net.Pipe() }

// writeMagic puts a 2-byte magic on the wire so the responder's read
// loop wakes up and dispatches.
func writeMagic(t *testing.T, conn net.Conn, magic uint16) {
	t.Helper()
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], magic)
	if _, err := conn.Write(buf[:]); err != nil {
		t.Fatalf("write magic: %v", err)
	}
}

// TestRegisterFrameKind_CustomHandler verifies a consumer-registered
// frame kind fires on matching magic.
func TestRegisterFrameKind_CustomHandler(t *testing.T) {
	const customMagic uint16 = 0xABCD
	engine := NewEngine()

	var called atomic.Int32
	err := engine.RegisterFrameKind(customMagic, FrameHandlerFunc(
		func(_ context.Context, _ net.Conn, _ string) FrameAction {
			called.Add(1)
			return FrameReturn
		}))
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	a, b := newPipeConns()
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go func() {
		writeMagic(t, a, customMagic)
	}()

	if err := engine.RunResponder(ctx, b, "peer-1"); err != nil {
		t.Fatalf("RunResponder: %v", err)
	}
	if got := called.Load(); got != 1 {
		t.Fatalf("handler called %d times, want 1", got)
	}
}

// TestRegisterFrameKind_DuplicateRejected ensures two handlers can't
// claim the same magic — catches consumer wiring errors early.
func TestRegisterFrameKind_DuplicateRejected(t *testing.T) {
	const m uint16 = 0xABCD
	engine := NewEngine()
	if err := engine.RegisterFrameKind(m, FrameHandlerFunc(
		func(_ context.Context, _ net.Conn, _ string) FrameAction { return FrameContinue })); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := engine.RegisterFrameKind(m, FrameHandlerFunc(
		func(_ context.Context, _ net.Conn, _ string) FrameAction { return FrameContinue })); err == nil {
		t.Fatalf("duplicate register: expected error")
	}
}

// TestRunResponder_UnknownMagic exits with an error when a peer sends
// a magic no handler recognises.
func TestRunResponder_UnknownMagic(t *testing.T) {
	engine := NewEngine()
	a, b := newPipeConns()
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go func() {
		writeMagic(t, a, 0x9999) // no handler registered
	}()

	err := engine.RunResponder(ctx, b, "peer-1")
	if err == nil {
		t.Fatalf("expected unknown-magic error, got nil")
	}
}

// TestRunResponder_EventBus verifies SubscribeEvents delivers events
// from the default handlers (here: EventDigestMatch).
func TestRunResponder_EventBus(t *testing.T) {
	engine := NewEngine()
	events := make(chan GossipEvent, 8)
	engine.SubscribeEvents(events)

	// Install a FingerprintProvider so the G2 handler can match.
	WithFingerprintProvider(fixedFingerprint(0xAABB))(engine)

	a, b := newPipeConns()
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	go func() {
		// Write a G2 probe body that matches our fingerprint.
		if err := WriteDigestProbe(a, 0xAABB, 0); err != nil {
			t.Errorf("write probe: %v", err)
		}
		// Read the full 12-byte reply (magic + flags + fingerprint)
		// so the responder's Write doesn't block mid-frame.
		var reply [12]byte
		if _, err := io.ReadFull(a, reply[:]); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Logf("read reply: %v", err)
		}
	}()

	go func() { _ = engine.RunResponder(ctx, b, "peer-1") }()

	select {
	case ev := <-events:
		if ev.Kind != EventDigestMatch {
			t.Fatalf("expected EventDigestMatch, got %v", ev.Kind)
		}
		if ev.PeerNodeID != "peer-1" {
			t.Fatalf("peer = %q, want peer-1", ev.PeerNodeID)
		}
	case <-ctx.Done():
		t.Fatalf("no event within deadline")
	}
}

// fixedFingerprint is a FingerprintProvider that returns a constant.
type fixedFingerprint uint64

func (f fixedFingerprint) Fingerprint() uint64 { return uint64(f) }

// TestDefaultFatalClassifier covers the common error surfaces the
// library uses so migration retains the same semantics.
func TestDefaultFatalClassifier(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		fatal bool
	}{
		{"nil", nil, false},
		{"EOF", errors.New("EOF"), true},
		{"connection reset by peer", errors.New("read tcp: connection reset by peer"), true},
		{"broken pipe", errors.New("write tcp: broken pipe"), true},
		{"use of closed network connection", errors.New("use of closed network connection"), true},
		{"transient i/o", errors.New("i/o error: temporary"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := defaultFatalClassifier(c.err); got != c.fatal {
				t.Fatalf("got %v, want %v", got, c.fatal)
			}
		})
	}
}
