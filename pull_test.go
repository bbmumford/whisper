/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"
)

// fakeConn wraps bytes.Buffer to implement net.Conn for WritePullRequest
// tests. We only need Write; the other net.Conn methods are unused but
// must satisfy the interface.
type fakePullConn struct {
	bytes.Buffer
}

func (f *fakePullConn) Read(b []byte) (int, error)         { return f.Buffer.Read(b) }
func (f *fakePullConn) Close() error                       { return nil }
func (f *fakePullConn) LocalAddr() net.Addr                { return nil }
func (f *fakePullConn) RemoteAddr() net.Addr               { return nil }
func (f *fakePullConn) SetDeadline(t time.Time) error      { return nil }
func (f *fakePullConn) SetReadDeadline(t time.Time) error  { return nil }
func (f *fakePullConn) SetWriteDeadline(t time.Time) error { return nil }

// Round-trip: WritePullRequest → consume magic → ReadPullRequestBody
// produces the same request. Verifies the wire format is self-consistent.
func TestPullRequest_WriteReadRoundTrip(t *testing.T) {
	req := PullRequest{
		Topic:           "lad/members",
		SinceWatermark:  time.Unix(1_700_000_000, 0),
		MaxResponseSize: 512 * 1024,
	}

	var conn fakePullConn
	if err := WritePullRequest(&conn, req); err != nil {
		t.Fatalf("WritePullRequest: %v", err)
	}

	// Consume magic (responder multiplexer contract).
	var magic [2]byte
	if _, err := conn.Read(magic[:]); err != nil {
		t.Fatalf("read magic: %v", err)
	}
	if got := binary.BigEndian.Uint16(magic[:]); got != PullMagic {
		t.Errorf("magic: got 0x%x, want 0x%x", got, PullMagic)
	}

	got, err := ReadPullRequestBody(&conn)
	if err != nil {
		t.Fatalf("ReadPullRequestBody: %v", err)
	}
	if got.Topic != req.Topic {
		t.Errorf("Topic: got %q, want %q", got.Topic, req.Topic)
	}
	if !got.SinceWatermark.Equal(req.SinceWatermark) {
		t.Errorf("SinceWatermark: got %v, want %v", got.SinceWatermark, req.SinceWatermark)
	}
	if got.MaxResponseSize != req.MaxResponseSize {
		t.Errorf("MaxResponseSize: got %d, want %d", got.MaxResponseSize, req.MaxResponseSize)
	}
}

// Zero SinceWatermark encodes as 0 nanoseconds and decodes back to a
// zero time.Time (full-sync request).
func TestPullRequest_ZeroWatermarkFullSync(t *testing.T) {
	req := PullRequest{Topic: "x", MaxResponseSize: 1024}

	var conn fakePullConn
	if err := WritePullRequest(&conn, req); err != nil {
		t.Fatalf("WritePullRequest: %v", err)
	}
	conn.Next(2) // skip magic

	got, err := ReadPullRequestBody(&conn)
	if err != nil {
		t.Fatalf("ReadPullRequestBody: %v", err)
	}
	if !got.SinceWatermark.IsZero() {
		t.Errorf("zero input decoded to non-zero time: %v", got.SinceWatermark)
	}
}

// Topic length cap is enforced at both write and read sides.
func TestPullRequest_TopicTooLongRejected(t *testing.T) {
	tooLong := make([]byte, MaxPullTopicLen+1)
	for i := range tooLong {
		tooLong[i] = 'a'
	}
	var conn fakePullConn
	err := WritePullRequest(&conn, PullRequest{Topic: string(tooLong)})
	if err == nil {
		t.Fatal("expected error for oversized topic, got nil")
	}
}

// HandlePullRequest respects the rate-limiter gate: when allowFn returns
// false, fetch is never called and an error surfaces.
func TestHandlePullRequest_RateLimiterBlocks(t *testing.T) {
	fetchCalls := 0
	fetch := func(topic string, since time.Time, max int) ([]byte, error) {
		fetchCalls++
		return nil, nil
	}
	resp, err := HandlePullRequest(
		PullRequest{Topic: "x"},
		func() bool { return false }, // rate-limited
		fetch,
	)
	if err == nil {
		t.Fatal("expected rate-limited error, got nil")
	}
	if resp != nil {
		t.Errorf("expected nil response when rate-limited, got %d bytes", len(resp))
	}
	if fetchCalls != 0 {
		t.Errorf("fetch should not run when rate-limited, got %d calls", fetchCalls)
	}
}

// Response size cap is enforced: fetch returning more than the cap is
// surfaced as an error rather than truncated silently.
func TestHandlePullRequest_ResponseTooBig(t *testing.T) {
	fetch := func(topic string, since time.Time, max int) ([]byte, error) {
		// Return 2KB even though caller caps at 1KB.
		return make([]byte, 2048), nil
	}
	resp, err := HandlePullRequest(
		PullRequest{Topic: "x", MaxResponseSize: 1024},
		func() bool { return true },
		fetch,
	)
	if err == nil {
		t.Fatal("expected cap error")
	}
	_ = resp
}

// When MaxResponseSize is 0, DefaultPullResponseMaxSize applies.
func TestHandlePullRequest_DefaultCap(t *testing.T) {
	var capSeen int
	fetch := func(topic string, since time.Time, max int) ([]byte, error) {
		capSeen = max
		return []byte("ok"), nil
	}
	_, err := HandlePullRequest(
		PullRequest{Topic: "x", MaxResponseSize: 0},
		func() bool { return true },
		fetch,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capSeen != int(DefaultPullResponseMaxSize) {
		t.Errorf("cap passed to fetch: got %d, want %d", capSeen, DefaultPullResponseMaxSize)
	}
}

// Helper: assert fmt.Errorf wrapper doesn't panic on partial-read inputs.
func TestReadPullRequestBody_TruncatedInput(t *testing.T) {
	// Only 3 bytes past the magic — not enough even for the topic-len
	// prefix + empty topic + tail.
	r := bytes.NewReader([]byte{0x00}) // topicLen=0 but no tail
	_, err := ReadPullRequestBody(r)
	if err == nil {
		t.Fatal("expected error on truncated input, got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("tail")) &&
		!bytes.Contains([]byte(err.Error()), []byte("topic")) {
		t.Errorf("unexpected error shape: %v", err)
	}
}

// Ensure pull magic doesn't collide with existing gossip magics.
func TestPullMagic_NoCollision(t *testing.T) {
	if PullMagic == GossipMagic {
		t.Errorf("PullMagic collides with GossipMagic (G1): 0x%x", PullMagic)
	}
	if PullMagic == DigestMagic {
		t.Errorf("PullMagic collides with DigestMagic (G2): 0x%x", PullMagic)
	}
	if PullMagic == RumorMagic {
		t.Errorf("PullMagic collides with RumorMagic (G3): 0x%x", PullMagic)
	}
	if PullMagic != 0x4734 {
		t.Errorf("PullMagic: got 0x%x, want 0x4734 (G4)", PullMagic)
	}
	// Silence unused-import in case formatting moves things around.
	_ = fmt.Sprintf
}
