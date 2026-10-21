/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"bytes"
	"encoding/binary"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"path/filepath"
	"strings"
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

// whisperFrameMagic is one 2-byte frame magic the package defines: the
// value its writer puts in the frame's first two bytes, and the value
// Engine.RunResponder picks a handler by and consumeReconcileMagic checks.
type whisperFrameMagic struct {
	name  string // the constant's identifier
	value uint16 // the constant
	wire  uint16 // the value a peer writes and dispatches on; a change here changes both ends' wire
}

// whisperFrameMagics lists every frame magic in the package with the
// value peers rely on. Engine.RunResponder dispatches on the magic alone,
// looking each frame up in one handler map, so two frames sharing a value
// would be read as each other.
func whisperFrameMagics() []whisperFrameMagic {
	return []whisperFrameMagic{
		{"GossipMagic", GossipMagic, 0x4731},             // G1
		{"DigestMagic", DigestMagic, 0x4732},             // G2
		{"DigestBucketMagic", DigestBucketMagic, 0x4742}, // GB
		{"RumorMagic", RumorMagic, 0x4733},               // G3
		{"ReconcileMagic", ReconcileMagic, 0x4734},       // G4 reconcile
		{"PullMagic", PullMagic, 0x4737},                 // bounded record pull
		{"RumorACKMagic", RumorACKMagic, 0x4735},         // G5
		{"SnapshotMagic", SnapshotMagic, 0x4736},         // G6
	}
}

// TestPullMagic_NoCollision fails when any two whisper frame magics
// share a value, when a magic is not the value its peers expect, or when
// the package defines a frame magic this list does not name.
func TestPullMagic_NoCollision(t *testing.T) {
	magics := whisperFrameMagics()
	for i := range magics {
		for j := i + 1; j < len(magics); j++ {
			if magics[i].value == magics[j].value {
				t.Errorf("%s and %s share magic 0x%04X", magics[i].name, magics[j].name, magics[i].value)
			}
		}
		if magics[i].value != magics[i].wire {
			t.Errorf("%s: got 0x%04X, want 0x%04X", magics[i].name, magics[i].value, magics[i].wire)
		}
	}

	listed := make(map[string]bool, len(magics))
	for _, m := range magics {
		listed[m.name] = true
	}
	for _, name := range packageMagicConsts(t) {
		if !listed[name] {
			t.Errorf("%s is a frame magic missing from whisperFrameMagics, so no collision check covers it", name)
		}
	}
}

// packageMagicConsts returns the name of every package-level constant in
// the package's non-test sources whose identifier ends in "Magic": the
// naming every frame magic in this package carries. Reading the sources
// keeps TestPullMagic_NoCollision from passing over a magic added later.
func packageMagicConsts(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob package sources: %v", err)
	}
	fset := token.NewFileSet()
	seen := map[string]bool{}
	var names []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		if file.Name.Name != "whisper" {
			continue
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				for _, id := range spec.(*ast.ValueSpec).Names {
					if strings.HasSuffix(id.Name, "Magic") && !seen[id.Name] {
						seen[id.Name] = true
						names = append(names, id.Name)
					}
				}
			}
		}
	}
	if len(names) == 0 {
		t.Fatal("no frame magic constants found in the package sources")
	}
	return names
}
