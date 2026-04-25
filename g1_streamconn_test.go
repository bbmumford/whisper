/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper_test

// This is the integration test that catches the SetDeadline-mid-frame
// class of bug. The unit tests in g1_test.go use net.Pipe which has
// byte-stream semantics — every Read returns whatever bytes are
// available without buffering across Receive boundaries. Production
// uses aether.Stream via adapter.StreamConn, where each Send is a
// discrete message and Read consumes them via Receive() with
// per-Receive buffering. Bugs that interact with the buffer (e.g.
// SetDeadline flushing it mid-frame) only surface against the real
// StreamConn semantics.
//
// This test wires up a fake aether.Stream that preserves message
// boundaries (one Send → one Receive) and runs the full
// initiator/responder G1 round-trip through a real adapter.StreamConn.
// If anyone reintroduces a SetDeadline call inside the G1 handler,
// or any other mid-frame buffer mutation, this test fails because
// the responder reads a misaligned length and rejects the frame.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	aether "github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	"github.com/bbmumford/whisper"
)

// fakeAetherStream pairs two queues so a Send on one end becomes a
// Receive on the other. Each queued slice is a discrete message —
// adapter.StreamConn's Read pulls one queued message per underlying
// Receive call, which is the property that exercises the buffer-
// flush bug.
type fakeAetherStream struct {
	id       uint64
	in       chan []byte // messages this side reads
	out      chan []byte // messages this side writes (= peer's `in`)
	closed   chan struct{}
	closeMu  sync.Mutex
	closeErr error
}

// newFakeStreamPair returns two streams already wired together — Send
// on one shows up on the other's Receive in send order.
func newFakeStreamPair(streamID uint64) (*fakeAetherStream, *fakeAetherStream) {
	a2b := make(chan []byte, 64)
	b2a := make(chan []byte, 64)
	closed := make(chan struct{})
	a := &fakeAetherStream{id: streamID, in: b2a, out: a2b, closed: closed}
	b := &fakeAetherStream{id: streamID, in: a2b, out: b2a, closed: closed}
	return a, b
}

func (f *fakeAetherStream) StreamID() uint64 { return f.id }

func (f *fakeAetherStream) Send(ctx context.Context, data []byte) error {
	cp := append([]byte(nil), data...)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.closed:
		return errors.New("stream closed")
	case f.out <- cp:
		return nil
	}
}

func (f *fakeAetherStream) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.closed:
		return nil, io.EOF
	case data, ok := <-f.in:
		if !ok {
			return nil, io.EOF
		}
		return data, nil
	}
}

func (f *fakeAetherStream) Close() error {
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	select {
	case <-f.closed:
	default:
		close(f.closed)
	}
	return f.closeErr
}

func (f *fakeAetherStream) Reset(_ aether.ResetReason) error      { return f.Close() }
func (f *fakeAetherStream) SetPriority(_ uint8, _ uint64)          {}
func (f *fakeAetherStream) Config() aether.StreamConfig            { return aether.StreamConfig{StreamID: f.id} }
func (f *fakeAetherStream) IsOpen() bool {
	select {
	case <-f.closed:
		return false
	default:
		return true
	}
}
func (f *fakeAetherStream) Conn() net.Conn { return adapter.NewStreamConn(f) }

// TestG1NativeHandler_OverStreamConn drives an end-to-end G1 exchange
// across a real adapter.StreamConn pair so the responder reads its
// length header and body across discrete aether messages — same
// shape as the production noise-stream path that exposed the
// SetDeadline-flush regression.
func TestG1NativeHandler_OverStreamConn(t *testing.T) {
	store := newMemStateStoreLocal()
	_ = store.Apply([]byte("local-1"))
	_ = store.Apply([]byte("local-2"))

	codec := jsonCodec{}

	var observed whisper.GossipResult
	var observeMu sync.Mutex
	engine := whisper.NewEngine(
		whisper.WithG1Store(store),
		whisper.WithG1Codec(codec),
		whisper.WithG1ExchangeObserver(func(res whisper.GossipResult, _ string) {
			observeMu.Lock()
			observed = res
			observeMu.Unlock()
		}),
	)

	streamA, streamB := newFakeStreamPair(0)
	connInit := adapter.NewStreamConn(streamA)
	connResp := adapter.NewStreamConn(streamB)
	defer streamA.Close()
	defer streamB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Responder runs in the background.
	go func() { _ = engine.RunResponder(ctx, connResp, "peer-stream") }()

	// Initiator writes [magic][length] in ONE message and [body] in a
	// SECOND message — exactly what GossipOverConn does. This is the
	// shape that exposes the buffer-flush bug: after RunResponder reads
	// 2 bytes of magic, the remaining 4 bytes of length are buffered in
	// StreamConn.buf. Any mid-frame SetDeadline in the handler would
	// drop those buffered bytes and the next Read would fetch message 2
	// (the body), reading its first 4 bytes as the length.
	body, err := codec.EncodeExchange([][]byte{[]byte("remote-1")}, &whisper.ExchangeMeta{CacheFingerprint: 99})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Frame 1: magic + length, sent as one aether message.
	hdr := make([]byte, 6)
	binary.BigEndian.PutUint16(hdr[0:2], whisper.GossipMagic)
	binary.BigEndian.PutUint32(hdr[2:6], uint32(len(body)))
	if _, err := connInit.Write(hdr); err != nil {
		t.Fatalf("write hdr: %v", err)
	}
	// Frame 2: body, separate message.
	if _, err := connInit.Write(body); err != nil {
		t.Fatalf("write body: %v", err)
	}

	// Read the responder's reply (also two messages: header + body).
	var replyHdr [6]byte
	if _, err := io.ReadFull(connInit, replyHdr[:]); err != nil {
		t.Fatalf("read reply hdr: %v", err)
	}
	if magic := binary.BigEndian.Uint16(replyHdr[0:2]); magic != whisper.GossipMagic {
		t.Fatalf("reply magic 0x%04X, want 0x%04X", magic, whisper.GossipMagic)
	}
	replyLen := binary.BigEndian.Uint32(replyHdr[2:6])
	if replyLen == 0 || replyLen > 1<<20 {
		t.Fatalf("reply len out of range: %d", replyLen)
	}
	replyBody := make([]byte, replyLen)
	if _, err := io.ReadFull(connInit, replyBody); err != nil {
		t.Fatalf("read reply body: %v", err)
	}

	decoded, err := codec.DecodeExchange(replyBody)
	if err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	// Reply contains the responder's snapshot AFTER applying the
	// inbound record: the 2 originals + the just-applied remote =
	// 3 records.
	if got := len(decoded.Records); got != 3 {
		t.Fatalf("reply expected 3 records (2 local + 1 just-applied remote), got %d", got)
	}
	if decoded.Meta == nil || decoded.Meta.CacheFingerprint == 0 {
		t.Errorf("reply meta missing fingerprint")
	}

	// Wait for observer to fire (handler runs concurrently).
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		observeMu.Lock()
		applied := observed.RecordsApplied
		observeMu.Unlock()
		if applied >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	observeMu.Lock()
	defer observeMu.Unlock()
	if observed.RecordsApplied != 1 {
		t.Errorf("expected 1 record applied, observer saw %d", observed.RecordsApplied)
	}
	if store.applies != 3 {
		t.Errorf("store expected 3 total applies, got %d", store.applies)
	}
}

// TestG1NativeHandler_RepeatedExchangesOverStreamConn drives several
// back-to-back exchanges over the same StreamConn pair so the
// responder loop's iteration boundaries get exercised. Catches bugs
// where state from one exchange (e.g. a reset deadline, a sticky
// buffer) leaks into the next.
func TestG1NativeHandler_RepeatedExchangesOverStreamConn(t *testing.T) {
	store := newMemStateStoreLocal()
	codec := jsonCodec{}

	engine := whisper.NewEngine(
		whisper.WithG1Store(store),
		whisper.WithG1Codec(codec),
	)

	streamA, streamB := newFakeStreamPair(0)
	connInit := adapter.NewStreamConn(streamA)
	connResp := adapter.NewStreamConn(streamB)
	defer streamA.Close()
	defer streamB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = engine.RunResponder(ctx, connResp, "peer-iter") }()

	for i := 0; i < 5; i++ {
		body, err := codec.EncodeExchange(
			[][]byte{[]byte("rec-" + string(rune('A'+i)))},
			&whisper.ExchangeMeta{CacheFingerprint: uint64(i + 1)},
		)
		if err != nil {
			t.Fatalf("iter %d encode: %v", i, err)
		}
		hdr := make([]byte, 6)
		binary.BigEndian.PutUint16(hdr[0:2], whisper.GossipMagic)
		binary.BigEndian.PutUint32(hdr[2:6], uint32(len(body)))
		if _, err := connInit.Write(hdr); err != nil {
			t.Fatalf("iter %d write hdr: %v", i, err)
		}
		if _, err := connInit.Write(body); err != nil {
			t.Fatalf("iter %d write body: %v", i, err)
		}

		var replyHdr [6]byte
		if _, err := io.ReadFull(connInit, replyHdr[:]); err != nil {
			t.Fatalf("iter %d read reply hdr: %v", i, err)
		}
		replyLen := binary.BigEndian.Uint32(replyHdr[2:6])
		if replyLen == 0 || replyLen > 1<<20 {
			t.Fatalf("iter %d reply len %d out of range", i, replyLen)
		}
		replyBody := make([]byte, replyLen)
		if _, err := io.ReadFull(connInit, replyBody); err != nil {
			t.Fatalf("iter %d read reply body: %v", i, err)
		}
	}

	if store.applies != 5 {
		t.Errorf("expected 5 records applied across iterations, got %d", store.applies)
	}
}

// --- Local copies of the test fixtures from g1_test.go ---
//
// g1_test.go lives in `package whisper` (internal test); this file is
// `package whisper_test` (external) so it can drive the engine through
// its public API. The tiny fixtures are duplicated rather than exported
// so the production package surface stays small.

type jsonCodec struct{}

type jsonExchangeBody struct {
	Records [][]byte              `json:"r"`
	Meta    *whisper.ExchangeMeta `json:"m,omitempty"`
	NodeIDs []string              `json:"n,omitempty"`
}

func (jsonCodec) EncodeExchange(records [][]byte, meta *whisper.ExchangeMeta) ([]byte, error) {
	return json.Marshal(jsonExchangeBody{Records: records, Meta: meta})
}

func (jsonCodec) DecodeExchange(body []byte) (whisper.DecodedExchange, error) {
	var e jsonExchangeBody
	if err := json.Unmarshal(body, &e); err != nil {
		return whisper.DecodedExchange{}, err
	}
	return whisper.DecodedExchange{Records: e.Records, Meta: e.Meta, SeenNodeIDs: e.NodeIDs}, nil
}

type memStateStoreLocal struct {
	mu      sync.Mutex
	records map[string][]byte
	applies int
}

func newMemStateStoreLocal() *memStateStoreLocal {
	return &memStateStoreLocal{records: make(map[string][]byte)}
}

func (s *memStateStoreLocal) key(data []byte) string {
	if len(data) >= 8 {
		return string(data[:8])
	}
	return string(data)
}

func (s *memStateStoreLocal) Fingerprint() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var fp uint64
	for k := range s.records {
		for i := 0; i < len(k) && i < 8; i++ {
			fp ^= uint64(k[i]) << (uint(i) * 8)
		}
	}
	return fp
}

func (s *memStateStoreLocal) Snapshot() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, 0, len(s.records))
	for _, v := range s.records {
		cp := append([]byte(nil), v...)
		out = append(out, cp)
	}
	return out
}

func (s *memStateStoreLocal) Delta(_ time.Time) [][]byte { return s.Snapshot() }

func (s *memStateStoreLocal) Apply(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[s.key(data)] = append([]byte(nil), data...)
	s.applies++
	return nil
}
