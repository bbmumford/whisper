/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"
)

// jsonG1Codec is a minimal G1Codec implementation used by the
// interop tests — exercises the same wire path a real consumer's
// codec (protobuf, CBOR) traverses, without pulling those
// dependencies into the whisper test binary.
type jsonG1Codec struct{}

type jsonExchange struct {
	Records [][]byte      `json:"r"`
	Meta    *ExchangeMeta `json:"m,omitempty"`
	NodeIDs []string      `json:"n,omitempty"`
}

func (c jsonG1Codec) EncodeExchange(records [][]byte, meta *ExchangeMeta) ([]byte, error) {
	return json.Marshal(jsonExchange{Records: records, Meta: meta})
}

func (c jsonG1Codec) DecodeExchange(body []byte) (DecodedExchange, error) {
	var e jsonExchange
	if err := json.Unmarshal(body, &e); err != nil {
		return DecodedExchange{}, err
	}
	return DecodedExchange{Records: e.Records, Meta: e.Meta, SeenNodeIDs: e.NodeIDs}, nil
}

// memStateStore is a thread-safe in-memory StateStore used by the
// interop tests. Records are keyed by their content hash (first 8
// bytes) so duplicate Apply calls are idempotent.
type memStateStore struct {
	mu      sync.Mutex
	records map[string][]byte
	applies int
}

func newMemStateStore() *memStateStore {
	return &memStateStore{records: make(map[string][]byte)}
}

func (s *memStateStore) key(data []byte) string {
	if len(data) >= 8 {
		return string(data[:8])
	}
	return string(data)
}

func (s *memStateStore) Fingerprint() uint64 {
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

func (s *memStateStore) Snapshot() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, 0, len(s.records))
	for _, v := range s.records {
		cp := append([]byte(nil), v...)
		out = append(out, cp)
	}
	return out
}

func (s *memStateStore) Delta(_ time.Time) [][]byte {
	return s.Snapshot()
}

func (s *memStateStore) Apply(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[s.key(data)] = append([]byte(nil), data...)
	s.applies++
	return nil
}

// writeG1Frame produces a well-formed G1 frame on the wire so the
// responder's native handler decodes and applies it.
func writeG1Frame(t *testing.T, w io.Writer, body []byte) {
	t.Helper()
	var hdr [6]byte
	binary.BigEndian.PutUint16(hdr[0:2], GossipMagic)
	binary.BigEndian.PutUint32(hdr[2:6], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		t.Fatalf("write hdr: %v", err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("write body: %v", err)
	}
}

// readG1Frame reads the responder's G1 reply and returns the body.
func readG1Frame(t *testing.T, r io.Reader) []byte {
	t.Helper()
	var hdr [6]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		t.Fatalf("read hdr: %v", err)
	}
	magic := binary.BigEndian.Uint16(hdr[0:2])
	if magic != GossipMagic {
		t.Fatalf("bad magic: 0x%04X", magic)
	}
	n := binary.BigEndian.Uint32(hdr[2:6])
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	return body
}

// TestG1NativeHandler_RoundTrip verifies that a whisper engine with a
// StateStore + G1Codec wired via WithG1Store / WithG1Codec applies
// inbound records, writes back a correct response containing the
// store's current snapshot, and fires the exchange observer.
func TestG1NativeHandler_RoundTrip(t *testing.T) {
	store := newMemStateStore()
	_ = store.Apply([]byte("local-rec-A"))
	_ = store.Apply([]byte("local-rec-B"))

	var obsCalls int
	var obsApplied int
	engine := NewEngine(
		WithG1Store(store),
		WithG1Codec(jsonG1Codec{}),
		WithG1ExchangeObserver(func(res GossipResult, _ string) {
			obsCalls++
			obsApplied = res.RecordsApplied
		}),
	)

	a, b := newPipeConns()
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Responder side.
	go func() { _ = engine.RunResponder(ctx, b, "peer-1") }()

	// Initiator writes a frame with one incoming record.
	inbound := [][]byte{[]byte("remote-rec-X")}
	body, err := (jsonG1Codec{}).EncodeExchange(inbound, &ExchangeMeta{CacheFingerprint: 42})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	writeG1Frame(t, a, body)

	// Read the responder's reply.
	reply := readG1Frame(t, a)
	decoded, err := (jsonG1Codec{}).DecodeExchange(reply)
	if err != nil {
		t.Fatalf("decode reply: %v", err)
	}

	if got := len(decoded.Records); got != 2 {
		t.Fatalf("expected 2 records in reply, got %d", got)
	}
	if decoded.Meta == nil {
		t.Fatal("reply missing meta")
	}
	if decoded.Meta.CacheFingerprint == 0 {
		t.Error("reply meta missing fingerprint")
	}

	// Wait briefly for the observer (runs inline after write — should
	// already have fired, but observability via a pipe can race).
	for i := 0; i < 50 && obsCalls == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if obsCalls == 0 {
		t.Fatal("exchange observer never fired")
	}
	if obsApplied != 1 {
		t.Errorf("expected observer to see 1 applied record, got %d", obsApplied)
	}

	// The store should now contain the original two plus the remote record.
	if applied := store.applies; applied != 3 {
		t.Errorf("expected 3 total Apply calls, got %d", applied)
	}
}

// TestG1NativeHandler_DigestMatchSkipsG1 verifies that a G2 digest
// probe whose fingerprint matches the store's causes the responder
// to emit EventDigestMatch and respond with a matching-flag reply,
// without any G1 traffic being required.
func TestG1NativeHandler_DigestMatchSkipsG1(t *testing.T) {
	store := newMemStateStore()
	_ = store.Apply([]byte("rec"))
	fp := store.Fingerprint()

	engine := NewEngine(
		WithG1Store(store),
		WithG1Codec(jsonG1Codec{}),
	)
	events := make(chan GossipEvent, 4)
	engine.SubscribeEvents(events)

	a, b := newPipeConns()
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _ = engine.RunResponder(ctx, b, "peer-2") }()

	// Send a G2 probe with the matching fingerprint.
	if err := WriteDigestProbe(a, fp, 0); err != nil {
		t.Fatalf("write probe: %v", err)
	}

	// Responder replies with a probe — drain it.
	peerFP, flags, err := readDigestReply(a)
	if err != nil {
		t.Fatalf("read probe reply: %v", err)
	}
	if peerFP != fp {
		t.Errorf("expected reply fingerprint %d, got %d", fp, peerFP)
	}
	if flags&FlagDigestMatch == 0 {
		t.Error("reply should set FlagDigestMatch")
	}

	// EventDigestMatch should fire.
	select {
	case ev := <-events:
		if ev.Kind != EventDigestMatch {
			t.Errorf("expected EventDigestMatch, got %v", ev.Kind)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("EventDigestMatch never fired")
	}
}

// readDigestReply reads a G2 frame reply (magic + fingerprint + flags).
func readDigestReply(r io.Reader) (uint64, uint16, error) {
	var magic [2]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return 0, 0, err
	}
	if binary.BigEndian.Uint16(magic[:]) != DigestMagic {
		return 0, 0, io.ErrUnexpectedEOF
	}
	fp, flags, err := ReadDigestBody(r)
	return fp, flags, err
}

// TestG1NativeHandler_MaxPayloadEnforced verifies WithMaxPayloadSize
// rejects oversized frames before attempting to allocate the body.
func TestG1NativeHandler_MaxPayloadEnforced(t *testing.T) {
	store := newMemStateStore()
	engine := NewEngine(
		WithG1Store(store),
		WithG1Codec(jsonG1Codec{}),
		WithMaxPayloadSize(64), // tiny cap
	)

	a, b := newPipeConns()
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- engine.RunResponder(ctx, b, "peer-3") }()

	// Craft a frame with a length that exceeds the cap.
	var hdr [6]byte
	binary.BigEndian.PutUint16(hdr[0:2], GossipMagic)
	binary.BigEndian.PutUint32(hdr[2:6], 128) // > 64
	if _, err := a.Write(hdr[:]); err != nil {
		t.Fatalf("write hdr: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Error("expected responder to fail on oversized frame")
		}
	case <-time.After(time.Second):
		t.Fatal("responder never returned on oversized frame")
	}

	if store.applies != 0 {
		t.Errorf("store shouldn't have seen any applies, got %d", store.applies)
	}
}
