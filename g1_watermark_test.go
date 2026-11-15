/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"context"
	"sync"
	"testing"
	"time"
)

// arrivalStore keeps each record beside the local time it was stored and
// filters Delta on that time, the shape a store needs when its records carry
// their author's time rather than this node's. afterSelect, when set, runs
// once right after the store has handed g1Handler its outbound records: it
// stands in for a record that reaches this node from a third peer while the
// exchange is still writing its reply.
type arrivalStore struct {
	mu          sync.Mutex
	stored      map[string]time.Time
	afterSelect func()
}

func newArrivalStore() *arrivalStore {
	return &arrivalStore{stored: make(map[string]time.Time)}
}

func (s *arrivalStore) put(rec string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.stored[rec]; !ok {
		s.stored[rec] = time.Now()
	}
}

func (s *arrivalStore) Fingerprint() uint64 { return 0 }

func (s *arrivalStore) Snapshot() [][]byte { return s.Delta(time.Time{}) }

func (s *arrivalStore) Delta(since time.Time) [][]byte {
	s.mu.Lock()
	var out [][]byte
	for rec, at := range s.stored {
		if since.IsZero() || !at.Before(since) {
			out = append(out, []byte(rec))
		}
	}
	hook := s.afterSelect
	s.afterSelect = nil
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return out
}

func (s *arrivalStore) Apply(data []byte) error {
	s.put(string(data))
	return nil
}

func (s *arrivalStore) has(records [][]byte, rec string) bool {
	for _, r := range records {
		if string(r) == rec {
			return true
		}
	}
	return false
}

// TestG1Watermark_RecordStoredDuringTheReplyReachesTheNextDelta pins the
// watermark g1Handler records for a peer to the moment it selected that
// peer's records, not to the end of the reply: a record stored after the
// selection was not in the reply, so the next Delta for that peer must
// still return it.
func TestG1Watermark_RecordStoredDuringTheReplyReachesTheNextDelta(t *testing.T) {
	store := newArrivalStore()
	store.put("held-before")
	store.afterSelect = func() {
		// Leave a measurable gap on either side so the stored time
		// sits strictly between the selection and the reply's end.
		time.Sleep(5 * time.Millisecond)
		store.put("arrived-during-reply")
		time.Sleep(5 * time.Millisecond)
	}

	dt := NewDeltaTracker()
	engine := NewEngine(
		WithG1Store(store),
		WithG1Codec(jsonG1Codec{}),
		WithDelta(dt),
	)

	a, b := newPipeConns()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = engine.RunResponder(ctx, b, "peer-1") }()

	body, err := (jsonG1Codec{}).EncodeExchange(nil, &ExchangeMeta{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	writeG1Frame(t, a, body)
	reply, err := (jsonG1Codec{}).DecodeExchange(readG1Frame(t, a))
	if err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if store.has(reply.Records, "arrived-during-reply") {
		t.Fatal("the record stored after selection was already in the reply; the test no longer exercises the gap")
	}

	var wm time.Time
	for i := 0; i < 100; i++ {
		if wm = dt.RecordWatermark("peer-1"); !wm.IsZero() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if wm.IsZero() {
		t.Fatal("no watermark recorded for peer-1")
	}
	if next := store.Delta(wm); !store.has(next, "arrived-during-reply") {
		t.Fatalf("Delta(watermark) left out the record stored while the reply was written: the watermark is later than the selection")
	}
}
