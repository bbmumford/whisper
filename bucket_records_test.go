/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"testing"
	"time"
)

// bucketMockStore implements StateStore + BucketRecordsProvider so the G1
// handler's bucket-scoped selection path can be exercised in isolation.
type bucketMockStore struct {
	snap        [][]byte
	bucketResp  [][]byte
	gotN        uint16
	gotWant     []bool
	bucketCalls int
}

func (s *bucketMockStore) Fingerprint() uint64        { return 1 }
func (s *bucketMockStore) Snapshot() [][]byte          { return s.snap }
func (s *bucketMockStore) Delta(_ time.Time) [][]byte  { return s.snap }
func (s *bucketMockStore) Apply(_ []byte) error        { return nil }
func (s *bucketMockStore) RecordsInBuckets(n uint16, want []bool) [][]byte {
	s.gotN = n
	s.gotWant = want
	s.bucketCalls++
	return s.bucketResp
}

// After a G2B mismatch arms a peer's diverging buckets, the G1 follow-up serves
// only those buckets' records — and the arming is one-shot.
func TestSelectOutboundRecords_BucketScoped(t *testing.T) {
	store := &bucketMockStore{
		snap:       [][]byte{[]byte("full1"), []byte("full2"), []byte("full3")},
		bucketResp: [][]byte{[]byte("bucketrec")},
	}
	cfg := &responderConfig{bucketRecords: store}
	h := &g1Handler{cfg: cfg, g1: &g1Package{store: store}}

	want := make([]bool, 64)
	want[7] = true
	cfg.setDivergingBuckets("peerA", 64, want)

	got := h.selectOutboundRecords("peerA")
	if len(got) != 1 || string(got[0]) != "bucketrec" {
		t.Fatalf("expected the bucket-scoped record, got %v", got)
	}
	if store.gotN != 64 || len(store.gotWant) != 64 || !store.gotWant[7] {
		t.Fatalf("RecordsInBuckets called with wrong args: n=%d want[7]=%v", store.gotN, store.gotWant)
	}

	// One-shot: the set was consumed, so the next select falls back to snapshot.
	got2 := h.selectOutboundRecords("peerA")
	if len(got2) != 3 {
		t.Fatalf("expected full snapshot fallback after one-shot, got %d", len(got2))
	}
	if store.bucketCalls != 1 {
		t.Fatalf("RecordsInBuckets must be called exactly once, got %d", store.bucketCalls)
	}
}

// With no diverging set armed, the G1 handler uses the normal snapshot path and
// never touches the bucket-records provider.
func TestSelectOutboundRecords_NoDivergingSet(t *testing.T) {
	store := &bucketMockStore{snap: [][]byte{[]byte("a"), []byte("b")}}
	cfg := &responderConfig{bucketRecords: store}
	h := &g1Handler{cfg: cfg, g1: &g1Package{store: store}}

	got := h.selectOutboundRecords("peerB")
	if len(got) != 2 {
		t.Fatalf("expected snapshot of 2, got %d", len(got))
	}
	if store.bucketCalls != 0 {
		t.Fatal("RecordsInBuckets must not be called without a diverging set")
	}
}

// setDivergingBuckets rejects empty/degenerate inputs so a peer is never armed
// with a meaningless bucket scope.
func TestSetDivergingBuckets_RejectsDegenerate(t *testing.T) {
	cfg := &responderConfig{}
	cfg.setDivergingBuckets("", 64, []bool{true})
	cfg.setDivergingBuckets("p", 0, []bool{true})
	cfg.setDivergingBuckets("p", 64, nil)
	if _, _, ok := cfg.takeDivergingBuckets("p"); ok {
		t.Fatal("degenerate arms must be ignored")
	}
	if _, _, ok := cfg.takeDivergingBuckets(""); ok {
		t.Fatal("empty peer must never be armed")
	}
}
