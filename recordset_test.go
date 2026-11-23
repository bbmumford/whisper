/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package whisper

import (
	"fmt"
	"testing"
	"time"
)

// Peers holding different records report different fingerprints, including
// the shape a timestamp XOR could not tell apart: {x, y, z} against {z}.
func TestRecordSet_DifferentRecordsDifferentFingerprints(t *testing.T) {
	a, b := NewRecordSet(), NewRecordSet()
	a.Put("x", []byte("same-bytes"))
	a.Put("y", []byte("same-bytes"))
	a.Put("z", []byte("z"))
	b.Put("z", []byte("z"))
	if a.Fingerprint() == b.Fingerprint() {
		t.Fatalf("{x, y, z} and {z} reported the same fingerprint %#x", a.Fingerprint())
	}

	c, d := NewRecordSet(), NewRecordSet()
	c.Put("k", []byte("v1"))
	d.Put("k", []byte("v2"))
	if c.Fingerprint() == d.Fingerprint() {
		t.Fatal("the same key holding different content reported the same fingerprint")
	}
}

// Peers holding the same records report the same non-zero fingerprint in
// any insertion order; the empty set reports 0.
func TestRecordSet_SameRecordsSameFingerprint(t *testing.T) {
	a, b := NewRecordSet(), NewRecordSet()
	if a.Fingerprint() != 0 {
		t.Fatalf("empty set reported %#x, want 0", a.Fingerprint())
	}
	for i := 0; i < 50; i++ {
		a.Put(fmt.Sprint("k", i), []byte(fmt.Sprint("v", i)))
	}
	for i := 49; i >= 0; i-- {
		b.Put(fmt.Sprint("k", i), []byte(fmt.Sprint("v", i)))
	}
	if a.Fingerprint() == 0 || a.Fingerprint() != b.Fingerprint() {
		t.Fatalf("same records reported %#x and %#x", a.Fingerprint(), b.Fingerprint())
	}
}

// The fingerprint kept on Put and Remove equals the one recomputed from the
// records held, through replacements and removals.
func TestRecordSet_FingerprintFollowsPutAndRemove(t *testing.T) {
	s := NewRecordSet()
	s.Put("a", []byte("1"))
	s.Put("b", []byte("2"))
	s.Put("a", []byte("3"))
	s.Remove("b")
	s.Remove("missing")

	fresh := NewRecordSet()
	fresh.Put("a", []byte("3"))
	if s.Fingerprint() != fresh.Fingerprint() {
		t.Fatalf("after replace and remove: %#x, recomputed %#x", s.Fingerprint(), fresh.Fingerprint())
	}
	s.Remove("a")
	if s.Fingerprint() != 0 || s.Len() != 0 {
		t.Fatalf("emptied set: fingerprint %#x, len %d", s.Fingerprint(), s.Len())
	}
}

// PutContent summarises by the content given, so records whose served bytes
// differ between nodes (a per-node signature) but whose content agrees
// report the same fingerprint, and the served bytes are what Snapshot sends.
func TestRecordSet_PutContentSummarisesTheContent(t *testing.T) {
	a, b := NewRecordSet(), NewRecordSet()
	a.PutContent("k", []byte("content+sigA"), []byte("content"))
	b.PutContent("k", []byte("content+sigB"), []byte("content"))
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatal("the same content under different served bytes reported different fingerprints")
	}
	if snap := a.Snapshot(); len(snap) != 1 || string(snap[0]) != "content+sigA" {
		t.Fatalf("Snapshot served %q, want the served bytes", snap)
	}
}

// Delta returns what was stored here at or after since, whatever time the
// record itself carries; re-putting unchanged content keeps the stored time.
func TestRecordSet_DeltaFiltersOnLocalStoreTime(t *testing.T) {
	s := NewRecordSet()
	clock := time.Unix(1000, 0)
	s.now = func() time.Time { return clock }

	s.Put("early", []byte("authored long ago, stored at 1000"))
	lastExchange := time.Unix(1500, 0)
	clock = time.Unix(2000, 0)
	s.Put("late", []byte("authored long ago, stored at 2000"))
	s.Put("early", []byte("authored long ago, stored at 1000"))

	got := s.Delta(lastExchange)
	if len(got) != 1 || string(got[0]) != "authored long ago, stored at 2000" {
		t.Fatalf("Delta(1500) = %q, want only the record stored at 2000", got)
	}
	if len(s.Delta(time.Time{})) != 2 {
		t.Fatal("Delta(zero) is not the snapshot")
	}
	if len(s.Delta(time.Unix(2000, 0))) != 1 {
		t.Fatal("a record stored at the same instant as since was left out")
	}
}

// The buckets XOR to the fingerprint for every bucket count, a replaced
// record changes exactly the bucket of its key, and RecordsInBuckets serves
// the records of the buckets two peers disagree on.
func TestRecordSet_BucketsLocaliseADivergence(t *testing.T) {
	a, b := NewRecordSet(), NewRecordSet()
	for i := 0; i < 40; i++ {
		k, v := fmt.Sprint("k", i), []byte(fmt.Sprint("v", i))
		a.Put(k, v)
		b.Put(k, v)
	}
	b.Put("k7", []byte("changed"))

	for _, n := range []uint16{0, 1, 8, 64, 256, 1000} {
		var xor uint64
		for _, d := range a.BucketDigest(n) {
			xor ^= d
		}
		if xor != a.Fingerprint() {
			t.Fatalf("XOR(BucketDigest(%d)) = %#x, Fingerprint = %#x", n, xor, a.Fingerprint())
		}
	}
	if got := len(a.BucketDigest(1000)); got != maxDigestBuckets {
		t.Fatalf("BucketDigest(1000) returned %d buckets, want the clamp %d", got, maxDigestBuckets)
	}

	da, db := a.BucketDigest(16), b.BucketDigest(16)
	want := make([]bool, 16)
	diverging := 0
	for i := range da {
		if da[i] != db[i] {
			want[i] = true
			diverging++
		}
	}
	if diverging != 1 {
		t.Fatalf("one replaced record changed %d buckets, want 1", diverging)
	}
	found := false
	for _, rec := range b.RecordsInBuckets(16, want) {
		if string(rec) == "changed" {
			found = true
		}
	}
	if !found {
		t.Fatal("RecordsInBuckets did not serve the replaced record from its diverging bucket")
	}
	if got := b.RecordsInBuckets(16, want[:0]); len(got) != 0 {
		t.Fatalf("an empty want selected %d records", len(got))
	}
}

// Put copies the record, so a caller reusing its buffer does not change
// what the set serves or summarises.
func TestRecordSet_PutCopiesTheRecord(t *testing.T) {
	s := NewRecordSet()
	buf := []byte("original")
	s.Put("k", buf)
	fp := s.Fingerprint()
	copy(buf, "mutated!")
	if got, _ := s.Get("k"); string(got) != "original" || s.Fingerprint() != fp {
		t.Fatalf("the set served %q after the caller reused its buffer", got)
	}
}

// A store that embeds a RecordSet and adds Apply satisfies StateStore and
// both bucket extensions, the shape the G1 and G2B handlers auto-wire.
func TestRecordSet_EmbeddedAnswersStateStore(t *testing.T) {
	var s interface {
		StateStore
		BucketFingerprintProvider
		BucketRecordsProvider
	} = &recordSetStore{RecordSet: NewRecordSet()}
	if err := s.Apply([]byte("r")); err != nil || s.Fingerprint() == 0 {
		t.Fatalf("Apply through the embedding store: err %v, fingerprint %#x", err, s.Fingerprint())
	}
}

type recordSetStore struct{ *RecordSet }

func (s *recordSetStore) Apply(data []byte) error {
	s.Put(string(data), data)
	return nil
}
