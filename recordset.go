/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package whisper

import (
	"encoding/binary"
	"hash/fnv"
	"sync"
	"time"
)

// RecordSet holds the records a StatefulMerge store serves, keyed by the
// consumer's record identity, and answers StateStore's read half
// (Fingerprint, Snapshot, Delta) together with BucketFingerprintProvider
// and BucketRecordsProvider from exactly the records it holds. A consumer
// store embeds a *RecordSet and supplies Apply: the consumer's merge
// decides whether an inbound record is admitted and calls Put with the
// record it keeps, so the summary the G2 probe compares (frame.go, the
// digest handler) always describes the records Snapshot and Delta serve.
//
// Fingerprint is the XOR of one 64-bit hash per entry, the FNV-1a of the
// length-prefixed key followed by the record's content, kept current on
// every Put and Remove so a probe costs no record encoding. Two sets with
// the same keys holding the same contents report the same value in any
// insertion order; a set differing in any key or any content reports a
// different value except by hash collision; the empty set reports 0,
// which the digest handler never counts as agreement.
//
// An entry falls in bucket FNV-1a(key) mod n. A record replaced under the
// same key therefore lands in the same bucket on every peer, the buckets
// of BucketDigest XOR to Fingerprint (the invariant the G2B escalation in
// frame.go relies on), and RecordsInBuckets serves every record whose
// bucket the two peers' digests disagree on.
//
// Delta filters on the local time each entry was stored here, never on a
// time the record carries: g1Handler passes Delta the local time it last
// selected records for that peer, so a record authored before that moment
// but stored here after it is still sent. A Put that leaves an entry's
// content unchanged keeps its stored time, so an unchanged record is not
// re-sent by every delta.
//
// Records are copied on Put and never modified afterwards; the slices
// Snapshot, Delta and RecordsInBuckets return are the set's own and must
// not be modified by the caller. All methods are safe for concurrent use.
type RecordSet struct {
	mu      sync.RWMutex
	entries map[string]recordSetEntry
	fp      uint64
	now     func() time.Time
}

// recordSetEntry is one key's record with the two hashes derived from it
// and the local time the current content was stored.
type recordSetEntry struct {
	record  []byte
	hash    uint64
	keyHash uint64
	stored  time.Time
}

// NewRecordSet returns an empty RecordSet.
func NewRecordSet() *RecordSet {
	return &RecordSet{entries: make(map[string]recordSetEntry), now: time.Now}
}

// Put stores record under key, the record's own bytes serving as its
// content. It reports whether the set's content changed: false when key
// already held a record with the same content. Use PutContent when the
// served bytes carry something that differs between nodes holding the
// same record (a per-node signature), so that the summary compares only
// what the nodes agree on.
func (s *RecordSet) Put(key string, record []byte) bool {
	return s.PutContent(key, record, record)
}

// PutContent stores record under key and summarises it by content, the
// bytes that identify the record's content identically on every node. It
// reports whether the set's content changed. When key already holds the
// same content the served bytes are replaced and the stored time kept.
func (s *RecordSet) PutContent(key string, record, content []byte) bool {
	h := recordSetEntryHash(key, content)
	cp := append([]byte(nil), record...)

	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.entries[key]
	if ok && old.hash == h {
		old.record = cp
		s.entries[key] = old
		return false
	}
	if ok {
		s.fp ^= old.hash
	}
	s.fp ^= h
	s.entries[key] = recordSetEntry{record: cp, hash: h, keyHash: hashKey(key), stored: s.now()}
	return true
}

// Remove drops key's record and reports whether one was held.
func (s *RecordSet) Remove(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.entries[key]
	if !ok {
		return false
	}
	s.fp ^= old.hash
	delete(s.entries, key)
	return true
}

// Get returns key's record and whether one is held.
func (s *RecordSet) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key]
	return e.record, ok
}

// Len returns the number of records held.
func (s *RecordSet) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// Fingerprint returns the XOR of every entry's hash, 0 for the empty set.
func (s *RecordSet) Fingerprint() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fp
}

// Snapshot returns every record held.
func (s *RecordSet) Snapshot() [][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([][]byte, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e.record)
	}
	return out
}

// Delta returns the records stored here at or after since; the zero time
// returns the snapshot. A record stored at the same instant as since is
// included, since over-sending is what the StateStore contract tolerates.
func (s *RecordSet) Delta(since time.Time) [][]byte {
	if since.IsZero() {
		return s.Snapshot()
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out [][]byte
	for _, e := range s.entries {
		if !e.stored.Before(since) {
			out = append(out, e.record)
		}
	}
	return out
}

// BucketDigest returns, for each of buckets buckets, the XOR of the hashes
// of the entries whose key falls in it. buckets is clamped to
// [1, maxDigestBuckets], the bound the G2B frame carries.
func (s *RecordSet) BucketDigest(buckets uint16) []uint64 {
	n := clampRecordSetBuckets(buckets)
	out := make([]uint64, n)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, e := range s.entries {
		out[e.keyHash%uint64(n)] ^= e.hash
	}
	return out
}

// RecordsInBuckets returns the records whose key falls in a bucket with
// want[i] true, over the partition BucketDigest(buckets) uses. A want
// shorter than the clamped bucket count selects nothing beyond its end.
func (s *RecordSet) RecordsInBuckets(buckets uint16, want []bool) [][]byte {
	n := uint64(clampRecordSetBuckets(buckets))
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out [][]byte
	for _, e := range s.entries {
		if b := e.keyHash % n; b < uint64(len(want)) && want[b] {
			out = append(out, e.record)
		}
	}
	return out
}

// clampRecordSetBuckets bounds a requested bucket count to the range the
// G2B frame can carry.
func clampRecordSetBuckets(buckets uint16) uint16 {
	if buckets == 0 {
		return 1
	}
	if buckets > maxDigestBuckets {
		return maxDigestBuckets
	}
	return buckets
}

// recordSetEntryHash is the FNV-1a hash of a 4-byte big-endian key length,
// the key and the content. The length prefix keeps a key and content pair
// from hashing like a different split of the same bytes.
func recordSetEntryHash(key string, content []byte) uint64 {
	h := fnv.New64a()
	var ln [4]byte
	binary.BigEndian.PutUint32(ln[:], uint32(len(key)))
	_, _ = h.Write(ln[:])
	_, _ = h.Write([]byte(key))
	_, _ = h.Write(content)
	return h.Sum64()
}

// The StateStore read half and both bucket extensions are answered by a
// RecordSet; a consumer adds Apply.
var (
	_ FingerprintProvider       = (*RecordSet)(nil)
	_ BucketFingerprintProvider = (*RecordSet)(nil)
	_ BucketRecordsProvider     = (*RecordSet)(nil)
)
