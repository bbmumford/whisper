/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"testing"
	"time"
)

// inflatingCodec is batchTestCodec with a DecodeShardChunk that answers a fixed body whatever
// frame it is given, standing for a codec whose decoded chunk is larger than the frame it came in.
type inflatingCodec struct {
	batchTestCodec
	body []byte
}

func (c inflatingCodec) DecodeShardChunk([]byte) (SnapshotChunk, error) {
	return SnapshotChunk{ShardIndex: 0, ChunkIndex: 0, TotalChunks: 1, Body: c.body}, nil
}

// Every refusal of the shard fetch and of the erasure decode is ErrSnapshotBatchMalformed.
func TestSnapshotBatchRefusalsAreTyped(t *testing.T) {
	countPastBody := make([]byte, 8)
	binary.BigEndian.PutUint32(countPastBody[0:4], 1<<22)
	recordPastBody := packBatch([][]byte{[]byte("first"), []byte("second")})
	binary.BigEndian.PutUint32(recordPastBody[13:17], 1<<20)
	maxLength := make([]byte, 8)
	binary.BigEndian.PutUint32(maxLength[0:4], 1)
	binary.BigEndian.PutUint32(maxLength[4:8], math.MaxUint32)

	// Two records declared, room for both counts; the first record fills the bytes, leaving none for
	// the second record's length.
	noLengthRoom := []byte{0, 0, 0, 2, 0, 0, 0, 4, 'a', 'b', 'c', 'd'}

	for name, body := range map[string][]byte{
		"count past the body":       countPastBody,
		"record past the body":      recordPastBody,
		"length of 2^32-1":          maxLength,
		"three bytes, no count":     {0, 0, 1},
		"count with no length room": noLengthRoom,
	} {
		refused(t, "shard fetch, "+name, func() error { _, err, _ := fetchOneShard(t, body); return err })
		refused(t, "erasure decode, "+name, func() error {
			_, err := (ReedSolomonErasure{}).DecodeErasure([][][]byte{{body}}, 1)
			return err
		})
	}
}

// refused fails the test unless run returns ErrSnapshotBatchMalformed; a panic inside run is
// reported as a failure of the same assertion.
func refused(t *testing.T, what string, run func() error) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("%s: panicked (%v), want ErrSnapshotBatchMalformed", what, p)
		}
	}()
	if err := run(); !errors.Is(err, ErrSnapshotBatchMalformed) {
		t.Errorf("%s: err = %v, want ErrSnapshotBatchMalformed", what, err)
	}
}

// A chunk batch is read under the frame's own bound: a codec that hands the fetch a body larger
// than any frame cannot make it accept more records than one frame carries, even when the body's
// bytes would hold them.
func TestSnapshotShardBatchIsBoundedByOneFrame(t *testing.T) {
	over := snapshotBatchMaxRecords + 1
	body := make([]byte, 4+4*over) // every record empty, so the bytes hold all of them
	binary.BigEndian.PutUint32(body[0:4], uint32(over))
	initiator, peer := pipePair(t)
	answerShardRequest(t, peer, []byte{0})
	store := &countingStore{}
	d := NewSnapshotDriver(inflatingCodec{body: body}, store, nil)
	applied, err := d.fetchShardSequential(initiator, 0, 1)
	if !errors.Is(err, ErrSnapshotBatchMalformed) {
		t.Fatalf("a %d-record chunk batch: err = %v, want ErrSnapshotBatchMalformed", over, err)
	}
	if applied != 0 || store.applied != 0 {
		t.Fatalf("applied %d (store %d) records of a refused batch", applied, store.applied)
	}

	at := snapshotBatchMaxRecords
	binary.BigEndian.PutUint32(body[0:4], uint32(at))
	initiator, peer = pipePair(t)
	answerShardRequest(t, peer, []byte{0})
	store = &countingStore{}
	d = NewSnapshotDriver(inflatingCodec{body: body[:4+4*at]}, store, nil)
	applied, err = d.fetchShardSequential(initiator, 0, 1)
	if err != nil || uint64(applied) != at {
		t.Fatalf("a %d-record chunk batch: applied %d, err = %v; want all applied", at, applied, err)
	}
}

// The decoder reads back what both encoders write, an empty batch included, and ignores the zero
// padding after a shard's last record.
func TestDecodeRecordBatchRoundTrips(t *testing.T) {
	records := [][]byte{[]byte("a"), {}, bytes.Repeat([]byte{7}, 300)}
	for name, buf := range map[string][]byte{
		"packBatch":               packBatch(records),
		"packShardRecords":        packShardRecords(records),
		"padded packShardRecords": append(packShardRecords(records), make([]byte, 64)...),
	} {
		got, err := decodeRecordBatch(buf, snapshotBatchMaxRecords)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got) != len(records) {
			t.Fatalf("%s: %d records, want %d", name, len(got), len(records))
		}
		for i := range records {
			if !bytes.Equal(got[i], records[i]) {
				t.Fatalf("%s: record %d = %q, want %q", name, i, got[i], records[i])
			}
		}
	}
	for name, buf := range map[string][]byte{"nil": nil, "zero count": packBatch(nil)} {
		got, err := decodeRecordBatch(buf, snapshotBatchMaxRecords)
		if err != nil || len(got) != 0 {
			t.Fatalf("%s batch: %d records, err = %v; want none and no error", name, len(got), err)
		}
	}
}

// pipePair is a connected initiator and neighbour pair, each with a deadline, closed when the
// test ends.
func pipePair(t *testing.T) (initiator, peer net.Conn) {
	t.Helper()
	initiator, peer = net.Pipe()
	deadline := time.Now().Add(10 * time.Second)
	_ = initiator.SetDeadline(deadline)
	_ = peer.SetDeadline(deadline)
	t.Cleanup(func() { initiator.Close(); peer.Close() })
	return initiator, peer
}
