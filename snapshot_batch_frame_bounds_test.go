/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package whisper

import (
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"testing"
	"time"
)

// batchTestCodec is the smallest SnapshotCodec the initiator's shard fetch runs over: a shard
// request is [shardIndex][shardCount] and a chunk is [shardIndex][chunkIndex u16][totalChunks u16]
// followed by the chunk body. Only the shard-request and chunk methods are exercised.
type batchTestCodec struct{ ReedSolomonErasure }

func (batchTestCodec) EncodeManifest(SnapshotManifest) ([]byte, error) { return nil, nil }
func (batchTestCodec) DecodeManifest([]byte) (SnapshotManifest, error) {
	return SnapshotManifest{}, nil
}
func (batchTestCodec) PartitionForShard(r [][]byte, _ SnapshotShardSpec) [][]byte { return r }

func (batchTestCodec) EncodeShardRequest(s SnapshotShardSpec) ([]byte, error) {
	return []byte{s.ShardIndex, s.ShardCount}, nil
}

func (batchTestCodec) DecodeShardRequest(b []byte) (SnapshotShardSpec, error) {
	return SnapshotShardSpec{ShardIndex: b[0], ShardCount: b[1]}, nil
}

func (batchTestCodec) EncodeShardChunk(c SnapshotChunk) ([]byte, error) {
	out := make([]byte, 5+len(c.Body))
	out[0] = c.ShardIndex
	binary.BigEndian.PutUint16(out[1:3], c.ChunkIndex)
	binary.BigEndian.PutUint16(out[3:5], c.TotalChunks)
	copy(out[5:], c.Body)
	return out, nil
}

func (batchTestCodec) DecodeShardChunk(b []byte) (SnapshotChunk, error) {
	return SnapshotChunk{
		ShardIndex:  b[0],
		ChunkIndex:  binary.BigEndian.Uint16(b[1:3]),
		TotalChunks: binary.BigEndian.Uint16(b[3:5]),
		Body:        b[5:],
	}, nil
}

// countingStore counts the records the initiator applies.
type countingStore struct{ applied int }

func (s *countingStore) Snapshot() ([][]byte, uint64, uint64) { return nil, 0, 0 }
func (s *countingStore) Apply([]byte) error                   { s.applied++; return nil }

// answerShardRequest plays the neighbour of one shard fetch: it reads the initiator's shard
// request frame ([magic][length][kind][body]) and answers, as a responder does, with one shard
// frame writeSnapshotFrame writes, carrying chunkBody as the only, final chunk of shard 0.
func answerShardRequest(t *testing.T, peer net.Conn, chunkBody []byte) {
	t.Helper()
	go func() {
		var hdr [snapshotHeaderSize]byte
		if _, err := io.ReadFull(peer, hdr[:]); err != nil {
			return
		}
		req := make([]byte, binary.BigEndian.Uint32(hdr[2:6]))
		if _, err := io.ReadFull(peer, req); err != nil {
			return
		}
		frame, _ := batchTestCodec{}.EncodeShardChunk(SnapshotChunk{ShardIndex: 0, ChunkIndex: 0, TotalChunks: 1, Body: chunkBody})
		_ = writeSnapshotFrame(peer, SnapshotKindShard, frame)
	}()
}

// fetchOneShard runs the initiator's shard fetch against a neighbour answering with chunkBody and
// reports what it applied, its error, and the bytes the heap allocated while it ran.
func fetchOneShard(t *testing.T, chunkBody []byte) (applied int, err error, allocated uint64) {
	t.Helper()
	initiator, peer := net.Pipe()
	defer initiator.Close()
	defer peer.Close()
	_ = initiator.SetDeadline(time.Now().Add(10 * time.Second))
	_ = peer.SetDeadline(time.Now().Add(10 * time.Second))
	store := &countingStore{}
	d := NewSnapshotDriver(batchTestCodec{}, store, nil)
	answerShardRequest(t, peer, chunkBody)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	n, err := d.fetchShardSequential(initiator, 0, 1)
	runtime.ReadMemStats(&after)
	if n != store.applied {
		t.Fatalf("fetch reported %d applied, store saw %d", n, store.applied)
	}
	return n, err, after.TotalAlloc - before.TotalAlloc
}

// A neighbour's chunk that declares far more records than its body can hold is refused before
// the decoder allocates for them: the fetch fails, applies nothing, and the heap grows by no more
// than the frame it read, whatever count the neighbour wrote.
func TestSnapshotShardRefusesACountItsBodyCannotHold(t *testing.T) {
	body := make([]byte, 8) // count, then 4 bytes: room for one empty record at most
	binary.BigEndian.PutUint32(body[0:4], 1<<22)
	applied, err, allocated := fetchOneShard(t, body)
	if allocated > 1<<20 {
		t.Errorf("decoding an %d-byte chunk allocated %d bytes: the count sized the allocation", len(body), allocated)
	}
	if err == nil {
		t.Errorf("a chunk declaring %d records in %d bytes was accepted", 1<<22, len(body))
	}
	if applied != 0 {
		t.Errorf("applied %d records from a refused chunk", applied)
	}
}

// A chunk whose records run past its body is refused whole: no record of a malformed batch is
// applied.
func TestSnapshotShardRefusesARecordPastItsBody(t *testing.T) {
	body := packBatch([][]byte{[]byte("first"), []byte("second")})
	binary.BigEndian.PutUint32(body[4+4+5:], 1<<20) // the second record's length now runs past the body
	applied, err, _ := fetchOneShard(t, body)
	if err == nil {
		t.Fatal("a chunk whose second record runs past its body was accepted")
	}
	if applied != 0 {
		t.Fatalf("applied %d records of a malformed chunk", applied)
	}
}

// A reconstructed shard that declares more records than it holds is refused before the decoder
// allocates for them.
func TestErasureDecodeRefusesACountItsShardCannotHold(t *testing.T) {
	shard := make([]byte, 8)
	binary.BigEndian.PutUint32(shard[0:4], 1<<22)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	out, err := ReedSolomonErasure{}.DecodeErasure([][][]byte{{shard}}, 1)
	runtime.ReadMemStats(&after)
	if got := after.TotalAlloc - before.TotalAlloc; got > 1<<20 {
		t.Errorf("decoding an %d-byte shard allocated %d bytes: the count sized the allocation", len(shard), got)
	}
	if err == nil {
		t.Errorf("a shard declaring %d records in %d bytes decoded to %d shards", 1<<22, len(shard), len(out))
	}
}
