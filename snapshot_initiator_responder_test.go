/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package whisper

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"
)

// recordStore holds records a responder snapshots and counts what an initiator applies.
type recordStore struct {
	records [][]byte
	applied [][]byte
}

func (s *recordStore) Snapshot() ([][]byte, uint64, uint64) { return s.records, 7, 99 }
func (s *recordStore) Apply(r []byte) error {
	s.applied = append(s.applied, append([]byte(nil), r...))
	return nil
}

// RunInitiator completes a cold start against a whisper engine's responder: every frame the
// responder writes carries the snapshot magic, the initiator reads it, and every record the
// responder's store holds is applied.
func TestRunInitiatorFetchesTheSnapshotAWhisperResponderServes(t *testing.T) {
	served := &recordStore{records: [][]byte{[]byte("one"), []byte("two"), []byte("three")}}
	engine := NewEngine(WithSnapshot(NewSnapshotDriver(batchTestCodec{}, served, nil)))
	initiatorConn, responderConn := net.Pipe()
	defer func() {
		if err := initiatorConn.Close(); err != nil {
			t.Errorf("initiatorConn.Close: %v", err)
		}
	}()
	defer func() {
		if err := responderConn.Close(); err != nil {
			t.Errorf("responderConn.Close: %v", err)
		}
	}()
	_ = initiatorConn.SetDeadline(time.Now().Add(10 * time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _ = engine.RunResponder(ctx, responderConn, "initiator") }()

	fetched := &recordStore{}
	applied, _, err := NewSnapshotDriver(batchTestCodec{}, fetched, nil).RunInitiator([]net.Conn{initiatorConn})
	if err != nil {
		t.Fatalf("RunInitiator: %v", err)
	}
	if applied != len(served.records) || len(fetched.applied) != len(served.records) {
		t.Fatalf("applied %d (store %d) records, want %d", applied, len(fetched.applied), len(served.records))
	}
	for i, r := range served.records {
		if string(fetched.applied[i]) != string(r) {
			t.Fatalf("record %d = %q, want %q", i, fetched.applied[i], r)
		}
	}
}

// A reply that does not begin with the snapshot magic is refused as a protocol error before
// its length is read.
func TestTheInitiatorRefusesAReplyWithoutTheSnapshotMagic(t *testing.T) {
	initiator, peer := net.Pipe()
	defer func() {
		if err := initiator.Close(); err != nil {
			t.Errorf("initiator.Close: %v", err)
		}
	}()
	defer func() {
		if err := peer.Close(); err != nil {
			t.Errorf("peer.Close: %v", err)
		}
	}()
	_ = initiator.SetDeadline(time.Now().Add(5 * time.Second))
	go func() {
		var hdr [snapshotHeaderSize]byte
		if _, err := readFull(peer, hdr[:]); err != nil {
			return
		}
		req := make([]byte, binary.BigEndian.Uint32(hdr[2:6]))
		_, _ = readFull(peer, req)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], 6)
		_, _ = peer.Write(append(lenBuf[:], byte(SnapshotKindShard), 0, 0, 0, 0, 1))
	}()
	_, err := NewSnapshotDriver(batchTestCodec{}, &recordStore{}, nil).fetchShardSequential(initiator, 0, 1)
	if err == nil || !strings.Contains(err.Error(), "bad magic") {
		t.Fatalf("fetch err = %v, want the bad-magic refusal", err)
	}
}

func readFull(c net.Conn, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		m, err := c.Read(b[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
