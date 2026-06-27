/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
)

// mockBucketFP returns a fixed bucket vector (padded/truncated to n) as the
// local BucketDigest.
type mockBucketFP struct{ vec []uint64 }

func (m *mockBucketFP) BucketDigest(n uint16) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		if i < len(m.vec) {
			out[i] = m.vec[i]
		}
	}
	return out
}

// The G2B handler reads the peer's bucket vector, replies with its own, and
// stores the diverging buckets for the bucket-scoped G1 follow-up.
func TestBucketDigestHandler_ComputesDivergingSet(t *testing.T) {
	respVec := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	cfg := &responderConfig{bucketFingerprintProvider: &mockBucketFP{vec: respVec}}
	h := &bucketDigestHandler{cfg: cfg}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	initVec := []uint64{1, 2, 99, 4, 5, 6, 7, 8} // differs from respVec at index 2
	replyc := make(chan []uint64, 1)
	errc := make(chan error, 1)
	go func() {
		if err := WriteBucketDigest(client, initVec, 0); err != nil {
			errc <- err
			return
		}
		var magic [2]byte
		if _, err := io.ReadFull(client, magic[:]); err != nil {
			errc <- err
			return
		}
		if binary.BigEndian.Uint16(magic[:]) != DigestBucketMagic {
			errc <- fmt.Errorf("bad reply magic 0x%04X", binary.BigEndian.Uint16(magic[:]))
			return
		}
		rv, _, err := ReadBucketDigestBody(client)
		if err != nil {
			errc <- err
			return
		}
		replyc <- rv
		errc <- nil
	}()

	// Responder consumes the inbound magic (the multiplexer's job), then Handle.
	var magic [2]byte
	if _, err := io.ReadFull(server, magic[:]); err != nil {
		t.Fatalf("read inbound magic: %v", err)
	}
	if binary.BigEndian.Uint16(magic[:]) != DigestBucketMagic {
		t.Fatalf("bad inbound magic 0x%04X", binary.BigEndian.Uint16(magic[:]))
	}
	if act := h.Handle(context.Background(), server, "peerA"); act != FrameContinue {
		t.Fatalf("Handle action = %v, want FrameContinue", act)
	}
	if err := <-errc; err != nil {
		t.Fatalf("initiator: %v", err)
	}
	reply := <-replyc

	// Responder replied with its own vector.
	if len(reply) != 8 || reply[2] != 3 {
		t.Fatalf("reply vector wrong: %v", reply)
	}

	// The diverging set for peerA is exactly bucket 2.
	n, want, ok := cfg.takeDivergingBuckets("peerA")
	if !ok {
		t.Fatal("a diverging set must be stored after a bucket mismatch")
	}
	if n != 8 {
		t.Fatalf("diverging N = %d, want 8", n)
	}
	cnt := 0
	for i, w := range want {
		if w {
			cnt++
			if i != 2 {
				t.Fatalf("unexpected diverging bucket %d", i)
			}
		}
	}
	if cnt != 1 {
		t.Fatalf("expected exactly 1 diverging bucket, got %d", cnt)
	}
}

// When the two bucket vectors agree, no diverging set is stored (the scalar G2
// already matched, but a G2B that finds full agreement must not arm a scope).
func TestBucketDigestHandler_NoDivergenceStoresNothing(t *testing.T) {
	vec := []uint64{10, 20, 30, 40}
	cfg := &responderConfig{bucketFingerprintProvider: &mockBucketFP{vec: vec}}
	h := &bucketDigestHandler{cfg: cfg}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	errc := make(chan error, 1)
	go func() {
		if err := WriteBucketDigest(client, vec, 0); err != nil {
			errc <- err
			return
		}
		var magic [2]byte
		if _, err := io.ReadFull(client, magic[:]); err != nil {
			errc <- err
			return
		}
		if _, _, err := ReadBucketDigestBody(client); err != nil {
			errc <- err
			return
		}
		errc <- nil
	}()

	var magic [2]byte
	if _, err := io.ReadFull(server, magic[:]); err != nil {
		t.Fatal(err)
	}
	h.Handle(context.Background(), server, "peerB")
	if err := <-errc; err != nil {
		t.Fatalf("initiator: %v", err)
	}
	if _, _, ok := cfg.takeDivergingBuckets("peerB"); ok {
		t.Fatal("identical vectors must not arm a diverging set")
	}
}
