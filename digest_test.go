/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
)

func TestDigestWriteRead(t *testing.T) {
	// Use a pipe as a net.Conn
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	fp := uint64(0xDEADBEEF12345678)
	flags := FlagDigestMatch

	// Write on client
	go func() {
		if err := WriteDigestProbe(client, fp, flags); err != nil {
			t.Errorf("WriteDigestProbe: %v", err)
		}
	}()

	// Read on server — first read magic
	var magicBuf [2]byte
	if _, err := server.Read(magicBuf[:]); err != nil {
		t.Fatalf("Read magic: %v", err)
	}
	magic := binary.BigEndian.Uint16(magicBuf[:])
	if magic != DigestMagic {
		t.Fatalf("expected magic 0x%04X, got 0x%04X", DigestMagic, magic)
	}

	// Read body
	gotFP, gotFlags, err := ReadDigestBody(server)
	if err != nil {
		t.Fatalf("ReadDigestBody: %v", err)
	}
	if gotFP != fp {
		t.Errorf("fingerprint: got %016x, want %016x", gotFP, fp)
	}
	if gotFlags != flags {
		t.Errorf("flags: got %d, want %d", gotFlags, flags)
	}
}

func TestDigestFrameSize(t *testing.T) {
	// Use a pipe — write on one end, count bytes on the other
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan int, 1)
	go func() {
		var buf [64]byte
		n, _ := server.Read(buf[:])
		done <- n
	}()

	if err := WriteDigestProbe(client, 0x1234, 0); err != nil {
		t.Fatalf("WriteDigestProbe: %v", err)
	}
	n := <-done
	if n != digestFrameSize {
		t.Errorf("frame size: got %d, want %d", n, digestFrameSize)
	}
}

// TestBucketDigestWriteRead round-trips the G2B frame for N ∈ {1, 64, 256},
// including the N=256 case that exercises the wire-byte-0 sentinel.
func TestBucketDigestWriteRead(t *testing.T) {
	for _, n := range []int{1, 64, 256} {
		n := n
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			buckets := make([]uint64, n)
			for i := range buckets {
				buckets[i] = uint64(i)*0x0101010101010101 + 0xABCD
			}
			flags := uint16(0x55AA)

			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()

			errc := make(chan error, 1)
			go func() { errc <- WriteBucketDigest(client, buckets, flags) }()

			var magicBuf [2]byte
			if _, err := io.ReadFull(server, magicBuf[:]); err != nil {
				t.Fatalf("read magic: %v", err)
			}
			if m := binary.BigEndian.Uint16(magicBuf[:]); m != DigestBucketMagic {
				t.Fatalf("magic: got 0x%04X, want 0x%04X", m, DigestBucketMagic)
			}
			gotBuckets, gotFlags, err := ReadBucketDigestBody(server)
			if err != nil {
				t.Fatalf("ReadBucketDigestBody: %v", err)
			}
			if err := <-errc; err != nil {
				t.Fatalf("WriteBucketDigest: %v", err)
			}
			if gotFlags != flags {
				t.Errorf("flags: got 0x%04X, want 0x%04X", gotFlags, flags)
			}
			if len(gotBuckets) != n {
				t.Fatalf("bucket count: got %d, want %d", len(gotBuckets), n)
			}
			for i := range buckets {
				if gotBuckets[i] != buckets[i] {
					t.Fatalf("bucket[%d]: got %016x, want %016x", i, gotBuckets[i], buckets[i])
				}
			}
		})
	}
}

// WriteBucketDigest must reject a bucket count outside [1, maxDigestBuckets]
// before touching the connection.
func TestBucketDigestRejectsBadCount(t *testing.T) {
	_, client := net.Pipe()
	defer client.Close()
	if err := WriteBucketDigest(client, nil, 0); err == nil {
		t.Error("WriteBucketDigest(0 buckets) must error")
	}
	if err := WriteBucketDigest(client, make([]uint64, maxDigestBuckets+1), 0); err == nil {
		t.Errorf("WriteBucketDigest(%d buckets) must error", maxDigestBuckets+1)
	}
}

// The G2B magic must be 0x4742 and distinct from every other gossip frame magic.
func TestDigestBucketMagic_NoCollision(t *testing.T) {
	if DigestBucketMagic != 0x4742 {
		t.Errorf("DigestBucketMagic: got 0x%04X, want 0x4742 (GB)", DigestBucketMagic)
	}
	others := map[string]uint16{
		"GossipMagic":   GossipMagic,
		"DigestMagic":   DigestMagic,
		"RumorMagic":    RumorMagic,
		"PullMagic":     PullMagic,
		"ReconcileMagic": ReconcileMagic,
		"RumorACKMagic": RumorACKMagic,
		"SnapshotMagic": SnapshotMagic,
	}
	for name, m := range others {
		if DigestBucketMagic == m {
			t.Errorf("DigestBucketMagic collides with %s: 0x%04X", name, m)
		}
	}
}
