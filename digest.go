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
)

// Digest-based gossip exchange (G2). Before sending a full G1 data exchange,
// the initiator sends a 12-byte digest frame containing its cache fingerprint.
// The responder compares fingerprints and replies with its own. If they match,
// both sides skip the exchange — 12 bytes total instead of multi-KB.
//
// Wire format: [2-byte magic 0x4732][2-byte flags][8-byte fingerprint]

const DigestMagic uint16 = 0x4732 // "G2"
const digestFrameSize = 12        // 2 (magic) + 2 (flags) + 8 (fingerprint)

// Digest frame flags.
const (
	FlagDigestMatch uint16 = 1 << 0 // responder sets when fingerprints match
)

// WriteDigestProbe writes a 12-byte digest frame to conn.
func WriteDigestProbe(conn net.Conn, fingerprint uint64, flags uint16) error {
	var buf [digestFrameSize]byte
	binary.BigEndian.PutUint16(buf[0:2], DigestMagic)
	binary.BigEndian.PutUint16(buf[2:4], flags)
	binary.BigEndian.PutUint64(buf[4:12], fingerprint)
	_, err := conn.Write(buf[:])
	return err
}

// ReadDigestBody reads the remaining 10 bytes of a digest frame after the
// 2-byte magic has already been consumed by the multiplexer.
func ReadDigestBody(r io.Reader) (fingerprint uint64, flags uint16, err error) {
	var buf [10]byte // flags(2) + fingerprint(8)
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, 0, fmt.Errorf("digest: read body: %w", err)
	}
	flags = binary.BigEndian.Uint16(buf[0:2])
	fingerprint = binary.BigEndian.Uint64(buf[2:10])
	return fingerprint, flags, nil
}

// Bucketed digest exchange (G2B). When the scalar G2 fingerprints disagree, the
// initiator escalates to a bucket-vector digest so the divergence can be
// localised to a few key-prefix buckets instead of dumping the whole cache.
//
// Wire format: [2-byte magic 0x4742][2-byte flags][1-byte bucketCount][bucketCount × 8-byte BE digest]
//
// bucketCount is a u8 whose value 0 on the wire denotes 256 buckets (the cap),
// so the single byte carries the full [1, maxDigestBuckets] range and the field
// structurally bounds the frame — a peer can never request more than 256
// buckets, so there is no oversize case to reject.
const DigestBucketMagic uint16 = 0x4742 // "GB" — distinct from DigestMagic 0x4732
const maxDigestBuckets = 256            // hard cap; bounds the frame to 256*8 + hdr bytes
const defaultDigestBuckets uint16 = 64  // default N: 64*8 = 512-byte digest, ~1/64 localisation

// WriteBucketDigest writes a G2B frame carrying one 64-bit digest per bucket.
// len(buckets) must be in [1, maxDigestBuckets]; 256 is encoded as the wire
// byte 0 (the u8 field's sentinel for the cap).
func WriteBucketDigest(conn net.Conn, buckets []uint64, flags uint16) error {
	n := len(buckets)
	if n < 1 || n > maxDigestBuckets {
		return fmt.Errorf("digest: bucket count %d out of range [1,%d]", n, maxDigestBuckets)
	}
	buf := make([]byte, 5+n*8)
	binary.BigEndian.PutUint16(buf[0:2], DigestBucketMagic)
	binary.BigEndian.PutUint16(buf[2:4], flags)
	buf[4] = byte(n) // 256 wraps to 0 — the documented sentinel
	off := 5
	for _, b := range buckets {
		binary.BigEndian.PutUint64(buf[off:off+8], b)
		off += 8
	}
	_, err := conn.Write(buf)
	return err
}

// ReadBucketDigestBody reads a G2B frame after the 2-byte magic has already been
// consumed by the multiplexer. The wire byte 0 decodes to 256 buckets (the
// cap), so the returned slice length is always in [1, maxDigestBuckets].
func ReadBucketDigestBody(r io.Reader) (buckets []uint64, flags uint16, err error) {
	var hdr [3]byte // flags(2) + bucketCount(1)
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, 0, fmt.Errorf("digest: read bucket header: %w", err)
	}
	flags = binary.BigEndian.Uint16(hdr[0:2])
	n := int(hdr[2])
	if n == 0 {
		n = maxDigestBuckets // sentinel: 0 on the wire == 256 buckets
	}
	body := make([]byte, n*8)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, 0, fmt.Errorf("digest: read bucket body: %w", err)
	}
	buckets = make([]uint64, n)
	for i := 0; i < n; i++ {
		buckets[i] = binary.BigEndian.Uint64(body[i*8 : i*8+8])
	}
	return buckets, flags, nil
}
