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
