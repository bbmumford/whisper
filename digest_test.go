/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"encoding/binary"
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
