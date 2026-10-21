/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// countingBucketFP is a BucketFingerprintProvider that records the bucket
// counts it was asked for, so a test can tell whether the responder computed a
// digest at all, and at which count.
type countingBucketFP struct {
	asked []uint16
}

func (c *countingBucketFP) BucketDigest(n uint16) []uint64 {
	c.asked = append(c.asked, n)
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i) + 1
	}
	return out
}

// runBucketExchange writes one G2B request of n buckets to h over a pipe and
// returns the handler's action, the reply vector (nil when none arrived within
// a short wait) and the error the requester's read ended with.
func runBucketExchange(t *testing.T, h *bucketDigestHandler, n int) (FrameAction, []uint64, error) {
	t.Helper()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	type reply struct {
		vec []uint64
		err error
	}
	replyc := make(chan reply, 1)
	go func() {
		req := make([]uint64, n)
		for i := range req {
			req[i] = 0xFFFF // differs from every bucket countingBucketFP returns
		}
		if err := WriteBucketDigest(client, req, 0); err != nil {
			replyc <- reply{err: err}
			return
		}
		_ = client.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		var magic [2]byte
		if _, err := io.ReadFull(client, magic[:]); err != nil {
			replyc <- reply{err: err}
			return
		}
		if got := binary.BigEndian.Uint16(magic[:]); got != DigestBucketMagic {
			replyc <- reply{err: errors.New("reply is not a G2B frame")}
			return
		}
		vec, _, err := ReadBucketDigestBody(client)
		replyc <- reply{vec: vec, err: err}
	}()

	var magic [2]byte
	if _, err := io.ReadFull(server, magic[:]); err != nil {
		t.Fatalf("read inbound magic: %v", err)
	}
	act := h.Handle(context.Background(), server, "peer")
	// A refusing handler leaves the request body unread; closing the
	// responder's end releases a requester still writing it.
	_ = server.Close()
	r := <-replyc
	return act, r.vec, r.err
}

// The responder digests at the count the requester sent, not at a count of its
// own: with the responder's configured count at 16 and a 64-bucket request, the
// reply has 64 buckets, the provider was asked for 64, and the diverging set
// the G1 follow-up serves from is over 64 buckets. Two different counts would
// compare bucket i of one partition with bucket i of another.
func TestBucketDigestHandler_AnswersAtTheRequestersCount(t *testing.T) {
	fp := &countingBucketFP{}
	cfg := &responderConfig{bucketFingerprintProvider: fp, digestBuckets: 16}
	h := &bucketDigestHandler{cfg: cfg}

	act, reply, err := runBucketExchange(t, h, 64)
	if err != nil {
		t.Fatalf("requester: %v", err)
	}
	if act != FrameContinue {
		t.Fatalf("Handle action = %v, want FrameContinue", act)
	}
	if len(reply) != 64 {
		t.Fatalf("reply has %d buckets, want the requester's 64", len(reply))
	}
	if len(fp.asked) != 1 || fp.asked[0] != 64 {
		t.Fatalf("provider asked for %v, want [64]", fp.asked)
	}
	n, want, ok := cfg.takeDivergingBuckets("peer")
	if !ok || n != 64 || len(want) != 64 {
		t.Fatalf("diverging set = (%d, %d buckets, %v), want 64 buckets", n, len(want), ok)
	}
}

// A count outside the bounds WithDigestBucketBounds set is refused before the
// responder allocates the body or computes a digest: the handler fails the
// frame, writes no reply, asks the provider for nothing and arms no diverging
// set. Counts on either bound are answered.
func TestBucketDigestHandler_RefusesACountOutsideItsBounds(t *testing.T) {
	e := NewEngine(WithDigestBucketBounds(8, 128))
	for _, tc := range []struct {
		n      int
		refuse bool
	}{
		{4, true}, {7, true}, {8, false}, {64, false}, {128, false}, {129, true}, {256, true},
	} {
		fp := &countingBucketFP{}
		cfg := &e.resp
		cfg.bucketFingerprintProvider = fp
		h := &bucketDigestHandler{cfg: cfg}
		act, reply, err := runBucketExchange(t, h, tc.n)
		if !tc.refuse {
			if err != nil || act != FrameContinue || len(reply) != tc.n {
				t.Errorf("n=%d inside [8,128]: action %v, %d reply buckets, err %v; want an answer of %d", tc.n, act, len(reply), err, tc.n)
			}
			cfg.takeDivergingBuckets("peer")
			continue
		}
		if act != FrameFail {
			t.Errorf("n=%d outside [8,128]: action %v, want FrameFail", tc.n, act)
		}
		if reply != nil {
			t.Errorf("n=%d outside [8,128]: a reply of %d buckets was written", tc.n, len(reply))
		}
		if len(fp.asked) != 0 {
			t.Errorf("n=%d outside [8,128]: the provider was asked for %v", tc.n, fp.asked)
		}
		if _, _, ok := cfg.takeDivergingBuckets("peer"); ok {
			t.Errorf("n=%d outside [8,128]: a diverging set was armed", tc.n)
		}
	}
}

// An engine given no bounds answers the whole range the one-byte count field
// carries, 1 to 256, and a range that holds no count refuses every request, so
// a misconfigured profile fails closed.
func TestWithDigestBucketBounds_DefaultsAndEmptyRange(t *testing.T) {
	open := NewEngine()
	if lo, hi := open.resp.digestBucketBounds(); lo != 1 || hi != maxDigestBuckets {
		t.Fatalf("unset bounds = [%d,%d], want [1,%d]", lo, hi, maxDigestBuckets)
	}
	empty := NewEngine(WithDigestBucketBounds(65, 64))
	fp := &countingBucketFP{}
	empty.resp.bucketFingerprintProvider = fp
	act, reply, _ := runBucketExchange(t, &bucketDigestHandler{cfg: &empty.resp}, 64)
	if act != FrameFail || reply != nil || len(fp.asked) != 0 {
		t.Fatalf("empty range: action %v, reply %d buckets, provider asked %v; want a refusal", act, len(reply), fp.asked)
	}
}

// ReadBucketDigestBodyIn checks the declared count against its bounds before it
// reads or allocates the body, and names the count and bounds in a typed error.
func TestReadBucketDigestBodyIn_RefusesBeforeTheBody(t *testing.T) {
	// flags 0, count 200, then no body at all: a reader that reached for the
	// body would report a short read instead of the bound.
	hdr := []byte{0, 0, 200}
	_, _, err := ReadBucketDigestBodyIn(bytes.NewReader(hdr), 1, 64)
	var ce *DigestBucketCountError
	if !errors.As(err, &ce) || !errors.Is(err, ErrDigestBucketCount) {
		t.Fatalf("err = %v, want a *DigestBucketCountError matching ErrDigestBucketCount", err)
	}
	if ce.Count != 200 || ce.Min != 1 || ce.Max != 64 {
		t.Fatalf("error names count %d bounds [%d,%d], want 200 [1,64]", ce.Count, ce.Min, ce.Max)
	}
	// The wire byte 0 is 256 buckets, and is refused by a bound below it.
	if _, _, err := ReadBucketDigestBodyIn(bytes.NewReader([]byte{0, 0, 0}), 1, 255); !errors.Is(err, ErrDigestBucketCount) {
		t.Fatalf("count byte 0 under max 255: err = %v, want ErrDigestBucketCount", err)
	}
}
