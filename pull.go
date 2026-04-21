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
	"time"
)

// Pull-based gossip (G4) — the dual of G1 push-delta. A peer with a
// stale watermark for some topic explicitly asks a neighbour for records
// newer than that watermark, instead of waiting for the next adaptive-
// interval G1 tick.
//
// Use cases (2C in the mesh-stabilization plan):
//
//  1. Browser peers (v0.0.169 browser transport) that connect briefly,
//     pull current LAD state to catch up, disconnect. No gossip-cycle
//     participation needed — they just need a one-shot sync.
//  2. Post-partition convergence — after a netsplit heals, a lagging
//     peer can pull to catch up in one round-trip instead of waiting
//     up to the full adaptive-full-sync cycle.
//  3. Diagnostic pulls — help.orbtr.io can pull from any peer for
//     on-demand LAD state without disturbing the peer's gossip cadence.
//
// Wire format (G4 request):
//
//	[2-byte magic 0x4734]
//	[1-byte topicLen]
//	[topicLen-byte topic]
//	[8-byte sinceWatermark — unix nanoseconds, 0 = full sync]
//	[4-byte maxResponseSize — responder caps at min(this, its own cap)]
//
// The RESPONSE shape is implementation-defined (passes through the same
// G1 payload encoder typically). The caller wires its own response
// handler via HandlePullRequest — the package provides the wire-format
// primitives and per-peer rate-limiting hooks only, not an opinion on
// the payload shape.
const PullMagic uint16 = 0x4734 // "G4"

// pullRequestHeaderTail is everything after the 2-byte magic and
// variable-length topic field: 1-byte topicLen (already consumed before
// the header tail) + 8-byte sinceNs + 4-byte maxSize.
const pullRequestHeaderTail = 8 + 4

// MaxPullTopicLen caps the advertised topic name at 255 bytes. Any
// legitimate topic is far shorter; an oversized field is a protocol
// violation.
const MaxPullTopicLen = 255

// DefaultPullResponseMaxSize is the response-size cap used when the
// requester does not supply one (maxResponseSize=0). Matches the G1
// push-delta cap (1 MB) so the pull path doesn't accidentally become a
// larger-payload channel than the push path.
const DefaultPullResponseMaxSize uint32 = 1 << 20

// PullRequest is the decoded form of a G4 PULL_REQUEST frame.
type PullRequest struct {
	Topic           string    // topic to pull
	SinceWatermark  time.Time // records strictly newer than this; zero = full sync
	MaxResponseSize uint32    // responder-side cap (bytes); 0 = use DefaultPullResponseMaxSize
}

// WritePullRequest writes a G4 pull request frame to conn. Counterpart
// of ReadPullRequestBody (which expects the caller to have consumed the
// 2-byte magic already, matching the responder's multiplexer contract
// used by G1/G2/G3).
func WritePullRequest(conn net.Conn, req PullRequest) error {
	if len(req.Topic) > MaxPullTopicLen {
		return fmt.Errorf("pull: topic too long (%d > %d)", len(req.Topic), MaxPullTopicLen)
	}
	buf := make([]byte, 2+1+len(req.Topic)+pullRequestHeaderTail)
	binary.BigEndian.PutUint16(buf[0:2], PullMagic)
	buf[2] = byte(len(req.Topic))
	copy(buf[3:3+len(req.Topic)], req.Topic)
	off := 3 + len(req.Topic)
	var sinceNs int64
	if !req.SinceWatermark.IsZero() {
		sinceNs = req.SinceWatermark.UnixNano()
	}
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(sinceNs))
	off += 8
	binary.BigEndian.PutUint32(buf[off:off+4], req.MaxResponseSize)
	_, err := conn.Write(buf)
	return err
}

// ReadPullRequestBody reads the body of a G4 frame after the 2-byte
// magic has been consumed by the responder multiplexer. Symmetric with
// ReadGossipBody / ReadDigestBody / ReadRumorBody.
func ReadPullRequestBody(r io.Reader) (PullRequest, error) {
	var topicLenBuf [1]byte
	if _, err := io.ReadFull(r, topicLenBuf[:]); err != nil {
		return PullRequest{}, fmt.Errorf("pull: read topicLen: %w", err)
	}
	topicLen := int(topicLenBuf[0])
	if topicLen > MaxPullTopicLen {
		return PullRequest{}, fmt.Errorf("pull: topic length %d exceeds MaxPullTopicLen (%d)", topicLen, MaxPullTopicLen)
	}
	topic := make([]byte, topicLen)
	if _, err := io.ReadFull(r, topic); err != nil {
		return PullRequest{}, fmt.Errorf("pull: read topic: %w", err)
	}
	var tail [pullRequestHeaderTail]byte
	if _, err := io.ReadFull(r, tail[:]); err != nil {
		return PullRequest{}, fmt.Errorf("pull: read tail: %w", err)
	}
	sinceNs := int64(binary.BigEndian.Uint64(tail[0:8]))
	maxSize := binary.BigEndian.Uint32(tail[8:12])
	req := PullRequest{
		Topic:           string(topic),
		MaxResponseSize: maxSize,
	}
	if sinceNs != 0 {
		req.SinceWatermark = time.Unix(0, sinceNs)
	}
	return req, nil
}

// PullFetchFn fetches records matching the pull request. Returns the
// serialised response payload (implementation-specific — typically a G1
// records block), or an error. The returned byte slice is already size-
// capped per the request's MaxResponseSize (or DefaultPullResponseMaxSize
// when zero) — HandlePullRequest validates the cap before calling.
type PullFetchFn func(topic string, since time.Time, maxBytes int) ([]byte, error)

// HandlePullRequest validates the request's rate limit, applies the
// response-size cap, and calls the consumer-supplied fetch function.
// Returns the response bytes (ready to write back on the stream) or an
// error if rate-limited, malformed, or the fetch failed.
//
// The rate limiter is the existing per-peer PEXRateLimiter-style
// limiter consumers already run for G1 gossip — pulls reuse that
// mechanism so a peer can't bypass normal gossip backpressure by
// spamming pulls.
func HandlePullRequest(req PullRequest, allowFn func() bool, fetch PullFetchFn) ([]byte, error) {
	if allowFn != nil && !allowFn() {
		return nil, fmt.Errorf("pull: rate-limited")
	}
	if fetch == nil {
		return nil, fmt.Errorf("pull: no fetch function configured")
	}
	cap := req.MaxResponseSize
	if cap == 0 || cap > DefaultPullResponseMaxSize {
		cap = DefaultPullResponseMaxSize
	}
	resp, err := fetch(req.Topic, req.SinceWatermark, int(cap))
	if err != nil {
		return nil, fmt.Errorf("pull: fetch: %w", err)
	}
	if uint32(len(resp)) > cap {
		return nil, fmt.Errorf("pull: response exceeds cap (%d > %d)", len(resp), cap)
	}
	return resp, nil
}
