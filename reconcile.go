/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/bbmumford/whisper/iblt"
)

// ReconcileMagic is the 2-byte prefix on G4 set-reconciliation
// frames. Coexists with G1 (full/delta), G2 (digest), G3 (rumor) as
// a fourth gossip mode. Capability-gated via PeerCapabilities —
// peers without `Reconciliation` advertised continue to use G1.
const ReconcileMagic uint16 = 0x4734 // "G4"

// reconcileHeaderSize is the fixed prefix size: 2-byte magic +
// 4-byte body length.
const reconcileHeaderSize = 6

// reconcileMaxPayload caps a single G4 body at 1 MB. Larger
// reconciliations flip to rateless mode where each batch is bounded.
const reconcileMaxPayload uint32 = 1 << 20

// ReconcileMode is the per-peer mode the protocol driver runs in.
type ReconcileMode uint8

const (
	// ReconcileRate15 is rate-1.5 mode: sender ships a single
	// fixed-size IBLT (cells = 1.5 × d_max). Receiver decodes once.
	// Most efficient when d_max is a good estimate.
	ReconcileRate15 ReconcileMode = iota

	// ReconcileRateless is rateless mode: sender streams cells;
	// receiver attempts decode after each batch. Adapts to any
	// |d| at the cost of slightly higher bandwidth.
	ReconcileRateless
)

// ReconcileFlags is the per-frame flag byte in the G4 body header.
// Distinguishes the three frame variants:
//
//	0x00 = TableFrame (initiator → responder): the IBLT itself
//	0x01 = ReplyFrame (responder → initiator): records the
//	       initiator is missing
//	0x02 = RequestFrame (responder → initiator): rumor IDs the
//	       responder is missing (initiator pulls these from its
//	       store and sends them as a follow-up TableFrame variant)
type ReconcileFlags uint8

const (
	ReconcileFlagTable   ReconcileFlags = 0x00
	ReconcileFlagReply   ReconcileFlags = 0x01
	ReconcileFlagRequest ReconcileFlags = 0x02

	// ReconcileFlagDecodeFailed is sent by the responder when its IBLT
	// subtract did not converge — body is empty, used purely to signal
	// the initiator that the empty reply is a decode failure (not "we
	// were already in sync"). Without this flag, the initiator can't
	// distinguish "0 records both ways because of failure" from "0
	// records both ways because diff was empty," which means the
	// mode-flip state machine never advances out of rate-1.5 even when
	// the table consistently fails to decode.
	ReconcileFlagDecodeFailed ReconcileFlags = 0x03
)

// ReconcileCodec is the consumer-supplied serialiser for the bodies
// of G4 frames. Whisper owns the [magic][length][flags] framing; the
// codec owns everything inside.
type ReconcileCodec interface {
	// EncodeTable serialises an IBLT into a wire body.
	EncodeTable(t *iblt.IBLT, mode ReconcileMode) ([]byte, error)

	// DecodeTable deserialises an IBLT from a wire body. Receiver-
	// side path; mode is decoded from the body.
	DecodeTable(body []byte) (*iblt.IBLT, ReconcileMode, error)

	// EncodeReply packs records the responder is sending back to
	// the initiator (records the initiator was missing). Records
	// are opaque to whisper; codec defines the per-record format.
	EncodeReply(records [][]byte) ([]byte, error)

	// DecodeReply unpacks a reply body.
	DecodeReply(body []byte) ([][]byte, error)

	// EncodeRequest packs a list of record IDs (16-byte content
	// hashes) the responder is asking the initiator to send.
	EncodeRequest(ids []iblt.Key) ([]byte, error)

	// DecodeRequest unpacks a request body into IDs.
	DecodeRequest(body []byte) ([]iblt.Key, error)
}

// ReconcileStore is the consumer's record store, keyed by content
// hash. Whisper's reconciliation driver calls Snapshot to populate
// the IBLT, FetchByID to pull records the peer is missing, and
// Apply to ingest records the peer sent.
type ReconcileStore interface {
	// Snapshot returns the full set of (content_hash, record_bytes)
	// pairs currently in the store. Used to seed the IBLT.
	Snapshot() ([]iblt.Key, [][]byte)

	// FetchByID returns the record bytes for a given content hash,
	// or false if the store doesn't have it (peer is asking for
	// something we evicted; benign).
	FetchByID(id iblt.Key) ([]byte, bool)

	// Apply ingests an inbound record. Same contract as
	// StateStore.Apply — merge/tombstone resolution is the
	// consumer's responsibility.
	Apply(record []byte) error
}

// ReconcilePeer represents a peer the driver can run a
// reconciliation round with. Wraps the per-peer wire conn plus
// state for mode-flip decisions.
type ReconcilePeer struct {
	NodeID string
	Conn   net.Conn

	// dMaxEstimate is the moving average of recent successful
	// decode record counts × 1.5 (the rate-1.5 sizing factor).
	// Bumps up on a successful decode that returned more than
	// the current estimate; decays slowly otherwise.
	dMaxEstimate uint32

	// Mode-flip state.
	mode             ReconcileMode
	consecutiveFails uint32
	consecutiveOK    uint32
}

// ReconcileDriver is the per-process protocol driver. One instance
// shared across all peer connections; per-peer state lives on
// ReconcilePeer entries.
type ReconcileDriver struct {
	mu     sync.Mutex
	codec  ReconcileCodec
	store  ReconcileStore
	peers  map[string]*ReconcilePeer
	policy NetworkPolicy
}

// NewReconcileDriver returns a driver wiring the consumer's codec
// and store into whisper's protocol logic. policy is optional —
// when present, RunRound uses NetworkPolicy.Profile().ReconcilePacing
// to throttle cell streams on metered links.
func NewReconcileDriver(codec ReconcileCodec, store ReconcileStore, policy NetworkPolicy) *ReconcileDriver {
	return &ReconcileDriver{
		codec:  codec,
		store:  store,
		peers:  map[string]*ReconcilePeer{},
		policy: policy,
	}
}

// Codec exposes the codec for handlers that need to decode frames
// outside the driver's run loops.
func (d *ReconcileDriver) Codec() ReconcileCodec { return d.codec }

// Store exposes the store for handlers that need to read/apply
// records outside the driver's run loops.
func (d *ReconcileDriver) Store() ReconcileStore { return d.store }

// Policy returns the NetworkPolicy reference if one was wired.
func (d *ReconcileDriver) Policy() NetworkPolicy { return d.policy }

// RegisterPeer adds a peer. Idempotent.
func (d *ReconcileDriver) RegisterPeer(p *ReconcilePeer) {
	if p == nil || p.NodeID == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if existing, ok := d.peers[p.NodeID]; ok {
		// Update the conn but preserve mode-flip state.
		existing.Conn = p.Conn
		return
	}
	if p.dMaxEstimate < 16 {
		p.dMaxEstimate = 16 // cold-start floor
	}
	d.peers[p.NodeID] = p
}

// UnregisterPeer drops a peer's state.
func (d *ReconcileDriver) UnregisterPeer(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.peers, nodeID)
}

// PeerCount is the number of registered peers.
func (d *ReconcileDriver) PeerCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.peers)
}

// RunInitiatorRound drives one reconciliation round as the
// initiator. Builds an IBLT from the store, sizes per the peer's
// current mode, sends the table frame, waits for the reply +
// request, applies received records, and serves requested records.
//
// Returns the count of records applied (received from peer) and
// records sent (peer was missing). Errors are wire-level —
// protocol-level failures (decode incomplete) trigger mode flips
// internally and return success with 0 counts.
func (d *ReconcileDriver) RunInitiatorRound(nodeID string, timeout time.Duration) (applied, sent int, err error) {
	d.mu.Lock()
	p, ok := d.peers[nodeID]
	d.mu.Unlock()
	if !ok {
		return 0, 0, errors.New("reconcile: unknown peer")
	}
	if p.Conn == nil {
		return 0, 0, errors.New("reconcile: nil conn")
	}

	// Snapshot the store and build the IBLT at the current mode.
	keys, records := d.store.Snapshot()
	keyToRecord := make(map[iblt.Key][]byte, len(keys))
	for i, k := range keys {
		if i < len(records) {
			keyToRecord[k] = records[i]
		}
	}

	cells := computeCellCount(p.mode, p.dMaxEstimate, len(keys))
	tbl := iblt.New(cells, 3, computeSeed(nodeID))
	for _, k := range keys {
		tbl.Insert(k)
	}

	// Send table frame.
	body, err := d.codec.EncodeTable(tbl, p.mode)
	if err != nil {
		return 0, 0, fmt.Errorf("reconcile: encode table: %w", err)
	}
	if err := writeReconcileFrame(p.Conn, ReconcileFlagTable, body); err != nil {
		return 0, 0, fmt.Errorf("reconcile: write table: %w", err)
	}

	// Read reply (records peer is sending) + request (IDs peer
	// wants from us). Frames may arrive in either order.
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	// NO SetReadDeadline here — aether StreamConn's underlying noise
	// stream flushes its read buffer when SetDeadline is called,
	// killing the session mid-frame. Instead we run the read loop on
	// a goroutine and select on a timer; if the timer fires, we
	// return early with a timeout error. The read goroutine continues
	// until the underlying conn closes (which happens within ~30s via
	// aether's session-stall watchdog), so the leak is bounded.
	type roundResult struct {
		applied, sent int
		decodeFailed  bool
		requestedIDs  []iblt.Key
		err           error
	}
	resultCh := make(chan roundResult, 1)
	go func() {
		var local roundResult
		gotReply := false
		gotRequest := false
		for !gotReply || !gotRequest {
			// Consume the [magic] prefix the responder writes via
			// writeReconcileFrame. On the engine path the frame
			// dispatcher consumes magic and routes to the handler;
			// the initiator side has no engine in front of it so it
			// must consume magic itself before reading the rest of
			// the frame.
			if err := consumeReconcileMagic(p.Conn); err != nil {
				local.err = fmt.Errorf("reconcile: read magic: %w", err)
				resultCh <- local
				return
			}
			flags, body, err := readReconcileFrame(p.Conn)
			if err != nil {
				local.err = fmt.Errorf("reconcile: read: %w", err)
				resultCh <- local
				return
			}
			switch flags {
			case ReconcileFlagReply:
				recs, err := d.codec.DecodeReply(body)
				if err != nil {
					local.err = fmt.Errorf("reconcile: decode reply: %w", err)
					resultCh <- local
					return
				}
				for _, r := range recs {
					if applyErr := d.store.Apply(r); applyErr == nil {
						local.applied++
					}
				}
				gotReply = true
			case ReconcileFlagRequest:
				ids, err := d.codec.DecodeRequest(body)
				if err != nil {
					local.err = fmt.Errorf("reconcile: decode request: %w", err)
					resultCh <- local
					return
				}
				local.requestedIDs = ids
				gotRequest = true
			case ReconcileFlagDecodeFailed:
				// Responder couldn't decode our table — peer's mode-flip
				// state machine treats this as a true failure. Break out
				// of the read loop early; no reply or request is coming.
				local.decodeFailed = true
				gotReply = true
				gotRequest = true
			default:
				// Unknown flag — skip and continue.
			}
		}
		resultCh <- local
	}()

	var res roundResult
	timer := time.NewTimer(timeout)
	select {
	case res = <-resultCh:
		timer.Stop()
		if res.err != nil {
			return res.applied, res.sent, res.err
		}
	case <-timer.C:
		// Read goroutine still hung on conn read. Surface a timeout
		// error so the caller can fall back to G1 instead of waiting
		// for the aether watchdog (30s) to close the conn.
		return 0, 0, fmt.Errorf("reconcile: timeout after %v", timeout)
	}
	applied = res.applied
	decodeFailed := res.decodeFailed
	requestedIDs := res.requestedIDs

	// Send the records the peer requested. Skip on decode failure —
	// no IDs were transmitted so there's nothing to fulfil.
	if !decodeFailed && len(requestedIDs) > 0 {
		out := make([][]byte, 0, len(requestedIDs))
		for _, id := range requestedIDs {
			if rec, ok := d.store.FetchByID(id); ok {
				out = append(out, rec)
			}
		}
		body, err := d.codec.EncodeReply(out)
		if err == nil {
			_ = writeReconcileFrame(p.Conn, ReconcileFlagReply, body)
			sent = len(out)
		}
	}

	// Update mode-flip state based on outcome. ok=false ONLY when
	// the responder explicitly signalled DecodeFailed; an in-sync
	// round (empty diff in both directions) is a successful round.
	d.recordOutcome(p, !decodeFailed)
	return applied, sent, nil
}

// RunResponderRound is called by a frame-handler when it has just
// consumed a G4 TableFrame's [magic][length][flags=0x00] header and
// the body bytes. Builds the symmetric difference and writes back
// the reply + request frames.
func (d *ReconcileDriver) RunResponderRound(conn net.Conn, body []byte) error {
	encTable, _, err := d.codec.DecodeTable(body)
	if err != nil {
		return fmt.Errorf("reconcile: responder decode table: %w", err)
	}

	// Build our own table over the same parameters and subtract.
	keys, records := d.store.Snapshot()
	keyToRecord := make(map[iblt.Key][]byte, len(keys))
	for i, k := range keys {
		if i < len(records) {
			keyToRecord[k] = records[i]
		}
	}

	ourTable := iblt.New(len(encTable.Cells()), encTable.K(), encTable.Seed())
	for _, k := range keys {
		ourTable.Insert(k)
	}

	encTable.Subtract(ourTable)
	res := encTable.Decode()
	if !res.Complete {
		// Decode failed — emit the explicit DecodeFailed flag so the
		// initiator's mode-flip state machine can distinguish this
		// case from "diff is genuinely empty." The flag carries no
		// body. Initiator side promotes consecutiveFails on receipt.
		_ = writeReconcileFrame(conn, ReconcileFlagDecodeFailed, nil)
		return nil
	}

	// res.Positive: keys the initiator has but we don't — request
	// these.
	// res.Negative: keys we have but initiator doesn't — send these
	// as records.
	outRecords := make([][]byte, 0, len(res.Negative))
	for _, k := range res.Negative {
		if r, ok := keyToRecord[k]; ok {
			outRecords = append(outRecords, r)
		}
	}
	replyBody, _ := d.codec.EncodeReply(outRecords)
	if err := writeReconcileFrame(conn, ReconcileFlagReply, replyBody); err != nil {
		return fmt.Errorf("reconcile: responder write reply: %w", err)
	}
	requestBody, _ := d.codec.EncodeRequest(res.Positive)
	if err := writeReconcileFrame(conn, ReconcileFlagRequest, requestBody); err != nil {
		return fmt.Errorf("reconcile: responder write request: %w", err)
	}

	// DO NOT block reading the follow-up reply here. Returning
	// promptly hands control back to the engine's frame loop, which
	// is what reads inbound frames on this stream. The initiator's
	// follow-up reply (ReconcileFlagReply with the records we
	// requested) arrives as another G4 frame and gets routed back
	// to reconcileHandler.Handle — which now branches on flags and
	// applies inbound records when seen.
	//
	// Earlier revisions read the followup synchronously here, which
	// blocked the engine's frame loop for the duration of the
	// followup write. With a stalled or non-responsive initiator,
	// that wait stretched until Aether keepalive tore down the
	// whole session — exactly the cascade we observed in production
	// (RPC dispatch failures coinciding with reconcile rounds).
	return nil
}

// recordOutcome updates a peer's mode-flip state based on the
// outcome of a reconciliation round. The state machine:
//   - rate-1.5 + 2 consecutive decode failures → flip to rateless
//   - rateless + 5 consecutive single-batch successes → flip to
//     rate-1.5
//
// "Decode failure" here = the round completed but produced empty
// reply + request, indicating the responder's IBLT subtract didn't
// converge. Empty results that match an empty-difference state
// (peer already in sync) are not failures.
func (d *ReconcileDriver) recordOutcome(p *ReconcilePeer, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if ok {
		p.consecutiveOK++
		p.consecutiveFails = 0
		if p.mode == ReconcileRateless && p.consecutiveOK >= 5 {
			p.mode = ReconcileRate15
			p.consecutiveOK = 0
		}
		return
	}
	p.consecutiveFails++
	p.consecutiveOK = 0
	if p.mode == ReconcileRate15 && p.consecutiveFails >= 2 {
		p.mode = ReconcileRateless
		p.consecutiveFails = 0
	}
	// Rateless growth: each consecutive failure in rateless mode
	// doubles the estimate, capped at 1<<20 (≈ 1M cells, the wire
	// max). This is the "rateless streaming" property in single-shot
	// form — instead of streaming more cells per round-trip, we grow
	// the per-round size on failure so the next round has a much
	// higher decode probability.
	if p.mode == ReconcileRateless {
		next := p.dMaxEstimate * 2
		if next < 32 {
			next = 32
		}
		const maxEstimate uint32 = 1 << 20
		if next > maxEstimate {
			next = maxEstimate
		}
		p.dMaxEstimate = next
	}
}

// computeCellCount returns the IBLT cell count for the current mode
// and per-peer estimate. Rate-1.5 sizes at 1.5 × d_max + headroom —
// efficient when d_max is well-estimated. Rateless oversizes
// aggressively to 2.5 × d_max + larger headroom — burns more
// bandwidth in exchange for very high decode probability when the
// estimate is poor (post-redeploy, post-partition, fresh peers).
//
// totalKeys caps the cell count: there's no point sending more cells
// than total store keys × 2 — a fully-disjoint difference is a worse
// case for IBLT than streaming the whole store, so we let the
// mode-flip state machine + caller's higher-level reconciler decide
// when to escalate to a full G1 snapshot.
func computeCellCount(mode ReconcileMode, dMax uint32, totalKeys int) int {
	var c int
	switch mode {
	case ReconcileRate15:
		c = int(dMax) + int(dMax)/2 + 8 // 1.5x + small headroom
		if c < 16 {
			c = 16
		}
	case ReconcileRateless:
		// Aggressive oversize: 2.5 × d_max + 32 floor. Materially
		// different from rate-1.5 so a peer that fails twice in
		// rate-1.5 sees a meaningfully larger second attempt — not
		// just a slightly-bigger one. Pairs with consecutiveFails
		// in recordOutcome which doubles dMaxEstimate so the size
		// grows on every miss.
		c = 2*int(dMax) + int(dMax)/2 + 32
		if c < 64 {
			c = 64
		}
	default:
		return 32
	}
	// Hard ceiling: no point sending more cells than 2× total keys.
	if totalKeys > 0 && c > 2*totalKeys+8 {
		c = 2*totalKeys + 8
	}
	return c
}

// computeSeed derives a per-peer IBLT seed so the encoder and
// receiver agree on hash positions without coordination.
func computeSeed(peerID string) uint64 {
	var s uint64
	for i, b := range []byte(peerID) {
		s ^= uint64(b) << (uint(i%8) * 8)
	}
	if s == 0 {
		s = 0xC0DECAFE
	}
	return s
}

// writeReconcileFrame writes a [magic][length][flags][body] frame.
func writeReconcileFrame(conn net.Conn, flags ReconcileFlags, body []byte) error {
	if uint32(len(body)+1) > reconcileMaxPayload {
		return fmt.Errorf("reconcile: payload too large (%d > %d)", len(body)+1, reconcileMaxPayload)
	}
	hdr := make([]byte, reconcileHeaderSize)
	binary.BigEndian.PutUint16(hdr[0:2], ReconcileMagic)
	binary.BigEndian.PutUint32(hdr[2:6], uint32(len(body)+1))
	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	if _, err := conn.Write([]byte{byte(flags)}); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := conn.Write(body); err != nil {
			return err
		}
	}
	return nil
}

// readReconcileFrame reads a frame body assuming the [magic] has
// already been consumed by the engine's frame multiplexer.
// Returns flags + body bytes (excluding the flags byte).
func readReconcileFrame(conn net.Conn) (ReconcileFlags, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 || n > reconcileMaxPayload {
		return 0, nil, fmt.Errorf("reconcile: bad length %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return 0, nil, err
	}
	return ReconcileFlags(buf[0]), buf[1:], nil
}

// consumeReconcileMagic reads and validates the 2-byte ReconcileMagic
// prefix off the wire. The engine's frame multiplexer consumes it on
// the responder path; initiator-side reads (RunInitiatorRound,
// followups inside RunResponderRound) call this directly because no
// engine sits in front of them. Returns an error on any byte other
// than ReconcileMagic, so a misordered frame surfaces as a clear
// protocol error rather than a corrupted-length read.
func consumeReconcileMagic(conn net.Conn) error {
	var magicBuf [2]byte
	if _, err := io.ReadFull(conn, magicBuf[:]); err != nil {
		return err
	}
	got := binary.BigEndian.Uint16(magicBuf[:])
	if got != ReconcileMagic {
		return fmt.Errorf("reconcile: bad magic 0x%04x (want 0x%04x)", got, ReconcileMagic)
	}
	return nil
}

// mustEncode wraps encoding helpers that return (body, err) so
// inline error-discarding fits the responder's flow. Empty bodies
// are valid for both reply and request (means "nothing to send").
func mustEncode[T any](fn func(T) ([]byte, error), arg T) []byte {
	body, err := fn(arg)
	if err != nil {
		return nil
	}
	return body
}

// reconcileHandler is the engine's built-in G4 frame handler. The
// outer responder loop has already consumed the [magic] prefix; we
// read [length][flags][body] and dispatch a TableFrame into the
// driver's RunResponderRound. ReplyFrame and RequestFrame arrive
// only as part of an in-flight initiator round (the initiator's
// RunInitiatorRound reads them inline) so the responder handler
// drops anything other than a TableFrame as a misordered frame.
type reconcileHandler struct {
	cfg    *responderConfig
	driver *ReconcileDriver
}

// Handle implements FrameHandler. Reads the body bytes after the
// already-consumed magic and dispatches based on flag:
//
//	ReconcileFlagTable          — initiator's table frame; build diff +
//	                              write reply + request, then return.
//	                              The follow-up Reply (records the
//	                              initiator was missing) is read from
//	                              the engine loop on its OWN call to
//	                              this handler.
//	ReconcileFlagReply          — initiator's follow-up reply with
//	                              records we requested; apply them.
//	ReconcileFlagRequest        — should not arrive here (initiator
//	                              never sends Request frames); ignored.
//	ReconcileFlagDecodeFailed   — peer signalled decode failure; nothing
//	                              to do at the responder side.
//
// Returns FrameContinue on success — reconciliation is one frame per
// call; multi-frame round happens via multiple invocations.
func (h *reconcileHandler) Handle(_ context.Context, conn net.Conn, peerNodeID string) FrameAction {
	flags, body, err := readReconcileFrame(conn)
	if err != nil {
		if h.cfg.classifyFatal(err) {
			return FrameFail
		}
		h.cfg.emit(EventFrameError, peerNodeID)
		return FrameContinue
	}
	switch flags {
	case ReconcileFlagTable:
		h.cfg.emit(EventReconcileStart, peerNodeID)
		if err := h.driver.RunResponderRound(conn, body); err != nil {
			h.cfg.emit(EventReconcileDecodeFailure, peerNodeID)
			return FrameContinue
		}
		h.cfg.emit(EventReconcileComplete, peerNodeID)
	case ReconcileFlagReply:
		// Initiator's follow-up reply with records we requested.
		// Apply them to the store; no response needed.
		recs, err := h.driver.codec.DecodeReply(body)
		if err == nil {
			for _, r := range recs {
				_ = h.driver.store.Apply(r)
			}
			h.cfg.emit(EventReconcileComplete, peerNodeID)
		} else {
			h.cfg.emit(EventFrameError, peerNodeID)
		}
	case ReconcileFlagRequest, ReconcileFlagDecodeFailed:
		// Stray frame on the responder path — drop quietly.
	default:
		h.cfg.emit(EventFrameError, peerNodeID)
	}
	return FrameContinue
}
