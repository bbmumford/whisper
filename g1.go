/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"context"
	"encoding/binary"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// g1HeaderSize is the fixed size of the G1 frame header: 2-byte
// magic + 4-byte big-endian length.
const g1HeaderSize = 6

// defaultG1MaxPayload caps the body size of a single G1 frame when
// WithMaxPayloadSize wasn't configured. Picked to catch corrupted
// length headers (e.g. WebSocket proxy frame reassembly errors) while
// still accepting full-state dumps on reasonably-sized fleets.
const defaultG1MaxPayload uint32 = 1 << 20 // 1 MiB

// defaultG1Exchange is the per-exchange wall-clock deadline when
// WithExchangeDeadline wasn't configured. A stalled peer blocks only
// this long before the handler gives up and the responder loop exits
// rather than leaking goroutines on a dead socket.
const defaultG1Exchange = 30 * time.Second

// G1ExchangeObserver is invoked once per successful G1 exchange with
// the observed result and the peer's identity. Consumers use this for
// latency metrics, per-peer liveness bookkeeping, and callbacks that
// want to publish ledger records (e.g. round-trip telemetry) after
// each exchange.
type G1ExchangeObserver func(res GossipResult, peerNodeID string)

// g1Package groups the G1-specific wiring the responder loop reads.
// Populated by With* options; all fields are nil/zero by default.
// Transport-level tunables (buffer pool, deadline, max payload, seen-ID
// tracking) live on responderConfig because they apply to every frame
// kind, not just G1.
type g1Package struct {
	store    StateStore
	codec    G1Codec
	delta    *DeltaTracker
	observer G1ExchangeObserver
	metaHook func() *ExchangeMeta
	rumor    *RumorPusher
}

// WithG1Store wires the state store whose records drive G1 exchanges.
// Required for the native G1 handler to activate.
func WithG1Store(s StateStore) EngineOption {
	return func(e *Engine) { e.g1.store = s }
}

// WithG1Codec wires the record serialisation used by the native G1
// handler. Required alongside WithG1Store — without a codec the
// handler can't translate between StateStore records ([]byte) and
// wire bytes.
func WithG1Codec(c G1Codec) EngineOption {
	return func(e *Engine) { e.g1.codec = c }
}

// WithG1ExchangeObserver registers a callback fired after every
// successful G1 exchange on this engine. Observer invocations are
// synchronous on the responder goroutine — keep them quick or
// dispatch to a worker.
func WithG1ExchangeObserver(obs G1ExchangeObserver) EngineOption {
	return func(e *Engine) { e.g1.observer = obs }
}

// g1Handler is the native FrameHandler for the G1 gossip magic. It
// reads the frame body with pooled buffers, hands it to the codec,
// applies each decoded record via the StateStore, emits
// EventRecordApplied when records land, and writes the consumer's
// outbound response (delta or full snapshot) back on the same stream.
type g1Handler struct {
	cfg *responderConfig
	g1  *g1Package
}

// Handle implements FrameHandler.
func (h *g1Handler) Handle(ctx context.Context, conn net.Conn, peerNodeID string) FrameAction {
	if h.g1.store == nil || h.g1.codec == nil {
		// Handler registered but dependencies missing — drain the
		// frame to keep the stream aligned and fail the loop so the
		// misconfiguration isn't masked.
		var lenBuf [4]byte
		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			return FrameFail
		}
		peerLen := binary.BigEndian.Uint32(lenBuf[:])
		if peerLen > h.maxPayload() {
			return FrameFail
		}
		_, _ = io.CopyN(io.Discard, conn, int64(peerLen))
		return FrameFail
	}

	exchangeStart := time.Now()

	// Per-exchange deadline so a stalled remote can't hold the
	// responder indefinitely.
	if d := h.exchangeDeadline(); d > 0 {
		prev := time.Now().Add(d)
		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(prev) {
			prev = ctxDeadline
		}
		_ = conn.SetDeadline(prev)
	}

	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		if h.cfg.classifyFatal(err) {
			return FrameFail
		}
		h.cfg.emit(EventFrameError, peerNodeID)
		return FrameContinue
	}
	peerLen := binary.BigEndian.Uint32(lenBuf[:])
	if peerLen == 0 {
		// Keepalive — no body, no response.
		return FrameContinue
	}
	if peerLen > h.maxPayload() {
		log.Printf("[G1] peer %s payload too large (%d > %d)", peerNodeID, peerLen, h.maxPayload())
		return FrameFail
	}

	// Pooled inbound buffer; grown when the pooled slice is too small
	// for the announced length but not returned oversized (the pool's
	// own sizing policy lives in the consumer-supplied pool).
	pool := h.bufferPool()
	bufPtr := pool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < int(peerLen) {
		buf = make([]byte, peerLen)
	} else {
		buf = buf[:peerLen]
	}
	if _, err := io.ReadFull(conn, buf); err != nil {
		h.returnBuf(pool, bufPtr, buf)
		if h.cfg.classifyFatal(err) {
			return FrameFail
		}
		h.cfg.emit(EventFrameError, peerNodeID)
		return FrameContinue
	}

	decoded, err := h.g1.codec.DecodeExchange(buf)
	h.returnBuf(pool, bufPtr, buf)
	if err != nil {
		log.Printf("[G1] decode from %s: %v", peerNodeID, err)
		h.cfg.emit(EventFrameError, peerNodeID)
		return FrameContinue
	}

	var result GossipResult
	result.PeerMeta = decoded.Meta
	for _, rec := range decoded.Records {
		if err := h.g1.store.Apply(rec); err != nil {
			continue
		}
		result.RecordsApplied++
	}
	if h.cfg.seenIDTracking {
		result.SeenNodeIDs = decoded.SeenNodeIDs
	}
	if result.RecordsApplied > 0 {
		h.cfg.emit(EventRecordApplied, peerNodeID)
	}

	// Build the outbound payload: meta (with fingerprint + PEX
	// piggyback + backpressure signal), then delta-or-full record set
	// per the DeltaTracker's per-peer watermark.
	meta := h.buildOutboundMeta(peerNodeID)
	records := h.selectOutboundRecords(peerNodeID)
	result.RecordsSent = len(records)

	body, err := h.g1.codec.EncodeExchange(records, meta)
	if err != nil {
		log.Printf("[G1] encode to %s: %v", peerNodeID, err)
		h.cfg.emit(EventFrameError, peerNodeID)
		return FrameContinue
	}

	hdr := make([]byte, g1HeaderSize)
	binary.BigEndian.PutUint16(hdr[0:2], GossipMagic)
	binary.BigEndian.PutUint32(hdr[2:6], uint32(len(body)))
	if _, err := conn.Write(hdr); err != nil {
		if h.cfg.classifyFatal(err) {
			return FrameFail
		}
		return FrameContinue
	}
	if _, err := conn.Write(body); err != nil {
		if h.cfg.classifyFatal(err) {
			return FrameFail
		}
		return FrameContinue
	}

	result.NetworkRTT = time.Since(exchangeStart)
	result.RTT = result.NetworkRTT

	if h.g1.delta != nil && peerNodeID != "" {
		h.g1.delta.UpdateWatermark(peerNodeID, time.Now())
	}
	if h.g1.observer != nil {
		h.g1.observer(result, peerNodeID)
	}
	return FrameContinue
}

// buildOutboundMeta constructs the meta attached to the responder's
// reply. Starts from the consumer-supplied metaHook (so roles,
// connection-offer, connection-count piggybacks flow through) and
// overlays the cache fingerprint + PEX entries + backpressure signal
// the engine owns.
func (h *g1Handler) buildOutboundMeta(peerNodeID string) *ExchangeMeta {
	var meta *ExchangeMeta
	if h.g1.metaHook != nil {
		meta = h.g1.metaHook()
	}
	if meta == nil {
		meta = &ExchangeMeta{}
	}
	meta.CacheFingerprint = h.g1.store.Fingerprint()
	return meta
}

// selectOutboundRecords returns the records to include in the
// response — a delta when the peer has a watermark and hasn't hit
// the full-sync interval, otherwise the full snapshot.
func (h *g1Handler) selectOutboundRecords(peerNodeID string) [][]byte {
	dt := h.g1.delta
	if dt == nil || peerNodeID == "" || dt.ShouldFullSync(peerNodeID) {
		return h.g1.store.Snapshot()
	}
	wm := dt.RecordWatermark(peerNodeID)
	if wm.IsZero() {
		return h.g1.store.Snapshot()
	}
	return h.g1.store.Delta(wm)
}

// maxPayload returns the configured or default max G1 body size.
func (h *g1Handler) maxPayload() uint32 {
	if h.cfg.maxPayloadSize > 0 {
		return h.cfg.maxPayloadSize
	}
	return defaultG1MaxPayload
}

// exchangeDeadline returns the configured or default per-exchange
// wall-clock deadline.
func (h *g1Handler) exchangeDeadline() time.Duration {
	if h.cfg.exchangeDeadline > 0 {
		return h.cfg.exchangeDeadline
	}
	return defaultG1Exchange
}

// bufferPool returns the consumer-supplied or default pool for G1
// inbound reads.
func (h *g1Handler) bufferPool() *sync.Pool {
	if h.cfg.bufferPool != nil {
		return h.cfg.bufferPool
	}
	return &defaultG1BufferPool
}

// returnBuf clips oversized slices before returning to the pool so a
// single fat payload doesn't permanently inflate subsequent
// allocations. Mirrors the pattern the library's hand-rolled
// respondToG1Exchange used before the native handler replaced it.
func (h *g1Handler) returnBuf(pool *sync.Pool, bufPtr *[]byte, buf []byte) {
	if pool == nil {
		return
	}
	const cap = 128 * 1024
	if cap > 0 && len(buf) > 0 && cap < len(buf) {
		// discard — slice exceeds cap policy
		return
	}
	b := buf[:0]
	*bufPtr = b
	pool.Put(bufPtr)
}

// defaultG1BufferPool is the fallback when neither WithBufferPool nor
// a g1-specific pool was provided. 32 KiB starting capacity matches
// typical small deltas without penalising full-snapshot payloads that
// transiently grow the slice on first read.
var defaultG1BufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 32*1024)
		return &b
	},
}
