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
	"strings"
	"sync"
	"time"
)

// FrameAction is returned by a FrameHandler to drive the responder loop's
// next step. The handler has already consumed whatever body follows the
// frame magic; the action tells the loop whether to keep reading the
// next frame, exit cleanly (peer closed, no error), or exit with a
// failure (e.g. malformed frame that may leave the stream in an
// undefined state).
type FrameAction int

const (
	// FrameContinue keeps the responder loop running — read the next
	// frame magic.
	FrameContinue FrameAction = iota

	// FrameReturn exits the loop cleanly without error. Used when the
	// handler determines the peer-side has no more frames to send
	// (e.g. graceful goaway).
	FrameReturn

	// FrameFail exits the loop with an error — the stream is in an
	// unrecoverable state and the caller should tear down the
	// connection.
	FrameFail
)

// FrameHandler handles a single frame whose 2-byte magic has already
// been read by the responder loop. The handler reads the remaining
// bytes of the frame from conn, processes it, and returns a
// FrameAction to continue or exit.
//
// peerNodeID is the remote session's identity — threaded through from
// Engine.RunResponder so handlers can discriminate per-peer without a
// separate lookup.
type FrameHandler interface {
	Handle(ctx context.Context, conn net.Conn, peerNodeID string) FrameAction
}

// FrameHandlerFunc adapts a free function into a FrameHandler.
type FrameHandlerFunc func(ctx context.Context, conn net.Conn, peerNodeID string) FrameAction

// Handle delegates to the underlying function.
func (f FrameHandlerFunc) Handle(ctx context.Context, conn net.Conn, peerNodeID string) FrameAction {
	return f(ctx, conn, peerNodeID)
}

// FingerprintProvider is implemented by any StateStore whose entire
// state reduces to a single 64-bit fingerprint (fast equality probe
// before a full G1 exchange). When a consumer's store satisfies this
// interface AND the consumer registers the default G2 handler, peers
// can skip the full G1 exchange when fingerprints match.
type FingerprintProvider interface {
	Fingerprint() uint64
}

// GossipEventKind identifies an out-of-band event the responder loop
// observed. Consumers subscribe via Engine.SubscribeEvents to react
// without reaching into handler internals.
type GossipEventKind int

const (
	// EventDigestMatch fires when a G2 probe found local and peer
	// fingerprints equal — consumers typically update per-peer
	// watermarks to avoid re-exchanging already-converged state.
	EventDigestMatch GossipEventKind = iota

	// EventRecordApplied fires when the G1 handler applies at least
	// one record from a peer exchange.
	EventRecordApplied

	// EventRumorReceived fires when a G3 rumor frame was accepted
	// (passed the dedup tracker and was applied).
	EventRumorReceived

	// EventFrameError fires on any frame-level error the loop
	// recovered from (non-fatal; fatal errors exit the loop
	// without producing an event).
	EventFrameError
)

// GossipEvent is the payload for SubscribeEvents subscribers.
type GossipEvent struct {
	Kind       GossipEventKind
	PeerNodeID string
	Time       time.Time
}

// responderConfig groups the tunables the responder loop consults.
// Populated from the engine's With* options at Run time.
type responderConfig struct {
	// handlers dispatches inbound frames by 2-byte magic. Contains
	// both the built-in G1/G2/G3 handlers (installed by default) and
	// any consumer-supplied custom frame kinds registered via
	// RegisterFrameKind.
	handlers map[uint16]FrameHandler

	// firstFrameGrace is the window to wait after a fatal error
	// reading the very first frame — aether sessions routinely die
	// between stream-open and first-read (dedup race, grade reject)
	// and a single retry after a short delay distinguishes that from
	// a genuine fatal error. Zero disables the grace (return
	// immediately on first fatal).
	firstFrameGrace time.Duration

	// fatalClassifier reports whether an io error from the conn is
	// terminal (session gone) vs. transient (stream stall). Nil =
	// use the package default (EOF / reset / broken pipe).
	fatalClassifier func(error) bool

	// metaProvider builds the outbound ExchangeMeta at the start of
	// each G1 exchange so consumers can attach BackpressureSignal,
	// CacheFingerprint, or other per-tick metadata.
	metaProvider func() *ExchangeMeta

	// bufferPool returns a pooled []byte for inbound G1 payloads.
	// Default is whisper's internal pool.
	bufferPool *sync.Pool

	// exchangeDeadline caps each G1 exchange's wall-clock time. Zero
	// disables the deadline.
	exchangeDeadline time.Duration

	// maxPayloadSize rejects inbound frames declaring a length above
	// this threshold — prevents a malformed or malicious peer from
	// forcing unbounded allocations.
	maxPayloadSize uint32

	// fingerprintProvider is the G2 handler's state source. Required
	// for the default G2 handler to answer digest probes; nil disables
	// G2 (probes are ignored, G1 always runs).
	fingerprintProvider FingerprintProvider

	// rumorTracker / rumorDedupFn / rumorApplyFn power the default G3
	// handler. All three must be non-nil for G3 to be active; leaving
	// any nil skips G3 frames (reading and discarding the body).
	rumorTracker  *RumorTracker
	rumorDedupFn  RumorIDFunc
	rumorApplyFn  func([]byte) error

	// eventBus feeds SubscribeEvents listeners.
	eventBus *eventBus

	// seenIDTracking, when true, causes the default G1 handler to
	// populate GossipResult.SeenNodeIDs from applied records for
	// downstream liveness tracking.
	seenIDTracking bool
}

// eventBus is a simple fan-out to subscriber channels. Non-blocking:
// subscribers that aren't draining their channel miss events — no
// backpressure on the gossip hot path.
type eventBus struct {
	mu   sync.RWMutex
	subs []chan<- GossipEvent
}

func (b *eventBus) subscribe(ch chan<- GossipEvent) {
	b.mu.Lock()
	b.subs = append(b.subs, ch)
	b.mu.Unlock()
}

func (b *eventBus) emit(ev GossipEvent) {
	b.mu.RLock()
	subs := b.subs
	b.mu.RUnlock()
	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

// WithFirstFrameGrace sets the grace period for the very first inbound
// frame on a responder connection. See responderConfig.firstFrameGrace.
func WithFirstFrameGrace(d time.Duration) EngineOption {
	return func(e *Engine) { e.resp.firstFrameGrace = d }
}

// WithFatalErrorClassifier replaces the default terminal-error check.
// The classifier returns true when the error means "session gone,
// exit loop" and false when "transient, keep trying."
func WithFatalErrorClassifier(fn func(error) bool) EngineOption {
	return func(e *Engine) { e.resp.fatalClassifier = fn }
}

// WithMetaProvider supplies the outbound ExchangeMeta builder for
// G1 handlers. Called once per exchange tick; nil = send an empty meta.
func WithMetaProvider(fn func() *ExchangeMeta) EngineOption {
	return func(e *Engine) { e.resp.metaProvider = fn }
}

// WithBufferPool shares a single buffer pool across gossip inbound
// reads. Useful when the consumer already owns a pool tuned for its
// typical payload size distribution.
func WithBufferPool(p *sync.Pool) EngineOption {
	return func(e *Engine) { e.resp.bufferPool = p }
}

// WithExchangeDeadline caps each G1 exchange at the given duration.
func WithExchangeDeadline(d time.Duration) EngineOption {
	return func(e *Engine) { e.resp.exchangeDeadline = d }
}

// WithMaxPayloadSize rejects inbound frames declaring a length above
// the threshold. Prevents malicious or malformed peers from forcing
// unbounded allocations.
func WithMaxPayloadSize(n uint32) EngineOption {
	return func(e *Engine) { e.resp.maxPayloadSize = n }
}

// WithFingerprintProvider wires the G2 digest probe handler's state
// source. Without this, G2 probes are dropped and G1 always runs.
func WithFingerprintProvider(fp FingerprintProvider) EngineOption {
	return func(e *Engine) { e.resp.fingerprintProvider = fp }
}

// WithRumorDedupKeyFunc sets the dedup key function for the default
// G3 handler. Defaults to a sha256 of the payload when nil.
func WithRumorDedupKeyFunc(fn RumorIDFunc) EngineOption {
	return func(e *Engine) { e.resp.rumorDedupFn = fn }
}

// WithRumorApplyFn sets the record-apply callback for the default G3
// handler. Consumers typically route by magic-byte prefix or
// unmarshal as their record type and apply to the local store.
func WithRumorApplyFn(fn func([]byte) error) EngineOption {
	return func(e *Engine) { e.resp.rumorApplyFn = fn }
}

// WithSeenIDTracking enables/disables NodeID collection on G1
// applies (for liveness tracking).
func WithSeenIDTracking(enabled bool) EngineOption {
	return func(e *Engine) { e.resp.seenIDTracking = enabled }
}

// RegisterFrameKind installs a custom FrameHandler for a specific
// 2-byte magic. Returns an error if the magic is already registered or
// collides with a built-in (DigestMagic, gossipMagic, RumorMagic).
// Call before RunResponder — concurrent registration during
// responder loop execution is a race.
func (e *Engine) RegisterFrameKind(magic uint16, handler FrameHandler) error {
	if handler == nil {
		return errors.New("whisper: nil FrameHandler")
	}
	if e.resp.handlers == nil {
		e.resp.handlers = make(map[uint16]FrameHandler)
	}
	if _, exists := e.resp.handlers[magic]; exists {
		return fmt.Errorf("whisper: frame magic 0x%04X already registered", magic)
	}
	e.resp.handlers[magic] = handler
	return nil
}

// SubscribeEvents registers a non-blocking subscriber for gossip
// events. Unbuffered or slow channels drop events. Safe for
// concurrent use.
func (e *Engine) SubscribeEvents(ch chan<- GossipEvent) {
	if e.resp.eventBus == nil {
		e.resp.eventBus = &eventBus{}
	}
	e.resp.eventBus.subscribe(ch)
}

// RunResponder runs the responder-side loop on conn: read a 2-byte
// magic, dispatch to the registered FrameHandler, repeat until the
// context cancels or a handler returns FrameReturn/FrameFail.
//
// Unlike Run (which is timer-driven and initiator-only), RunResponder
// is purely reactive — it blocks reading frames until the peer sends
// one. Consumers typically run ONE side as initiator (via Run) and
// the OTHER side as responder (via RunResponder) over the same
// transport; some protocols (library LAD gossip) run responder on
// both sides and accept either-side initiation.
//
// The built-in G1/G2/G3 handlers are installed on first call so the
// standard gossip protocol works without custom RegisterFrameKind
// calls. Consumers that want to extend the protocol register before
// calling RunResponder.
func (e *Engine) RunResponder(ctx context.Context, conn net.Conn, peerNodeID string) error {
	e.resp.ensureDefaults(e)

	firstRead := true
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var magicBuf [2]byte
		if _, err := io.ReadFull(conn, magicBuf[:]); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if e.resp.classifyFatal(err) {
				if firstRead && e.resp.firstFrameGrace > 0 {
					// Session died before the first frame. Give it a
					// grace window then retry once; exit silently on
					// repeat failure so the peer's retrying initiator
					// doesn't find itself spamming a dead responder
					// that logs every restart.
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(e.resp.firstFrameGrace):
					}
					if _, err2 := io.ReadFull(conn, magicBuf[:]); err2 != nil {
						return nil
					}
					firstRead = false
				} else {
					return err
				}
			} else {
				// Transient — emit event and loop to retry.
				e.resp.emit(EventFrameError, peerNodeID)
				continue
			}
		} else {
			firstRead = false
		}

		magic := binary.BigEndian.Uint16(magicBuf[:])
		handler, ok := e.resp.handlers[magic]
		if !ok {
			// Unknown magic — stream may be desynchronised.
			return fmt.Errorf("whisper: unknown frame magic 0x%04X", magic)
		}
		switch handler.Handle(ctx, conn, peerNodeID) {
		case FrameContinue:
			continue
		case FrameReturn:
			return nil
		case FrameFail:
			return errors.New("whisper: frame handler failed")
		}
	}
}

// ensureDefaults installs the built-in G1/G2/G3 handlers the first
// time the responder loop runs on this engine. Idempotent.
func (c *responderConfig) ensureDefaults(e *Engine) {
	if c.handlers == nil {
		c.handlers = make(map[uint16]FrameHandler)
	}
	if _, ok := c.handlers[DigestMagic]; !ok {
		c.handlers[DigestMagic] = &digestHandler{cfg: c}
	}
	// Wire the rumor handler only when the engine has a rumor pusher
	// AND the consumer wired an apply fn. Without both, rumor frames
	// have nowhere meaningful to go — better to leave the handler
	// unregistered so RunResponder returns cleanly on unknown magic
	// rather than silently dropping rumors.
	if _, ok := c.handlers[RumorMagic]; !ok && e.rumor != nil && c.rumorApplyFn != nil {
		if c.rumorTracker == nil {
			c.rumorTracker = e.rumor.Tracker()
		}
		if c.rumorDedupFn == nil {
			c.rumorDedupFn = defaultRumorDedupKey
		}
		c.handlers[RumorMagic] = &rumorHandler{cfg: c, engine: e}
	}
	// G1 (gossipMagic) is not built-in here — the consumer must
	// register it explicitly because the G1 body format depends on
	// the consumer's topic codec. Library LAD uses a protobuf
	// envelope; other consumers may use JSON or a custom wire.
	if c.fatalClassifier == nil {
		c.fatalClassifier = defaultFatalClassifier
	}
}

// defaultRumorDedupKey hashes the first 32 bytes of a payload with
// sha256 and returns 16 hex chars — deterministic, no allocation
// beyond the hash state, collision probability ~1/(2^64).
func defaultRumorDedupKey(payload []byte) string {
	n := len(payload)
	if n > 32 {
		n = 32
	}
	return fmt.Sprintf("rh:%x", payload[:n])
}

// classifyFatal invokes the configured classifier or the default.
func (c *responderConfig) classifyFatal(err error) bool {
	if c.fatalClassifier != nil {
		return c.fatalClassifier(err)
	}
	return defaultFatalClassifier(err)
}

// emit non-blocking publishes an event through the engine's event bus.
func (c *responderConfig) emit(kind GossipEventKind, peerNodeID string) {
	if c.eventBus == nil {
		return
	}
	c.eventBus.emit(GossipEvent{Kind: kind, PeerNodeID: peerNodeID, Time: time.Now()})
}

// defaultFatalClassifier marks EOF/connection-reset/broken-pipe as
// terminal. Any other error is treated as transient.
func defaultFatalClassifier(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "EOF") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "use of closed")
}

// digestHandler is the built-in G2 handler — reads the peer's
// fingerprint, replies with the local one, and emits EventDigestMatch
// when they agree. Requires a FingerprintProvider; if none is wired,
// replies with 0 and emits no event.
type digestHandler struct {
	cfg *responderConfig
}

// Handle implements FrameHandler.
func (h *digestHandler) Handle(_ context.Context, conn net.Conn, peerNodeID string) FrameAction {
	peerFP, _, err := ReadDigestBody(conn)
	if err != nil {
		if h.cfg.classifyFatal(err) {
			return FrameFail
		}
		h.cfg.emit(EventFrameError, peerNodeID)
		return FrameContinue
	}
	var localFP uint64
	if h.cfg.fingerprintProvider != nil {
		localFP = h.cfg.fingerprintProvider.Fingerprint()
	}
	var respFlags uint16
	if localFP != 0 && localFP == peerFP {
		respFlags = FlagDigestMatch
	}
	if err := WriteDigestProbe(conn, localFP, respFlags); err != nil {
		return FrameFail
	}
	if localFP != 0 && localFP == peerFP {
		h.cfg.emit(EventDigestMatch, peerNodeID)
	}
	return FrameContinue
}

// rumorHandler is the built-in G3 handler — wraps HandleRumorFrame
// with the configured dedup fn + apply fn.
type rumorHandler struct {
	cfg    *responderConfig
	engine *Engine
}

// Handle implements FrameHandler.
func (h *rumorHandler) Handle(_ context.Context, conn net.Conn, peerNodeID string) FrameAction {
	if h.cfg.rumorTracker == nil || h.cfg.rumorDedupFn == nil || h.cfg.rumorApplyFn == nil {
		// No rumor handler configured — consume the body and drop so
		// the stream stays aligned for subsequent frames.
		if _, _, _, err := ReadRumorBody(conn); err != nil {
			if h.cfg.classifyFatal(err) {
				return FrameFail
			}
		}
		return FrameContinue
	}
	HandleRumorFrame(conn, h.cfg.rumorTracker, h.engine.rumor, peerNodeID,
		h.cfg.rumorDedupFn, h.cfg.rumorApplyFn)
	h.cfg.emit(EventRumorReceived, peerNodeID)
	return FrameContinue
}
