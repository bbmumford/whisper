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
	"sync"
	"testing"
	"time"

	"github.com/bbmumford/whisper/iblt"
)

// The tests in this file hold the G4 reconciliation driver to its ceilings
// and its authority rule: a frame length outside the admitted range, a table
// a codec cannot decode or whose mode this build does not have, a decode the
// responder cannot complete, a reply of records the store refuses, a request
// for records the initiator never held and a frame on an engine with no
// reconciliation driver each change no state, and the estimate the mode-flip
// state machine grows stays under its cap.

// readerConn is a net.Conn whose reads come from r and whose writes are
// recorded, so a test can see exactly what a reader pulled and what a writer
// sent. The embedded pipe end serves every other method.
type readerConn struct {
	net.Conn
	r     io.Reader
	mu    sync.Mutex
	wrote []byte
}

func newReaderConn(r io.Reader) *readerConn {
	a, _ := net.Pipe()
	return &readerConn{Conn: a, r: r}
}

func (c *readerConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func (c *readerConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wrote = append(c.wrote, p...)
	return len(p), nil
}

func (c *readerConn) written() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.wrote...)
}

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

// zeroReader yields zero bytes without end.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// g4Body is the [length][flags][body] a G4 frame carries after its magic.
func g4Body(flags ReconcileFlags, body []byte) []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(len(body)+1))
	out = append(out, byte(flags))
	return append(out, body...)
}

// TestReconcileFrameLengthRefusedBeforeTheBody: readReconcileFrame refuses a
// declared length of zero and one of reconcileMaxPayload+1 after the four
// length bytes, reading no body; writeReconcileFrame refuses a body the
// ceiling does not admit and writes nothing; a body at the ceiling travels.
func TestReconcileFrameLengthRefusedBeforeTheBody(t *testing.T) {
	for _, n := range []uint32{0, reconcileMaxPayload + 1, 0xFFFFFFFF} {
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], n)
		cr := &countingReader{r: io.MultiReader(bytes.NewReader(hdr[:]), zeroReader{})}
		conn := newReaderConn(cr)
		_, body, err := readReconcileFrame(conn)
		if err == nil || body != nil {
			t.Fatalf("length %d: body %d bytes, err %v; want a refusal", n, len(body), err)
		}
		if cr.n != 4 {
			t.Fatalf("length %d: read %d bytes before refusing, want the 4 length bytes", n, cr.n)
		}
	}

	conn := newReaderConn(bytes.NewReader(nil))
	over := make([]byte, reconcileMaxPayload) // with its flag byte, one over
	if err := writeReconcileFrame(conn, ReconcileFlagTable, over); err == nil {
		t.Fatalf("a body of %d bytes plus its flag was written", len(over))
	}
	if got := conn.written(); len(got) != 0 {
		t.Fatalf("the refused write sent %d bytes", len(got))
	}
	atCap := make([]byte, reconcileMaxPayload-1)
	atCap[0] = 0x5A
	if err := writeReconcileFrame(conn, ReconcileFlagReply, atCap); err != nil {
		t.Fatalf("a body at the ceiling: %v", err)
	}
	wire := conn.written()
	if len(wire) != reconcileHeaderSize+1+len(atCap) {
		t.Fatalf("wrote %d bytes for a body at the ceiling", len(wire))
	}
	rd := newReaderConn(bytes.NewReader(wire[2:])) // the engine consumes the magic
	flags, body, err := readReconcileFrame(rd)
	if err != nil || flags != ReconcileFlagReply || len(body) != len(atCap) || body[0] != 0x5A {
		t.Fatalf("read back flags %d, %d bytes, %v", flags, len(body), err)
	}
}

// failingCodec is a codec whose table decode fails, as the LAD codec's does
// on a malformed cell stream; its other methods are the fake codec's.
type failingCodec struct{ fakeReconcileCodec }

var errMalformedTable = errors.New("test: malformed cell stream")

func (failingCodec) DecodeTable([]byte) (*iblt.IBLT, ReconcileMode, error) {
	return nil, 0, errMalformedTable
}

// TestResponderRefusesAMalformedOrIncoherentTableBeforeWriting: a table the
// codec refuses, a table the codec returns nothing for, and a table whose
// mode byte names neither mode each make RunResponderRound return a typed
// error and write no frame; the store's records are untouched.
func TestResponderRefusesAMalformedOrIncoherentTableBeforeWriting(t *testing.T) {
	store := newFakeStore()
	store.put([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0xAA})
	snapshotBefore, _ := store.Snapshot()

	conn := newReaderConn(bytes.NewReader(nil))
	err := NewReconcileDriver(failingCodec{}, store, nil).RunResponderRound(conn, []byte("any"))
	if !errors.Is(err, errMalformedTable) {
		t.Fatalf("malformed table: err = %v, want the codec's refusal", err)
	}
	if n := len(conn.written()); n != 0 {
		t.Fatalf("malformed table: the responder wrote %d bytes", n)
	}

	conn = newReaderConn(bytes.NewReader(nil))
	err = NewReconcileDriver(fakeReconcileCodec{}, store, nil).RunResponderRound(conn, []byte{0, 1, 2})
	if err == nil {
		t.Fatalf("a body too short for the codec's header decoded")
	}
	if n := len(conn.written()); n != 0 {
		t.Fatalf("short table: the responder wrote %d bytes", n)
	}

	codec := fakeReconcileCodec{}
	for _, mode := range []ReconcileMode{2, 7, 0xFF} {
		tbl := iblt.New(16, 3, computeSeed("A"))
		body, err := codec.EncodeTable(tbl, mode)
		if err != nil {
			t.Fatal(err)
		}
		conn = newReaderConn(bytes.NewReader(nil))
		err = NewReconcileDriver(codec, store, nil).RunResponderRound(conn, body)
		if !errors.Is(err, ErrReconcileMode) {
			t.Fatalf("mode %d: err = %v, want ErrReconcileMode", mode, err)
		}
		if n := len(conn.written()); n != 0 {
			t.Fatalf("mode %d: the responder wrote %d bytes", mode, n)
		}
	}
	for _, mode := range []ReconcileMode{ReconcileRate15, ReconcileRateless} {
		tbl := iblt.New(16, 3, computeSeed("A"))
		body, _ := codec.EncodeTable(tbl, mode)
		conn = newReaderConn(bytes.NewReader(nil))
		if err := NewReconcileDriver(codec, store, nil).RunResponderRound(conn, body); err != nil {
			t.Fatalf("control: mode %d refused: %v", mode, err)
		}
		if n := len(conn.written()); n == 0 {
			t.Fatalf("control: mode %d wrote nothing", mode)
		}
	}

	snapshotAfter, _ := store.Snapshot()
	if len(snapshotAfter) != len(snapshotBefore) {
		t.Fatalf("the store changed from %d to %d records", len(snapshotBefore), len(snapshotAfter))
	}
}

// TestADecodeTheResponderCannotCompleteIsSignalledAndBounded: a responder
// whose difference from the initiator exceeds the table it was sent writes
// one DecodeFailed frame with no body and applies nothing; the initiator
// records the failure, applies and sends nothing, and after two such rounds
// flips to the rateless mode with a larger estimate. The estimate the
// state machine grows never exceeds its cap, and the cell count it sizes
// never exceeds twice the store's keys plus its headroom.
func TestADecodeTheResponderCannotCompleteIsSignalledAndBounded(t *testing.T) {
	codec := fakeReconcileCodec{}
	initiator := newFakeStore()
	responder := newFakeStore()
	// 200 records only the responder holds, against the 32 cells a cold
	// initiator sizes for an estimate of 16, cannot peel.
	for i := 0; i < 200; i++ {
		rec := make([]byte, 32)
		rec[0] = byte(i)
		rec[1] = byte(i >> 8)
		rec[2] = 0xB1
		responder.put(rec)
	}
	driverA := NewReconcileDriver(codec, initiator, nil)
	driverB := NewReconcileDriver(codec, responder, nil)

	for round := 1; round <= 2; round++ {
		connA, connB := net.Pipe()
		driverA.RegisterPeer(&ReconcilePeer{NodeID: "B", Conn: connA})
		driverB.RegisterPeer(&ReconcilePeer{NodeID: "A", Conn: connB})

		type responderResult struct {
			flagsSeen []ReconcileFlags
			wrote     []byte
			err       error
		}
		resc := make(chan responderResult, 1)
		go func() {
			var res responderResult
			if err := consumeReconcileMagic(connB); err != nil {
				res.err = err
				resc <- res
				return
			}
			flags, body, err := readReconcileFrame(connB)
			if err != nil {
				res.err = err
				resc <- res
				return
			}
			res.flagsSeen = append(res.flagsSeen, flags)
			rec := newReaderConn(bytes.NewReader(nil))
			rec.Conn = connB
			if err := driverB.RunResponderRound(rec, body); err != nil {
				res.err = err
			}
			res.wrote = rec.written()
			// Forward what the responder would have written to the initiator.
			if _, err := connB.Write(res.wrote); err != nil {
				res.err = err
			}
			resc <- res
		}()

		applied, sent, err := driverA.RunInitiatorRound("B", 5*time.Second)
		res := <-resc
		_ = connA.Close()
		_ = connB.Close()
		if err != nil || res.err != nil {
			t.Fatalf("round %d: initiator %v, responder %v", round, err, res.err)
		}
		if applied != 0 || sent != 0 {
			t.Fatalf("round %d: applied %d sent %d on a failed decode", round, applied, sent)
		}
		want := append([]byte{0x47, 0x34, 0, 0, 0, 1}, byte(ReconcileFlagDecodeFailed))
		if !bytes.Equal(res.wrote, want) {
			t.Fatalf("round %d: the responder wrote % x, want one DecodeFailed frame with no body", round, res.wrote)
		}
		if n := len(responder.applied); n != 0 {
			t.Fatalf("round %d: the responder applied %d records", round, n)
		}
		if n := len(initiator.applied); n != 0 {
			t.Fatalf("round %d: the initiator applied %d records", round, n)
		}
		driverA.mu.Lock()
		p := driverA.peers["B"]
		mode, fails, est := p.mode, p.consecutiveFails, p.dMaxEstimate
		driverA.mu.Unlock()
		switch round {
		case 1:
			if mode != ReconcileRate15 || fails != 1 || est != 16 {
				t.Fatalf("after one failure: mode %d fails %d estimate %d", mode, fails, est)
			}
		case 2:
			if mode != ReconcileRateless || fails != 0 || est != 32 {
				t.Fatalf("after two failures: mode %d fails %d estimate %d; want rateless with the estimate raised to 32", mode, fails, est)
			}
		}
	}

	// The estimate saturates at its cap under any number of failures, and
	// every cell count the modes size stays within the store's bound.
	d := NewReconcileDriver(codec, initiator, nil)
	p := &ReconcilePeer{NodeID: "C", mode: ReconcileRateless, dMaxEstimate: 1 << 19}
	d.RegisterPeer(p)
	for i := 0; i < 40; i++ {
		d.recordOutcome(p, false)
		if p.dMaxEstimate > 1<<20 {
			t.Fatalf("estimate %d above the cap after %d failures", p.dMaxEstimate, i+1)
		}
	}
	if p.dMaxEstimate != 1<<20 {
		t.Fatalf("estimate %d after 40 failures, want the cap", p.dMaxEstimate)
	}
	for _, keys := range []int{1, 10, 100, 10_000} {
		for _, mode := range []ReconcileMode{ReconcileRate15, ReconcileRateless} {
			for _, est := range []uint32{0, 16, 1 << 10, 1 << 20} {
				c := computeCellCount(mode, est, keys)
				if c < 1 {
					t.Fatalf("mode %d estimate %d keys %d: %d cells", mode, est, keys, c)
				}
				if c > 2*keys+8 {
					t.Fatalf("mode %d estimate %d keys %d: %d cells, above twice the keys plus headroom", mode, est, keys, c)
				}
			}
		}
	}

	// An empty store has no key count to cap the cells, so a grown estimate
	// sizes a table from the estimate alone; one past the frame ceiling is
	// refused at the write, before a byte reaches the peer, and the round
	// ends with that refusal rather than a stream of cells.
	empty := newFakeStore()
	d = NewReconcileDriver(codec, empty, nil)
	conn := newReaderConn(bytes.NewReader(nil))
	d.RegisterPeer(&ReconcilePeer{NodeID: "D", Conn: conn, mode: ReconcileRateless, dMaxEstimate: 1 << 16})
	if cells := computeCellCount(ReconcileRateless, 1<<16, 0); cells*28 <= int(reconcileMaxPayload) {
		t.Fatalf("control: %d cells fit the frame ceiling; the case needs a table past it", cells)
	}
	_, _, err := d.RunInitiatorRound("D", time.Second)
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("payload too large")) {
		t.Fatalf("a table past the frame ceiling: err = %v, want the write refused for its size", err)
	}
	if n := len(conn.written()); n != 0 {
		t.Fatalf("a table past the frame ceiling: %d bytes were written", n)
	}
}

// refusingStore refuses every record Apply is handed and records nothing.
type refusingStore struct {
	fakeReconcileStore
	refused int
}

var errUnauthenticated = errors.New("test: record failed authentication")

func (s *refusingStore) Apply([]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refused++
	return errUnauthenticated
}

// TestARecordTheStoreRefusesMutatesNothingAndCountsNothing: records in a
// reply the store refuses are not counted as applied and leave the store as
// it was, and a request for keys the initiator never held sends no record;
// the round itself completes and is recorded as a success, since the store,
// not the driver, holds the authority to admit a record.
func TestARecordTheStoreRefusesMutatesNothingAndCountsNothing(t *testing.T) {
	codec := fakeReconcileCodec{}
	store := &refusingStore{fakeReconcileStore: fakeReconcileStore{records: map[iblt.Key][]byte{}}}
	seed := make([]byte, 32)
	seed[0] = 0x11
	store.fakeReconcileStore.put(seed)
	d := NewReconcileDriver(codec, store, nil)

	connA, connB := net.Pipe()
	defer connA.Close()
	defer connB.Close()
	d.RegisterPeer(&ReconcilePeer{NodeID: "hostile", Conn: connA})

	poison := make([]byte, 32)
	poison[0] = 0xEE
	var unknown iblt.Key
	unknown[0] = 0x99
	followUp := make(chan int, 1) // records in the initiator's follow-up reply, -1 on a read error
	go func() {
		// A hostile responder: it reads the table, then replies with a
		// record and asks for a key the initiator never held.
		if err := consumeReconcileMagic(connB); err != nil {
			followUp <- -1
			return
		}
		if _, _, err := readReconcileFrame(connB); err != nil {
			followUp <- -1
			return
		}
		reply, _ := codec.EncodeReply([][]byte{poison, poison})
		_ = writeReconcileFrame(connB, ReconcileFlagReply, reply)
		req, _ := codec.EncodeRequest([]iblt.Key{unknown})
		_ = writeReconcileFrame(connB, ReconcileFlagRequest, req)
		// Read the initiator's follow-up reply and report what it carried.
		if err := consumeReconcileMagic(connB); err != nil {
			followUp <- -1
			return
		}
		_, body, err := readReconcileFrame(connB)
		if err != nil {
			followUp <- -1
			return
		}
		recs, _ := codec.DecodeReply(body)
		followUp <- len(recs)
	}()

	applied, sent, err := d.RunInitiatorRound("hostile", 5*time.Second)
	if err != nil {
		t.Fatalf("RunInitiatorRound: %v", err)
	}
	select {
	case n := <-followUp:
		if n != 0 {
			t.Fatalf("the initiator's follow-up reply carried %d records for a key it never held (-1: no reply read)", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("the hostile responder never saw the initiator's follow-up reply")
	}
	if applied != 0 || sent != 0 {
		t.Fatalf("applied %d sent %d; want 0 and 0", applied, sent)
	}
	store.mu.Lock()
	refused, n, appliedLog := store.refused, len(store.records), len(store.applied)
	store.mu.Unlock()
	if refused != 2 {
		t.Fatalf("the store was offered %d records, want the 2 poisoned ones", refused)
	}
	if n != 1 || appliedLog != 0 {
		t.Fatalf("the store holds %d records and applied %d; want the one seed and none applied", n, appliedLog)
	}
}

// TestAG4FrameOnAnEngineWithoutADriverIsRefusedUnread: an engine with no
// reconciliation driver has no G4 handler, so a G4 frame ends RunResponder
// with the unknown-magic error after the two magic bytes, with the frame's
// length, flags and body unread: there is no path that reads a table a
// session did not negotiate a driver for.
func TestAG4FrameOnAnEngineWithoutADriverIsRefusedUnread(t *testing.T) {
	engine := NewEngine()
	var magic [2]byte
	binary.BigEndian.PutUint16(magic[:], ReconcileMagic)
	frame := append(magic[:], g4Body(ReconcileFlagTable, []byte{1, 2, 3})...)
	cr := &countingReader{r: io.MultiReader(bytes.NewReader(frame), zeroReader{})}
	conn := newReaderConn(cr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := engine.RunResponder(ctx, conn, "peer")
	if err == nil || ctx.Err() != nil {
		t.Fatalf("RunResponder = %v (ctx %v); want the unknown-magic refusal", err, ctx.Err())
	}
	if cr.n != 2 {
		t.Fatalf("read %d bytes before refusing, want the 2 magic bytes", cr.n)
	}
	if n := len(conn.written()); n != 0 {
		t.Fatalf("the engine wrote %d bytes in reply to an unknown frame", n)
	}
}

// TestTheResponderHandlerDropsStrayAndUnknownFlagsWithoutMutation: on the
// responder path a Request, a DecodeFailed and an unknown flag each continue
// the loop, write nothing and apply nothing; a Reply whose records the store
// refuses applies nothing, the round being complete and the store's refusal
// the authority; the engine's event bus sees a frame error for the unknown
// flag, the completion for the reply and nothing for the stray frames.
func TestTheResponderHandlerDropsStrayAndUnknownFlagsWithoutMutation(t *testing.T) {
	codec := fakeReconcileCodec{}
	store := &refusingStore{fakeReconcileStore: fakeReconcileStore{records: map[iblt.Key][]byte{}}}
	engine := NewEngine(WithReconcile(NewReconcileDriver(codec, store, nil)))
	events := make(chan GossipEvent, 16)
	engine.SubscribeEvents(events)
	engine.resp.ensureDefaults(engine)
	h, ok := engine.resp.handlers[ReconcileMagic].(*reconcileHandler)
	if !ok {
		t.Fatalf("no G4 handler on an engine with a driver")
	}

	req, _ := codec.EncodeRequest([]iblt.Key{{1}})
	reply, _ := codec.EncodeReply([][]byte{make([]byte, 32)})
	noEvent := GossipEventKind(255)
	for _, tc := range []struct {
		name  string
		flags ReconcileFlags
		body  []byte
		event GossipEventKind
	}{
		{"stray request", ReconcileFlagRequest, req, noEvent},
		{"stray decode-failed", ReconcileFlagDecodeFailed, nil, noEvent},
		{"unknown flag", ReconcileFlags(0x09), []byte{1, 2, 3}, EventFrameError},
		{"reply the store refuses", ReconcileFlagReply, reply, EventReconcileComplete},
	} {
		conn := newReaderConn(bytes.NewReader(g4Body(tc.flags, tc.body)))
		if act := h.Handle(context.Background(), conn, "peer"); act != FrameContinue {
			t.Fatalf("%s: action %v, want FrameContinue", tc.name, act)
		}
		if n := len(conn.written()); n != 0 {
			t.Fatalf("%s: the handler wrote %d bytes", tc.name, n)
		}
		select {
		case ev := <-events:
			if ev.Kind != tc.event {
				t.Fatalf("%s: event %v, want %v", tc.name, ev.Kind, tc.event)
			}
		case <-time.After(50 * time.Millisecond):
			if tc.event != noEvent {
				t.Fatalf("%s: no event, want %v", tc.name, tc.event)
			}
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.records) != 0 || len(store.applied) != 0 || store.refused != 1 {
		t.Fatalf("store: %d records, %d applied, %d refused; want 0, 0 and the 1 refused reply record", len(store.records), len(store.applied), store.refused)
	}
}
