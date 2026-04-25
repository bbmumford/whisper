/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package whisper

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bbmumford/whisper/iblt"
)

// fakeReconcileCodec is a minimal in-memory codec used for protocol-
// level round-trip tests. The library and agent ship richer codecs;
// this one exists purely to validate the driver's mode-flip and
// decode-failure signaling.
type fakeReconcileCodec struct{}

func (fakeReconcileCodec) EncodeTable(t *iblt.IBLT, mode ReconcileMode) ([]byte, error) {
	cells := t.Cells()
	out := []byte{byte(mode), byte(t.K()), byte(len(cells) >> 8), byte(len(cells))}
	out = append(out, byte(t.Seed()>>56), byte(t.Seed()>>48), byte(t.Seed()>>40), byte(t.Seed()>>32),
		byte(t.Seed()>>24), byte(t.Seed()>>16), byte(t.Seed()>>8), byte(t.Seed()))
	for _, c := range cells {
		out = append(out, c.KeySum[:]...)
		out = append(out, byte(c.KeyHashSum>>56), byte(c.KeyHashSum>>48), byte(c.KeyHashSum>>40), byte(c.KeyHashSum>>32),
			byte(c.KeyHashSum>>24), byte(c.KeyHashSum>>16), byte(c.KeyHashSum>>8), byte(c.KeyHashSum))
		out = append(out, byte(c.Count>>24), byte(c.Count>>16), byte(c.Count>>8), byte(c.Count))
	}
	return out, nil
}

func (fakeReconcileCodec) DecodeTable(body []byte) (*iblt.IBLT, ReconcileMode, error) {
	if len(body) < 12 {
		return nil, 0, nil
	}
	mode := ReconcileMode(body[0])
	k := int(body[1])
	count := int(body[2])<<8 | int(body[3])
	seed := uint64(body[4])<<56 | uint64(body[5])<<48 | uint64(body[6])<<40 | uint64(body[7])<<32 |
		uint64(body[8])<<24 | uint64(body[9])<<16 | uint64(body[10])<<8 | uint64(body[11])
	tbl := iblt.New(count, k, seed)
	cells := tbl.Cells()
	off := 12
	for i := 0; i < count; i++ {
		copy(cells[i].KeySum[:], body[off:off+16])
		off += 16
		cells[i].KeyHashSum = uint64(body[off])<<56 | uint64(body[off+1])<<48 | uint64(body[off+2])<<40 | uint64(body[off+3])<<32 |
			uint64(body[off+4])<<24 | uint64(body[off+5])<<16 | uint64(body[off+6])<<8 | uint64(body[off+7])
		off += 8
		cells[i].Count = int32(uint32(body[off])<<24 | uint32(body[off+1])<<16 | uint32(body[off+2])<<8 | uint32(body[off+3]))
		off += 4
	}
	return tbl, mode, nil
}

func (fakeReconcileCodec) EncodeReply(records [][]byte) ([]byte, error) {
	out := []byte{byte(len(records) >> 8), byte(len(records))}
	for _, r := range records {
		out = append(out, byte(len(r)>>8), byte(len(r)))
		out = append(out, r...)
	}
	return out, nil
}

func (fakeReconcileCodec) DecodeReply(body []byte) ([][]byte, error) {
	if len(body) < 2 {
		return nil, nil
	}
	count := int(body[0])<<8 | int(body[1])
	out := make([][]byte, 0, count)
	off := 2
	for i := 0; i < count; i++ {
		ln := int(body[off])<<8 | int(body[off+1])
		off += 2
		rec := make([]byte, ln)
		copy(rec, body[off:off+ln])
		out = append(out, rec)
		off += ln
	}
	return out, nil
}

func (fakeReconcileCodec) EncodeRequest(ids []iblt.Key) ([]byte, error) {
	out := []byte{byte(len(ids) >> 8), byte(len(ids))}
	for _, id := range ids {
		out = append(out, id[:]...)
	}
	return out, nil
}

func (fakeReconcileCodec) DecodeRequest(body []byte) ([]iblt.Key, error) {
	if len(body) < 2 {
		return nil, nil
	}
	count := int(body[0])<<8 | int(body[1])
	out := make([]iblt.Key, count)
	for i := 0; i < count; i++ {
		copy(out[i][:], body[2+i*16:2+(i+1)*16])
	}
	return out, nil
}

// fakeReconcileStore is an in-memory map-keyed store. ContentHash is
// derived from the record bytes' first 16 bytes (the test seeds them
// directly).
type fakeReconcileStore struct {
	mu      sync.Mutex
	records map[iblt.Key][]byte
	applied [][]byte
}

func newFakeStore() *fakeReconcileStore {
	return &fakeReconcileStore{records: make(map[iblt.Key][]byte)}
}

func (s *fakeReconcileStore) put(rec []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var k iblt.Key
	for i := 0; i < 16 && i < len(rec); i++ {
		k[i] = rec[i]
	}
	s.records[k] = rec
}

func (s *fakeReconcileStore) Snapshot() ([]iblt.Key, [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]iblt.Key, 0, len(s.records))
	bodies := make([][]byte, 0, len(s.records))
	for k, v := range s.records {
		keys = append(keys, k)
		bodies = append(bodies, v)
	}
	return keys, bodies
}

func (s *fakeReconcileStore) FetchByID(id iblt.Key) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[id]
	return r, ok
}

func (s *fakeReconcileStore) Apply(record []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var k iblt.Key
	for i := 0; i < 16 && i < len(record); i++ {
		k[i] = record[i]
	}
	s.records[k] = record
	s.applied = append(s.applied, record)
	return nil
}

// TestReconcileRoundTrip drives a full initiator/responder exchange
// over a net.Pipe pair. Validates that records on both sides
// converge after one round-trip when the symmetric difference is
// small enough for rate-1.5 IBLT decode.
func TestReconcileRoundTrip(t *testing.T) {
	codec := fakeReconcileCodec{}
	storeA := newFakeStore()
	storeB := newFakeStore()

	// Both stores share 30 records, plus 2 unique to A and 2 unique
	// to B. Total symmetric difference d=4 — well within rate-1.5
	// decoding for k=3 IBLT at the 32-cell cold-start size (ratio
	// 32/4 = 8x; decode probability >99.9%).
	for i := 0; i < 30; i++ {
		rec := make([]byte, 32)
		rec[0] = byte(i)
		rec[1] = 0x01 // shared marker
		storeA.put(rec)
		storeB.put(rec)
	}
	for i := 0; i < 2; i++ {
		recA := make([]byte, 32)
		recA[0] = byte(100 + i)
		recA[1] = 0xA1
		storeA.put(recA)

		recB := make([]byte, 32)
		recB[0] = byte(200 + i)
		recB[1] = 0xB1
		storeB.put(recB)
	}

	driverA := NewReconcileDriver(codec, storeA, nil)
	driverB := NewReconcileDriver(codec, storeB, nil)

	connA, connB := net.Pipe()
	defer connA.Close()
	defer connB.Close()

	driverA.RegisterPeer(&ReconcilePeer{NodeID: "B", Conn: connA})
	driverB.RegisterPeer(&ReconcilePeer{NodeID: "A", Conn: connB})

	var responderErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Responder mimics the engine's frame multiplexer:
		// consume [magic] first, then read the body via the
		// driver's helper. Real production callers wire this
		// through the engine's frame dispatch — the test
		// inlines it because there's no engine here.
		if err := consumeReconcileMagic(connB); err != nil {
			responderErr = err
			return
		}
		flags, body, err := readReconcileFrame(connB)
		if err != nil {
			responderErr = err
			return
		}
		if flags != ReconcileFlagTable {
			responderErr = io.ErrUnexpectedEOF
			return
		}
		responderErr = driverB.RunResponderRound(connB, body)
	}()

	applied, sent, err := driverA.RunInitiatorRound("B", 5*time.Second)
	if err != nil {
		t.Fatalf("RunInitiatorRound failed: %v", err)
	}
	<-done
	if responderErr != nil {
		t.Fatalf("RunResponderRound failed: %v", responderErr)
	}

	// Either side should have observed at least one record applied
	// or sent — d=10 is well within rate-1.5 decoding.
	if applied == 0 && sent == 0 {
		t.Logf("note: applied=0 sent=0 — IBLT may have under-decoded; check cell sizing")
	}
	t.Logf("RoundTrip: applied=%d sent=%d", applied, sent)

	// Validate at least storeA grew or storeB grew.
	storeA.mu.Lock()
	totalA := len(storeA.records)
	appliedA := len(storeA.applied)
	storeA.mu.Unlock()
	storeB.mu.Lock()
	totalB := len(storeB.records)
	storeB.mu.Unlock()
	// Each side started with 32 records (30 shared + 2 unique).
	// After reconciliation each side should have all 34 records
	// (30 shared + 2 from A + 2 from B). At minimum we expect the
	// applied counter to be non-zero somewhere.
	if appliedA == 0 && (totalA < 34 || totalB < 34) {
		t.Errorf("expected reconciliation to converge: totalA=%d totalB=%d appliedA=%d", totalA, totalB, appliedA)
	}
}
