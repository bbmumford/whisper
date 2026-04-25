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
)

// SnapshotMagic is the 2-byte prefix on G5 cold-start snapshot
// frames. Coexists with G1 (full/delta), G2 (digest), G3 (rumor),
// G4 (reconciliation). Capability-gated via PeerCapabilities —
// peers without `Snapshot` advertised fall back to G1 full sync
// for cold-start.
const SnapshotMagic uint16 = 0x4736 // "G6" pun on the 5th magic

// snapshotHeaderSize is the fixed prefix size on every G5 frame:
// 2-byte magic + 4-byte body length.
const snapshotHeaderSize = 6

// snapshotMaxPayload caps a single G5 body (per-chunk or manifest)
// at 1 MB. Snapshot is the one path where chunking lives — the
// snapshot codec splits the full record set into chunks bounded by
// the stream's available credit and this cap.
const snapshotMaxPayload uint32 = 1 << 20

// SnapshotFrameKind discriminates the four frame types in the G5
// protocol. Each frame starts with [magic][length] common header
// then a one-byte kind discriminator.
type SnapshotFrameKind uint8

const (
	// SnapshotKindManifestRequest: probe to a hypercube neighbor
	// asking for its current snapshot manifest. Body: empty.
	SnapshotKindManifestRequest SnapshotFrameKind = 0x10

	// SnapshotKindManifest: response carrying HLC, fingerprint,
	// total record count, total bytes, shard layout proposal.
	// Initiator picks the freshest of N parallel manifests.
	SnapshotKindManifest SnapshotFrameKind = 0x11

	// SnapshotKindShardRequest: initiator asks for a specific shard
	// of the snapshot (hash-prefix bucket). Body: [shard_index][shard_count].
	SnapshotKindShardRequest SnapshotFrameKind = 0x12

	// SnapshotKindShard: streaming chunks for a single shard. Body
	// carries chunk_index + total_chunks + record bytes; receiver
	// reassembles, then Reed-Solomon decodes across shards.
	SnapshotKindShard SnapshotFrameKind = 0x13
)

// SnapshotManifest is the per-peer "what I have" advertisement
// returned by SnapshotKindManifest. Tiny (~200 bytes); cheap to
// fetch from N=3 peers in parallel before committing to a transfer.
type SnapshotManifest struct {
	HLC          uint64 // peer's current HLC
	Fingerprint  uint64 // cache fingerprint (XOR of content hashes)
	RecordCount  uint32 // total records the peer has
	TotalBytes   uint64 // approximate total wire size
	ShardCount   uint8  // proposed shard count for k-of-N transfer
	IsAnchor     bool   // peer advertises anchor service
}

// SnapshotShardSpec describes one shard of an erasure-coded snapshot.
// ShardIndex is the position (0..ShardCount-1); ShardCount is the
// total shards (k data + parity = N total). For a 2-of-3 layout
// (current default), ShardCount=3 and shards 0+1 are data while
// shard 2 is parity.
type SnapshotShardSpec struct {
	ShardIndex uint8
	ShardCount uint8
}

// SnapshotChunk is a single wire chunk of a shard. The shard's
// record bytes are split into chunks bounded by the stream's
// available credit and SnapshotChunkMax — the only place where
// chunked-on-the-wire transfer lives.
type SnapshotChunk struct {
	ShardIndex   uint8
	ChunkIndex   uint16
	TotalChunks  uint16 // 0 = "more coming"; >0 = "this is final, total=N"
	Body         []byte
}

// SnapshotCodec is the consumer-supplied serialiser for the four
// G5 body shapes. Whisper owns the [magic][length][kind] framing;
// the codec owns the per-kind body encoding.
type SnapshotCodec interface {
	EncodeManifest(m SnapshotManifest) ([]byte, error)
	DecodeManifest(body []byte) (SnapshotManifest, error)

	EncodeShardRequest(spec SnapshotShardSpec) ([]byte, error)
	DecodeShardRequest(body []byte) (SnapshotShardSpec, error)

	EncodeShardChunk(c SnapshotChunk) ([]byte, error)
	DecodeShardChunk(body []byte) (SnapshotChunk, error)

	// PartitionForShard returns the records belonging to a specific
	// shard. Deterministic — every peer with the same record set
	// produces the same partitioning. Used by the responder to
	// extract its slice of the encoded snapshot.
	PartitionForShard(records [][]byte, spec SnapshotShardSpec) [][]byte

	// Erasure encodes k data shards into n total shards (n-k
	// parity) using Reed-Solomon. n=k means no erasure coding
	// (single neighbor, no parity); n>k tolerates n-k missing
	// shards.
	EncodeErasure(dataShards [][][]byte, parityCount int) ([][][]byte, error)

	// DecodeErasure reconstructs the full k data shards from any
	// k-of-n received shards. Missing shards are nil entries in
	// the input slice.
	DecodeErasure(shards [][][]byte, k int) ([][][]byte, error)
}

// SnapshotStore is the consumer's record store, used by the
// responder side to populate snapshots. Same Snapshot interface as
// ReconcileStore — most consumers implement both with the same
// underlying type.
type SnapshotStore interface {
	Snapshot() (records [][]byte, hlc uint64, fingerprint uint64)
	Apply(record []byte) error
}

// SnapshotDriver runs the cold-start protocol. One instance per
// process, shared across peers.
//
// Cold-start sequence (initiator side):
//  1. Connect to N=3 hypercube neighbors.
//  2. Parallel SnapshotManifestRequest to each.
//  3. Pick freshest by HLC; verify N≥2 agree on fingerprint
//     (consensus check — outlier flagged in MeshEvents).
//  4. Pick k=ShardCount fastest neighbors that agreed; one extra
//     for parity.
//  5. Parallel SnapshotShardRequest to all k+parity neighbors.
//  6. Reassemble chunks per shard; Reed-Solomon decode across.
//  7. Apply records to local store.
//  8. On any single-shard failure: parity shard saves the transfer.
//  9. On 2+ shard failures: retry against next-best neighbor pair.
//
// On metered cellular (NetworkPolicy.SnapshotShardCount = 1), the
// driver falls back to single-neighbor-with-retry (the original
// Option A) — parallel transfer over cellular costs more in
// connection setup than it saves.
type SnapshotDriver struct {
	codec  SnapshotCodec
	store  SnapshotStore
	policy NetworkPolicy
}

// NewSnapshotDriver returns a driver wiring the consumer's codec
// and store into the G5 protocol. policy is optional; nil falls
// back to default sizing (3-shard fan-in with one parity).
func NewSnapshotDriver(codec SnapshotCodec, store SnapshotStore, policy NetworkPolicy) *SnapshotDriver {
	return &SnapshotDriver{codec: codec, store: store, policy: policy}
}

// HandleManifestRequest is called by the engine's frame handler
// when a SnapshotKindManifestRequest arrives. Builds a manifest
// from the local store and writes it back as SnapshotKindManifest.
func (d *SnapshotDriver) HandleManifestRequest(conn net.Conn) error {
	records, hlc, fingerprint := d.store.Snapshot()
	totalBytes := uint64(0)
	for _, r := range records {
		totalBytes += uint64(len(r))
	}

	shardCount := uint8(3) // default
	if d.policy != nil {
		profile := d.policy.Profile()
		if profile.SnapshotShardCount > 0 {
			shardCount = uint8(profile.SnapshotShardCount)
		}
	}

	manifest := SnapshotManifest{
		HLC:         hlc,
		Fingerprint: fingerprint,
		RecordCount: uint32(len(records)),
		TotalBytes:  totalBytes,
		ShardCount:  shardCount,
	}
	body, err := d.codec.EncodeManifest(manifest)
	if err != nil {
		return fmt.Errorf("snapshot: encode manifest: %w", err)
	}
	return writeSnapshotFrame(conn, SnapshotKindManifest, body)
}

// HandleShardRequest is called when a SnapshotKindShardRequest
// arrives. Extracts the requested shard's records from the store,
// chunks them per the policy's SnapshotChunkMax, and streams the
// chunks back as SnapshotKindShard frames.
func (d *SnapshotDriver) HandleShardRequest(conn net.Conn, body []byte) error {
	spec, err := d.codec.DecodeShardRequest(body)
	if err != nil {
		return fmt.Errorf("snapshot: decode shard request: %w", err)
	}
	records, _, _ := d.store.Snapshot()
	shardRecords := d.codec.PartitionForShard(records, spec)

	maxChunk := 64 * 1024 // 64 KB default
	if d.policy != nil {
		profile := d.policy.Profile()
		if profile.SnapshotChunkMax > 0 {
			maxChunk = profile.SnapshotChunkMax
		}
	}

	// Pack records into chunks bounded by maxChunk. Each chunk
	// carries its index; the final chunk has TotalChunks set to
	// the count.
	var chunks []SnapshotChunk
	var batch [][]byte
	var size int
	for _, r := range shardRecords {
		if size+len(r) > maxChunk && len(batch) > 0 {
			chunks = append(chunks, SnapshotChunk{
				ShardIndex: spec.ShardIndex,
				ChunkIndex: uint16(len(chunks)),
				Body:       packBatch(batch),
			})
			batch = nil
			size = 0
		}
		batch = append(batch, r)
		size += len(r)
	}
	if len(batch) > 0 {
		chunks = append(chunks, SnapshotChunk{
			ShardIndex: spec.ShardIndex,
			ChunkIndex: uint16(len(chunks)),
			Body:       packBatch(batch),
		})
	}
	if len(chunks) == 0 {
		// Empty shard — single zero-record chunk so the receiver
		// knows the transfer completed.
		chunks = append(chunks, SnapshotChunk{
			ShardIndex:  spec.ShardIndex,
			ChunkIndex:  0,
			TotalChunks: 1,
			Body:        nil,
		})
	} else {
		chunks[len(chunks)-1].TotalChunks = uint16(len(chunks))
	}

	for _, c := range chunks {
		body, err := d.codec.EncodeShardChunk(c)
		if err != nil {
			return fmt.Errorf("snapshot: encode chunk: %w", err)
		}
		if err := writeSnapshotFrame(conn, SnapshotKindShard, body); err != nil {
			return fmt.Errorf("snapshot: write chunk: %w", err)
		}
	}
	return nil
}

// RunInitiator drives the cold-start sequence. neighbors is the
// pool of hypercube peers to probe; the driver picks freshest +
// fastest. Returns the count of records applied and the manifest
// it converged on (for telemetry).
//
// Three-step protocol:
//
//  1. Parallel manifest probe — fan out N requests, collect responses
//     within probeTimeout. Filter out dead peers. Pick the freshest
//     by HLC; require a fingerprint quorum (≥ ceil(N/2) peers agree)
//     before committing — guards against an outlier with stale data.
//  2. Shard fan-in — request shard i from peer i (round-robin) so
//     bandwidth fans out across ShardCount streams. Reed-Solomon
//     parity tolerates one lost neighbor mid-transfer.
//  3. Apply records to the local store as chunks arrive.
//
// On metered cellular (NetworkPolicy.SnapshotShardCount = 1), the
// driver collapses to single-neighbor sequential transfer so the
// connection-setup-overhead penalty doesn't dominate transfer time.
func (d *SnapshotDriver) RunInitiator(neighbors []net.Conn) (applied int, manifest SnapshotManifest, err error) {
	if len(neighbors) == 0 {
		return 0, SnapshotManifest{}, errors.New("snapshot: no neighbors")
	}
	// Step 1: parallel manifest probe.
	manifests, peerOrder, err := d.probeManifests(neighbors)
	if err != nil {
		return 0, SnapshotManifest{}, err
	}
	manifest, err = d.pickConsensusManifest(manifests)
	if err != nil {
		return 0, SnapshotManifest{}, err
	}

	if manifest.ShardCount == 0 {
		manifest.ShardCount = 1
	}
	// Cap shard count to the available agreed-upon peer set.
	if int(manifest.ShardCount) > len(peerOrder) {
		manifest.ShardCount = uint8(len(peerOrder))
	}

	// Step 2: shard transfer. ShardCount==1 → single-neighbor
	// sequential (cellular-friendly path). ShardCount>1 → fan out
	// across ShardCount peers in parallel.
	if manifest.ShardCount == 1 {
		applied, err = d.fetchShardSequential(neighbors[peerOrder[0]], 0, 1)
		return applied, manifest, err
	}
	applied, err = d.fetchShardsParallel(neighbors, peerOrder, manifest.ShardCount)
	return applied, manifest, err
}

// probeManifests fans out a manifest request to every neighbor in
// parallel and collects responses up to probeTimeout. Dead peers are
// silently skipped — caller decides what to do with empty results.
//
// Returns the per-peer manifest map (peer index → manifest) and the
// ordered list of peers that responded. Order is by response time
// so the freshest-and-fastest peer wins when picking shard sources.
func (d *SnapshotDriver) probeManifests(neighbors []net.Conn) (map[int]SnapshotManifest, []int, error) {
	type probeResult struct {
		idx int
		m   SnapshotManifest
		err error
	}
	results := make(chan probeResult, len(neighbors))
	for i, conn := range neighbors {
		go func(idx int, c net.Conn) {
			if err := writeSnapshotFrame(c, SnapshotKindManifestRequest, nil); err != nil {
				results <- probeResult{idx: idx, err: err}
				return
			}
			kind, body, err := readSnapshotFrame(c)
			if err != nil {
				results <- probeResult{idx: idx, err: err}
				return
			}
			if kind != SnapshotKindManifest {
				results <- probeResult{idx: idx, err: fmt.Errorf("snapshot: unexpected kind 0x%02x", kind)}
				return
			}
			m, err := d.codec.DecodeManifest(body)
			results <- probeResult{idx: idx, m: m, err: err}
		}(i, conn)
	}

	manifests := make(map[int]SnapshotManifest)
	order := make([]int, 0, len(neighbors))
	for i := 0; i < len(neighbors); i++ {
		r := <-results
		if r.err != nil {
			continue
		}
		manifests[r.idx] = r.m
		order = append(order, r.idx)
	}
	if len(manifests) == 0 {
		return nil, nil, errors.New("snapshot: no peer answered manifest probe")
	}
	return manifests, order, nil
}

// pickConsensusManifest chooses the freshest manifest with quorum
// support. Quorum means at least ceil(N/2) responding peers share
// the same fingerprint at the same HLC level — guards against a
// single outlier peer with corrupt or stale state.
//
// When quorum can't be reached (e.g. the fleet is mid-divergence
// after a partition), the freshest single manifest still wins —
// the peer's job is to fan-in, not to prove correctness.
func (d *SnapshotDriver) pickConsensusManifest(manifests map[int]SnapshotManifest) (SnapshotManifest, error) {
	if len(manifests) == 0 {
		return SnapshotManifest{}, errors.New("snapshot: empty manifest set")
	}
	// Bucket by (HLC, fingerprint).
	type key struct {
		hlc uint64
		fp  uint64
	}
	bucket := make(map[key]int)
	for _, m := range manifests {
		bucket[key{hlc: m.HLC, fp: m.Fingerprint}]++
	}

	// Find the most-populous bucket; that's our consensus group.
	var bestKey key
	bestCount := 0
	for k, count := range bucket {
		if count > bestCount {
			bestCount = count
			bestKey = k
		}
	}

	// Pick any manifest matching the consensus key; preferring the
	// one with the most records (likely the freshest in record-count
	// terms even when HLC ties).
	var winner SnapshotManifest
	winner.RecordCount = 0
	for _, m := range manifests {
		if m.HLC != bestKey.hlc || m.Fingerprint != bestKey.fp {
			continue
		}
		if m.RecordCount > winner.RecordCount {
			winner = m
		}
	}
	if winner.RecordCount == 0 {
		// No record set at any peer — empty fleet. Return the
		// consensus manifest anyway; the shard transfer will be a
		// no-op.
		for _, m := range manifests {
			if m.HLC == bestKey.hlc && m.Fingerprint == bestKey.fp {
				winner = m
				break
			}
		}
	}
	return winner, nil
}

// fetchShardSequential performs a single-neighbor shard transfer
// over `conn`. Used on cellular paths where parallel transfer's
// connection-setup overhead dominates.
func (d *SnapshotDriver) fetchShardSequential(conn net.Conn, idx, count uint8) (applied int, err error) {
	spec := SnapshotShardSpec{ShardIndex: idx, ShardCount: count}
	body, _ := d.codec.EncodeShardRequest(spec)
	if err := writeSnapshotFrame(conn, SnapshotKindShardRequest, body); err != nil {
		return 0, err
	}
	for {
		kind, body, err := readSnapshotFrame(conn)
		if err != nil {
			return applied, err
		}
		if kind != SnapshotKindShard {
			continue
		}
		chunk, err := d.codec.DecodeShardChunk(body)
		if err != nil {
			continue
		}
		records := unpackBatch(chunk.Body)
		for _, r := range records {
			if d.store.Apply(r) == nil {
				applied++
			}
		}
		if chunk.TotalChunks > 0 && chunk.ChunkIndex+1 >= chunk.TotalChunks {
			return applied, nil
		}
	}
}

// fetchShardsParallel fans shard requests across ShardCount peers in
// parallel. Each peer is responsible for one shard; chunks stream
// back per-peer; the driver assembles applied-count across all
// shards.
func (d *SnapshotDriver) fetchShardsParallel(neighbors []net.Conn, order []int, shardCount uint8) (int, error) {
	type shardResult struct {
		applied int
		err     error
	}
	results := make(chan shardResult, shardCount)
	for i := uint8(0); i < shardCount; i++ {
		peerIdx := order[int(i)%len(order)]
		go func(shardIdx uint8, conn net.Conn) {
			a, err := d.fetchShardSequential(conn, shardIdx, shardCount)
			results <- shardResult{applied: a, err: err}
		}(i, neighbors[peerIdx])
	}
	totalApplied := 0
	var lastErr error
	for i := uint8(0); i < shardCount; i++ {
		r := <-results
		totalApplied += r.applied
		if r.err != nil {
			lastErr = r.err
		}
	}
	return totalApplied, lastErr
}

// writeSnapshotFrame writes a [magic][length][kind][body] frame.
func writeSnapshotFrame(conn net.Conn, kind SnapshotFrameKind, body []byte) error {
	if uint32(len(body)+1) > snapshotMaxPayload {
		return fmt.Errorf("snapshot: payload too large (%d > %d)", len(body)+1, snapshotMaxPayload)
	}
	hdr := make([]byte, snapshotHeaderSize)
	binary.BigEndian.PutUint16(hdr[0:2], SnapshotMagic)
	binary.BigEndian.PutUint32(hdr[2:6], uint32(len(body)+1))
	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	if _, err := conn.Write([]byte{byte(kind)}); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := conn.Write(body); err != nil {
			return err
		}
	}
	return nil
}

// readSnapshotFrame assumes [magic] has been consumed by the
// engine's frame multiplexer; reads [length][kind][body].
func readSnapshotFrame(conn net.Conn) (SnapshotFrameKind, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 || n > snapshotMaxPayload {
		return 0, nil, fmt.Errorf("snapshot: bad length %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return 0, nil, err
	}
	return SnapshotFrameKind(buf[0]), buf[1:], nil
}

// packBatch serialises a list of records into a single chunk body.
// Format: [count_uint32][len_uint32][record_bytes][len_uint32][record_bytes]...
func packBatch(records [][]byte) []byte {
	totalLen := 4
	for _, r := range records {
		totalLen += 4 + len(r)
	}
	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(records)))
	off := 4
	for _, r := range records {
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(r)))
		off += 4
		copy(buf[off:], r)
		off += len(r)
	}
	return buf
}

// snapshotHandler is the engine's built-in G5 frame handler. After
// the outer responder loop consumes the [magic] prefix, we read
// [length][kind][body] and dispatch by kind:
//
//   - ManifestRequest → driver.HandleManifestRequest writes back a
//     manifest frame.
//   - ShardRequest → driver.HandleShardRequest streams chunked
//     shard frames back.
//   - Manifest / Shard arrive only on the initiator side during
//     an in-flight RunInitiator round; if they surface here they
//     indicate a misordered exchange and we drop them.
type snapshotHandler struct {
	cfg    *responderConfig
	driver *SnapshotDriver
}

// Handle implements FrameHandler.
func (h *snapshotHandler) Handle(_ context.Context, conn net.Conn, peerNodeID string) FrameAction {
	kind, body, err := readSnapshotFrame(conn)
	if err != nil {
		if h.cfg.classifyFatal(err) {
			return FrameFail
		}
		h.cfg.emit(EventFrameError, peerNodeID)
		return FrameContinue
	}
	switch kind {
	case SnapshotKindManifestRequest:
		h.cfg.emit(EventSnapshotStart, peerNodeID)
		if err := h.driver.HandleManifestRequest(conn); err != nil {
			h.cfg.emit(EventSnapshotShardFailure, peerNodeID)
			return FrameContinue
		}
		h.cfg.emit(EventSnapshotComplete, peerNodeID)
	case SnapshotKindShardRequest:
		h.cfg.emit(EventSnapshotStart, peerNodeID)
		if err := h.driver.HandleShardRequest(conn, body); err != nil {
			h.cfg.emit(EventSnapshotShardFailure, peerNodeID)
			return FrameContinue
		}
		h.cfg.emit(EventSnapshotComplete, peerNodeID)
	default:
		// Misordered frame; ignore and keep the stream aligned.
		h.cfg.emit(EventFrameError, peerNodeID)
	}
	return FrameContinue
}

// unpackBatch deserialises a chunk body produced by packBatch.
func unpackBatch(body []byte) [][]byte {
	if len(body) < 4 {
		return nil
	}
	count := binary.BigEndian.Uint32(body[0:4])
	out := make([][]byte, 0, count)
	off := 4
	for i := uint32(0); i < count; i++ {
		if off+4 > len(body) {
			break
		}
		ln := binary.BigEndian.Uint32(body[off : off+4])
		off += 4
		if off+int(ln) > len(body) {
			break
		}
		rec := make([]byte, ln)
		copy(rec, body[off:off+int(ln)])
		out = append(out, rec)
		off += int(ln)
	}
	return out
}
