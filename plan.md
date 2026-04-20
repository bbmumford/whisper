# Whisper — Gossip Engine Extraction Plan

> Extract the generic gossip engine from `mesh/core/directory/gossip/` (currently scoped for Aether extraction) into a standalone package: `github.com/bbmumford/whisper`.
>
> **Why separate from Aether?** Three consumers (HSTLES/Ledger, Hospitium, Mercury) each need gossip with different semantics. Without Whisper, each would re-implement topic routing, delta sync, fingerprinting, and payload marshalling. Whisper provides the shared engine; consumers register topics with their own protos and modes.
>
> **Depends on:** Aether (wire protocol, streams)
> **Depended on by:** Ledger, Hospitium, Mercury

---

## 1. What Gets Extracted

### From `mesh/core/directory/gossip/` → Whisper

| Current Path | Whisper Path | Description |
|---|---|---|
| `gossip/sync.go` (wire framing) | `engine.go` | G1/G2/G3 wire framing, exchange loop, parameterised payload |
| `gossip/adaptive_interval.go` | `adaptive.go` | Adaptive gossip interval (renamed from hwp_adaptive.go) |
| `gossip/delta.go` | `delta.go` | Per-peer watermarks for delta sync |
| `gossip/digest.go` | `digest.go` | G2 fingerprint probe |
| `gossip/hypercube.go` | `hypercube.go` | O(log N) dimension-ordered rumor routing |
| `gossip/pex.go` | `pex.go` | Signed peer exchange |
| `gossip/signaling.go` | `signaling.go` | Connection offers, exchange meta |
| `gossip/rtt.go` | `rtt.go` | Per-peer RTT sample tracking |
| `gossip/backpressure.go` | `backpressure.go` | Generic backpressure signaling |
| `gossip/rumor.go` (wire framing) | `rumor.go` | G3 immediate push, parameterised payload |
| `gossip/gossip.proto` (envelope only) | `pb/envelope.proto` | GossipEnvelope + ExchangeMeta protobuf (Step 1 Task 11) |

### Previously in Aether extraction scope

These files were listed in `aether/extraction-plan.md` under `aether/gossip/`. They now move to Whisper instead. Aether becomes pure wire protocol with zero gossip logic.

---

## 2. What Stays Elsewhere

| File | Where | Reason |
|---|---|---|
| `gossip/protocol.go` | HSTLES | LAD-specific envelope (implements `whisper.TopicHandler` for StatefulMerge) |
| `gossip/stream_gossip.go` | HSTLES | Takes `cache.DirectoryCache` — wraps Whisper engine (renamed from hwp_gossip.go) |
| `gossip/codec.go` | HSTLES/Ledger | LADRecord protobuf marshal/unmarshal (Step 1 Task 11) |
| `gossip/pb/gossip.proto` | Split: envelope → Whisper, LADRecord → Ledger |
| `gossip/peer.go` | ALREADY DELETED | Legacy pre-HWP (deleted in Step 2 Phase 1) |
| `gossip/ratelimit.go` | ALREADY DELETED | Legacy wrapper (deleted in Step 2 Phase 1) |
| `gossip/discovery.go` | ALREADY DELETED | Legacy DiscoveryService (deleted in Step 2 Phase 1) |
| `gossip/adaptive.go` | ALREADY DELETED | Legacy net.Conn-based (deleted in HWP→Mesh rename) |

> **Post-Aether updates:** Files renamed during Aether extraction:
> - `hwp_adaptive.go` → `adaptive_interval.go` (AdaptiveHWPInterval → AdaptiveInterval)
> - `hwp_gossip.go` → `stream_gossip.go` (RunHWPGossipLoop → RunGossipLoop)
> - Legacy `adaptive.go` deleted (replaced by adaptive_interval.go)
> - Legacy gossip files (peer.go, ratelimit.go, discovery.go) already deleted
> - All functions renamed from HWP → generic names
> - Gossip package now imports `github.com/ORBTR/aether` (not mesh/core/transport)
| `LADRecord` proto | Ledger (`ledger/pb/record.proto`) | Record wire format is state-layer, not gossip-layer |

---

## 3. Repository Structure

```
github.com/bbmumford/whisper/
├── LICENSE                          (MIT)
├── README.md
├── go.mod                           (module: github.com/bbmumford/whisper)
│
├── engine.go                        (G1/G2/G3 exchange loop, topic dispatch)
├── topic.go                         (TopicRegistry, TopicConfig, TopicMode)
├── adaptive.go                      (AdaptiveInterval — HWP-aware timing)
├── delta.go                         (DeltaTracker — per-peer HLC watermarks)
├── digest.go                        (G2 fingerprint probe frame)
├── hypercube.go                     (O(log N) topology routing)
├── pex.go                           (Signed peer exchange)
├── signaling.go                     (ConnectionOffer, ExchangeMeta)
├── rtt.go                           (Per-peer RTT sample tracking)
├── backpressure.go                  (Congestion signaling)
├── rumor.go                         (G3 immediate push)
├── interfaces.go                    (StateStore, TopicHandler, Subscriber)
├── errors.go                        (Error sentinels)
│
├── pb/
│   ├── envelope.proto               (GossipEnvelope, ExchangeMeta)
│   └── envelope.pb.go               (generated)
│
└── docs/
    └── gossip-protocol-spec.md      (G1/G2/G3 spec, topic semantics)
```

**~15 files, ~2,500 lines**

---

## 4. Core Types

### TopicMode

```go
type TopicMode int

const (
    // BroadcastOnly — firehose pub/sub. No state, no merge, no delta sync.
    // Messages propagate to all subscribers and are not stored.
    // Used by: Mercury category channels, witness announcements, arbitrator profiles.
    BroadcastOnly TopicMode = iota

    // StatefulMerge — delta sync with merge-on-apply.
    // Records are stored in a StateStore. Delta sync uses per-peer watermarks.
    // Merge conflicts resolved by the store's registered MergeFunc (via Ledger).
    // Used by: HSTLES directory (member, role, reach), Hospitium CRDTs.
    StatefulMerge

    // RequestResponse — solicit/reply pattern.
    // One peer sends a query, matching peers reply.
    // No state stored. No propagation beyond responders.
    // Used by: Mercury chain witness queries, peer capability probes.
    RequestResponse
)
```

### TopicConfig

```go
type TopicConfig struct {
    Mode    TopicMode
    Proto   proto.Message    // type hint for unmarshal (nil = opaque bytes)
    Store   StateStore       // required for StatefulMerge, nil for others
    Handler TopicHandler     // required for RequestResponse, optional for others
    TTL     time.Duration    // BroadcastOnly: message expiry (0 = no expiry)
    Rumor   bool             // StatefulMerge: enable G3 rumor push on mutation (default true)
}
```

### StateStore interface

```go
// StateStore is the interface Whisper uses to read/write topic state.
// Ledger's DirectoryCache implements this.
type StateStore interface {
    // Fingerprint returns a cache fingerprint for delta-sync optimisation.
    Fingerprint(topic string) uint64

    // DeltaSince returns records changed since the given watermark.
    DeltaSince(topic string, since uint64) ([][]byte, uint64, error)

    // Apply stores an incoming record, merging via the registered MergeFunc.
    Apply(topic string, data []byte) error
}
```

### TopicHandler interface

```go
// TopicHandler handles incoming messages for a topic.
type TopicHandler interface {
    // OnMessage is called when a message arrives for this topic.
    // For BroadcastOnly: informational (message already propagated).
    // For RequestResponse: must return response bytes (nil = no response).
    OnMessage(ctx context.Context, from NodeID, topic string, payload []byte) (response []byte, err error)
}
```

### Subscriber

```go
// Subscriber receives messages for subscribed topics.
type Subscriber interface {
    OnBroadcast(topic string, from NodeID, payload []byte)
}
```

### Engine

```go
type Engine struct {
    registry     *TopicRegistry
    aetherStream aether.Stream      // gossip stream (ID 0)
    delta        *DeltaTracker
    hypercube    *HypercubeRouter
    pex          *PEXManager
    adaptive     *AdaptiveInterval
    rtt          *RTTTracker
    backpressure *BackpressureMonitor
}

func NewEngine(stream aether.Stream, opts ...EngineOption) *Engine
func (e *Engine) RegisterTopic(name string, config TopicConfig) error
func (e *Engine) Publish(topic string, payload []byte) error        // BroadcastOnly
func (e *Engine) Query(ctx context.Context, topic string, payload []byte) ([][]byte, error)  // RequestResponse
func (e *Engine) Subscribe(topic string, sub Subscriber)
func (e *Engine) Run(ctx context.Context) error                     // starts exchange loop
func (e *Engine) Stop()
```

---

## 5. Gossip Exchange Flow (unchanged from current)

### G1 — Full Exchange (StatefulMerge topics)
```
Initiator                          Responder
    │                                   │
    │── G1 header + ExchangeMeta ──────>│
    │── payload (delta records) ───────>│
    │                                   │
    │<── G1 header + ExchangeMeta ──────│
    │<── payload (delta records) ───────│
    │                                   │
    │  (both apply received records)    │
```

### G2 — Digest Probe (fingerprint check)
```
Responder                          Initiator
    │                                   │
    │<── G2 header + fingerprint ───────│
    │                                   │
    │── (if mismatch) G1 exchange ─────>│
```

### G3 — Rumor Push (immediate propagation)
```
Any peer                           Neighbors
    │                                   │
    │── G3 header + record ────────────>│ (hypercube routing)
    │                                   │── forward to next dimension ──>
```

### BroadcastOnly — Rumor-only (no G1/G2)
```
Publisher                          All subscribers
    │                                   │
    │── G3 rumor ──────────────────────>│ (hypercube flood)
    │                                   │── forward ──>
    │                                   │
    │  (no state, no delta sync)        │
```

### RequestResponse — Solicit/Reply
```
Querier                            Matching peers
    │                                   │
    │── G3 query (marked REQUEST) ─────>│
    │                                   │
    │<── G3 reply (marked RESPONSE) ────│
    │<── G3 reply (marked RESPONSE) ────│  (multiple responders)
    │                                   │
    │  (collect responses, quorum)      │
```

---

## 6. Protobuf Envelope

```protobuf
syntax = "proto3";
package whisper;

option go_package = "github.com/bbmumford/whisper/pb";

message GossipEnvelope {
    ExchangeMeta meta = 1;
    string topic = 2;                // topic name
    TopicMode mode = 3;              // broadcast, stateful, request_response
    repeated bytes records = 4;      // opaque serialised payloads (consumer-defined proto)
}

message ExchangeMeta {
    uint64 cache_fingerprint = 1;
    string region = 2;
    bool backpressure_signal = 3;
    uint64 hlc_watermark = 4;        // for delta sync
    bytes peer_id = 5;
    uint64 timestamp = 6;
}

enum TopicMode {
    TOPIC_BROADCAST = 0;
    TOPIC_STATEFUL = 1;
    TOPIC_REQUEST_RESPONSE = 2;
}
```

---

## 7. Extraction Phases

### Phase 1: Repository Setup (1 hour)
- Create `github.com/bbmumford/whisper`
- Initialise `go.mod` with `github.com/bbmumford/whisper`
- Add MIT LICENSE
- Create README.md (done)

### Phase 2: Code Extraction (1 day)
- Copy generic gossip files from `mesh/core/directory/gossip/` to Whisper
- Rename packages: `package gossip` → `package whisper`
- Create `topic.go` with `TopicRegistry`, `TopicConfig`, `TopicMode`
- Create `interfaces.go` with `StateStore`, `TopicHandler`, `Subscriber`
- Split `gossip.proto`: envelope → `whisper/pb/envelope.proto`
- Extract `engine.go` from `sync.go` wire framing (parameterise with `TopicRegistry`)
- Extract `rumor.go` wire framing (parameterise with topic dispatch)
- Update all internal imports
- Remove HSTLES-specific references (DirectoryCache, lad.Record)

### Phase 3: Test Migration (0.5 days)
- Copy `*_test.go` files
- Update test imports
- Run `go test ./...` — all must pass
- Add CI pipeline

### Phase 4: Consumer Integration (1 day)

#### HSTLES Library
- Add `github.com/bbmumford/whisper` to go.mod
- Create `gossip/lad_whisper.go` — implements `whisper.StateStore` wrapping `DirectoryCache`
- Replace direct gossip engine usage with `whisper.Engine`
- Register HSTLES topics: `member`, `role`, `reach`, `latency` as `StatefulMerge`
- Delete extracted gossip files from HSTLES
- Run full test suite

#### ORBTR Agent
- No direct Whisper import needed — agent uses gossip via HSTLES Library transitively
- `go.work` updated to include `../../_PACKAGES/whisper`

### Phase 5: Cleanup (1 hour)
- Delete `gossip/peer.go`, `gossip/ratelimit.go`, `gossip/discovery.go` (if not already deleted)
- Verify `gossip/adaptive.go` already deleted (Step 1 Task 10a)
- Run fleet verification

---

## 8. Consumer Registration Examples

### HSTLES/ORBTR (mesh directory)
```go
// In mesh/node/runtime.go or hwp_connection.go:
whisperEngine := whisper.NewEngine(gossipStream)

whisperEngine.RegisterTopic("member", whisper.TopicConfig{
    Mode:  whisper.StatefulMerge,
    Store: &LADStateStore{cache: directoryCache},
    Proto: &pb.LADRecord{},
    Rumor: true,
})
whisperEngine.RegisterTopic("role", whisper.TopicConfig{
    Mode:  whisper.StatefulMerge,
    Store: &LADStateStore{cache: directoryCache},
    Proto: &pb.LADRecord{},
})
// ... reach, latency, keyops, quorum
```

### Hospitium (CRDTs)
```go
whisperEngine.RegisterTopic("crdt.lww_map", whisper.TopicConfig{
    Mode:  whisper.StatefulMerge,
    Store: &CRDTStateStore{collection: lwwMapCollection},
    Proto: &pb.LWWMapDelta{},
    Rumor: true,
})
whisperEngine.RegisterTopic("crdt.or_set", whisper.TopicConfig{
    Mode:  whisper.StatefulMerge,
    Store: &CRDTStateStore{collection: orSetCollection},
    Proto: &pb.ORSetDelta{},
    Rumor: true,
})
```

### Mercury (marketplace)
```go
// Broadcast topics — no state, just propagation
whisperEngine.RegisterTopic("mercury.cat.electronics", whisper.TopicConfig{
    Mode: whisper.BroadcastOnly,
    Proto: &pb.CategoryAnnouncement{},
    TTL:  6 * time.Hour,
})
whisperEngine.RegisterTopic("mercury.stores.replicate", whisper.TopicConfig{
    Mode: whisper.BroadcastOnly,
    Proto: &pb.PinAnnouncement{},
    TTL:  1 * time.Hour,
})
whisperEngine.RegisterTopic("mercury.arbitrators", whisper.TopicConfig{
    Mode: whisper.BroadcastOnly,
    Proto: &pb.ArbitratorProfile{},
    TTL:  6 * time.Hour,
})

// Request/response — witness queries
whisperEngine.RegisterTopic("mercury.witness.query", whisper.TopicConfig{
    Mode:    whisper.RequestResponse,
    Proto:   &pb.WitnessQuery{},
    Handler: &WitnessQueryHandler{registry: witnessRegistry},
})

// Stateful — reputation CRDTs (via Hospitium/Ledger)
whisperEngine.RegisterTopic("mercury.reputation.trades", whisper.TopicConfig{
    Mode:  whisper.StatefulMerge,
    Store: &CRDTStateStore{collection: tradeCounter},
    Proto: &pb.GCounterDelta{},
})
```

---

## 9. Dependencies

### Whisper imports
- `github.com/aether-protocol/aether` — `aether.Stream`, `aether.NodeID`
- `google.golang.org/protobuf` — proto marshal/unmarshal

### Whisper does NOT import
- Ledger (Whisper doesn't know about records, merge, or cache)
- Hospitium (Whisper doesn't know about CRDTs)
- Mercury (Whisper doesn't know about listings or witnesses)
- HSTLES (Whisper doesn't know about LAD or tenants)

The dependency arrow is one-way: consumers → Whisper → Aether.

---

## 10. What Changes in Aether After Whisper Extraction

Aether's extraction plan (`aether/extraction-plan.md`) currently lists `aether/gossip/` with ~9 files. After Whisper:

- **Remove from Aether:** All gossip/ files (engine.go, delta.go, digest.go, hypercube.go, pex.go, signaling.go, backpressure.go, rtt.go, rumor.go, adaptive.go, pb/gossip.proto)
- **Remove from Aether:** `GossipPayload` interface
- **Aether becomes:** Pure wire protocol (streams, frames, flow control, encryption, adapters, discovery, RPC)
- **Aether exports:** `aether.Stream` (which Whisper uses for gossip framing)

This makes Aether's scope cleaner — it's the TCP-equivalent, not the pub/sub layer.

---

## 11. Timeline

| Phase | Duration |
|---|---|
| Phase 1 (repo setup) | 1 hour |
| Phase 2 (code extraction) | 1 day |
| Phase 3 (test migration) | 0.5 days |
| Phase 4 (consumer integration) | 1 day |
| Phase 5 (cleanup) | 1 hour |
| **Total** | **~2.5 days** |

Whisper extraction happens AFTER Aether extraction (needs `aether.Stream`) and BEFORE Ledger extraction (Ledger's gossip adapter wraps Whisper).

---

## 12. Implementation Order Position

```
Step 1: ORBTR Memory Optimization + HWP Improvements
Step 2: Aether Extraction (wire protocol only — no gossip)
Step 3: Whisper Extraction (gossip engine)          ← NEW
Step 4: Ledger Extraction + Agnostic Redesign
Step 5: Hospitium Platform
Step 6: Mercury Marketplace
```
