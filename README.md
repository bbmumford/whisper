# Whisper — Topic-Based Gossip Engine

```
go get github.com/bbmumford/whisper
```

**Related:** [Aether](https://github.com/ORBTR/Aether) (wire protocol) · [Ledger](https://github.com/bbmumford/ledger) (state directory)

> **Whisper** is a generic, topic-based gossip engine for peer-to-peer state propagation. It provides the pub/sub, delta sync, and topology routing layers that sit between a wire protocol and application state. Transport-agnostic — it operates on any bidirectional byte stream (Aether streams, raw TCP, WebSocket, gRPC bidi) but knows nothing about what it carries.

---

## Status

| Area | Status |
|------|--------|
| **Topic registry + three modes** (BroadcastOnly, StatefulMerge, RequestResponse) | Stable |
| **G1/G2/G3 wire framing** (full exchange / digest probe / rumor push) | Stable |
| **Delta sync with per-peer HLC watermarks** | Stable |
| **Hypercube routing** (O(log N) dimension-ordered propagation) | Stable |
| **Peer exchange (PEX)** with signed address advertisements | Stable |
| **Adaptive cadence** (backs off when idle, accelerates during convergence) | Stable |
| **Backpressure signalling** between peers | Stable |
| **RTT tracking** (per-peer round-trip measurement) | Stable |
| **Protobuf envelope** with opaque payload | Stable |
| **Rate limiting** + rumor tracker | Stable |
| **v0.0.1 release** | Shipped (2026-04-20) |

In production use by the HSTLES mesh directory gossip across `bootstrap.hstles.com` + `node.hstles.com` + every ORBTR endpoint, carrying Ledger records between peers with delta sync cutting gossip bandwidth by ~90% vs full-state exchange.

---

## What Whisper Provides

| Capability | Description |
|------------|-------------|
| **Topic registry** | Named topics with typed payloads, per-topic configuration |
| **Three gossip modes** | `BroadcastOnly` (firehose), `StatefulMerge` (delta sync + merge), `RequestResponse` (solicit/reply) |
| **G1/G2/G3 wire framing** | Full exchange (G1), digest probe (G2), rumor push (G3) |
| **Delta sync** | Per-peer hybrid-logical-clock watermarks for ~90% bandwidth reduction |
| **Hypercube routing** | O(log N) dimension-ordered routing for rumor propagation |
| **Peer exchange (PEX)** | Signed peer address advertisements |
| **Adaptive interval** | Gossip timing that backs off when idle, accelerates during convergence |
| **Backpressure signaling** | Generic congestion signaling between peers |
| **RTT tracking** | Per-peer round-trip time measurement |
| **Protobuf envelope** | Generic `GossipEnvelope` with opaque `repeated bytes` payload |
| **Connection offers** | Signaling for NAT traversal and peer introduction |
| **Fingerprint matching** | Cache fingerprint comparison to skip redundant exchanges |

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Applications                                   │
│      HSTLES / ORBTR      custom app             │
│             │                  │                │
│             └──────┬───────────┘                │
│                    │ RegisterTopic()            │
│            ┌───────▼─────────────┐              │
│            │      Whisper        │              │
│            │                     │              │
│            │  BroadcastOnly      │              │
│            │  StatefulMerge      │              │
│            │  RequestResponse    │              │
│            └───────┬─────────────┘              │
│                    │ bidirectional byte stream  │
│            ┌───────▼─────────────┐              │
│            │   Aether / TCP /    │              │
│            │   WS / gRPC / etc.  │              │
│            └─────────────────────┘              │
└─────────────────────────────────────────────────┘
```

---

## Gossip exchange

Each topic is registered with a `TopicConfig` (`Engine.RegisterTopic`):

| Field | Meaning |
|-------|---------|
| `Mode` | `BroadcastOnly`: pub/sub with nothing stored or merged. `StatefulMerge`: records kept in a `StateStore` and kept in step by delta sync. `RequestResponse`: a query answered by a handler; nothing stored. |
| `Store` | the topic's `StateStore`; required for `StatefulMerge` |
| `Handler` | the topic's `TopicHandler`; required for `RequestResponse` |
| `TTL` | how long a `BroadcastOnly` message lives (0: no expiry) |
| `Rumor` | whether a `StatefulMerge` mutation is also pushed at once as a rumor |
| `Priority` | dispatch priority under egress pressure (higher is preferred) |

A `StateStore` is the consumer's side of a stateful topic: `Fingerprint()` (a 64-bit summary that changes on every mutation), `Snapshot()`, `Delta(since)` and `Apply(record)`. Whisper moves opaque record bytes; merge, tombstone and last-writer-wins rules belong to the store. `TopicHandler.OnMessage` receives a topic's messages and, for `RequestResponse`, returns the reply.

`Engine.Run` drives one peer connection as the initiator and `Engine.RunResponder` as the responder. `Publish` sends on a topic and `Subscribe` adds a `Subscriber`. `Query` runs a `RequestResponse` topic's handler and returns its reply; it answers from the local handler only and does not yet solicit other peers.

Every frame starts with a 2-byte magic:

- **G2 digest probe** (`0x4732`): before a full exchange the initiator sends a 12-byte frame holding its store fingerprint, and the responder answers with its own. Equal fingerprints end the round with no records sent. When they differ, the initiator can escalate to a bucketed digest (G2B, `0x4742`) that narrows the divergence to a few key-prefix buckets.
- **G1 full exchange** (`0x4731`, then a 4-byte length): each side sends an `ExchangeMeta` and the records changed since its per-peer watermark (the whole snapshot when it has none), and each applies what it receives.
- **G3 rumor push** (`0x4733`): one record pushed to neighbours at once and forwarded dimension by dimension over the hypercube overlay. `BroadcastOnly` topics travel only this way.

---

## Consumer Examples

```go
// HSTLES — mesh directory (delta sync + merge via Ledger)
whisper.RegisterTopic("member", whisper.TopicConfig{
    Mode:  whisper.StatefulMerge,
    Store: ladCache, // implements whisper.StateStore
    Proto: &pb.LADRecord{},
})

// Application-level broadcast topic (no state, no merge)
whisper.RegisterTopic("app.announcements", whisper.TopicConfig{
    Mode:  whisper.BroadcastOnly,
    Proto: &pb.Announcement{},
})

// Application-level request/response topic
whisper.RegisterTopic("app.query", whisper.TopicConfig{
    Mode:    whisper.RequestResponse,
    Proto:   &pb.Query{},
    Handler: queryHandler,
})
```

---

## Depends On

- **[Aether](https://github.com/ORBTR/Aether)** — wire protocol, stream multiplexing, encryption (whisper runs on Aether streams in typical deployments)

## Depended On By

- **[Ledger](https://github.com/bbmumford/ledger)** — registers StatefulMerge topics for mesh directory records

---

## What Whisper Is NOT

- **Not a state store** — Whisper propagates; Ledger (or whatever `StateStore` backend you provide) stores and merges
- **Not a wire protocol** — Whisper uses any bidirectional byte stream; it doesn't define low-level frame formats
- **Not application-specific** — Whisper knows about topics and bytes, not domain objects

---

## API Docs

Generated API reference (pkgsite) for each tagged release is published to GitHub Pages:

- Latest: https://bbmumford.github.io/whisper/github.com/bbmumford/whisper/
- Published by `.github/workflows/docs.yml` on every `v*` tag.

---

## License

[MIT](LICENSE)

Copyright (c) 2026 HSTLES / ORBTR Pty Ltd
