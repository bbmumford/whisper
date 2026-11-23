# whisper

whisper is a topic-based gossip engine. It moves records between peers over
any bidirectional byte stream (an aether stream, TCP, WebSocket, a gRPC bidi
stream) and keeps stateful topics in step with delta sync, but it never looks
inside what it carries: the application's store decides how records merge.

An application registers topics on an `Engine`, runs one exchange loop per
peer connection, and publishes or subscribes. whisper supplies the framing, the
delta sync, rumor propagation over a hypercube overlay, peer exchange, pacing,
backpressure and round-trip measurement.

## Gossip exchange

Each topic is registered with a `TopicConfig` (`Engine.RegisterTopic`):

| Field | Meaning |
|-------|---------|
| `Mode` | `BroadcastOnly`: pub/sub with nothing stored or merged. `StatefulMerge`: records kept in a `StateStore` and kept in step by delta sync. `RequestResponse`: a query answered by a handler; nothing stored. |
| `Proto` | the topic's payload message type |
| `Store` | the topic's `StateStore`; required for `StatefulMerge` |
| `Handler` | the topic's `TopicHandler`; required for `RequestResponse` |
| `TTL` | how long a `BroadcastOnly` message lives (0: no expiry) |
| `Rumor` | whether a `StatefulMerge` mutation is also pushed at once as a rumor |
| `Priority` | dispatch priority under egress pressure (higher is preferred) |

A `StateStore` is the consumer's side of a stateful topic: `Fingerprint()` (a
64-bit summary that changes on every mutation), `Snapshot()`, `Delta(since)`
and `Apply(record)`. whisper moves opaque record bytes; merge, tombstone and
last-writer-wins rules belong to the store. `TopicHandler.OnMessage` receives
a topic's messages and, for `RequestResponse`, returns the reply.

`Engine.Run` drives one peer connection as the initiator and
`Engine.RunResponder` as the responder. `Publish` sends on a topic and
`Subscribe` adds a `Subscriber`. `Query` runs a `RequestResponse` topic's
handler and returns its reply; it answers from the local handler only and does
not yet solicit other peers.

Every frame starts with a 2-byte magic:

- **G2 digest probe** (`0x4732`): before a full exchange the initiator sends a
  12-byte frame holding its store fingerprint, and the responder answers with
  its own. Equal fingerprints end the round with no records sent. When they
  differ, the initiator can escalate to a bucketed digest (G2B, `0x4742`) that
  narrows the divergence to a few key-prefix buckets.
- **G1 full exchange** (`0x4731`, then a 4-byte length): each side sends an
  `ExchangeMeta` and the records changed since its per-peer watermark (the
  whole snapshot when it has none), and each applies what it receives.
- **G3 rumor push** (`0x4733`): one record pushed to neighbours at once and
  forwarded dimension by dimension over the hypercube overlay. `BroadcastOnly`
  topics travel only this way.

Beside the exchange, `NewEngine` takes options for the rest of the machinery:
`WithReconcile` (set reconciliation through the invertible Bloom lookup table
in `iblt`), `WithSnapshot` (snapshot transfer, erasure-coded with Reed-Solomon),
`WithPEX` (signed peer-address advertisements, `SignPEXEntry`,
`VerifyPEXEntry`), `WithHypercube`, `WithAdaptive` and `WithNetworkPolicy`
(gossip intervals that back off when idle and speed up while converging, tuned
by network profile), `WithBackpressure`, `WithRateLimiter` and
`WithPeerRateLimit`, `WithRTT`, and the CPU gate (`NewCPUGate`). `vivaldi.go`
and `coords.go` keep Vivaldi network coordinates, so the distance between two
nodes' coordinates predicts their round-trip time.

## Use

```sh
go get github.com/bbmumford/whisper
```

```go
eng := whisper.NewEngine(
	whisper.WithDelta(whisper.NewDeltaTracker()),
	whisper.WithRTT(whisper.NewNetworkRTTMeasurer(32)),
)

// A stateful topic: the store merges, whisper keeps peers in step.
_ = eng.RegisterTopic("member", whisper.TopicConfig{
	Mode:  whisper.StatefulMerge,
	Store: cache, // implements whisper.StateStore
})

// A broadcast topic: nothing stored.
_ = eng.RegisterTopic("app.announcements", whisper.TopicConfig{
	Mode: whisper.BroadcastOnly,
})

// One loop per peer connection.
go eng.Run(ctx, conn, peerNodeID, func(r whisper.GossipResult) {})
```

whisper is not a state store (the `StateStore` the caller provides stores and
merges), not a wire protocol (it runs over any byte stream) and not
application-specific (it knows topics and bytes, not domain objects).

| package | what it holds |
|---|---|
| `whisper` | `Engine`, `TopicConfig`, `StateStore`, `TopicHandler`, the frame readers and writers, PEX, rumor, snapshot, reconciliation, pacing and coordinates |
| `whisper/iblt` | an invertible Bloom lookup table, which transfers the symmetric difference of two peers' record sets in bytes proportional to the difference |
| `whisper/pb` | the protobuf gossip envelope |

## Dependencies

- No other package of this estate in its production code; the tests run
  exchanges over `github.com/ORBTR/aether` streams.
- `github.com/klauspost/reedsolomon` (snapshot erasure coding),
  `golang.org/x/time/rate` (rate limits) and `google.golang.org/protobuf`.

## Used by

Importers in the ORBTR estate:

- `github.com/bbmumford/ledger`: registers its record topics on whisper.
- `github.com/bbmumford/loom/core/directory/gossip` (with `iblt`) and
  `github.com/bbmumford/loom/node`: the platform mesh's directory gossip.
- `github.com/bbmumford/mercury` (the root package, `arbitration`,
  `discovery`, `moderation`, `profile`, `reputation`, `runtime/mesh`, `store`
  and more), `github.com/bbmumford/minerva` (`discovery`, `gossip`,
  `governance`, `messaging`, `registry`, `sdk`) and `github.com/bbmumford/plutus`
  (the root package, `arbitrator`, `reputation`, `wasm`, `witness`).
- `github.com/ORBTR/agent/internal/core/wire` (with `iblt`),
  `internal/core/wire/peers`, `internal/store` and `systempart`: the device
  agent.

## Platforms and build

Pure Go, no cgo and no build tags. The module's `go` directive is 1.25.0.
Releases are cut by release-please from conventional commits, and each tag
publishes the API reference to the repository's GitHub Pages site
(`.github/workflows/docs.yml`).

## Licence

MIT. Copyright (c) 2026 Brandon Miller-Mumford. The full text is in
[LICENSE](LICENSE). `coords.go` is a port of pollen's `pkg/coords/coords.go`
(github.com/sambigeara/pollen) and keeps its own notice: copyright 2026 Sam
Lock, Apache-2.0.

## Contributing

[AGENTS.md](AGENTS.md) holds the repository's two rules, for people and coding
agents alike: no temporary documents in the repository, and conventional commit
messages that stand on their own. [CLAUDE.md](CLAUDE.md) imports it for Claude
Code. Changes are listed in [CHANGELOG.md](CHANGELOG.md).
