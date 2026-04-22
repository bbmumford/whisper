/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package whisper is a generic, topic-based gossip engine for peer-to-peer
// state propagation. It provides the pub/sub, delta sync, and topology
// routing layers that sit between a wire protocol (Aether) and application
// state (Ledger, Minerva, Mercury).
//
// Whisper is transport-agnostic — it operates on Aether streams but knows
// nothing about what it carries.
//
// Three gossip modes:
//   - BroadcastOnly: firehose pub/sub, no state, no merge
//   - StatefulMerge: delta sync with merge-on-apply via StateStore
//   - RequestResponse: solicit/reply pattern
//
// Three wire frame types:
//   - G1 (magic 0x4731): full bidirectional exchange with ExchangeMeta
//   - G2 (magic 0x4732): 12-byte digest probe for fingerprint comparison
//   - G3 (magic 0x4733): immediate rumor push via hypercube routing
package whisper
