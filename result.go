/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import "time"

// GossipResult captures the outcome of a single gossip exchange round.
type GossipResult struct {
	RTT            time.Duration // Total exchange time (serialize+write+read+deserialize)
	NetworkRTT     time.Duration // Network round-trip from header exchange timing
	RecordsSent    int           // Number of records we sent
	RecordsApplied int           // Number of records applied from peer
	SeenNodeIDs    []string      // All unique NodeIDs received (for liveness tracking)
	PeerMeta       *ExchangeMeta // metadata from the peer's exchange
	DigestSkipped  bool          // true when G2 digest matched — no data exchanged
}
