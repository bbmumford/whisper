/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"strings"
	"time"
)

// ConnectionOffer represents a request from a peer to establish a direct connection.
// Included in gossip exchange metadata when a node wants to upgrade from indirect
// gossip to a direct transport connection.
type ConnectionOffer struct {
	// WantDirectConnect signals that the sender wants a direct connection.
	WantDirectConnect bool `json:"want_direct,omitempty"`

	// OfferedTransports lists transports the sender can accept (e.g., "noise-udp", "websocket").
	// Empty means "any available".
	OfferedTransports []string `json:"offered_transports,omitempty"`

	// SenderNodeID is the offering node's identity. Populated by the exchange layer,
	// not set by the caller.
	SenderNodeID string `json:"-"`

	// Timestamp when the offer was created. Used for deduplication.
	Timestamp time.Time `json:"offer_ts,omitempty"`
}

// ShouldInitiateDial implements the deterministic tiebreaker for simultaneous offers.
// When both sides include WantDirectConnect in simultaneous gossip exchanges, the
// node with the HIGHER NodeID initiates the dial. The lower NodeID defers and waits
// for an inbound connection. This ensures exactly one connection attempt per pair.
//
// Returns true if localNodeID should initiate the dial to peerNodeID.
func ShouldInitiateDial(localNodeID, peerNodeID string) bool {
	return strings.Compare(localNodeID, peerNodeID) > 0
}

// ExchangeMeta extends gossip exchange metadata with connection signaling fields
// and connection map data. This is the wire format addition to each gossip exchange.
type ExchangeMeta struct {
	// Offer signals desire for a direct connection.
	Offer *ConnectionOffer `json:"conn_offer,omitempty"`

	// ConnectionCounts is the gossip-propagated connection map.
	// Maps nodeID -> current inbound+outbound connection count.
	ConnectionCounts map[string]int `json:"conn_counts,omitempty"`

	// PEXEntries carries signed peer advertisements piggybacked on gossip.
	// Max PEXMaxEntriesPerExchange entries per exchange, rate-limited per peer.
	PEXEntries []PEXEntry `json:"pex_entries,omitempty"`

	// CacheFingerprint is an order-independent XOR hash of all cache keys.
	// When two peers exchange fingerprints and they differ, the next exchange
	// should be a full sync (not delta) to reconcile the divergence.
	CacheFingerprint uint64 `json:"cache_fp,omitempty"`

	// BackpressureSignal indicates the sender is overwhelmed and cannot process
	// gossip exchanges fast enough. When true, receiving peers should
	// apply exponential backoff to their gossip interval for this peer:
	//   - Initial backoff: 30s
	//   - Doubles each consecutive signal: 30s -> 60s -> 120s (capped)
	//   - Resets to 0 when the peer stops signaling backpressure
	// The signal is set by BackpressureMonitor when pendingExchanges exceeds
	// the threshold (default 5). The field is omitempty so peers that never
	// set it are treated as healthy (no backoff applied).
	BackpressureSignal bool `json:"backpressure,omitempty"`
}
