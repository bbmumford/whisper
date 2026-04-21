/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"crypto/ed25519"
	"fmt"
	"sync"
	"time"
)

// PEXEntry represents a single peer advertisement in a Peer Exchange response.
// Entries are signed by the advertising node's ed25519 key to prove ownership.
type PEXEntry struct {
	NodeID    string   `json:"node_id"`
	Addresses []string `json:"addresses"`
	Region    string   `json:"region"`
	Signature []byte   `json:"signature"` // ed25519 sig over NodeID+Addresses+SignedAt
	SignedAt  time.Time `json:"signed_at"`
}

// PEXRequest is sent by a node to request peer lists from a connected peer.
type PEXRequest struct {
	RequestingNodeID string `json:"requesting_node_id"`
	MaxEntries       int    `json:"max_entries,omitempty"` // 0 = use default (20)
}

// PEXResponse contains the peer list from the responding node.
type PEXResponse struct {
	Entries []PEXEntry `json:"entries"`
}

// PEXMaxAge is the maximum age of a PEX entry before it's treated as stale.
const PEXMaxAge = 1 * time.Hour

// PEXInterval is the minimum time between PEX sends to the same peer.
const PEXInterval = 5 * time.Minute

// PEXMaxEntriesPerExchange caps entries piggybacked on a single gossip exchange.
const PEXMaxEntriesPerExchange = 10

// PEXMaxKnownPeers caps the in-memory PEX peer table to prevent unbounded growth.
const PEXMaxKnownPeers = 500

// PEXMaxRateLimitEntries caps the rate limiter's per-peer tracking map.
const PEXMaxRateLimitEntries = 500

// pexSignaturePayload builds the canonical byte string signed by the entry's node.
// Format: "PEX:v1:<nodeID>:<addr1>,<addr2>,...:<signedAt RFC3339>"
func pexSignaturePayload(nodeID string, addresses []string, signedAt time.Time) []byte {
	msg := fmt.Sprintf("PEX:v1:%s:", nodeID)
	for i, addr := range addresses {
		if i > 0 {
			msg += ","
		}
		msg += addr
	}
	msg += ":" + signedAt.UTC().Format(time.RFC3339)
	return []byte(msg)
}

// SignPEXEntry creates a signed PEX entry for the local node.
func SignPEXEntry(nodeID string, addresses []string, region string, privateKey ed25519.PrivateKey) PEXEntry {
	now := time.Now().UTC()
	payload := pexSignaturePayload(nodeID, addresses, now)
	sig := ed25519.Sign(privateKey, payload)
	return PEXEntry{
		NodeID:    nodeID,
		Addresses: addresses,
		Region:    region,
		Signature: sig,
		SignedAt:  now,
	}
}

// VerifyPEXEntry checks the ed25519 signature on a PEX entry.
// The publicKey must correspond to the entry's NodeID (looked up from LAD member records).
func VerifyPEXEntry(entry PEXEntry, publicKey ed25519.PublicKey) bool {
	if len(entry.Signature) == 0 || len(publicKey) == 0 {
		return false
	}
	payload := pexSignaturePayload(entry.NodeID, entry.Addresses, entry.SignedAt)
	return ed25519.Verify(publicKey, payload, entry.Signature)
}

// IsStalePEXEntry returns true if the entry is older than PEXMaxAge.
func IsStalePEXEntry(entry PEXEntry) bool {
	return time.Since(entry.SignedAt) > PEXMaxAge
}

// PEXRateLimiter enforces rate limits on PEX sends per peer.
// Each peer is allowed one PEX send per PEXInterval.
type PEXRateLimiter struct {
	mu       sync.Mutex
	lastSend map[string]time.Time // peerNodeID -> last PEX send time
	interval time.Duration
}

// NewPEXRateLimiter creates a rate limiter with the default interval.
func NewPEXRateLimiter() *PEXRateLimiter {
	return &PEXRateLimiter{
		lastSend: make(map[string]time.Time),
		interval: PEXInterval,
	}
}

// Allow returns true if a PEX send to peerNodeID is allowed.
func (rl *PEXRateLimiter) Allow(peerNodeID string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if last, ok := rl.lastSend[peerNodeID]; ok {
		if time.Since(last) < rl.interval {
			return false
		}
	}
	rl.lastSend[peerNodeID] = time.Now()
	rl.pruneRateLimiter()
	return true
}

// Forget removes a peer's rate-limit state. Called from
// PEXManager.RemovePeer so a disconnected peer's lastSend entry doesn't
// linger (otherwise it only clears via age-based prune, which ignores
// stale peer identity). Safe no-op if the peer is unknown.
func (rl *PEXRateLimiter) Forget(peerNodeID string) {
	rl.mu.Lock()
	delete(rl.lastSend, peerNodeID)
	rl.mu.Unlock()
}

// pruneRateLimiter evicts entries older than 2x interval, and enforces a hard cap.
// Must be called with mu held.
func (rl *PEXRateLimiter) pruneRateLimiter() {
	// First pass: evict stale entries (>2x interval means they'll be allowed anyway)
	staleThreshold := 2 * rl.interval
	for id, last := range rl.lastSend {
		if time.Since(last) > staleThreshold {
			delete(rl.lastSend, id)
		}
	}
	// Hard cap: if still over limit, evict oldest entries
	if len(rl.lastSend) > PEXMaxRateLimitEntries {
		// Find oldest and delete until under cap
		for len(rl.lastSend) > PEXMaxRateLimitEntries {
			var oldestID string
			var oldestTime time.Time
			for id, last := range rl.lastSend {
				if oldestID == "" || last.Before(oldestTime) {
					oldestID = id
					oldestTime = last
				}
			}
			delete(rl.lastSend, oldestID)
		}
		dbgPeer.Printf("PEX rate limiter pruned to %d entries (cap=%d)", len(rl.lastSend), PEXMaxRateLimitEntries)
	}
}

// PEXManager coordinates PEX entry creation, verification, and sharing for a node.
type PEXManager struct {
	mu          sync.RWMutex
	localEntry  PEXEntry              // our own signed PEX entry, refreshed periodically
	knownPeers  map[string]PEXEntry   // nodeID -> latest verified PEX entry
	rateLimiter *PEXRateLimiter
	privateKey  ed25519.PrivateKey
	publicKeys  func(nodeID string) ed25519.PublicKey // lookup from LAD member records
}

// NewPEXManager creates a PEX manager for the local node.
func NewPEXManager(
	nodeID string,
	addresses []string,
	region string,
	privateKey ed25519.PrivateKey,
	publicKeyLookup func(nodeID string) ed25519.PublicKey,
) *PEXManager {
	mgr := &PEXManager{
		knownPeers:  make(map[string]PEXEntry),
		rateLimiter: NewPEXRateLimiter(),
		privateKey:  privateKey,
		publicKeys:  publicKeyLookup,
	}
	mgr.localEntry = SignPEXEntry(nodeID, addresses, region, privateKey)
	return mgr
}

// RefreshLocalEntry re-signs our PEX entry (call when addresses change).
func (m *PEXManager) RefreshLocalEntry(nodeID string, addresses []string, region string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.localEntry = SignPEXEntry(nodeID, addresses, region, m.privateKey)
}

// RemovePeer drops a disconnected peer's state — both the known-peers
// entry AND the rate-limiter entry. Previously only known-peers was
// touched (via pruneKnownPeers' age-based cleanup), leaving the
// rate-limiter's lastSend map with a stale entry that only timed out
// after 2× PEXInterval. Call this from the ConnectionManager teardown
// path so PEX state tracks peer lifecycle exactly.
func (m *PEXManager) RemovePeer(peerNodeID string) {
	m.mu.Lock()
	delete(m.knownPeers, peerNodeID)
	m.mu.Unlock()
	if m.rateLimiter != nil {
		m.rateLimiter.Forget(peerNodeID)
	}
}

// BuildPEXEntries returns up to PEXMaxEntriesPerExchange entries to piggyback
// on a gossip exchange to the given peer. Rate-limited per peer.
func (m *PEXManager) BuildPEXEntries(peerNodeID string) []PEXEntry {
	if !m.rateLimiter.Allow(peerNodeID) {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]PEXEntry, 0, PEXMaxEntriesPerExchange)

	// Always include our own entry first
	entries = append(entries, m.localEntry)

	// Add known peers, skip stale entries and the target peer itself
	for _, entry := range m.knownPeers {
		if len(entries) >= PEXMaxEntriesPerExchange {
			break
		}
		if entry.NodeID == peerNodeID {
			continue // don't tell a peer about itself
		}
		if IsStalePEXEntry(entry) {
			continue
		}
		entries = append(entries, entry)
	}

	return entries
}

// ProcessPEXEntries validates and stores inbound PEX entries from a peer.
// Returns the list of newly discovered node IDs.
func (m *PEXManager) ProcessPEXEntries(entries []PEXEntry) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var newNodeIDs []string
	for _, entry := range entries {
		// Skip stale entries (>1h old)
		if IsStalePEXEntry(entry) {
			dbgPeer.Printf("PEX skipping stale entry for %s (signed %v ago)",
				truncPEXID(entry.NodeID), time.Since(entry.SignedAt).Round(time.Second))
			continue
		}

		// Skip entries with no addresses
		if len(entry.Addresses) == 0 {
			continue
		}

		// Verify signature against known public key
		pubKey := m.publicKeys(entry.NodeID)
		if pubKey == nil {
			dbgPeer.Printf("PEX no public key for %s, skipping", truncPEXID(entry.NodeID))
			continue
		}
		if !VerifyPEXEntry(entry, pubKey) {
			dbgPeer.Printf("PEX invalid signature for %s, discarding", truncPEXID(entry.NodeID))
			continue
		}

		// Store or update if newer
		existing, exists := m.knownPeers[entry.NodeID]
		if !exists || entry.SignedAt.After(existing.SignedAt) {
			m.knownPeers[entry.NodeID] = entry
			if !exists {
				newNodeIDs = append(newNodeIDs, entry.NodeID)
			}
		}
	}

	// Enforce size cap: evict stale first, then oldest by SignedAt
	m.pruneKnownPeers()

	return newNodeIDs
}

// pruneKnownPeers evicts stale entries and enforces the PEXMaxKnownPeers cap.
// Must be called with mu held (write lock).
func (m *PEXManager) pruneKnownPeers() {
	// First pass: remove stale entries
	for id, entry := range m.knownPeers {
		if IsStalePEXEntry(entry) {
			delete(m.knownPeers, id)
		}
	}

	// Hard cap: evict oldest entries by SignedAt
	if len(m.knownPeers) > PEXMaxKnownPeers {
		evictCount := len(m.knownPeers) - PEXMaxKnownPeers
		for i := 0; i < evictCount; i++ {
			var oldestID string
			var oldestTime time.Time
			for id, entry := range m.knownPeers {
				if oldestID == "" || entry.SignedAt.Before(oldestTime) {
					oldestID = id
					oldestTime = entry.SignedAt
				}
			}
			delete(m.knownPeers, oldestID)
		}
		dbgPeer.Printf("PEX pruned knownPeers to %d entries (cap=%d, evicted=%d)",
			len(m.knownPeers), PEXMaxKnownPeers, evictCount)
	}
}

// KnownPeers returns all verified, non-stale PEX entries.
func (m *PEXManager) KnownPeers() []PEXEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []PEXEntry
	for _, entry := range m.knownPeers {
		if !IsStalePEXEntry(entry) {
			result = append(result, entry)
		}
	}
	return result
}

// truncPEXID returns a short prefix of a node ID for logging.
func truncPEXID(id string) string {
	if len(id) > 12 {
		return id[:12] + "..."
	}
	return id
}
