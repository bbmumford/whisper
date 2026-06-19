// Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
// Queries: licensing@hstles.com

package whisper

import (
	"sync"
	"time"
)

// vivaldiEWMAAlpha weights a fresh predicted-RTT sample against the running
// per-peer average. Smoothing the prediction (not just the coordinate) dampens
// the flap a moving self-coordinate would otherwise impose on consumers that
// read PredictRTT between coordinate updates.
const vivaldiEWMAAlpha = 0.3

// peerCoordEntry is a peer's last-known gossiped coordinate plus an EWMA of the
// predicted RTT to it.
type peerCoordEntry struct {
	coord    Coord
	smoothed float64 // EWMA of predicted RTT (ms); 0 until the first observation
	hasEWMA  bool
}

// VivaldiCoords is the stateful wrapper around the pure coordinate math: it
// owns this node's self coordinate + local error estimate and a map of each
// peer's last-known coordinate. It is fed by the existing per-session RTT
// sampler (no new probing) and read at the dial-ordering / WDRR / far-routing
// decision points. Safe for concurrent use.
type VivaldiCoords struct {
	mu       sync.RWMutex
	self     Coord
	localErr float64
	peers    map[string]peerCoordEntry
}

// NewVivaldiCoords seeds a node with a randomised initial coordinate (so error
// drops from the first tick instead of stalling at the origin) and the default
// error estimate.
func NewVivaldiCoords() *VivaldiCoords {
	return &VivaldiCoords{
		self:     RandomCoord(),
		localErr: CeDefault,
		peers:    make(map[string]peerCoordEntry),
	}
}

// Observe folds one RTT sample for peerID into the self coordinate, records the
// peer's gossiped coordinate, and updates the EWMA-smoothed prediction for it.
// A zero/negative RTT leaves the self coordinate unchanged (Update guards it)
// but still records the peer's coordinate.
func (v *VivaldiCoords) Observe(peerID string, rtt time.Duration, peerCoord Coord, peerErr float64) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.self, v.localErr = Update(v.self, v.localErr, Sample{RTT: rtt, PeerCoord: peerCoord, PeerErr: peerErr})

	if peerID == "" {
		return
	}
	entry := v.peers[peerID]
	entry.coord = peerCoord
	predicted := Distance(v.self, peerCoord)
	if entry.hasEWMA {
		entry.smoothed = vivaldiEWMAAlpha*predicted + (1-vivaldiEWMAAlpha)*entry.smoothed
	} else {
		entry.smoothed = predicted
		entry.hasEWMA = true
	}
	v.peers[peerID] = entry
}

// Self returns the current self coordinate and local error estimate (the value
// a node gossips to peers).
func (v *VivaldiCoords) Self() (Coord, float64) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.self, v.localErr
}

// PredictRTTTo returns the predicted RTT (ms) from self to an arbitrary
// coordinate — the instantaneous prediction used when a fresh record arrives.
func (v *VivaldiCoords) PredictRTTTo(c Coord) float64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return Distance(v.self, c)
}

// PredictRTT returns the EWMA-smoothed predicted RTT (ms) to a known peer,
// ok=false when the peer has never been observed. Smoothed so a consumer
// reading it repeatedly between coordinate updates sees a stable value.
func (v *VivaldiCoords) PredictRTT(peerID string) (float64, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	entry, ok := v.peers[peerID]
	if !ok || !entry.hasEWMA {
		return 0, false
	}
	return entry.smoothed, true
}

// PeerCoord returns a peer's last-known gossiped coordinate.
func (v *VivaldiCoords) PeerCoord(peerID string) (Coord, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	entry, ok := v.peers[peerID]
	return entry.coord, ok
}

// Forget drops a peer's cached coordinate — call on peer departure / tombstone
// so a dead peer's stale coordinate stops influencing predictions.
func (v *VivaldiCoords) Forget(peerID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.peers, peerID)
}
