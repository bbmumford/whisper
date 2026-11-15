/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

// maxGossipPayload caps the size of a single gossip message (1 MB).
// With 11 nodes, typical exchanges are 15-50KB. 1MB provides generous headroom
// while catching corrupted length headers.
const maxGossipPayload = 1 << 20

// GossipMagic is the 2-byte prefix on all G1 gossip frames for corruption detection.
// Wire format: [2-byte magic 0x4731][4-byte big-endian length][payload]
const GossipMagic uint16 = 0x4731 // "G1"
