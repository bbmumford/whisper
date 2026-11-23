/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package whisper

// GossipMagic is the 2-byte prefix on all G1 gossip frames for corruption detection.
// Wire format: [2-byte magic 0x4731][4-byte big-endian length][payload]
const GossipMagic uint16 = 0x4731 // "G1"
