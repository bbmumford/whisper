/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"fmt"
	"testing"
)

func TestHypercubeBuild4Nodes(t *testing.T) {
	members := []string{"nodeA", "nodeB", "nodeC", "nodeD"}
	h := NewHypercube("nodeA")
	h.Rebuild(members)

	if h.Dimension() != 2 {
		t.Errorf("dimension: got %d, want 2", h.Dimension())
	}
	if h.MemberCount() != 4 {
		t.Errorf("members: got %d, want 4", h.MemberCount())
	}

	neighbors := h.Neighbors()
	if len(neighbors) != 2 {
		t.Errorf("neighbors: got %d, want 2", len(neighbors))
	}
}

func TestHypercubeBuild11Nodes(t *testing.T) {
	members := make([]string, 11)
	for i := range members {
		members[i] = fmt.Sprintf("node%02d", i)
	}
	h := NewHypercube("node05")
	h.Rebuild(members)

	if h.Dimension() != 4 { // ceil(log2(11)) = 4
		t.Errorf("dimension: got %d, want 4", h.Dimension())
	}

	neighbors := h.Neighbors()
	// Some dimensions may have no neighbor (non-power-of-2)
	if len(neighbors) < 1 || len(neighbors) > 4 {
		t.Errorf("neighbors: got %d, want 1-4", len(neighbors))
	}
}

func TestHypercubeBuild200Nodes(t *testing.T) {
	members := make([]string, 200)
	for i := range members {
		members[i] = fmt.Sprintf("node%03d", i)
	}
	h := NewHypercube("node100")
	h.Rebuild(members)

	if h.Dimension() != 8 { // ceil(log2(200)) = 8
		t.Errorf("dimension: got %d, want 8", h.Dimension())
	}

	neighbors := h.Neighbors()
	if len(neighbors) < 1 || len(neighbors) > 8 {
		t.Errorf("neighbors: got %d, want 1-8", len(neighbors))
	}
}

func TestDimensionOrderedRouting(t *testing.T) {
	members := []string{"A", "B", "C", "D"}
	// Sorted: A=0, B=1, C=2, D=3 → 2D hypercube

	h := NewHypercube("A") // position 0 (binary: 00)
	h.Rebuild(members)

	// Origin (fromDim=-1 → 255): forward on ALL dimensions
	dims := h.RouteRumor(-1)
	if len(dims) != 2 {
		t.Fatalf("origin route: got %d dims, want 2", len(dims))
	}
	// dim 0: flip bit 0 → pos 1 (B)
	// dim 1: flip bit 1 → pos 2 (C)
	if h.DimensionNeighbor(0) != "B" {
		t.Errorf("dim 0 neighbor: got %s, want B", h.DimensionNeighbor(0))
	}
	if h.DimensionNeighbor(1) != "C" {
		t.Errorf("dim 1 neighbor: got %s, want C", h.DimensionNeighbor(1))
	}

	// B (position 1, binary: 01) received on dim 0 → forward only on dims > 0
	hB := NewHypercube("B")
	hB.Rebuild(members)
	dimsB := hB.RouteRumor(0) // received on dim 0
	if len(dimsB) != 1 {
		t.Fatalf("B route from dim 0: got %d dims, want 1", len(dimsB))
	}
	if dimsB[0] != 1 {
		t.Errorf("B should forward on dim 1, got dim %d", dimsB[0])
	}
	// dim 1: flip bit 1 of pos 1 → pos 3 (D)
	if hB.DimensionNeighbor(1) != "D" {
		t.Errorf("B dim 1 neighbor: got %s, want D", hB.DimensionNeighbor(1))
	}

	// C (position 2, binary: 10) received on dim 1 → no dims > 1, stop
	hC := NewHypercube("C")
	hC.Rebuild(members)
	dimsC := hC.RouteRumor(1) // received on dim 1
	if len(dimsC) != 0 {
		t.Errorf("C route from dim 1: got %d dims, want 0 (stop)", len(dimsC))
	}
}

func TestOptimalMessageCount(t *testing.T) {
	// For N nodes, dimension-ordered routing produces exactly N-1 messages
	for _, n := range []int{4, 8, 16} {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			members := make([]string, n)
			for i := range members {
				members[i] = fmt.Sprintf("node%02d", i)
			}

			// Simulate: origin at position 0, count total forwards
			totalMessages := 0
			type pending struct {
				pos     int
				fromDim int
			}
			queue := []pending{{0, -1}} // origin

			for len(queue) > 0 {
				curr := queue[0]
				queue = queue[1:]

				h := NewHypercube(members[curr.pos])
				h.Rebuild(members)

				dims := h.RouteRumor(curr.fromDim)
				for _, d := range dims {
					neighbor := h.DimensionNeighbor(d)
					if neighbor == "" {
						continue
					}
					// Find neighbor position
					for i, m := range members {
						if m == neighbor {
							queue = append(queue, pending{i, d})
							totalMessages++
							break
						}
					}
				}
			}

			expected := n - 1
			if totalMessages != expected {
				t.Errorf("total messages: got %d, want %d (N-1)", totalMessages, expected)
			}
		})
	}
}

func TestHypercubeRebuildOnMemberChange(t *testing.T) {
	h := NewHypercube("B")

	// Initial: 4 nodes
	h.Rebuild([]string{"A", "B", "C", "D"})
	if h.Dimension() != 2 {
		t.Errorf("initial dimension: got %d, want 2", h.Dimension())
	}
	v1 := h.Version()

	// Add a node
	h.Rebuild([]string{"A", "B", "C", "D", "E"})
	if h.Dimension() != 3 { // ceil(log2(5)) = 3
		t.Errorf("after add dimension: got %d, want 3", h.Dimension())
	}
	v2 := h.Version()
	if v1 == v2 {
		t.Error("version should change after member list change")
	}

	// Remove a node
	h.Rebuild([]string{"A", "B", "C"})
	if h.Dimension() != 2 { // ceil(log2(3)) = 2
		t.Errorf("after remove dimension: got %d, want 2", h.Dimension())
	}
}

func TestHypercubeNodeNotInCube(t *testing.T) {
	h := NewHypercube("Z") // not in member list
	h.Rebuild([]string{"A", "B", "C"})

	if h.Position() != -1 {
		t.Errorf("position: got %d, want -1", h.Position())
	}
	if len(h.Neighbors()) != 0 {
		t.Errorf("neighbors: got %d, want 0", len(h.Neighbors()))
	}
	if len(h.RouteRumor(-1)) != 0 {
		t.Errorf("route: got dims, want nil")
	}
}
