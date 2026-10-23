/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package iblt

import (
	"testing"
	"time"
)

// TestDecodeDoesNotPeelAPoisonedCell: a cell whose count says one element but
// whose key-hash sum is not the hash of its key sum is a cell the peel never
// reads as pure, so a table of such cells decodes no key and reports the
// decode incomplete; a cell whose hash does match its fabricated key yields
// that key and nothing else, so the only thing a hostile table can put in a
// decode is a key, which the store decides on.
func TestDecodeDoesNotPeelAPoisonedCell(t *testing.T) {
	var k Key
	k[0], k[15] = 0xAB, 0xCD

	poisoned := New(8, 3, 1)
	for i := range poisoned.cells {
		poisoned.cells[i] = Cell{KeySum: k, KeyHashSum: ^hashKey(k), Count: 1}
	}
	res := poisoned.Decode()
	if len(res.Positive) != 0 || len(res.Negative) != 0 || res.Complete {
		t.Fatalf("poisoned cells decoded %d+%d keys, complete=%v", len(res.Positive), len(res.Negative), res.Complete)
	}
	for i, c := range poisoned.cells {
		if c.Count != 1 || c.KeySum != k {
			t.Fatalf("cell %d changed under a decode that peeled nothing: %+v", i, c)
		}
	}

	// A count of ±1 with a matching hash is the shape of a real element. One
	// such cell written alone, with the key absent from the other cells it
	// hashes to, is removed as positive, which drives those cells to -1 with
	// the key's sums, which are then peeled as negative and re-inserted, which
	// re-makes the first cell pure: the peel would run without end. The decode
	// stops once it has peeled more elements than the table has cells, reports
	// the table poisoned and incomplete, and returns.
	fabricated := New(64, 3, 1)
	fabricated.cells[fabricated.position(k, 0)] = Cell{KeySum: k, KeyHashSum: hashKey(k), Count: 1}
	done := make(chan DecodeResult, 1)
	go func() { done <- fabricated.Decode() }()
	select {
	case res = <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("a table with one fabricated cell held the decoder for 10s")
	}
	if res.Complete || !res.Poisoned {
		t.Fatalf("a table with one fabricated cell: complete=%v poisoned=%v", res.Complete, res.Poisoned)
	}
	if len(res.Positive)+len(res.Negative) > len(fabricated.cells)+1 {
		t.Fatalf("the peel went %d elements past a table of %d cells", len(res.Positive)+len(res.Negative), len(fabricated.cells))
	}

	// An honest table decodes whole and is never marked poisoned.
	honest := New(4, 2, 7)
	var keys []Key
	for i := 0; i < 2; i++ {
		var kk Key
		kk[0] = byte(0x10 + i)
		keys = append(keys, kk)
		honest.Insert(kk)
	}
	res = honest.Decode()
	if res.Poisoned {
		t.Fatalf("an honest table of %d elements in %d cells was marked poisoned", len(keys), 4)
	}

	// Subtracting a table of another shape is a no-op: the cells stay as
	// they were, so shapes that differ never combine into a false difference.
	a := New(8, 3, 1)
	a.Insert(k)
	before := append([]Cell(nil), a.cells...)
	a.Subtract(New(16, 3, 1))
	for i := range a.cells {
		if a.cells[i] != before[i] {
			t.Fatalf("cell %d changed under a subtract of another shape", i)
		}
	}
}
