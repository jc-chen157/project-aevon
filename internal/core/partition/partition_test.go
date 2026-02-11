package partition

import (
	"strconv"
	"testing"
)

func TestFor_Determinism(t *testing.T) {
	// Same input must always produce the same partition.
	id := For("tenant-abc")
	for i := 0; i < 100; i++ {
		if got := For("tenant-abc"); got != id {
			t.Fatalf("For(\"tenant-abc\") = %d on iteration %d, want %d", got, i, id)
		}
	}
}

func TestFor_Range(t *testing.T) {
	// All outputs must be in [0, Count).
	inputs := []string{"", "a", "tenant-1", "tenant-2", "very-long-tenant-id-that-should-still-hash-correctly"}
	for _, s := range inputs {
		p := For(s)
		if p < 0 || p >= Count {
			t.Errorf("For(%q) = %d, want [0, %d)", s, p, Count)
		}
	}
}

func TestFor_Distribution(t *testing.T) {
	// 1 000 tenants should hit at least 100 distinct partitions (sanity check
	// that FNV-32a spreads well). With 256 buckets and 1000 keys the expected
	// unique count is ~248 — 100 is a very conservative floor.
	seen := make(map[int]struct{})
	for i := 0; i < 1000; i++ {
		seen[For("tenant-"+strconv.Itoa(i))] = struct{}{}
	}
	if len(seen) < 100 {
		t.Errorf("only %d distinct partitions from 1000 inputs, want >= 100", len(seen))
	}
}
