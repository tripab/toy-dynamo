package tests

import (
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/storage/lss"
)

func TestIndexAddAndGet(t *testing.T) {
	idx := lss.NewIndex()

	entry := lss.IndexEntry{
		SegmentID: 1,
		Offset:    100,
		Timestamp: time.Now(),
	}

	idx.Add("key1", entry)

	entries := idx.Get("key1")
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].SegmentID != 1 || entries[0].Offset != 100 {
		t.Error("Entry data mismatch")
	}
}

func TestIndexGetLatest(t *testing.T) {
	idx := lss.NewIndex()

	// Add multiple versions with different timestamps
	t1 := time.Now().Add(-2 * time.Hour)
	t2 := time.Now().Add(-1 * time.Hour)
	t3 := time.Now()

	idx.Add("key1", lss.IndexEntry{SegmentID: 1, Offset: 100, Timestamp: t1})
	idx.Add("key1", lss.IndexEntry{SegmentID: 2, Offset: 200, Timestamp: t2})
	idx.Add("key1", lss.IndexEntry{SegmentID: 3, Offset: 300, Timestamp: t3})

	latest := idx.GetLatest("key1")
	if latest == nil {
		t.Fatal("Expected latest entry, got nil")
	}

	if latest.SegmentID != 3 || latest.Offset != 300 {
		t.Errorf("Expected latest entry (seg 3, offset 300), got (seg %d, offset %d)",
			latest.SegmentID, latest.Offset)
	}
}

func TestIndexGetNonExistent(t *testing.T) {
	idx := lss.NewIndex()

	entries := idx.Get("nonexistent")
	if entries != nil {
		t.Errorf("Expected nil for nonexistent key, got %v", entries)
	}

	latest := idx.GetLatest("nonexistent")
	if latest != nil {
		t.Errorf("Expected nil for nonexistent key, got %v", latest)
	}
}

func TestIndexSet(t *testing.T) {
	idx := lss.NewIndex()

	// Add multiple entries
	idx.Add("key1", lss.IndexEntry{SegmentID: 1, Offset: 100, Timestamp: time.Now()})
	idx.Add("key1", lss.IndexEntry{SegmentID: 2, Offset: 200, Timestamp: time.Now()})

	if len(idx.Get("key1")) != 2 {
		t.Fatal("Expected 2 entries before Set")
	}

	// Set replaces all entries with a single one
	idx.Set("key1", lss.IndexEntry{SegmentID: 5, Offset: 500, Timestamp: time.Now()})

	entries := idx.Get("key1")
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry after Set, got %d", len(entries))
	}

	if entries[0].SegmentID != 5 || entries[0].Offset != 500 {
		t.Error("Set did not properly replace entries")
	}
}

func TestIndexDelete(t *testing.T) {
	idx := lss.NewIndex()

	idx.Add("key1", lss.IndexEntry{SegmentID: 1, Offset: 100, Timestamp: time.Now()})
	idx.Add("key2", lss.IndexEntry{SegmentID: 1, Offset: 200, Timestamp: time.Now()})

	idx.Delete("key1")

	if idx.Get("key1") != nil {
		t.Error("key1 should be deleted")
	}

	if idx.Get("key2") == nil {
		t.Error("key2 should still exist")
	}
}

func TestIndexLen(t *testing.T) {
	idx := lss.NewIndex()

	if idx.Len() != 0 {
		t.Error("Empty index should have length 0")
	}

	idx.Add("key1", lss.IndexEntry{SegmentID: 1, Offset: 100, Timestamp: time.Now()})
	idx.Add("key2", lss.IndexEntry{SegmentID: 1, Offset: 200, Timestamp: time.Now()})

	if idx.Len() != 2 {
		t.Errorf("Expected length 2, got %d", idx.Len())
	}

	// Adding another entry for existing key shouldn't change len
	idx.Add("key1", lss.IndexEntry{SegmentID: 2, Offset: 300, Timestamp: time.Now()})

	if idx.Len() != 2 {
		t.Errorf("Expected length 2 after adding to existing key, got %d", idx.Len())
	}
}

func TestIndexClear(t *testing.T) {
	idx := lss.NewIndex()

	idx.Add("key1", lss.IndexEntry{SegmentID: 1, Offset: 100, Timestamp: time.Now()})
	idx.Add("key2", lss.IndexEntry{SegmentID: 1, Offset: 200, Timestamp: time.Now()})

	idx.Clear()

	if idx.Len() != 0 {
		t.Errorf("Expected length 0 after clear, got %d", idx.Len())
	}
}

func TestIndexGetAllKeys(t *testing.T) {
	idx := lss.NewIndex()

	idx.Add("key1", lss.IndexEntry{SegmentID: 1, Offset: 100, Timestamp: time.Now()})
	idx.Add("key2", lss.IndexEntry{SegmentID: 1, Offset: 200, Timestamp: time.Now()})
	idx.Add("key3", lss.IndexEntry{SegmentID: 1, Offset: 300, Timestamp: time.Now()})

	keys := idx.GetAllKeys()
	if len(keys) != 3 {
		t.Fatalf("Expected 3 keys, got %d", len(keys))
	}

	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}

	for _, expected := range []string{"key1", "key2", "key3"} {
		if !keySet[expected] {
			t.Errorf("Expected key %s not found", expected)
		}
	}
}

func TestIndexGetKeysInRange(t *testing.T) {
	idx := lss.NewIndex()

	idx.Add("a", lss.IndexEntry{SegmentID: 1, Offset: 100, Timestamp: time.Now()})
	idx.Add("b", lss.IndexEntry{SegmentID: 1, Offset: 200, Timestamp: time.Now()})
	idx.Add("c", lss.IndexEntry{SegmentID: 1, Offset: 300, Timestamp: time.Now()})
	idx.Add("d", lss.IndexEntry{SegmentID: 1, Offset: 400, Timestamp: time.Now()})

	// Range [b, d) should include b and c
	keys := idx.GetKeysInRange("b", "d")
	if len(keys) != 2 {
		t.Fatalf("Expected 2 keys in range [b,d), got %d: %v", len(keys), keys)
	}

	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}

	if !keySet["b"] || !keySet["c"] {
		t.Error("Expected keys b and c in range")
	}
	if keySet["a"] || keySet["d"] {
		t.Error("Keys a and d should not be in range")
	}
}

func TestIndexRemoveSegment(t *testing.T) {
	idx := lss.NewIndex()

	// Add entries from different segments
	idx.Add("key1", lss.IndexEntry{SegmentID: 1, Offset: 100, Timestamp: time.Now()})
	idx.Add("key1", lss.IndexEntry{SegmentID: 2, Offset: 200, Timestamp: time.Now()})
	idx.Add("key2", lss.IndexEntry{SegmentID: 1, Offset: 300, Timestamp: time.Now()})
	idx.Add("key3", lss.IndexEntry{SegmentID: 2, Offset: 400, Timestamp: time.Now()})

	// Remove segment 1
	idx.RemoveSegment(1)

	// key1 should only have entry from segment 2
	key1Entries := idx.Get("key1")
	if len(key1Entries) != 1 {
		t.Fatalf("Expected 1 entry for key1, got %d", len(key1Entries))
	}
	if key1Entries[0].SegmentID != 2 {
		t.Error("key1 should only have entry from segment 2")
	}

	// key2 should be completely removed (only had segment 1 entries)
	if idx.Get("key2") != nil {
		t.Error("key2 should be removed completely")
	}

	// key3 should still exist (only segment 2)
	if idx.Get("key3") == nil {
		t.Error("key3 should still exist")
	}
}

func TestIndexConcurrencySafety(t *testing.T) {
	idx := lss.NewIndex()

	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			idx.Add("key", lss.IndexEntry{
				SegmentID: uint64(i),
				Offset:    int64(i * 100),
				Timestamp: time.Now(),
			})
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			idx.Get("key")
			idx.GetLatest("key")
			idx.Len()
		}
		done <- true
	}()

	<-done
	<-done
	// Test passes if no race condition panic occurs
}
