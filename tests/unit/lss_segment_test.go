package tests

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/storage/lss"
)

func TestEntrySerializationRoundtrip(t *testing.T) {
	entry := &lss.Entry{
		Key:   "test-key",
		Value: []byte("test-value"),
		VectorClock: map[string]uint64{
			"node1": 5,
			"node2": 3,
		},
		Timestamp:   time.Now(),
		IsTombstone: false,
	}

	// Serialize
	data, err := lss.SerializeEntry(entry)
	if err != nil {
		t.Fatalf("SerializeEntry failed: %v", err)
	}

	// Deserialize
	restored, err := lss.DeserializeEntry(data)
	if err != nil {
		t.Fatalf("DeserializeEntry failed: %v", err)
	}

	// Verify
	if restored.Key != entry.Key {
		t.Errorf("Key mismatch: got %s, want %s", restored.Key, entry.Key)
	}
	if string(restored.Value) != string(entry.Value) {
		t.Errorf("Value mismatch: got %s, want %s", restored.Value, entry.Value)
	}
	if restored.IsTombstone != entry.IsTombstone {
		t.Errorf("IsTombstone mismatch: got %v, want %v", restored.IsTombstone, entry.IsTombstone)
	}
	for nodeID, counter := range entry.VectorClock {
		if restored.VectorClock[nodeID] != counter {
			t.Errorf("VectorClock[%s] mismatch: got %d, want %d", nodeID, restored.VectorClock[nodeID], counter)
		}
	}
}

func TestTombstoneEntrySerialization(t *testing.T) {
	entry := &lss.Entry{
		Key:         "deleted-key",
		Value:       nil,
		VectorClock: map[string]uint64{"node1": 10},
		Timestamp:   time.Now(),
		IsTombstone: true,
	}

	data, err := lss.SerializeEntry(entry)
	if err != nil {
		t.Fatalf("SerializeEntry failed: %v", err)
	}

	restored, err := lss.DeserializeEntry(data)
	if err != nil {
		t.Fatalf("DeserializeEntry failed: %v", err)
	}

	if !restored.IsTombstone {
		t.Error("Expected tombstone flag to be true")
	}
	if len(restored.Value) != 0 {
		t.Error("Expected empty value for tombstone")
	}
}

func TestCorruptedEntryDetection(t *testing.T) {
	entry := &lss.Entry{
		Key:         "test-key",
		Value:       []byte("test-value"),
		VectorClock: map[string]uint64{"node1": 1},
		Timestamp:   time.Now(),
	}

	data, err := lss.SerializeEntry(entry)
	if err != nil {
		t.Fatalf("SerializeEntry failed: %v", err)
	}

	// Corrupt the data
	data[len(data)-1] ^= 0xFF

	_, err = lss.DeserializeEntry(data)
	if err != lss.ErrCorruptedEntry {
		t.Errorf("Expected ErrCorruptedEntry, got: %v", err)
	}
}

func TestSegmentCreateAndOpen(t *testing.T) {
	dir := t.TempDir()

	// Create segment
	seg, err := lss.CreateSegment(dir, 1, 10*1024*1024)
	if err != nil {
		t.Fatalf("CreateSegment failed: %v", err)
	}

	if seg.ID() != 1 {
		t.Errorf("Segment ID mismatch: got %d, want 1", seg.ID())
	}

	// Write an entry
	entry := &lss.Entry{
		Key:         "key1",
		Value:       []byte("value1"),
		VectorClock: map[string]uint64{"node1": 1},
		Timestamp:   time.Now(),
	}

	offset, err := seg.Append(entry)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if offset != 32 { // Should be right after header
		t.Errorf("Expected offset 32, got %d", offset)
	}

	// Close segment
	if err := seg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen segment
	path := filepath.Join(dir, "seg-0000000000000001.lss")
	seg2, err := lss.OpenSegment(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("OpenSegment failed: %v", err)
	}
	defer seg2.Close()

	if seg2.ID() != 1 {
		t.Errorf("Reopened segment ID mismatch: got %d, want 1", seg2.ID())
	}

	if seg2.EntryCount() != 1 {
		t.Errorf("Entry count mismatch: got %d, want 1", seg2.EntryCount())
	}

	// Read entry back
	readEntry, err := seg2.ReadAt(offset)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	if readEntry.Key != entry.Key {
		t.Errorf("Key mismatch: got %s, want %s", readEntry.Key, entry.Key)
	}
}

func TestSegmentIterate(t *testing.T) {
	dir := t.TempDir()

	seg, err := lss.CreateSegment(dir, 1, 10*1024*1024)
	if err != nil {
		t.Fatalf("CreateSegment failed: %v", err)
	}
	defer seg.Close()

	// Write multiple entries
	keys := []string{"key1", "key2", "key3"}
	for _, key := range keys {
		entry := &lss.Entry{
			Key:         key,
			Value:       []byte("value-" + key),
			VectorClock: map[string]uint64{"node1": 1},
			Timestamp:   time.Now(),
		}
		if _, err := seg.Append(entry); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Iterate and collect keys
	var readKeys []string
	err = seg.Iterate(func(offset int64, entry *lss.Entry) error {
		readKeys = append(readKeys, entry.Key)
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(readKeys) != len(keys) {
		t.Errorf("Entry count mismatch: got %d, want %d", len(readKeys), len(keys))
	}

	for i, key := range keys {
		if readKeys[i] != key {
			t.Errorf("Key %d mismatch: got %s, want %s", i, readKeys[i], key)
		}
	}
}

func TestSegmentFreezeAndReadOnly(t *testing.T) {
	dir := t.TempDir()

	seg, err := lss.CreateSegment(dir, 1, 10*1024*1024)
	if err != nil {
		t.Fatalf("CreateSegment failed: %v", err)
	}
	defer seg.Close()

	// Write entry
	entry := &lss.Entry{
		Key:         "key1",
		Value:       []byte("value1"),
		VectorClock: map[string]uint64{"node1": 1},
		Timestamp:   time.Now(),
	}
	if _, err := seg.Append(entry); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Freeze segment
	if err := seg.Freeze(); err != nil {
		t.Fatalf("Freeze failed: %v", err)
	}

	if !seg.IsFrozen() {
		t.Error("Expected segment to be frozen")
	}

	// Try to write to frozen segment
	_, err = seg.Append(entry)
	if err != lss.ErrSegmentFrozen {
		t.Errorf("Expected ErrSegmentFrozen, got: %v", err)
	}
}

func TestSegmentFull(t *testing.T) {
	dir := t.TempDir()

	// Create a small segment (1KB max)
	seg, err := lss.CreateSegment(dir, 1, 1024)
	if err != nil {
		t.Fatalf("CreateSegment failed: %v", err)
	}
	defer seg.Close()

	// Write entries until full
	entry := &lss.Entry{
		Key:         "key",
		Value:       make([]byte, 200), // ~200+ bytes per entry
		VectorClock: map[string]uint64{"node1": 1},
		Timestamp:   time.Now(),
	}

	entriesWritten := 0
	gotFullError := false
	for {
		_, err := seg.Append(entry)
		if err == lss.ErrSegmentFull {
			gotFullError = true
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		entriesWritten++
		if entriesWritten > 100 {
			t.Fatal("Too many entries written, segment should be full")
		}
	}

	if !gotFullError {
		t.Error("Expected to get ErrSegmentFull")
	}

	// Verify some entries were written before getting full
	if entriesWritten == 0 {
		t.Error("Expected at least one entry to be written")
	}
}

func TestSegmentDelete(t *testing.T) {
	dir := t.TempDir()

	seg, err := lss.CreateSegment(dir, 1, 10*1024*1024)
	if err != nil {
		t.Fatalf("CreateSegment failed: %v", err)
	}

	path := seg.Path()

	// Verify file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("Segment file should exist")
	}

	// Delete segment
	if err := seg.Delete(); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify file is removed
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("Segment file should be deleted")
	}
}
