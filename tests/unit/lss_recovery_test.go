package tests

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/storage/lss"
)

func TestRecoveryEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	config := lss.DefaultConfig(dir)

	recovery := lss.NewRecovery(dir, config)
	index, segments, nextID, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if index == nil {
		t.Error("Index should not be nil")
	}

	if len(segments) != 0 {
		t.Errorf("Expected 0 segments, got %d", len(segments))
	}

	if nextID != 1 {
		t.Errorf("Expected next ID 1, got %d", nextID)
	}
}

func TestRecoverySingleSegment(t *testing.T) {
	dir := t.TempDir()
	config := lss.DefaultConfig(dir)

	// Create a segment with some entries
	seg, err := lss.CreateSegment(dir, 1, 10*1024*1024)
	if err != nil {
		t.Fatalf("CreateSegment failed: %v", err)
	}

	entries := []*lss.Entry{
		{Key: "key1", Value: []byte("value1"), VectorClock: map[string]uint64{"n1": 1}, Timestamp: time.Now()},
		{Key: "key2", Value: []byte("value2"), VectorClock: map[string]uint64{"n1": 2}, Timestamp: time.Now()},
		{Key: "key3", Value: []byte("value3"), VectorClock: map[string]uint64{"n1": 3}, Timestamp: time.Now()},
	}

	for _, e := range entries {
		if _, err := seg.Append(e); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Recover
	recovery := lss.NewRecovery(dir, config)
	index, segments, nextID, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if len(segments) != 1 {
		t.Fatalf("Expected 1 segment, got %d", len(segments))
	}

	if segments[0].ID() != 1 {
		t.Errorf("Expected segment ID 1, got %d", segments[0].ID())
	}

	if nextID != 2 {
		t.Errorf("Expected next ID 2, got %d", nextID)
	}

	// Check index was rebuilt
	if index.Len() != 3 {
		t.Errorf("Expected 3 keys in index, got %d", index.Len())
	}

	for _, e := range entries {
		indexEntry := index.GetLatest(e.Key)
		if indexEntry == nil {
			t.Errorf("Key %s not found in index", e.Key)
		}
	}

	// Cleanup
	for _, seg := range segments {
		seg.Close()
	}
}

func TestRecoveryMultipleSegments(t *testing.T) {
	dir := t.TempDir()
	config := lss.DefaultConfig(dir)

	// Create multiple segments
	for segID := uint64(1); segID <= 3; segID++ {
		seg, err := lss.CreateSegment(dir, segID, 10*1024*1024)
		if err != nil {
			t.Fatalf("CreateSegment failed: %v", err)
		}

		for i := 0; i < 3; i++ {
			entry := &lss.Entry{
				Key:         "key" + string(rune('a'+int(segID-1)*3+i)),
				Value:       []byte("value"),
				VectorClock: map[string]uint64{"n1": segID},
				Timestamp:   time.Now(),
			}
			if _, err := seg.Append(entry); err != nil {
				t.Fatalf("Append failed: %v", err)
			}
		}

		if err := seg.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}

	// Recover
	recovery := lss.NewRecovery(dir, config)
	index, segments, nextID, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if len(segments) != 3 {
		t.Fatalf("Expected 3 segments, got %d", len(segments))
	}

	if nextID != 4 {
		t.Errorf("Expected next ID 4, got %d", nextID)
	}

	// Segments should be in order
	for i, seg := range segments {
		if seg.ID() != uint64(i+1) {
			t.Errorf("Segment %d has ID %d, expected %d", i, seg.ID(), i+1)
		}
	}

	// All keys should be in index
	if index.Len() != 9 {
		t.Errorf("Expected 9 keys in index, got %d", index.Len())
	}

	// Cleanup
	for _, seg := range segments {
		seg.Close()
	}
}

func TestRecoveryWithTombstones(t *testing.T) {
	dir := t.TempDir()
	config := lss.DefaultConfig(dir)

	seg, err := lss.CreateSegment(dir, 1, 10*1024*1024)
	if err != nil {
		t.Fatalf("CreateSegment failed: %v", err)
	}

	// Write a regular entry
	t1 := time.Now()
	entry1 := &lss.Entry{
		Key:         "key1",
		Value:       []byte("value1"),
		VectorClock: map[string]uint64{"n1": 1},
		Timestamp:   t1,
		IsTombstone: false,
	}
	if _, err := seg.Append(entry1); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Write a tombstone for the same key
	t2 := time.Now()
	tombstone := &lss.Entry{
		Key:         "key1",
		Value:       nil,
		VectorClock: map[string]uint64{"n1": 2},
		Timestamp:   t2,
		IsTombstone: true,
	}
	if _, err := seg.Append(tombstone); err != nil {
		t.Fatalf("Append tombstone failed: %v", err)
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Recover
	recovery := lss.NewRecovery(dir, config)
	index, segments, _, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Both entries should be in the index (tombstones are tracked)
	entries := index.Get("key1")
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries for key1, got %d", len(entries))
	}

	// The latest should be the tombstone (newer timestamp)
	latest := index.GetLatest("key1")
	if latest == nil {
		t.Fatal("Expected latest entry for key1")
	}

	// The latest entry should point to the tombstone
	if latest.Timestamp.Before(t1) {
		t.Error("Latest entry should have the tombstone timestamp")
	}

	// Cleanup
	for _, seg := range segments {
		seg.Close()
	}
}

func TestRecoverySegmentsSortedByID(t *testing.T) {
	dir := t.TempDir()
	config := lss.DefaultConfig(dir)

	// Create segments out of order (5, 2, 8, 1)
	segIDs := []uint64{5, 2, 8, 1}
	for _, segID := range segIDs {
		seg, err := lss.CreateSegment(dir, segID, 10*1024*1024)
		if err != nil {
			t.Fatalf("CreateSegment failed: %v", err)
		}

		entry := &lss.Entry{
			Key:         "key",
			Value:       []byte("value"),
			VectorClock: map[string]uint64{"n1": segID},
			Timestamp:   time.Now(),
		}
		seg.Append(entry)
		seg.Close()
	}

	// Recover
	recovery := lss.NewRecovery(dir, config)
	_, segments, nextID, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Segments should be sorted: 1, 2, 5, 8
	expectedOrder := []uint64{1, 2, 5, 8}
	for i, seg := range segments {
		if seg.ID() != expectedOrder[i] {
			t.Errorf("Segment %d has ID %d, expected %d", i, seg.ID(), expectedOrder[i])
		}
	}

	// Next ID should be max + 1 = 9
	if nextID != 9 {
		t.Errorf("Expected next ID 9, got %d", nextID)
	}

	// Cleanup
	for _, seg := range segments {
		seg.Close()
	}
}

func TestRecoveryFrozenSegments(t *testing.T) {
	dir := t.TempDir()
	config := lss.DefaultConfig(dir)

	seg, err := lss.CreateSegment(dir, 1, 10*1024*1024)
	if err != nil {
		t.Fatalf("CreateSegment failed: %v", err)
	}

	entry := &lss.Entry{
		Key:         "key1",
		Value:       []byte("value1"),
		VectorClock: map[string]uint64{"n1": 1},
		Timestamp:   time.Now(),
	}
	seg.Append(entry)
	seg.Close()

	// Recover
	recovery := lss.NewRecovery(dir, config)
	_, segments, _, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Recovered segments should be frozen
	if !segments[0].IsFrozen() {
		t.Error("Recovered segment should be frozen")
	}

	// Writing to frozen segment should fail
	_, err = segments[0].Append(entry)
	if err != lss.ErrSegmentFrozen {
		t.Errorf("Expected ErrSegmentFrozen, got: %v", err)
	}

	// Cleanup
	for _, seg := range segments {
		seg.Close()
	}
}

func TestValidateSegment(t *testing.T) {
	dir := t.TempDir()
	config := lss.DefaultConfig(dir)

	// Create a valid segment
	seg, err := lss.CreateSegment(dir, 1, 10*1024*1024)
	if err != nil {
		t.Fatalf("CreateSegment failed: %v", err)
	}

	entry := &lss.Entry{
		Key:         "key1",
		Value:       []byte("value1"),
		VectorClock: map[string]uint64{"n1": 1},
		Timestamp:   time.Now(),
	}
	seg.Append(entry)
	seg.Close()

	// Validate should succeed
	recovery := lss.NewRecovery(dir, config)
	path := filepath.Join(dir, "seg-0000000000000001.lss")
	err = recovery.ValidateSegment(path)
	if err != nil {
		t.Errorf("ValidateSegment failed on valid segment: %v", err)
	}
}

func TestRecoveryNonexistentDirectory(t *testing.T) {
	config := lss.DefaultConfig("/nonexistent/path/that/does/not/exist")

	recovery := lss.NewRecovery("/nonexistent/path/that/does/not/exist", config)
	index, segments, nextID, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recovery should not fail for nonexistent directory: %v", err)
	}

	if index == nil {
		t.Error("Index should not be nil")
	}

	if len(segments) != 0 {
		t.Error("Should have 0 segments for nonexistent directory")
	}

	if nextID != 1 {
		t.Errorf("Expected next ID 1, got %d", nextID)
	}
}
