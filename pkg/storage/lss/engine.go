package lss

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// LSSEngine is the main Log-Structured Storage engine
// It implements the storage.Storage interface
type LSSEngine struct {
	config    *Config
	index     *Index
	segments  []*Segment
	activeWAL *Segment
	nextSegID uint64
	compactor *Compactor
	mu        sync.RWMutex
	closed    bool
}

// NewLSSEngine creates a new LSS engine
func NewLSSEngine(config *Config) (*LSSEngine, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	engine := &LSSEngine{
		config: config,
	}

	// Recover from existing segments
	if err := engine.recover(); err != nil {
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	// Create active WAL if none exists
	if engine.activeWAL == nil {
		seg, err := CreateSegment(config.DataDir, engine.nextSegID, config.MaxSegmentSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create initial segment: %w", err)
		}
		engine.activeWAL = seg
		engine.nextSegID++
	}

	// Start background compactor
	engine.compactor = NewCompactor(engine, config.CompactionInterval, config.MinSegmentsToCompact)
	engine.compactor.Start()

	return engine, nil
}

// recover rebuilds state from existing segments
func (e *LSSEngine) recover() error {
	recovery := NewRecovery(e.config.DataDir, e.config)
	index, segments, nextID, err := recovery.Recover()
	if err != nil {
		return err
	}

	e.index = index
	e.segments = segments
	e.nextSegID = nextID

	// The most recent segment can be unfrozen and used as active WAL
	if len(segments) > 0 {
		lastSeg := segments[len(segments)-1]
		if !lastSeg.IsFull() {
			// Unfreeze and use as active WAL
			// Note: For simplicity, we create a new segment instead
			// In production, we'd properly unfreeze the last segment
		}
	}

	return nil
}

// Get retrieves all versions of a key
func (e *LSSEngine) Get(key string) ([]versioning.VersionedValue, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, ErrEngineClosed
	}

	// Look up in index
	indexEntries := e.index.Get(key)
	if len(indexEntries) == 0 {
		return nil, nil
	}

	// Read entries from segments
	var values []versioning.VersionedValue
	for _, ie := range indexEntries {
		entry, err := e.readEntry(ie.SegmentID, ie.Offset)
		if err != nil {
			// Skip unreadable entries
			continue
		}

		// Skip tombstones from results (deleted keys)
		if entry.IsTombstone {
			continue
		}

		values = append(values, entry.ToVersionedValue())
	}

	return values, nil
}

// Put stores a version of a key
func (e *LSSEngine) Put(key string, value versioning.VersionedValue) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrEngineClosed
	}

	// Create entry
	entry := EntryFromVersionedValue(key, value)

	// Try to append to active WAL
	offset, err := e.activeWAL.Append(entry)
	if err == ErrSegmentFull {
		// Rotate WAL
		if err := e.rotateWAL(); err != nil {
			return fmt.Errorf("failed to rotate WAL: %w", err)
		}
		// Retry append
		offset, err = e.activeWAL.Append(entry)
	}
	if err != nil {
		return fmt.Errorf("failed to append entry: %w", err)
	}

	// Sync if configured
	if e.config.SyncOnWrite {
		if err := e.activeWAL.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync: %w", err)
		}
	}

	// Update index
	e.index.Add(key, IndexEntry{
		SegmentID: e.activeWAL.ID(),
		Offset:    offset,
		Timestamp: entry.Timestamp,
	})

	return nil
}

// Delete removes a key by writing a tombstone
func (e *LSSEngine) Delete(key string) error {
	// Deletion is implemented as writing a tombstone entry
	entry := &Entry{
		Key:         key,
		Value:       nil,
		VectorClock: nil,
		Timestamp:   time.Now(),
		IsTombstone: true,
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrEngineClosed
	}

	// Append tombstone
	offset, err := e.activeWAL.Append(entry)
	if err == ErrSegmentFull {
		if err := e.rotateWAL(); err != nil {
			return fmt.Errorf("failed to rotate WAL: %w", err)
		}
		offset, err = e.activeWAL.Append(entry)
	}
	if err != nil {
		return fmt.Errorf("failed to append tombstone: %w", err)
	}

	// Update index with tombstone entry
	e.index.Add(key, IndexEntry{
		SegmentID: e.activeWAL.ID(),
		Offset:    offset,
		Timestamp: entry.Timestamp,
	})

	return nil
}

// GetRange retrieves keys in a range [start, end)
func (e *LSSEngine) GetRange(start, end string) (map[string][]versioning.VersionedValue, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, ErrEngineClosed
	}

	keys := e.index.GetKeysInRange(start, end)
	result := make(map[string][]versioning.VersionedValue)

	for _, key := range keys {
		values, err := e.getWithoutLock(key)
		if err != nil {
			continue
		}
		if len(values) > 0 {
			result[key] = values
		}
	}

	return result, nil
}

// GetAllKeys returns all keys in storage
func (e *LSSEngine) GetAllKeys() ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, ErrEngineClosed
	}

	return e.index.GetAllKeys(), nil
}

// Close closes the engine
func (e *LSSEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true

	// Stop compactor
	if e.compactor != nil {
		e.compactor.Stop()
	}

	// Close active WAL
	if e.activeWAL != nil {
		e.activeWAL.Close()
	}

	// Close all segments
	for _, seg := range e.segments {
		seg.Close()
	}

	return nil
}

// rotateWAL creates a new active WAL segment
func (e *LSSEngine) rotateWAL() error {
	// Freeze current WAL
	if err := e.activeWAL.Freeze(); err != nil {
		return err
	}

	// Add to segments list
	e.segments = append(e.segments, e.activeWAL)

	// Create new WAL
	seg, err := CreateSegment(e.config.DataDir, e.nextSegID, e.config.MaxSegmentSize)
	if err != nil {
		return err
	}

	e.activeWAL = seg
	e.nextSegID++

	return nil
}

// readEntry reads an entry from a segment
func (e *LSSEngine) readEntry(segmentID uint64, offset int64) (*Entry, error) {
	// Check active WAL first
	if e.activeWAL != nil && e.activeWAL.ID() == segmentID {
		return e.activeWAL.ReadAt(offset)
	}

	// Search frozen segments
	for _, seg := range e.segments {
		if seg.ID() == segmentID {
			return seg.ReadAt(offset)
		}
	}

	return nil, fmt.Errorf("segment %d not found", segmentID)
}

// getWithoutLock is Get without acquiring lock (caller must hold lock)
func (e *LSSEngine) getWithoutLock(key string) ([]versioning.VersionedValue, error) {
	indexEntries := e.index.Get(key)
	if len(indexEntries) == 0 {
		return nil, nil
	}

	var values []versioning.VersionedValue
	for _, ie := range indexEntries {
		entry, err := e.readEntry(ie.SegmentID, ie.Offset)
		if err != nil {
			continue
		}

		if entry.IsTombstone {
			continue
		}

		values = append(values, entry.ToVersionedValue())
	}

	return values, nil
}

// SegmentCount returns the number of frozen segments (for compaction decisions)
func (e *LSSEngine) SegmentCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.segments)
}

// getSegmentsForCompaction returns segments eligible for compaction
func (e *LSSEngine) getSegmentsForCompaction(count int) []*Segment {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.segments) < count {
		return nil
	}

	// Return oldest segments for compaction
	result := make([]*Segment, count)
	copy(result, e.segments[:count])
	return result
}

// replaceSegments atomically replaces old segments with merged segment
func (e *LSSEngine) replaceSegments(oldSegs []*Segment, newSeg *Segment, newIndex *Index) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Build set of old segment IDs
	oldIDs := make(map[uint64]bool)
	for _, seg := range oldSegs {
		oldIDs[seg.ID()] = true
	}

	// Remove old segments from list
	var remaining []*Segment
	for _, seg := range e.segments {
		if !oldIDs[seg.ID()] {
			remaining = append(remaining, seg)
		}
	}

	// Add new merged segment
	remaining = append(remaining, newSeg)

	// Sort by ID
	sort.Slice(remaining, func(i, j int) bool {
		return remaining[i].ID() < remaining[j].ID()
	})

	e.segments = remaining

	// Update index: remove old segment entries and add new ones
	for _, seg := range oldSegs {
		e.index.RemoveSegment(seg.ID())
	}

	// Merge new index entries
	for _, key := range newIndex.GetAllKeys() {
		for _, entry := range newIndex.Get(key) {
			e.index.Add(key, entry)
		}
	}

	// Delete old segment files
	for _, seg := range oldSegs {
		seg.Delete()
	}

	return nil
}

var ErrEngineClosed = fmt.Errorf("engine is closed")
