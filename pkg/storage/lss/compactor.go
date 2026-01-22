package lss

import (
	"fmt"
	"sync"
	"time"
)

// Compactor performs background compaction of segments
type Compactor struct {
	engine    *LSSEngine
	interval  time.Duration
	threshold int
	stopCh    chan struct{}
	wg        sync.WaitGroup
	mu        sync.Mutex
	running   bool
}

// NewCompactor creates a new compactor for the given engine
func NewCompactor(engine *LSSEngine, interval time.Duration, threshold int) *Compactor {
	return &Compactor{
		engine:    engine,
		interval:  interval,
		threshold: threshold,
		stopCh:    make(chan struct{}),
	}
}

// Start begins the background compaction loop
func (c *Compactor) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return
	}

	c.running = true
	c.wg.Add(1)
	go c.runLoop()
}

// Stop halts the background compaction loop
func (c *Compactor) Stop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	c.running = false
	c.mu.Unlock()

	close(c.stopCh)
	c.wg.Wait()
}

// runLoop is the background compaction goroutine
func (c *Compactor) runLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.Compact(); err != nil {
				// Log error but continue running
				// In production, would use proper logging
			}
		case <-c.stopCh:
			return
		}
	}
}

// Compact performs a compaction cycle if enough segments exist
func (c *Compactor) Compact() error {
	// Check if we have enough segments to compact
	if c.engine.SegmentCount() < c.threshold {
		return nil
	}

	// Get segments for compaction
	segments := c.engine.getSegmentsForCompaction(c.threshold)
	if segments == nil {
		return nil
	}

	// Merge the segments
	newSeg, newIndex, err := c.mergeSegments(segments)
	if err != nil {
		return fmt.Errorf("compaction merge failed: %w", err)
	}

	// Atomically replace old segments with merged segment
	if err := c.engine.replaceSegments(segments, newSeg, newIndex); err != nil {
		// Clean up the new segment if replace fails
		newSeg.Delete()
		return fmt.Errorf("compaction replace failed: %w", err)
	}

	return nil
}

// mergeSegments combines multiple segments into a single new segment
// It keeps only the latest version of each key and removes tombstones
// for keys that have no other concurrent versions
func (c *Compactor) mergeSegments(segments []*Segment) (*Segment, *Index, error) {
	// Collect all entries from all segments, tracking the latest per key
	// Map: key -> list of entries (to handle concurrent versions)
	keyEntries := make(map[string][]*Entry)
	keyLatestTimestamp := make(map[string]time.Time)

	for _, seg := range segments {
		err := seg.Iterate(func(offset int64, entry *Entry) error {
			existing := keyEntries[entry.Key]

			// Check if this entry should replace existing ones
			// For compaction, we keep the entry with the latest timestamp
			// and handle concurrent versions properly
			if existing == nil {
				keyEntries[entry.Key] = []*Entry{entry}
				keyLatestTimestamp[entry.Key] = entry.Timestamp
			} else {
				// For simplicity, if this entry is newer, replace
				// In a full implementation, we'd reconcile based on vector clocks
				if entry.Timestamp.After(keyLatestTimestamp[entry.Key]) {
					keyEntries[entry.Key] = []*Entry{entry}
					keyLatestTimestamp[entry.Key] = entry.Timestamp
				}
			}
			return nil
		})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to iterate segment %d: %w", seg.ID(), err)
		}
	}

	// Create new merged segment
	newSegID := c.engine.nextSegID
	c.engine.nextSegID++

	newSeg, err := CreateSegment(c.engine.config.DataDir, newSegID, c.engine.config.MaxSegmentSize)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create merged segment: %w", err)
	}

	// Create new index for the merged segment
	newIndex := NewIndex()

	// Calculate TTL cutoff for tombstones
	now := time.Now()
	tombstoneCutoff := now.Add(-c.engine.config.TombstoneTTL)

	// Write all entries to new segment
	for key, entries := range keyEntries {
		for _, entry := range entries {
			// For tombstones, only skip if they're older than the TTL
			// Newer tombstones must be kept to ensure they propagate to all replicas
			if entry.IsTombstone {
				if entry.Timestamp.Before(tombstoneCutoff) {
					// Tombstone is old enough to be garbage collected
					continue
				}
				// Tombstone is recent, keep it to ensure propagation
			}

			offset, err := newSeg.Append(entry)
			if err != nil {
				newSeg.Delete()
				return nil, nil, fmt.Errorf("failed to write entry to merged segment: %w", err)
			}

			newIndex.Add(key, IndexEntry{
				SegmentID: newSegID,
				Offset:    offset,
				Timestamp: entry.Timestamp,
			})
		}
	}

	// Freeze the new segment
	if err := newSeg.Freeze(); err != nil {
		newSeg.Delete()
		return nil, nil, fmt.Errorf("failed to freeze merged segment: %w", err)
	}

	return newSeg, newIndex, nil
}
