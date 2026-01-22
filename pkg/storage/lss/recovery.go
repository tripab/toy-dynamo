package lss

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Recovery rebuilds the index by replaying all segments
type Recovery struct {
	dataDir string
	config  *Config
}

// NewRecovery creates a new Recovery instance
func NewRecovery(dataDir string, config *Config) *Recovery {
	return &Recovery{
		dataDir: dataDir,
		config:  config,
	}
}

// Recover loads all segments and rebuilds the index
// Returns the rebuilt index, list of segments, and the next segment ID to use
func (r *Recovery) Recover() (*Index, []*Segment, uint64, error) {
	// Find all segment files
	files, err := r.findSegmentFiles()
	if err != nil {
		return nil, nil, 1, fmt.Errorf("failed to find segment files: %w", err)
	}

	if len(files) == 0 {
		// No existing segments - fresh start
		return NewIndex(), nil, 1, nil
	}

	// Sort by segment ID
	sort.Slice(files, func(i, j int) bool {
		return extractSegmentID(files[i]) < extractSegmentID(files[j])
	})

	// Open and replay each segment
	index := NewIndex()
	var segments []*Segment
	var maxID uint64

	for _, path := range files {
		seg, err := r.recoverSegment(path, index)
		if err != nil {
			// Log and skip corrupted segments
			fmt.Printf("Warning: skipping corrupted segment %s: %v\n", path, err)
			continue
		}
		segments = append(segments, seg)
		if seg.ID() > maxID {
			maxID = seg.ID()
		}
	}

	return index, segments, maxID + 1, nil
}

// findSegmentFiles returns all segment file paths in the data directory
func (r *Recovery) findSegmentFiles() ([]string, error) {
	entries, err := os.ReadDir(r.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), "seg-") && strings.HasSuffix(entry.Name(), ".lss") {
			files = append(files, filepath.Join(r.dataDir, entry.Name()))
		}
	}

	return files, nil
}

// recoverSegment opens a segment and replays its entries into the index
func (r *Recovery) recoverSegment(path string, index *Index) (*Segment, error) {
	seg, err := OpenSegment(path, r.config.MaxSegmentSize)
	if err != nil {
		return nil, err
	}

	// Replay all entries to rebuild index
	err = seg.Iterate(func(offset int64, entry *Entry) error {
		indexEntry := IndexEntry{
			SegmentID: seg.ID(),
			Offset:    offset,
			Timestamp: entry.Timestamp,
		}

		if entry.IsTombstone {
			// For tombstones, we still need to track them to know the key was deleted
			// The engine will handle filtering tombstones on read
			index.Add(entry.Key, indexEntry)
		} else {
			index.Add(entry.Key, indexEntry)
		}

		return nil
	})

	if err != nil {
		seg.Close()
		return nil, fmt.Errorf("failed to replay segment: %w", err)
	}

	// Mark old segments as frozen (only most recent can be written to)
	seg.Freeze()

	return seg, nil
}

// extractSegmentID extracts the segment ID from a filename
func extractSegmentID(path string) uint64 {
	base := filepath.Base(path)
	// Format: seg-XXXXXXXXXXXXXXXX.lss
	if !strings.HasPrefix(base, "seg-") || !strings.HasSuffix(base, ".lss") {
		return 0
	}

	hexStr := strings.TrimPrefix(strings.TrimSuffix(base, ".lss"), "seg-")
	id, _ := strconv.ParseUint(hexStr, 16, 64)
	return id
}

// ValidateSegment checks if a segment file is valid without replaying
func (r *Recovery) ValidateSegment(path string) error {
	seg, err := OpenSegment(path, r.config.MaxSegmentSize)
	if err != nil {
		return err
	}
	defer seg.Close()

	// Iterate through all entries to validate checksums
	return seg.Iterate(func(offset int64, entry *Entry) error {
		// Just validating - deserialization checks CRC
		return nil
	})
}
