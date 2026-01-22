// Package lss implements a Log-Structured Storage engine for Dynamo.
// This provides a custom persistence layer that demonstrates storage engine
// internals including WAL, compaction, and crash recovery.
package lss

import "time"

// Config holds configuration for the LSS engine
type Config struct {
	// DataDir is the directory for storing segment files
	DataDir string

	// MaxSegmentSize is the maximum size of a segment before rotation (default: 10MB)
	MaxSegmentSize int64

	// CompactionInterval is how often to run background compaction (default: 5 minutes)
	CompactionInterval time.Duration

	// MinSegmentsToCompact is minimum number of segments before compaction triggers (default: 3)
	MinSegmentsToCompact int

	// SyncOnWrite forces fsync after each write (default: false for performance)
	SyncOnWrite bool
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig(dataDir string) *Config {
	return &Config{
		DataDir:              dataDir,
		MaxSegmentSize:       10 * 1024 * 1024, // 10MB
		CompactionInterval:   5 * time.Minute,
		MinSegmentsToCompact: 3,
		SyncOnWrite:          false,
	}
}
