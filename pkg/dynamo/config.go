package dynamo

import "time"

type Config struct {
	// Replication factor - number of nodes to replicate each key
	N int

	// Read quorum size - minimum nodes that must respond to a read
	R int

	// Write quorum size - minimum nodes that must acknowledge a write
	W int

	// Number of virtual nodes per physical node
	VirtualNodes int

	// Storage engine type: "memory", "boltdb", "badger"
	StorageEngine string

	// Storage path for persistent engines
	StoragePath string

	// Gossip interval for membership protocol
	GossipInterval time.Duration

	// Anti-entropy interval for replica synchronization
	AntiEntropyInterval time.Duration

	// Enable hinted handoff for temporary failures
	HintedHandoffEnabled bool

	// Timeout for delivering hints
	HintTimeout time.Duration

	// Maximum size of vector clock before pruning
	VectorClockMaxSize int

	// Request timeout
	RequestTimeout time.Duration

	// Enable read repair
	ReadRepairEnabled bool

	// Tombstone TTL - how long to keep tombstones before compaction removes them
	// This ensures tombstones have time to propagate to all replicas
	TombstoneTTL time.Duration

	// Tombstone compaction interval - how often to run tombstone cleanup
	TombstoneCompactionInterval time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		N:                    3,
		R:                    2,
		W:                    2,
		VirtualNodes:         256,
		StorageEngine:        "memory",
		StoragePath:          "./data",
		GossipInterval:       1 * time.Second,
		AntiEntropyInterval:  60 * time.Second,
		HintedHandoffEnabled: true,
		HintTimeout:          10 * time.Second,
		VectorClockMaxSize:   10,
		RequestTimeout:              300 * time.Millisecond,
		ReadRepairEnabled:           true,
		TombstoneTTL:                7 * 24 * time.Hour, // 7 days
		TombstoneCompactionInterval: 1 * time.Hour,
	}
}

// Getter methods to implement types.Config interface

func (c *Config) GetN() int                            { return c.N }
func (c *Config) GetR() int                            { return c.R }
func (c *Config) GetW() int                            { return c.W }
func (c *Config) GetGossipInterval() time.Duration     { return c.GossipInterval }
func (c *Config) GetAntiEntropyInterval() time.Duration { return c.AntiEntropyInterval }
func (c *Config) GetHintedHandoffEnabled() bool        { return c.HintedHandoffEnabled }
func (c *Config) GetHintTimeout() time.Duration        { return c.HintTimeout }
func (c *Config) GetRequestTimeout() time.Duration     { return c.RequestTimeout }
func (c *Config) GetReadRepairEnabled() bool           { return c.ReadRepairEnabled }
func (c *Config) GetVectorClockMaxSize() int                 { return c.VectorClockMaxSize }
func (c *Config) GetTombstoneTTL() time.Duration             { return c.TombstoneTTL }
func (c *Config) GetTombstoneCompactionInterval() time.Duration { return c.TombstoneCompactionInterval }
