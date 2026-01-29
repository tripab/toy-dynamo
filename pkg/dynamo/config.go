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

	// RPC retry configuration
	// MaxRetries is the maximum number of retry attempts for RPC calls
	MaxRetries int

	// InitialRetryBackoff is the initial delay before the first retry
	InitialRetryBackoff time.Duration

	// MaxRetryBackoff is the maximum delay between retries
	MaxRetryBackoff time.Duration

	// RetryBackoffMultiplier is the factor by which backoff increases each retry
	RetryBackoffMultiplier float64

	// Circuit breaker configuration
	// CircuitBreakerThreshold is the number of failures before opening the circuit
	CircuitBreakerThreshold int

	// CircuitBreakerResetTimeout is the time to wait before testing recovery
	CircuitBreakerResetTimeout time.Duration

	// EnableCircuitBreaker enables circuit breaker for RPC calls
	EnableCircuitBreaker bool

	// EnableRetry enables retry with exponential backoff for RPC calls
	EnableRetry bool

	// Admission control settings - throttles background tasks when foreground latency is high

	// AdmissionControlEnabled enables the admission controller
	AdmissionControlEnabled bool

	// AdmissionLatencyThreshold is the p99 latency threshold above which background tasks are throttled
	AdmissionLatencyThreshold time.Duration

	// AdmissionMaxBackgroundSlots is the maximum number of background task slots
	AdmissionMaxBackgroundSlots int

	// AdmissionMinBackgroundSlots is the minimum number of background task slots
	AdmissionMinBackgroundSlots int

	// AdmissionWindowSize is the number of latency samples for p99 calculation
	AdmissionWindowSize int

	// Coordinator selection settings - picks fastest node as coordinator for writes

	// CoordinatorSelectionEnabled enables latency-based coordinator selection
	CoordinatorSelectionEnabled bool

	// CoordinatorSelectionWindowSize is the number of latency samples per node
	CoordinatorSelectionWindowSize int
}

func DefaultConfig() *Config {
	return &Config{
		N:                           3,
		R:                           2,
		W:                           2,
		VirtualNodes:                256,
		StorageEngine:               "memory",
		StoragePath:                 "./data",
		GossipInterval:              1 * time.Second,
		AntiEntropyInterval:         60 * time.Second,
		HintedHandoffEnabled:        true,
		HintTimeout:                 10 * time.Second,
		VectorClockMaxSize:          10,
		RequestTimeout:              300 * time.Millisecond,
		ReadRepairEnabled:           true,
		TombstoneTTL:                7 * 24 * time.Hour, // 7 days
		TombstoneCompactionInterval: 1 * time.Hour,
		// Retry configuration
		MaxRetries:             3,
		InitialRetryBackoff:    100 * time.Millisecond,
		MaxRetryBackoff:        5 * time.Second,
		RetryBackoffMultiplier: 2.0,
		// Circuit breaker configuration
		CircuitBreakerThreshold:    5,
		CircuitBreakerResetTimeout: 30 * time.Second,
		EnableCircuitBreaker:       true,
		EnableRetry:                true,
		// Admission control defaults
		AdmissionControlEnabled:     true,
		AdmissionLatencyThreshold:   100 * time.Millisecond,
		AdmissionMaxBackgroundSlots: 10,
		AdmissionMinBackgroundSlots: 1,
		AdmissionWindowSize:         1000,
		// Coordinator selection defaults
		CoordinatorSelectionEnabled:    true,
		CoordinatorSelectionWindowSize: 100,
	}
}

// Getter methods to implement types.Config interface

func (c *Config) GetN() int                                     { return c.N }
func (c *Config) GetR() int                                     { return c.R }
func (c *Config) GetW() int                                     { return c.W }
func (c *Config) GetGossipInterval() time.Duration              { return c.GossipInterval }
func (c *Config) GetAntiEntropyInterval() time.Duration         { return c.AntiEntropyInterval }
func (c *Config) GetHintedHandoffEnabled() bool                 { return c.HintedHandoffEnabled }
func (c *Config) GetHintTimeout() time.Duration                 { return c.HintTimeout }
func (c *Config) GetRequestTimeout() time.Duration              { return c.RequestTimeout }
func (c *Config) GetReadRepairEnabled() bool                    { return c.ReadRepairEnabled }
func (c *Config) GetVectorClockMaxSize() int                    { return c.VectorClockMaxSize }
func (c *Config) GetTombstoneTTL() time.Duration                { return c.TombstoneTTL }
func (c *Config) GetTombstoneCompactionInterval() time.Duration { return c.TombstoneCompactionInterval }
func (c *Config) GetMaxRetries() int                            { return c.MaxRetries }
func (c *Config) GetInitialRetryBackoff() time.Duration         { return c.InitialRetryBackoff }
func (c *Config) GetMaxRetryBackoff() time.Duration             { return c.MaxRetryBackoff }
func (c *Config) GetRetryBackoffMultiplier() float64            { return c.RetryBackoffMultiplier }
func (c *Config) GetCircuitBreakerThreshold() int               { return c.CircuitBreakerThreshold }
func (c *Config) GetCircuitBreakerResetTimeout() time.Duration  { return c.CircuitBreakerResetTimeout }
func (c *Config) GetEnableCircuitBreaker() bool                 { return c.EnableCircuitBreaker }
func (c *Config) GetEnableRetry() bool                          { return c.EnableRetry }

// Admission control getters
func (c *Config) GetAdmissionControlEnabled() bool            { return c.AdmissionControlEnabled }
func (c *Config) GetAdmissionLatencyThreshold() time.Duration { return c.AdmissionLatencyThreshold }
func (c *Config) GetAdmissionMaxBackgroundSlots() int         { return c.AdmissionMaxBackgroundSlots }
func (c *Config) GetAdmissionMinBackgroundSlots() int         { return c.AdmissionMinBackgroundSlots }
func (c *Config) GetAdmissionWindowSize() int                 { return c.AdmissionWindowSize }

// Coordinator selection getters
func (c *Config) GetCoordinatorSelectionEnabled() bool    { return c.CoordinatorSelectionEnabled }
func (c *Config) GetCoordinatorSelectionWindowSize() int  { return c.CoordinatorSelectionWindowSize }
