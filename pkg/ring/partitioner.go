package ring

// Partitioner handles data partitioning strategy
type Partitioner struct {
	partitionCount int
	strategy       string
}

// NewPartitioner creates a new partitioner
func NewPartitioner(partitionCount int, strategy string) *Partitioner {
	return &Partitioner{
		partitionCount: partitionCount,
		strategy:       strategy,
	}
}

// GetPartition returns the partition number for a key
func (p *Partitioner) GetPartition(key string) int {
	// Simple modulo partitioning
	// In production, would use consistent hashing
	return 0
}
