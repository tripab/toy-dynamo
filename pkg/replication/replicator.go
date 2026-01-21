package replication

import (
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

type Replicator struct {
	node   interface{} // *Node
	config interface{} // *Config
}

func NewReplicator(node interface{}, config interface{}) *Replicator {
	return &Replicator{
		node:   node,
		config: config,
	}
}

// ReplicateKey sends a key-value pair to all replicas
func (r *Replicator) ReplicateKey(key string, value versioning.VersionedValue) error {
	// Implementation would send to N replicas
	return nil
}
