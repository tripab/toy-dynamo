package replication

import (
	"github.com/tripab/toy-dynamo/pkg/types"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// Replicator handles replication of key-value pairs to replica nodes
type Replicator struct {
	nodeInfo types.NodeInfo
	storage  types.Storage
	ring     types.Ring
	config   types.Config
}

// NewReplicator creates a new Replicator with typed dependencies
func NewReplicator(nodeInfo types.NodeInfo, storage types.Storage, ring types.Ring, config types.Config) *Replicator {
	return &Replicator{
		nodeInfo: nodeInfo,
		storage:  storage,
		ring:     ring,
		config:   config,
	}
}

// ReplicateKey sends a key-value pair to all replicas
func (r *Replicator) ReplicateKey(key string, value versioning.VersionedValue) error {
	// Get preference list for this key
	preferenceList := r.ring.GetPreferenceList(key, r.config.GetN())

	// In production, would send to each node in preference list via RPC
	// For now, store locally if we're in the preference list
	for _, nodeID := range preferenceList {
		if nodeID == r.nodeInfo.GetID() {
			if err := r.storage.Put(key, value); err != nil {
				return err
			}
			break
		}
	}

	return nil
}
