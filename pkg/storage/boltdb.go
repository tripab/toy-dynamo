package storage

import (
	"path/filepath"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// BoltDBStorage uses BoltDB for persistent storage.
//
// Deprecated: BoltDBStorage is a stub implementation that does not persist data.
// Use the LSS (Log-Structured Storage) engine instead for actual persistence:
//
//	storage, err := storage.NewStorage("lss", "/path/to/data", "node-id")
//
// The LSS engine provides a custom implementation that demonstrates storage engine
// internals including write-ahead logging, compaction, and crash recovery.
type BoltDBStorage struct {
	path   string
	nodeID string
	// This is a stub - no actual BoltDB integration
}

func NewBoltDBStorage(path, nodeID string) (*BoltDBStorage, error) {
	dbPath := filepath.Join(path, nodeID+".db")

	// In production:
	// db, err := bolt.Open(dbPath, 0600, nil)
	// if err != nil {
	//     return nil, err
	// }

	return &BoltDBStorage{
		path:   dbPath,
		nodeID: nodeID,
	}, nil
}

func (b *BoltDBStorage) Get(key string) ([]versioning.VersionedValue, error) {
	// In production, would query BoltDB
	return nil, ErrKeyNotFound
}

func (b *BoltDBStorage) Put(key string, value versioning.VersionedValue) error {
	// In production, would write to BoltDB
	return nil
}

func (b *BoltDBStorage) Delete(key string) error {
	return nil
}

func (b *BoltDBStorage) GetRange(start, end string) (map[string][]versioning.VersionedValue, error) {
	return make(map[string][]versioning.VersionedValue), nil
}

func (b *BoltDBStorage) GetAllKeys() ([]string, error) {
	return []string{}, nil
}

func (b *BoltDBStorage) Close() error {
	// In production: return b.db.Close()
	return nil
}
