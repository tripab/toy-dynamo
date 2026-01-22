package storage

import (
	"path/filepath"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// BadgerStorage uses BadgerDB for persistent storage.
//
// Deprecated: BadgerStorage is a stub implementation that does not persist data.
// Use the LSS (Log-Structured Storage) engine instead for actual persistence:
//
//	storage, err := storage.NewStorage("lss", "/path/to/data", "node-id")
//
// The LSS engine provides a custom implementation that demonstrates storage engine
// internals including write-ahead logging, compaction, and crash recovery.
type BadgerStorage struct {
	path   string
	nodeID string
	// This is a stub - no actual BadgerDB integration
}

func NewBadgerStorage(path, nodeID string) (*BadgerStorage, error) {
	dbPath := filepath.Join(path, nodeID)

	// In production:
	// opts := badger.DefaultOptions(dbPath)
	// db, err := badger.Open(opts)
	// if err != nil {
	//     return nil, err
	// }

	return &BadgerStorage{
		path:   dbPath,
		nodeID: nodeID,
	}, nil
}

func (b *BadgerStorage) Get(key string) ([]versioning.VersionedValue, error) {
	return nil, ErrKeyNotFound
}

func (b *BadgerStorage) Put(key string, value versioning.VersionedValue) error {
	return nil
}

func (b *BadgerStorage) Delete(key string) error {
	return nil
}

func (b *BadgerStorage) GetRange(start, end string) (map[string][]versioning.VersionedValue, error) {
	return make(map[string][]versioning.VersionedValue), nil
}

func (b *BadgerStorage) GetAllKeys() ([]string, error) {
	return []string{}, nil
}

func (b *BadgerStorage) Close() error {
	return nil
}
