package storage

import (
	"path/filepath"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// BadgerStorage uses BadgerDB for persistent storage
type BadgerStorage struct {
	path   string
	nodeID string
	// In production: db *badger.DB
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
