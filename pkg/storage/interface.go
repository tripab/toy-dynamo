package storage

import (
	"errors"

	"github.com/tripab/toy-dynamo/pkg/storage/lss"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrStorageClosed = errors.New("storage closed")
)

// Storage defines the interface for storage engines
type Storage interface {
	// Get retrieves all versions of a key
	Get(key string) ([]versioning.VersionedValue, error)

	// Put stores a version of a key
	Put(key string, value versioning.VersionedValue) error

	// Delete removes a key
	Delete(key string) error

	// GetRange retrieves keys in a range [start, end)
	GetRange(start, end string) (map[string][]versioning.VersionedValue, error)

	// GetAllKeys returns all keys in storage
	GetAllKeys() ([]string, error)

	// Close closes the storage
	Close() error
}

// NewStorage creates a storage engine of the specified type
func NewStorage(engineType, path, nodeID string) (Storage, error) {
	switch engineType {
	case "memory":
		return NewMemoryStorage(), nil
	case "lss":
		config := lss.DefaultConfig(path)
		return lss.NewLSSEngine(config)
	case "boltdb":
		return NewBoltDBStorage(path, nodeID)
	case "badger":
		return NewBadgerStorage(path, nodeID)
	default:
		return NewMemoryStorage(), nil
	}
}
