package storage

import (
	"sort"
	"sync"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// MemoryStorage provides an in-memory storage implementation
type MemoryStorage struct {
	data map[string][]versioning.VersionedValue
	mu   sync.RWMutex
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string][]versioning.VersionedValue),
	}
}

func (m *MemoryStorage) Get(key string) ([]versioning.VersionedValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	values, exists := m.data[key]
	if !exists || len(values) == 0 {
		return nil, ErrKeyNotFound
	}

	// Return a copy
	result := make([]versioning.VersionedValue, len(values))
	copy(result, values)
	return result, nil
}

func (m *MemoryStorage) Put(key string, value versioning.VersionedValue) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get existing versions
	existing := m.data[key]

	// Add new version and reconcile to remove dominated (ancestor) versions
	// This prevents unbounded version accumulation per Dynamo paper Section 4.4
	combined := append(existing, value)
	reconciled := versioning.ReconcileConcurrent(combined)
	m.data[key] = reconciled

	return nil
}

func (m *MemoryStorage) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

func (m *MemoryStorage) GetRange(start, end string) (map[string][]versioning.VersionedValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string][]versioning.VersionedValue)

	for key, values := range m.data {
		if key >= start && key < end {
			result[key] = values
		}
	}

	return result, nil
}

func (m *MemoryStorage) GetAllKeys() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	return keys, nil
}

func (m *MemoryStorage) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data = nil
	return nil
}
