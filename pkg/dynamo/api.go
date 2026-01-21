package dynamo

import "context"

// API defines the public interface for Dynamo operations
type API interface {
	// Get retrieves a value by key
	Get(ctx context.Context, key string) (*GetResult, error)

	// Put stores a key-value pair
	Put(ctx context.Context, key string, value []byte, context *Context) error

	// Delete removes a key
	Delete(ctx context.Context, key string, context *Context) error
}

// Ensure Node implements API
var _ API = (*Node)(nil)
