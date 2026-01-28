package rpc

import "errors"

var (
	// ErrNodeUnreachable is returned when a node cannot be contacted
	ErrNodeUnreachable = errors.New("node unreachable")

	// ErrRequestTimeout is returned when an RPC request times out
	ErrRequestTimeout = errors.New("request timeout")

	// ErrInvalidResponse is returned when the response cannot be parsed
	ErrInvalidResponse = errors.New("invalid response")

	// ErrServerError is returned when the remote server returns an error
	ErrServerError = errors.New("server error")

	// ErrKeyNotFound is returned when a key doesn't exist on the remote node
	ErrKeyNotFound = errors.New("key not found")

	// ErrWriteFailed is returned when a write operation fails
	ErrWriteFailed = errors.New("write failed")

	// ErrPoolClosed is returned when trying to use a closed connection pool
	ErrPoolClosed = errors.New("connection pool is closed")

	// ErrNodeUnhealthy is returned when the target node is marked as unhealthy
	ErrNodeUnhealthy = errors.New("node is unhealthy")
)
