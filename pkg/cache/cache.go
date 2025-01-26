package cache

import (
	"context"
	"time"

	envoycache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
)

type Cache interface {
	envoycache.Cache

	// Basic cache operations
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Connection management
	Ping(ctx context.Context) error
	Close() error
}
