package cache

import (
	"context"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoycache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
)

type Cache interface {
	envoycache.Cache

	// Snapshot management - Note the ResourceSnapshot type here
	SetSnapshot(nodeID string, snapshot envoycache.ResourceSnapshot) error
	GetSnapshot(nodeID string) (envoycache.ResourceSnapshot, error)
	ClearSnapshot(nodeID string)

	// Rest of the interface remains the same
	GetResourceVersions(typeURL string) map[string]string
	GetResourceNames(typeURL string) []string
	Status(node *core.Node) envoycache.StatusInfo
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Ping(ctx context.Context) error
	Close() error
}
