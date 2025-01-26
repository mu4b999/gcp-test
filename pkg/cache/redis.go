// Package cache provides a Redis-backed implementation of Envoy's control plane cache
package cache

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	envoycache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
	"github.com/redis/go-redis/v9"
)

// Config holds Redis connection parameters
type Config struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// RedisCache implements both Cache and envoycache.MutableCache interfaces
type RedisCache struct {
	client   *redis.Client
	snapshot envoycache.SnapshotCache
	mu       sync.RWMutex
}

// NewRedisCache creates a new Redis cache instance with the specified configuration
func NewRedisCache(ctx context.Context, cfg Config) (*RedisCache, error) {
	if cfg.PoolSize == 0 {
		cfg.PoolSize = 10
	}
	if cfg.MinIdleConns == 0 {
		cfg.MinIdleConns = 5
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}

	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	ctx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &RedisCache{
		client:   client,
		snapshot: envoycache.NewSnapshotCache(true, envoycache.IDHash{}, nil),
	}, nil
}

func (r *RedisCache) CreateWatch(req *envoycache.Request, state stream.StreamState, respond chan envoycache.Response) func() {
	return r.snapshot.CreateWatch(req, state, respond)
}

func (r *RedisCache) CreateDeltaWatch(req *envoycache.DeltaRequest, state stream.StreamState, resp chan envoycache.DeltaResponse) func() {
	return r.snapshot.CreateDeltaWatch(req, state, resp)
}

func (rc *RedisCache) Fetch(ctx context.Context, req *envoycache.Request) (envoycache.Response, error) {
	return rc.snapshot.Fetch(ctx, req)
}

// In redis.go
func (rc *RedisCache) SetSnapshot(nodeID string, snapshot envoycache.ResourceSnapshot) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Store in memory cache
	if err := rc.snapshot.SetSnapshot(context.Background(), nodeID, snapshot); err != nil {
		return fmt.Errorf("failed to set in-memory snapshot: %w", err)
	}

	// Store version info in Redis
	ctx := context.Background()
	versionKey := fmt.Sprintf("snapshot:%s:version", nodeID)

	// Get version from the snapshot
	versionMap := snapshot.GetVersionMap(versionKey)
	version := ""
	for _, v := range versionMap {
		version = v
		break
	}

	if err := rc.client.Set(ctx, versionKey, version, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to store version in redis: %w", err)
	}

	return nil
}

func (rc *RedisCache) GetSnapshot(nodeID string) (envoycache.ResourceSnapshot, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.snapshot.GetSnapshot(nodeID)
}

func (rc *RedisCache) ClearSnapshot(nodeID string) {
	rc.snapshot.ClearSnapshot(nodeID)
	rc.client.Del(context.Background(), fmt.Sprintf("snapshot:%s", nodeID))
}

// Implements additional Cache interface methods

func (rc *RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
	return rc.client.Get(ctx, key).Bytes()
}

func (rc *RedisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return rc.client.Set(ctx, key, value, ttl).Err()
}

func (rc *RedisCache) Status(node *core.Node) envoycache.StatusInfo {
	return rc.snapshot.GetStatusInfo(node.Id)
}

func (rc *RedisCache) GetResources(typeURL string) map[string]types.Resource {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	resources := make(map[string]types.Resource)

	// Get all node IDs with snapshots
	ctx := context.Background()
	pattern := "snapshot:*:version"
	keys, err := rc.client.Keys(ctx, pattern).Result()
	if err != nil {
		log.Printf("Failed to get snapshot keys: %v", err)
		return resources
	}

	// For each node, get its snapshot and extract resources
	for _, key := range keys {
		// Extract node ID from key
		nodeID := strings.TrimPrefix(key, "snapshot:")
		nodeID = strings.TrimSuffix(nodeID, ":version")

		// Get snapshot for this node
		snapshot, err := rc.GetSnapshot(nodeID)
		if err != nil {
			continue
		}

		// Get resources of requested type from snapshot
		if r := snapshot.GetResources(typeURL); r != nil {
			for name, resource := range r {
				resources[name] = resource
			}
		}
	}

	return resources
}

func (rc *RedisCache) GetResourceVersions(typeURL string) map[string]string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	versions := make(map[string]string)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get all resource keys for this type
	pattern := fmt.Sprintf("resource:%s:*", typeURL)
	keys, err := rc.client.Keys(ctx, pattern).Result()
	if err != nil {
		log.Printf("Failed to get resource keys: %v", err)
		return versions
	}

	// Use pipelining for better performance
	pipe := rc.client.Pipeline()
	cmds := make(map[string]*redis.StringCmd)

	for _, key := range keys {
		cmds[key] = pipe.HGet(ctx, key, "version")
	}

	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		log.Printf("Failed to execute pipeline: %v", err)
		return versions
	}

	for key, cmd := range cmds {
		version, err := cmd.Result()
		if err == nil {
			// Extract resource name from key
			name := strings.TrimPrefix(key, fmt.Sprintf("resource:%s:", typeURL))
			versions[name] = version
		}
	}

	return versions
}

func (rc *RedisCache) GetResourceNames(typeURL string) []string {
	ctx := context.Background()
	keys, err := rc.client.Keys(ctx, fmt.Sprintf("resource:%s:*", typeURL)).Result()
	if err != nil {
		return nil
	}
	return keys
}

func (rc *RedisCache) Close() error {
	return rc.client.Close()
}

func (rc *RedisCache) Ping(ctx context.Context) error {
	return rc.client.Ping(ctx).Err()
}
