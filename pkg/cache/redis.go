// Package cache provides a Redis-backed implementation of Envoy's control plane cache
package cache

import (
	"context"
	"encoding/json"
	"fmt"
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

// Implements envoycache.Cache interface

// func (rc *RedisCache) CreateWatch(req *envoycache.Request, state *stream.StreamState,
// 	callback func(response envoycache.Response)) func() {
// 	responseChan := make(chan envoycache.Response)
// 	go func() {
// 		for response := range responseChan {
// 			callback(response)
// 		}
// 	}()
// 	return rc.snapshot.CreateWatch(req, *state, responseChan)
// }

func (r *RedisCache) CreateWatch(req *envoycache.Request, state stream.StreamState, respond chan envoycache.Response) func() {
	return r.snapshot.CreateWatch(req, state, respond)
}

// func (rc *RedisCache) CreateDeltaWatch(req *envoycache.DeltaRequest, state *stream.StreamState,
// 	callback func(response envoycache.DeltaResponse)) func() {
// 	responseChan := make(chan envoycache.DeltaResponse)
// 	go func() {
// 		for response := range responseChan {
// 			callback(response)
// 		}
// 	}()
// 	return rc.snapshot.CreateDeltaWatch(req, *state, responseChan)
// }

// func (rc *RedisCache) CreateDeltaWatch(req *envoycache.DeltaRequest, state *stream.StreamState, respond chan envoycache.DeltaResponse) func() {
// 	return rc.snapshot.CreateDeltaWatch(req, *state, respond)
// }

func (r *RedisCache) CreateDeltaWatch(req *envoycache.DeltaRequest, state stream.StreamState, resp chan envoycache.DeltaResponse) func() {
	return r.snapshot.CreateDeltaWatch(req, state, resp)
}

// ...existing code...

func (rc *RedisCache) Fetch(ctx context.Context, req *envoycache.Request) (envoycache.Response, error) {
	return rc.snapshot.Fetch(ctx, req)
}

// Implements SnapshotCache methods

func (rc *RedisCache) SetSnapshot(nodeID string, snapshot envoycache.Snapshot) error {
	// Store in memory cache
	if err := rc.snapshot.SetSnapshot(context.Background(), nodeID, &snapshot); err != nil {
		return fmt.Errorf("failed to set in-memory snapshot: %w", err)
	}

	// Backup to Redis
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	ctx := context.Background()
	key := fmt.Sprintf("snapshot:%s", nodeID)
	if err := rc.client.Set(ctx, key, data, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to store snapshot in redis: %w", err)
	}

	return nil
}

func (rc *RedisCache) GetSnapshot(nodeID string) (envoycache.ResourceSnapshot, error) {
	resourceSnapshot, err := rc.snapshot.GetSnapshot(nodeID)
	if err != nil {
		return nil, err
	}
	return resourceSnapshot, nil
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
	resources := make(map[string]types.Resource)

	// Get all keys matching the typeURL pattern
	ctx := context.Background()
	keys, err := rc.client.Keys(ctx, fmt.Sprintf("resource:%s:*", typeURL)).Result()
	if err != nil {
		return nil
	}

	// Fetch and unmarshal each resource
	for _, key := range keys {
		data, err := rc.client.Get(ctx, key).Bytes()
		if err != nil {
			continue
		}

		var resource types.Resource
		if err := json.Unmarshal(data, &resource); err == nil {
			resources[key] = resource
		}
	}

	return resources
}

func (rc *RedisCache) GetResourceVersions(typeURL string) map[string]string {
	versions := make(map[string]string)

	// Get all keys matching the typeURL pattern
	ctx := context.Background()
	keys, err := rc.client.Keys(ctx, fmt.Sprintf("resource:%s:*", typeURL)).Result()
	if err != nil {
		return nil
	}

	// Fetch version for each resource
	for _, key := range keys {
		version, err := rc.client.HGet(ctx, key, "version").Result()
		if err == nil {
			versions[key] = version
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
