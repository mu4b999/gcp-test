package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"time"

	envoy_config_core_v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"github.com/mu4b999/gcp-test/pkg/cache"
	"github.com/mu4b999/gcp-test/pkg/snapshotter"
	"github.com/mu4b999/gcp-test/pkg/watcher"
	"google.golang.org/grpc"
)

type ControlPlane struct {
	gcpCache       cache.Cache
	snapshotter    snapshotter.Snapshotter
	serviceWatcher watcher.ServiceWatcher
}

// Implement server.Callbacks interface
func (cp *ControlPlane) OnStreamOpen(_ context.Context, id int64, typeURL string) error {
	log.Printf("Stream %d opened for %s", id, typeURL)
	return nil
}

func (cp *ControlPlane) OnStreamClosed(id int64, node *envoy_config_core_v3.Node) {
	log.Printf("Stream %d closed", id)
}

func (cp *ControlPlane) OnStreamRequest(id int64, req *discoveryv3.DiscoveryRequest) error {
	log.Printf("Stream %d received request: %+v", id, req)
	return nil
}

func (cp *ControlPlane) OnStreamDeltaRequest(id int64, req *discoveryv3.DeltaDiscoveryRequest) error {
	log.Printf("Stream %d received delta request: %+v", id, req)
	return nil
}

func (cp *ControlPlane) OnStreamDeltaResponse(id int64, req *discoveryv3.DeltaDiscoveryRequest, res *discoveryv3.DeltaDiscoveryResponse) {
	log.Printf("Stream %d sent delta response: %+v", id, res)
}

func (cp *ControlPlane) OnStreamResponse(ctx context.Context, id int64, req *discoveryv3.DiscoveryRequest, res *discoveryv3.DiscoveryResponse) {
	log.Printf("Stream %d sent response: %+v", id, res)
}
func (cp *ControlPlane) OnDeltaStreamClosed(id int64, node *envoy_config_core_v3.Node) {
	log.Printf("Delta stream %d closed for node: %+v", id, node)
}

func (cp *ControlPlane) OnFetchResponse(req *discoveryv3.DiscoveryRequest, res *discoveryv3.DiscoveryResponse) {
	log.Printf("Fetch response sent for request: %+v", req)
}

func (cp *ControlPlane) OnFetchRequest(ctx context.Context, req *discoveryv3.DiscoveryRequest) error {
	log.Printf("Fetch request received: %+v", req)
	return nil
}

func (cp *ControlPlane) OnDeltaStreamOpen(_ context.Context, id int64, typeURL string) error {
	log.Printf("Delta stream %d opened for %s", id, typeURL)
	return nil
}

func main() {
	ctx := context.Background()

	redisConfig := cache.Config{
		Addr:         "localhost:6379",
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	}

	cache, err := cache.NewRedisCache(ctx, redisConfig)
	if err != nil {
		log.Fatalf("Failed to initialize Redis cache: %v", err)
	}

	// Initialize your snapshotter with the cache
	snapshotter := snapshotter.NewGCPSnapshotter(ctx, "my-project")

	// Setup watchers
	watcher := watcher.NewServiceWatcher(ctx, "my-project")

	cp := &ControlPlane{
		gcpCache:       cache,
		snapshotter:    snapshotter,
		serviceWatcher: watcher,
	}

	// Create TCP listener
	lis, err := net.Listen("tcp", ":18000")
	if err != nil {
		log.Fatalf("Failed to create listener: %v", err)
	}

	// Setup xDS server
	srv := server.NewServer(ctx, cache, cp)
	grpcServer := grpc.NewServer()
	discoveryv3.RegisterAggregatedDiscoveryServiceServer(grpcServer, srv)

	// Start HTTP server for health checks
	go func() {
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	log.Println("Starting control plane on :18000")
	log.Fatal(grpcServer.Serve(lis))
}
