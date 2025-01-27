package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"time"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"github.com/mu4b999/gcp-test/pkg/cache"
	"github.com/mu4b999/gcp-test/pkg/controlplane"
	"github.com/mu4b999/gcp-test/pkg/watcher"
	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	// Setup watchers
	watcher := watcher.NewServiceWatcher(ctx, "my-project")

	// Create the enhanced control plane
	cp := controlplane.NewControlPlane(*cache, watcher)

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
