package controlplane

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"

	envoycache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/mu4b999/gcp-test/pkg/cache"
	"github.com/mu4b999/gcp-test/pkg/snapshotter"
	"github.com/mu4b999/gcp-test/pkg/watcher"
)

type ControlPlane struct {
	gcpCache       cache.Cache
	snapshotter    snapshotter.Snapshotter
	serviceWatcher watcher.ServiceWatcher

	// Track active streams
	streams   map[int64]*streamState
	streamsMu sync.RWMutex
}

type streamState struct {
	nodeID    string
	typeURL   string
	version   string
	resources map[string]types.Resource
}

func NewControlPlane(cache cache.Cache, snapshotter snapshotter.Snapshotter, watcher watcher.ServiceWatcher) *ControlPlane {
	cp := &ControlPlane{
		gcpCache:       cache,
		snapshotter:    snapshotter,
		serviceWatcher: watcher,
		streams:        make(map[int64]*streamState),
	}

	// Start watching for service changes
	go cp.watchServices(context.Background())

	return cp
}

func (cp *ControlPlane) watchServices(ctx context.Context) {
	serviceChan := cp.serviceWatcher.WatchServices(ctx, 30*time.Second)

	for {
		select {
		case services := <-serviceChan:
			if err := cp.handleServiceUpdate(ctx, services); err != nil {
				log.Printf("Error handling service update: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (cp *ControlPlane) handleServiceUpdate(ctx context.Context, services []string) error {
	// Create a new snapshot with updated services
	snapshot, err := cp.createSnapshot(services)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Update all active streams with new snapshot
	cp.streamsMu.RLock()
	defer cp.streamsMu.RUnlock()

	for _, state := range cp.streams {
		// snapshot is already a *envoycache.Snapshot which implements ResourceSnapshot
		if err := cp.gcpCache.SetSnapshot(state.nodeID, snapshot); err != nil {
			log.Printf("Failed to update snapshot for node %s: %v", state.nodeID, err)
		}
	}

	return nil
}

func (cp *ControlPlane) OnStreamOpen(ctx context.Context, id int64, typeURL string) error {
	cp.streamsMu.Lock()
	defer cp.streamsMu.Unlock()

	cp.streams[id] = &streamState{
		typeURL:   typeURL,
		resources: make(map[string]types.Resource),
	}

	log.Printf("Stream %d opened for %s", id, typeURL)
	return nil
}

func (cp *ControlPlane) OnStreamResponse(ctx context.Context, id int64, req *discoveryv3.DiscoveryRequest, res *discoveryv3.DiscoveryResponse) {
	cp.streamsMu.RLock()
	if state, exists := cp.streams[id]; exists {
		state.version = res.VersionInfo
	}
	cp.streamsMu.RUnlock()

	log.Printf("Stream %d sent response version %s", id, res.VersionInfo)
}

func (cp *ControlPlane) OnStreamClosed(id int64, node *core.Node) {
	cp.streamsMu.Lock()
	delete(cp.streams, id)
	cp.streamsMu.Unlock()

	log.Printf("Stream %d closed for node %s", id, node.GetId())
}

func (cp *ControlPlane) createSnapshot(services []string) (*envoycache.Snapshot, error) {
	version := fmt.Sprintf("%d", time.Now().UnixNano())

	// Create an empty snapshot
	snapshot, err := envoycache.NewSnapshot(
		version,
		map[resource.Type][]types.Resource{
			resource.ClusterType:  {},
			resource.EndpointType: {},
			resource.RouteType:    {},
			resource.ListenerType: {},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Validate the snapshot before returning
	if err := snapshot.Consistent(); err != nil {
		return nil, fmt.Errorf("inconsistent snapshot: %w", err)
	}

	return snapshot, nil
}

func (cp *ControlPlane) OnStreamRequest(id int64, req *discoveryv3.DiscoveryRequest) error {
	nodeID := req.Node.GetId()
	typeURL := req.TypeUrl

	cp.streamsMu.Lock()
	if state, exists := cp.streams[id]; exists {
		state.nodeID = nodeID
		state.typeURL = typeURL
	}
	cp.streamsMu.Unlock()

	// Check if we have a snapshot for this node
	_, err := cp.gcpCache.GetSnapshot(nodeID)
	if err != nil {
		// Create initial snapshot if none exists
		newSnapshot, err := cp.createSnapshot(nil)
		if err != nil {
			return fmt.Errorf("failed to create initial snapshot: %w", err)
		}

		// The pointer to Snapshot implements ResourceSnapshot
		if err := cp.gcpCache.SetSnapshot(nodeID, newSnapshot); err != nil {
			return fmt.Errorf("failed to set initial snapshot: %w", err)
		}
	}

	log.Printf("Stream %d received request from node %s for type %s", id, nodeID, typeURL)
	return nil
}

// Add this method to your ControlPlane struct implementation
func (cp *ControlPlane) OnDeltaStreamClosed(id int64, node *core.Node) {
	// Acquire lock to safely modify the streams map
	cp.streamsMu.Lock()
	defer cp.streamsMu.Unlock()

	// Clean up the stream state
	delete(cp.streams, id)

	// Log the delta stream closure
	if node != nil {
		log.Printf("Delta stream %d closed for node %s", id, node.GetId())
	} else {
		log.Printf("Delta stream %d closed for unknown node", id)
	}

	// You might want to perform additional cleanup here, such as:
	// - Removing any delta-specific resources
	// - Updating metrics
	// - Notifying other components
}

func (cp *ControlPlane) OnDeltaStreamRequest(id int64, req *discoveryv3.DeltaDiscoveryRequest) error {
	nodeID := req.Node.GetId()
	typeURL := req.TypeUrl

	cp.streamsMu.Lock()
	if state, exists := cp.streams[id]; exists {
		state.nodeID = nodeID
		state.typeURL = typeURL
	}
	cp.streamsMu.Unlock()

	log.Printf("Delta stream %d received request from node %s for type %s", id, nodeID, typeURL)
	return nil
}

func (cp *ControlPlane) OnDeltaStreamResponse(id int64, req *discoveryv3.DeltaDiscoveryRequest, res *discoveryv3.DeltaDiscoveryResponse) {
	cp.streamsMu.RLock()
	if state, exists := cp.streams[id]; exists {
		state.version = res.SystemVersionInfo
	}
	cp.streamsMu.RUnlock()

	log.Printf("Delta stream %d sent response version %s", id, res.SystemVersionInfo)
}

func (cp *ControlPlane) OnDeltaStreamOpen(ctx context.Context, id int64, typeURL string) error {
	// First, acquire a lock since we'll be modifying shared state
	cp.streamsMu.Lock()
	defer cp.streamsMu.Unlock()

	// Initialize a new stream state for this delta stream
	cp.streams[id] = &streamState{
		typeURL:   typeURL,
		resources: make(map[string]types.Resource),
	}

	// Log the new delta stream connection
	log.Printf("Delta stream %d opened for type %s", id, typeURL)

	// Here you could add additional initialization like:
	// - Setting up stream-specific resources
	// - Initializing metrics
	// - Recording stream metadata

	return nil
}

// OnFetchRequest is called when an Envoy client makes a synchronous (non-streaming) request
// for configuration data. The context allows for cancellation and timeouts, while the
// DiscoveryRequest contains details about what configuration the client is requesting.
func (cp *ControlPlane) OnFetchRequest(ctx context.Context, req *discoveryv3.DiscoveryRequest) error {
	// Log the fetch request with basic information about what the client is asking for
	log.Printf("Received fetch request from node %s for resource type %s",
		req.Node.GetId(), req.TypeUrl)

	return nil
}

// OnFetchResponse is called after the server has prepared and is about to send
// a response to a fetch request. This gives us a chance to inspect or modify
// the response before it goes out.
func (cp *ControlPlane) OnFetchResponse(req *discoveryv3.DiscoveryRequest, resp *discoveryv3.DiscoveryResponse) {
	// Log information about the response being sent
	log.Printf("Sending fetch response to node %s for type %s with version %s",
		req.Node.GetId(),
		req.TypeUrl,
		resp.VersionInfo)
}

// OnStreamDeltaRequest handles incremental configuration requests from Envoy clients.
// It's called whenever an Envoy instance wants to update its configuration using the delta xDS protocol.
func (cp *ControlPlane) OnStreamDeltaRequest(streamID int64, request *discoveryv3.DeltaDiscoveryRequest) error {
	// Log the incoming delta request with useful debugging information
	log.Printf("Stream %d received delta request from node %s for type %s",
		streamID,
		request.Node.GetId(),
		request.TypeUrl)

	// Here we could track what resources the client wants to add or remove
	if len(request.ResourceNamesSubscribe) > 0 {
		log.Printf("Client subscribing to resources: %v", request.ResourceNamesSubscribe)
	}
	if len(request.ResourceNamesUnsubscribe) > 0 {
		log.Printf("Client unsubscribing from resources: %v", request.ResourceNamesUnsubscribe)
	}

	return nil
}

// OnStreamDeltaResponse is called just before the server sends a delta configuration
// response to an Envoy client. This method gives us visibility into what configuration
// changes are being sent to each client.
func (cp *ControlPlane) OnStreamDeltaResponse(streamID int64, request *discoveryv3.DeltaDiscoveryRequest, response *discoveryv3.DeltaDiscoveryResponse) {
	// Log information about what configuration updates we're sending to the client
	log.Printf("Stream %d sending delta response to node %s for type %s (version: %s)",
		streamID,
		request.Node.GetId(),
		request.TypeUrl,
		response.SystemVersionInfo)

	// We can also log details about what resources are being added or removed
	if len(response.Resources) > 0 {
		log.Printf("Sending %d updated resources", len(response.Resources))
	}
	if len(response.RemovedResources) > 0 {
		log.Printf("Removing %d resources", len(response.RemovedResources))
	}
}
