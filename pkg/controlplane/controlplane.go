// Package controlplane provides a complete implementation of an Envoy xDS control plane
// with support for dynamic service discovery and configuration updates.
package controlplane

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	envoycache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	rediscache "github.com/mu4b999/gcp-test/pkg/cache"
	"github.com/mu4b999/gcp-test/pkg/watcher"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ControlPlane manages the xDS server and configuration distribution.
type ControlPlane struct {
	gcpCache       rediscache.RedisCache  // Interface for configuration storage
	serviceWatcher watcher.ServiceWatcher // Watches for service changes
	streams        map[int64]*streamState // Tracks active xDS streams
	streamsMu      sync.RWMutex           // Protects access to streams map
	logger         *log.Logger            // Structured logging
}

// streamState tracks the state of individual xDS streams.
type streamState struct {
	nodeID    string
	typeURL   string
	version   string
	resources map[string]types.Resource
}

// NewControlPlane creates a new control plane instance with proper initialization.
func NewControlPlane(cache rediscache.RedisCache, watcher watcher.ServiceWatcher) *ControlPlane {
	cp := &ControlPlane{
		gcpCache:       cache,
		serviceWatcher: watcher,
		streams:        make(map[int64]*streamState),
		logger:         log.New(os.Stdout, "[control-plane] ", log.LstdFlags),
	}

	// Start service watching in a goroutine with proper context
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		cp.watchServices(ctx)
		cancel() // Ensure context is canceled when watchServices exits
	}()

	return cp
}

// watchServices continuously monitors for service updates and triggers configuration updates.
func (cp *ControlPlane) watchServices(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			cp.logger.Println("Service watching stopped")
			return
		case services := <-cp.serviceWatcher.WatchServices(ctx, 30*time.Second):
			if err := cp.handleServiceUpdate(ctx, services); err != nil {
				cp.logger.Printf("Error handling service update: %v", err)
			}
		case <-ticker.C:
			// Periodic health check or cleanup could go here
		}
	}
}

// handleServiceUpdate processes service updates and updates configurations accordingly.
func (cp *ControlPlane) handleServiceUpdate(ctx context.Context, services []watcher.ServiceDiscovery) error {
	snapshot, err := cp.createSnapshot(services)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	cp.streamsMu.RLock()
	defer cp.streamsMu.RUnlock()

	for _, state := range cp.streams {
		if err := cp.gcpCache.SetSnapshot(state.nodeID, snapshot); err != nil {
			cp.logger.Printf("Failed to update snapshot for node %s: %v", state.nodeID, err)
		}
	}

	return nil
}

// createSnapshot generates a new configuration snapshot based on discovered services.
func (cp *ControlPlane) createSnapshot(services []watcher.ServiceDiscovery) (*envoycache.Snapshot, error) {
	version := fmt.Sprintf("%d", time.Now().UnixNano())

	clusters := make([]types.Resource, 0)
	endpoints := make([]types.Resource, 0)

	// Create cluster and endpoint configurations for each service
	for _, service := range services {
		// Create cluster configuration
		cluster := &cluster.Cluster{
			Name:           service.ServiceName,
			ConnectTimeout: durationpb.New(5 * time.Second),
			ClusterDiscoveryType: &cluster.Cluster_Type{
				Type: cluster.Cluster_EDS,
			},
			EdsClusterConfig: &cluster.Cluster_EdsClusterConfig{
				EdsConfig: &core.ConfigSource{
					ConfigSourceSpecifier: &core.ConfigSource_Ads{},
				},
			},
			LbPolicy: cluster.Cluster_ROUND_ROBIN,
		}
		clusters = append(clusters, cluster)

		// Create endpoint configuration
		loadAssignment := &endpoint.ClusterLoadAssignment{
			ClusterName: service.ServiceName,
			Endpoints: []*endpoint.LocalityLbEndpoints{
				{
					LbEndpoints: make([]*endpoint.LbEndpoint, 0, len(service.Endpoints)),
				},
			},
		}

		for _, ep := range service.Endpoints {
			lbEndpoint := &endpoint.LbEndpoint{
				HostIdentifier: &endpoint.LbEndpoint_Endpoint{
					Endpoint: &endpoint.Endpoint{
						Address: &core.Address{
							Address: &core.Address_SocketAddress{
								SocketAddress: &core.SocketAddress{
									Address: ep.IP,
									PortSpecifier: &core.SocketAddress_PortValue{
										PortValue: ep.Port,
									},
								},
							},
						},
					},
				},
			}
			loadAssignment.Endpoints[0].LbEndpoints = append(
				loadAssignment.Endpoints[0].LbEndpoints,
				lbEndpoint,
			)
		}

		endpoints = append(endpoints, loadAssignment)
	}

	// Create the snapshot with all configurations
	snapshot, err := envoycache.NewSnapshot(
		version,
		map[resource.Type][]types.Resource{
			resource.ClusterType:  clusters,
			resource.EndpointType: endpoints,
			resource.RouteType:    cp.createRoutes(services),
			resource.ListenerType: cp.createListeners(services),
		},
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	if err := snapshot.Consistent(); err != nil {
		return nil, fmt.Errorf("inconsistent snapshot: %w", err)
	}

	return snapshot, nil
}

// createRoutes generates route configurations for all services.
func (cp *ControlPlane) createRoutes(services []watcher.ServiceDiscovery) []types.Resource {
	routeConfig := &route.RouteConfiguration{
		Name: "main_route_config",
		VirtualHosts: []*route.VirtualHost{
			{
				Name:    "all_services",
				Domains: []string{"*"},
				Routes:  make([]*route.Route, 0, len(services)),
			},
		},
	}

	for _, service := range services {
		route := &route.Route{
			Match: &route.RouteMatch{
				PathSpecifier: &route.RouteMatch_Prefix{
					Prefix: fmt.Sprintf("/%s/", service.ServiceName),
				},
			},
			Action: &route.Route_Route{
				Route: &route.RouteAction{
					ClusterSpecifier: &route.RouteAction_Cluster{
						Cluster: service.ServiceName,
					},
					Timeout: durationpb.New(15 * time.Second),
					RetryPolicy: &route.RetryPolicy{
						RetryOn:    "connect-failure,refused-stream,unavailable",
						NumRetries: &wrapperspb.UInt32Value{Value: 3},
						RetryBackOff: &route.RetryPolicy_RetryBackOff{
							BaseInterval: durationpb.New(100 * time.Millisecond),
							MaxInterval:  durationpb.New(1 * time.Second),
						},
					},
				},
			},
		}
		routeConfig.VirtualHosts[0].Routes = append(routeConfig.VirtualHosts[0].Routes, route)
	}

	return []types.Resource{routeConfig}
}

// createListeners generates listener configurations.
func (cp *ControlPlane) createListeners(services []watcher.ServiceDiscovery) []types.Resource {
	manager := &hcm.HttpConnectionManager{
		CodecType:  hcm.HttpConnectionManager_AUTO,
		StatPrefix: "ingress_http",
		RouteSpecifier: &hcm.HttpConnectionManager_Rds{
			Rds: &hcm.Rds{
				ConfigSource: &core.ConfigSource{
					ConfigSourceSpecifier: &core.ConfigSource_Ads{},
				},
				RouteConfigName: "main_route_config",
			},
		},
		HttpFilters: []*hcm.HttpFilter{
			{
				Name: "envoy.filters.http.router",
				ConfigType: &hcm.HttpFilter_TypedConfig{
					TypedConfig: mustMarshalAny(&router.Router{}),
				},
			},
		},
		CommonHttpProtocolOptions: &core.HttpProtocolOptions{
			IdleTimeout: durationpb.New(5 * time.Minute),
		},
	}

	pbst, err := anypb.New(manager)
	if err != nil {
		cp.logger.Printf("Failed to marshal HTTP connection manager: %v", err)
		return nil
	}

	listener := &listener.Listener{
		Name: "main_listener",
		Address: &core.Address{
			Address: &core.Address_SocketAddress{
				SocketAddress: &core.SocketAddress{
					Address: "0.0.0.0",
					PortSpecifier: &core.SocketAddress_PortValue{
						PortValue: 80,
					},
				},
			},
		},
		FilterChains: []*listener.FilterChain{
			{
				Filters: []*listener.Filter{
					{
						Name: "envoy.filters.network.http_connection_manager",
						ConfigType: &listener.Filter_TypedConfig{
							TypedConfig: pbst,
						},
					},
				},
			},
		},
	}

	return []types.Resource{listener}
}

// Helper function to marshal protobuf messages to Any
func mustMarshalAny(message interface{}) *anypb.Any {
	any, err := anypb.New(message.(*router.Router))
	if err != nil {
		panic(fmt.Sprintf("failed to marshal message to Any: %v", err))
	}
	return any
}

// Implement server.Callbacks interface methods

func (cp *ControlPlane) OnStreamOpen(ctx context.Context, id int64, typeURL string) error {
	cp.streamsMu.Lock()
	defer cp.streamsMu.Unlock()

	cp.streams[id] = &streamState{
		typeURL:   typeURL,
		resources: make(map[string]types.Resource),
	}

	cp.logger.Printf("Stream %d opened for %s", id, typeURL)
	return nil
}

func (cp *ControlPlane) OnStreamClosed(id int64, node *core.Node) {
	cp.streamsMu.Lock()
	delete(cp.streams, id)
	cp.streamsMu.Unlock()

	cp.logger.Printf("Stream %d closed for node %s", id, node.GetId())
}

func (cp *ControlPlane) OnStreamRequest(id int64, req *discoveryv3.DiscoveryRequest) error {
	nodeID := req.Node.GetId()
	if nodeID == "" {
		return fmt.Errorf("missing node ID in request")
	}

	cp.streamsMu.Lock()
	if state, exists := cp.streams[id]; exists {
		state.nodeID = nodeID
		state.typeURL = req.TypeUrl
	}
	cp.streamsMu.Unlock()

	_, err := cp.gcpCache.GetSnapshot(nodeID)
	if err != nil {
		newSnapshot, err := cp.createSnapshot(nil)
		if err != nil {
			return fmt.Errorf("failed to create initial snapshot: %w", err)
		}

		if err := cp.gcpCache.SetSnapshot(nodeID, newSnapshot); err != nil {
			return fmt.Errorf("failed to set initial snapshot: %w", err)
		}
	}

	cp.logger.Printf("Stream %d request processed for node %s, type %s",
		id, nodeID, req.TypeUrl)
	return nil
}

func (cp *ControlPlane) OnStreamResponse(ctx context.Context, id int64, req *discoveryv3.DiscoveryRequest, res *discoveryv3.DiscoveryResponse) {
	cp.streamsMu.RLock()
	if state, exists := cp.streams[id]; exists {
		state.version = res.VersionInfo
	}
	cp.streamsMu.RUnlock()

	cp.logger.Printf("Stream %d sent response version %s", id, res.VersionInfo)
}

func (cp *ControlPlane) OnDeltaStreamOpen(ctx context.Context, id int64, typeURL string) error {
	cp.streamsMu.Lock()
	defer cp.streamsMu.Unlock()

	cp.streams[id] = &streamState{
		typeURL:   typeURL,
		resources: make(map[string]types.Resource),
	}

	cp.logger.Printf("Delta stream %d opened for type %s", id, typeURL)
	return nil
}

func (cp *ControlPlane) OnDeltaStreamClosed(id int64, node *core.Node) {
	cp.streamsMu.Lock()
	delete(cp.streams, id)
	cp.streamsMu.Unlock()

	if node != nil {
		cp.logger.Printf("Delta stream %d closed for node %s", id, node.GetId())
	} else {
		cp.logger.Printf("Delta stream %d closed for unknown node", id)
	}
}

func (cp *ControlPlane) OnStreamDeltaRequest(streamID int64, req *discoveryv3.DeltaDiscoveryRequest) error {
	cp.logger.Printf("Stream %d received delta request from node %s for type %s",
		streamID, req.Node.GetId(), req.TypeUrl)

	if len(req.ResourceNamesSubscribe) > 0 {
		cp.logger.Printf("Client subscribing to resources: %v", req.ResourceNamesSubscribe)
	}
	if len(req.ResourceNamesUnsubscribe) > 0 {
		cp.logger.Printf("Client unsubscribing from resources: %v", req.ResourceNamesUnsubscribe)
	}

	// Update stream state with the latest information
	cp.streamsMu.Lock()
	if state, exists := cp.streams[streamID]; exists {
		state.nodeID = req.Node.GetId()
		state.typeURL = req.TypeUrl
	}
	cp.streamsMu.Unlock()

	return nil
}

func (cp *ControlPlane) OnStreamDeltaResponse(streamID int64, req *discoveryv3.DeltaDiscoveryRequest, res *discoveryv3.DeltaDiscoveryResponse) {
	cp.logger.Printf("Stream %d sending delta response to node %s for type %s (version: %s)",
		streamID, req.Node.GetId(), req.TypeUrl, res.SystemVersionInfo)

	// Log resource changes
	if len(res.Resources) > 0 {
		cp.logger.Printf("Sending %d updated resources", len(res.Resources))
	}
	if len(res.RemovedResources) > 0 {
		cp.logger.Printf("Removing %d resources", len(res.RemovedResources))
	}

	// Update stream version information
	cp.streamsMu.RLock()
	if state, exists := cp.streams[streamID]; exists {
		state.version = res.SystemVersionInfo
	}
	cp.streamsMu.RUnlock()
}

func (cp *ControlPlane) OnFetchRequest(ctx context.Context, req *discoveryv3.DiscoveryRequest) error {
	if req.Node == nil || req.Node.GetId() == "" {
		return fmt.Errorf("missing or invalid node in fetch request")
	}

	cp.logger.Printf("Received fetch request from node %s for type %s (version: %s)",
		req.Node.GetId(), req.TypeUrl, req.VersionInfo)

	// Check if we have a snapshot for this node
	_, err := cp.gcpCache.GetSnapshot(req.Node.GetId())
	if err != nil {
		cp.logger.Printf("No existing snapshot for node %s, creating initial configuration",
			req.Node.GetId())
	}

	return nil
}

func (cp *ControlPlane) OnFetchResponse(req *discoveryv3.DiscoveryRequest, resp *discoveryv3.DiscoveryResponse) {
	cp.logger.Printf("Sending fetch response to node %s for type %s (version: %s, resources: %d)",
		req.Node.GetId(),
		req.TypeUrl,
		resp.VersionInfo,
		len(resp.Resources))
}

// Cleanup performs necessary cleanup when shutting down the control plane
func (cp *ControlPlane) Cleanup(ctx context.Context) error {
	cp.logger.Println("Starting control plane cleanup")

	// Clean up all active streams
	cp.streamsMu.Lock()
	cp.streams = make(map[int64]*streamState)
	cp.streamsMu.Unlock()

	// Additional cleanup tasks can be added here
	// For example:
	// - Close connections
	// - Flush caches
	// - Save state

	cp.logger.Println("Control plane cleanup completed")
	return nil
}

// Helper method to validate node information
func (cp *ControlPlane) validateNode(node *core.Node) error {
	if node == nil {
		return fmt.Errorf("nil node")
	}
	if node.GetId() == "" {
		return fmt.Errorf("empty node ID")
	}
	return nil
}
