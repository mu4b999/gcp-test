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

// // ControlPlane manages the xDS server and configuration distribution.
type ControlPlane struct {
	gcpCache       rediscache.RedisCache  // Interface for configuration storage
	serviceWatcher watcher.ServiceWatcher // Watches for service changes
	streams        map[int64]*streamState // Tracks active xDS streams
	streamsMu      sync.RWMutex           // Protects access to streams map
	logger         *log.Logger            // Structured logging
}

// convertResources converts a map of resources to a slice of *anypb.Any.
func convertResources(resources map[string]types.Resource) []*anypb.Any {
	anyResources := make([]*anypb.Any, 0, len(resources))
	for _, resource := range resources {
		anyResource, err := anypb.New(resource)
		if err != nil {
			log.Printf("Failed to convert resource to Any: %v", err)
			continue
		}
		anyResources = append(anyResources, anyResource)
	}
	return anyResources
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
	// Create a new snapshot based on the updated services
	snapshot, err := cp.createSnapshot(services)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Push the new snapshot to all relevant Envoy proxies
	cp.streamsMu.RLock()
	defer cp.streamsMu.RUnlock()

	for _, state := range cp.streams {
		if err := cp.PushUpdate(state.nodeID, snapshot); err != nil {
			cp.logger.Printf("Failed to push update to node %s: %v", state.nodeID, err)
		}
	}

	return nil
}

// PushUpdate sends a new configuration snapshot to a specific node
func (cp *ControlPlane) PushUpdate(nodeID string, newSnapshot envoycache.ResourceSnapshot) error {
	// Instead of managing streams directly, use the cache to handle distribution
	if err := cp.gcpCache.SetSnapshot(nodeID, newSnapshot); err != nil {
		return fmt.Errorf("failed to set snapshot for node %s: %w", nodeID, err)
	}

	cp.logger.Printf("Successfully set new snapshot for node %s", nodeID)
	return nil
}

// PushUpdate sends a new configuration snapshot to all Envoy proxies for a given nodeID.
func (cp *ControlPlane) PushUpdate2(nodeID string, newSnapshot envoycache.ResourceSnapshot) error {
	cp.streamsMu.RLock()
	defer cp.streamsMu.RUnlock()

	// Iterate over all streams for the given nodeID
	for streamID, state := range cp.streams {
		if state.nodeID == nodeID {
			// Create a DiscoveryResponse from the new snapshot
			res := &discoveryv3.DiscoveryResponse{
				VersionInfo: newSnapshot.GetVersion(state.typeURL),
				TypeUrl:     state.typeURL,
				Resources:   convertResources(newSnapshot.GetResources(state.typeURL)),
			}

			// Send the response to the Envoy proxy
			if err := cp.sendResponse(streamID, res); err != nil {
				cp.logger.Printf("Failed to push update to stream %d: %v", streamID, err)
			} else {
				cp.logger.Printf("Pushed update to stream %d for node %s, version %s",
					streamID, nodeID, newSnapshot.GetVersion(state.typeURL))
			}
		}
	}

	return nil
}

// sendResponse sends a DiscoveryResponse to the Envoy proxy over the gRPC stream.
func (cp *ControlPlane) sendResponse(streamID int64, res *discoveryv3.DiscoveryResponse) error {
	cp.streamsMu.RLock()
	defer cp.streamsMu.RUnlock()

	// if state, exists := cp.streams[streamID]; exists {
	// 	if err := state.stream.Send(res); err != nil {
	// 		return fmt.Errorf("failed to send response to stream %d: %w", streamID, err)
	// 	}
	// } else {
	// 	return fmt.Errorf("stream %d not found", streamID)
	// }

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
