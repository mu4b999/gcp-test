package controlplane

import (
	"context"
	"fmt"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// streamState tracks the state of an active xDS stream.
type streamState struct {
	nodeID  string // Node ID of the Envoy proxy
	typeURL string // Type URL of the resource being requested
	version string // Version of the last sent configuration
	// stream  discoveryv3.AggregatedDiscoveryService_StreamAggregatedResourcesServer // gRPC stream
}

// OnStreamOpen is called when a new xDS stream is opened.
func (cp *ControlPlane) OnStreamOpen(ctx context.Context, id int64, typeURL string) error {
	cp.streamsMu.Lock()
	defer cp.streamsMu.Unlock()

	// Initialize the stream state with just the typeURL
	cp.streams[id] = &streamState{
		typeURL: typeURL,
	}

	cp.logger.Printf("Stream %d opened for %s", id, typeURL)
	return nil
}

// OnStreamClosed is called when an xDS stream is closed.
func (cp *ControlPlane) OnStreamClosed(id int64, node *core.Node) {
	cp.streamsMu.Lock()
	delete(cp.streams, id)
	cp.streamsMu.Unlock()

	if node != nil {
		cp.logger.Printf("Stream %d closed for node %s", id, node.GetId())
	} else {
		cp.logger.Printf("Stream %d closed for unknown node", id)
	}
}

// OnStreamRequest is called when a DiscoveryRequest is received on an xDS stream.
func (cp *ControlPlane) OnStreamRequest(id int64, req *discoveryv3.DiscoveryRequest) error {
	nodeID := req.Node.GetId()
	if nodeID == "" {
		return fmt.Errorf("missing node ID in request")
	}

	cp.streamsMu.Lock()
	if state, exists := cp.streams[id]; exists {
		state.nodeID = nodeID
		state.typeURL = req.TypeUrl
		// Remove the stream assignment from here
	}
	cp.streamsMu.Unlock()

	// Check if a snapshot exists for the node
	_, err := cp.gcpCache.GetSnapshot(nodeID)
	if err != nil {
		// If no snapshot exists, create an initial one
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

// OnStreamResponse is called when a DiscoveryResponse is sent on an xDS stream.
func (cp *ControlPlane) OnStreamResponse(ctx context.Context, id int64, req *discoveryv3.DiscoveryRequest, res *discoveryv3.DiscoveryResponse) {
	cp.streamsMu.RLock()
	if state, exists := cp.streams[id]; exists {
		state.version = res.VersionInfo
	}
	cp.streamsMu.RUnlock()

	cp.logger.Printf("Stream %d sent response version %s", id, res.VersionInfo)
}

// OnFetchRequest is called when a Fetch request is received.
func (cp *ControlPlane) OnFetchRequest(ctx context.Context, req *discoveryv3.DiscoveryRequest) error {
	nodeID := req.Node.GetId()
	if nodeID == "" {
		return fmt.Errorf("missing node ID in fetch request")
	}

	cp.logger.Printf("Received fetch request from node %s for type %s (version: %s)",
		nodeID, req.TypeUrl, req.VersionInfo)

	// Check if a snapshot exists for the node
	_, err := cp.gcpCache.GetSnapshot(nodeID)
	if err != nil {
		// If no snapshot exists, create an initial one
		newSnapshot, err := cp.createSnapshot(nil)
		if err != nil {
			return fmt.Errorf("failed to create initial snapshot: %w", err)
		}

		if err := cp.gcpCache.SetSnapshot(nodeID, newSnapshot); err != nil {
			return fmt.Errorf("failed to set initial snapshot: %w", err)
		}
	}

	return nil
}

func (cp *ControlPlane) OnDeltaStreamOpen(ctx context.Context, id int64, typeURL string) error {
	cp.streamsMu.Lock()
	defer cp.streamsMu.Unlock()

	// Initialize the stream state with the typeURL
	cp.streams[id] = &streamState{
		typeURL: typeURL,
	}

	cp.logger.Printf("Delta stream %d opened for type %s", id, typeURL)
	return nil
}

// // OnDeltaStreamClosed is called when a delta xDS stream is closed.
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

// // OnStreamDeltaRequest is called when a DeltaDiscoveryRequest is received on a delta xDS stream.
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

// // OnStreamDeltaResponse is called when a DeltaDiscoveryResponse is sent on a delta xDS stream.
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

// OnFetchResponse is called when a Fetch response is sent.
func (cp *ControlPlane) OnFetchResponse(req *discoveryv3.DiscoveryRequest, resp *discoveryv3.DiscoveryResponse) {
	cp.logger.Printf("Sending fetch response to node %s for type %s (version: %s, resources: %d)",
		req.Node.GetId(),
		req.TypeUrl,
		resp.VersionInfo,
		len(resp.Resources))
}
