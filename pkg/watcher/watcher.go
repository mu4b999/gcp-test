package watcher

import (
	"context"
	"log"
	"time"
)

type ServiceEndpoint struct {
	IP       string
	Port     uint32
	Metadata map[string]string
}

type ServiceDiscovery struct {
	ServiceName string
	Endpoints   []ServiceEndpoint
}

type ServiceWatcher interface {
	WatchServices(ctx context.Context, interval time.Duration) <-chan []ServiceDiscovery
}

type GCPServiceWatcher struct {
	projectID string
}

func NewServiceWatcher(ctx context.Context, projectID string) *GCPServiceWatcher {
	return &GCPServiceWatcher{
		projectID: projectID,
	}
}

func (w *GCPServiceWatcher) WatchServices(ctx context.Context, interval time.Duration) <-chan []ServiceDiscovery {
	ch := make(chan []ServiceDiscovery)

	go func() {
		defer close(ch)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				services, err := w.fetchServices(ctx)
				if err != nil {
					log.Printf("Error fetching services: %v", err)
					continue
				}
				ch <- services
			}
		}
	}()

	return ch
}

func (w *GCPServiceWatcher) fetchServices(ctx context.Context) ([]ServiceDiscovery, error) {
	// Here you would actually query your infrastructure (e.g., GCP API)
	// to get service instances and their endpoints

	// Example of what real discovery might return:
	services := []ServiceDiscovery{
		{
			ServiceName: "payment-service",
			Endpoints: []ServiceEndpoint{
				{IP: "10.0.1.1", Port: 8080},
				{IP: "10.0.1.2", Port: 8080},
			},
		},
		{
			ServiceName: "auth-service",
			Endpoints: []ServiceEndpoint{
				{IP: "10.0.2.1", Port: 9090},
				{IP: "10.0.2.2", Port: 9090},
			},
		},
	}
	return services, nil
}
