package watcher

import (
	"context"
	"log"
	"time"
)

type ServiceWatcher interface {
	WatchServices(ctx context.Context, interval time.Duration) <-chan []string
}

type GCPServiceWatcher struct {
	projectID string
}

func NewServiceWatcher(ctx context.Context, projectID string) *GCPServiceWatcher {
	return &GCPServiceWatcher{
		projectID: projectID,
	}
}

func (w *GCPServiceWatcher) WatchServices(ctx context.Context, interval time.Duration) <-chan []string {
	ch := make(chan []string)

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

func (w *GCPServiceWatcher) fetchServices(context.Context) ([]string, error) {
	// For now, return mock services. In production, you would:
	// 1. Query GCP APIs to get service list
	// 2. Filter based on criteria
	// 3. Transform into the required format
	return []string{"service1", "service2"}, nil
}
