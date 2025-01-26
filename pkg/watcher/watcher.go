package watcher

import (
	"context"
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
		// TODO: Implement GCP Compute Engine instance group monitoring
		ch <- []string{"service1", "service2"}
	}()
	return ch
}
