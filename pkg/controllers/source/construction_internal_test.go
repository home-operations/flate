package source

import (
	"context"

	"github.com/home-operations/flate/pkg/discovery"

	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/task"
)

//nolint:gocritic // Test admission MUST preserve the caller configuration snapshot.
func runDiscovery(ctx context.Context, cfg discovery.Config) (*discovery.Result, error) {
	session, err := discovery.Begin(ctx, cfg)
	if err != nil {
		return nil, err
	}
	tasks := task.NewBounded(2)
	controller := New(cfg.Store, tasks, nil)
	controller.Configure(FetchOptions{Admission: session.Admission()}, cache.RetryConfig{})
	for {
		pending := session.PendingSources()
		failures := make([]error, len(pending))
		for i, id := range pending {
			tasks.Go(ctx, "construct source", func(ctx context.Context) { failures[i] = controller.Construct(ctx, id) })
		}
		tasks.BlockTillDone()
		for _, err := range failures {
			if err != nil {
				return nil, err
			}
		}
		result, err := session.Run(ctx)
		if result != nil || err != nil {
			return result, err
		}
	}
}
