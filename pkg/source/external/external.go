// Package external reports standalone ExternalArtifacts without accepted content.
package external

import (
	"context"
	"fmt"

	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher reports unavailable content when no materializer has published an artifact.
type Fetcher struct{}

// Fetch implements source.TypedFetcher[*manifest.ExternalArtifact].
// Wrapped via source.Wrap at orchestrator registration.
func (*Fetcher) Fetch(
	ctx context.Context,
	_ *manifest.ExternalArtifact,
	_ *store.ReadSet,
	_ *cache.Policy,
) (source.PreparedFetch, error) {
	if err := ctx.Err(); err != nil {
		return source.PreparedFetch{}, fmt.Errorf("acquire external artifact: %w", err)
	}
	return source.PreparedFetch{
		Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("acquire external artifact: %w", err)
			}
			if local != nil {
				return local, nil
			}
			return nil, manifest.NewError(manifest.ErrInput, nil, "no generated content")
		},
	}, nil
}
