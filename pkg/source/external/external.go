// Package external reports standalone ExternalArtifacts without accepted content.
package external

import (
	"context"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher reports unavailable content when no materializer has published an artifact.
type Fetcher struct{}

// Fetch implements source.TypedFetcher[*manifest.ExternalArtifact].
// Wrapped via source.Wrap at orchestrator registration.
func (*Fetcher) Fetch(_ context.Context, ea *manifest.ExternalArtifact, _ *store.ReadSet, _ cache.Policy) (*store.SourceArtifact, error) {
	return nil, fmt.Errorf("%w: ExternalArtifact %s has no generated content", manifest.ErrInput, ea.Named())
}
