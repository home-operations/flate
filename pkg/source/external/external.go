// Package external implements source.TypedFetcher for ExternalArtifact.
// ExternalArtifact is published by a third-party controller into a live
// cluster; flate runs offline and cannot reach that controller. Only
// status.artifact URLs pre-filled with a file:// scheme are resolvable.
package external

import (
	"context"
	"fmt"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher is the stateless source.TypedFetcher for ExternalArtifact CRs.
// Requires status.artifact.url to be a file:// path; all other URLs are
// offline-unresolvable and return an actionable error.
type Fetcher struct{}

// Fetch implements source.TypedFetcher[*manifest.ExternalArtifact].
// Wrapped via source.Wrap at orchestrator registration.
func (*Fetcher) Fetch(_ context.Context, ea *manifest.ExternalArtifact) (*store.SourceArtifact, error) {
	if ea.ArtifactURL == "" {
		return nil, fmt.Errorf(
			"ExternalArtifact %s/%s requires status.artifact to be populated for offline use — "+
				"flate cannot fetch from the live source-controller. Pre-fill status.artifact with a file:// URL "+
				"or suspend the resource",
			ea.Namespace, ea.Name,
		)
	}
	localPath, ok := strings.CutPrefix(ea.ArtifactURL, "file://")
	if !ok {
		return nil, fmt.Errorf(
			"ExternalArtifact %s/%s status.artifact.url %q is not a file:// URL — flate can only resolve local artifacts offline",
			ea.Namespace, ea.Name, ea.ArtifactURL,
		)
	}
	return &store.SourceArtifact{
		Kind:      manifest.KindExternalArtifact,
		URL:       ea.ArtifactURL,
		LocalPath: localPath,
		Revision:  ea.Revision,
		Digest:    ea.Digest,
	}, nil
}
