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
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher is the stateless source.TypedFetcher for ExternalArtifact CRs.
// Requires status.artifact.url to be a file:// path; all other URLs are
// offline-unresolvable and return an actionable error.
type Fetcher struct{}

// Fetch implements source.TypedFetcher[*manifest.ExternalArtifact].
// Wrapped via source.Wrap at orchestrator registration.
func (*Fetcher) Fetch(ctx context.Context, ea *manifest.ExternalArtifact, _ *store.ReadSet, _ sourcecache.Policy) (*store.SourceArtifact, error) {
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
	location, err := gitrepo.Locate(ctx, localPath, localPath)
	if err != nil {
		return nil, err
	}
	input, err := gittree.AdmitDirectory(ctx, location)
	if err != nil {
		return nil, err
	}
	return &store.SourceArtifact{
		Kind:      manifest.KindExternalArtifact,
		URL:       ea.ArtifactURL,
		LocalPath: localPath,
		FS:        input.FS,
		Revision:  ea.Revision,
		Digest:    ea.Digest,
	}, nil
}
