package source

import (
	"context"
	"fmt"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// ExternalArtifactFetcher resolves an ExternalArtifact CR into a
// SourceArtifact. Because ExternalArtifact is the contract a
// third-party controller uses to publish content into a live cluster,
// flate (which runs offline) has no general way to fetch from the
// upstream URL. Two modes are supported:
//
//  1. The CR's status.artifact has been pre-populated in the YAML with
//     a `file://` URL. flate trusts the local path and surfaces it
//     verbatim as a SourceArtifact so downstream Kustomizations or
//     HelmReleases referencing the artifact can resolve.
//
//  2. status.artifact is unset or its URL is not file://. flate cannot
//     resolve the content; the fetcher returns an error. Any consumer
//     that references this ExternalArtifact will fail-loud with a
//     "source artifact not found" message — preferable to silently
//     emitting empty output.
type ExternalArtifactFetcher struct{}

// Fetch implements source.Fetcher for *manifest.ExternalArtifact.
func (f *ExternalArtifactFetcher) Fetch(_ context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error) {
	ea, ok := obj.(*manifest.ExternalArtifact)
	if !ok {
		return nil, fmt.Errorf("%w: ExternalArtifactFetcher: unexpected payload %T", manifest.ErrInput, obj)
	}
	if ea.ArtifactURL == "" {
		return nil, fmt.Errorf(
			"ExternalArtifact %s/%s requires status.artifact to be populated for offline use — "+
				"flate cannot fetch from the live source-controller. Pre-fill status.artifact with a file:// URL "+
				"or suspend the resource",
			ea.Namespace, ea.Name,
		)
	}
	localPath := ""
	if rest, ok := strings.CutPrefix(ea.ArtifactURL, "file://"); ok {
		localPath = rest
	}
	if localPath == "" {
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
