// Package source defines SDK contracts and shared helpers for Flux sources.
// Per-kind fetchers acquire immutable artifacts through pkg/source/cache.
//
// Adding a new source kind:
//
//  1. Implement source.Fetcher for the new kind in a new file (or
//     subpackage) under pkg/source.
//  2. Register the fetcher with the orchestrator at construction time.
//
// The source controller (pkg/controllers/source) does not know about
// individual kinds — it dispatches via the Fetchers map keyed by
// id.Kind, so adding a new kind is one registration call.
package source

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// Artifact filenames are shared by source fetchers and chart loaders.
const (
	// OCICopiedLayerFilename is the unextracted OCI layer in an artifact FS.
	OCICopiedLayerFilename = "layer.tar.gz"
	// HelmChartArchiveFilename is the HTTP chart archive in an artifact FS.
	HelmChartArchiveFilename = "chart.tgz"
)

// Fetcher resolves a single source CR into an on-disk artifact. The
// source controller stores Fetchers in a map keyed by Kind and
// dispatches via this untyped interface. Concrete implementations
// satisfy it through Wrap[T] — see TypedFetcher.
type Fetcher interface {
	Fetch(ctx context.Context, obj manifest.BaseManifest, reads *store.ReadSet, policy sourcecache.Policy) (*store.SourceArtifact, error)
}

// TypedFetcher acquires artifacts of one manifest type with explicit freshness and retries.
type TypedFetcher[T manifest.BaseManifest] interface {
	Fetch(ctx context.Context, obj T, reads *store.ReadSet, policy sourcecache.Policy) (*store.SourceArtifact, error)
}

// Wrap converts a TypedFetcher into the untyped Fetcher interface
// used by the source-controller dispatcher map. The single
// type-assertion site is here — a mismatched payload returns
// ErrInput rather than panicking. Embeds the Kind label so the
// error message names the responsible fetcher.
func Wrap[T manifest.BaseManifest](kindLabel string, f TypedFetcher[T]) Fetcher {
	return typedAdapter[T]{label: kindLabel, inner: f}
}

type typedAdapter[T manifest.BaseManifest] struct {
	label string
	inner TypedFetcher[T]
}

func (a typedAdapter[T]) Fetch(ctx context.Context, obj manifest.BaseManifest, reads *store.ReadSet, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	typed, ok := obj.(T)
	if !ok {
		return nil, fmt.Errorf("%w: %s fetcher: unexpected payload %T", manifest.ErrInput, a.label, obj)
	}
	return a.inner.Fetch(ctx, typed, reads, policy)
}

// ErrUnsupportedProvider is the canonical "we only support X" error
// returned by every fetcher's provider gate. Centralizes the wording
// so a new provider added to one fetcher matches the others by default
// — and so an operator parsing logs sees one consistent shape across
// GitRepository / OCIRepository / Bucket failures.
//
// hint is a short parenthetical that names what the supported provider
// expects (e.g. "SecretRef-based credentials" or "S3-compatible").
func ErrUnsupportedProvider(owner manifest.NamedResource, got, want, hint string) error {
	return fmt.Errorf("%s provider %q is not implemented; flate currently supports only %q (%s)",
		QualifiedName(owner.Kind, owner.Namespace, owner.Name), got, want, hint)
}

// QualifiedName returns the canonical "<Kind> <namespace>/<name>" error identity.
// Cache slots and auth scopes use a distinct namespace-name representation.
func QualifiedName(kind, namespace, name string) string {
	return kind + " " + namespace + "/" + name
}

// SecretGetter resolves a Secret CR by namespace + name. Fetchers
// that read authentication, TLS, or proxy material from a Flux
// spec.*SecretRef accept one of these so they don't need a
// back-reference to the Store. Today: GitRepository (auth + TLS),
// OCIRepository (auth + TLS), Bucket (auth + TLS).
// The orchestrator wires it to the core Secret store lookup at construction time.
type SecretGetter func(namespace, name string) *manifest.Secret

// OperationView refreshes admitted mutable observations and retains cache acquisition identity.
func OperationView(ctx context.Context, input fs.FS) (fs.FS, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if current, ok := input.(*gittree.FS); ok {
		return gittree.New(ctx, nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: current})
	}
	return input, sourcecache.AcquisitionIdentity(input), nil
}
