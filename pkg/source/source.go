// Package source is the SDK adapter for Flux's source-controller CRs.
// It exposes per-kind Fetcher implementations (one per source type)
// plus a content-addressed disk Cache that all fetchers write into.
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

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher resolves a single source CR into an on-disk artifact. Each
// kind (GitRepository, OCIRepository, Bucket, …) provides its own
// implementation; the source controller dispatches by id.Kind.
//
// obj is the typed manifest payload (e.g. *manifest.GitRepository).
// Implementations type-assert and return an InputError on mismatch
// rather than panic.
type Fetcher interface {
	Fetch(ctx context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error)
}

// Suspendable is satisfied by source CR types that carry a
// spec.suspend bool. The source controller short-circuits suspended
// objects before invoking the Fetcher.
type Suspendable interface {
	Suspended() bool
}

// ExistenceFetcher is the no-op Fetcher registered for kinds whose
// existence alone is enough to satisfy downstream waits — used today
// for HelmRepository (always) and OCIRepository when EnableOCI is
// false. Returning nil artifact + nil error lands the resource in
// Ready without recording a SourceArtifact, so a HelmRelease that
// dependsOn a HelmRepository unblocks instantly without flate having
// to mirror the controllers' "did fetch succeed?" logic from outside
// the controller package.
type ExistenceFetcher struct{}

func (ExistenceFetcher) Fetch(_ context.Context, _ manifest.BaseManifest) (*store.SourceArtifact, error) {
	return nil, nil
}

// SecretGetter resolves a Secret CR by namespace + name. Fetchers that
// read authentication credentials (Bucket; future GitRepository
// SecretRef; future OCIRepository SecretRef) accept one of these so
// they don't need a back-reference to the Store. The orchestrator
// wires it to Store.GetByName at construction time.
type SecretGetter func(namespace, name string) *manifest.Secret
