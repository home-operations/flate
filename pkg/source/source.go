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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher resolves a single source CR into an on-disk artifact. The
// source controller stores Fetchers in a map keyed by Kind and
// dispatches via this untyped interface. Concrete implementations
// satisfy it through Wrap[T] — see TypedFetcher.
type Fetcher interface {
	Fetch(ctx context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error)
}

// TypedFetcher is the kind-specific Fetcher each concrete source kind
// implements (e.g. TypedFetcher[*manifest.GitRepository] for git).
// The typed signature removes the per-implementation
// `obj, ok := obj.(*manifest.X)` boilerplate every fetcher
// previously opened with — a missing assertion would have panicked.
// Wrap[T] turns a TypedFetcher into the untyped Fetcher the
// source controller's map needs.
type TypedFetcher[T manifest.BaseManifest] interface {
	Fetch(ctx context.Context, obj T) (*store.SourceArtifact, error)
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

func (a typedAdapter[T]) Fetch(ctx context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error) {
	typed, ok := obj.(T)
	if !ok {
		return nil, fmt.Errorf("%w: %s fetcher: unexpected payload %T", manifest.ErrInput, a.label, obj)
	}
	return a.inner.Fetch(ctx, typed)
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

// Fetch implements source.Fetcher as a no-op — see ExistenceFetcher.
func (ExistenceFetcher) Fetch(_ context.Context, _ manifest.BaseManifest) (*store.SourceArtifact, error) {
	return nil, nil
}

// ErrUnsupportedProvider is the canonical "we only support X" error
// returned by every fetcher's provider gate. Centralizes the wording
// so a new provider added to one fetcher matches the others by default
// — and so an operator parsing logs sees one consistent shape across
// GitRepository / OCIRepository / Bucket failures.
//
// hint is a short parenthetical that names what the supported provider
// expects (e.g. "SecretRef-based credentials" or "S3-compatible").
func ErrUnsupportedProvider(kind, namespace, name, got, want, hint string) error {
	return fmt.Errorf("%s %s/%s provider %q is not implemented; flate currently supports only %q (%s)",
		kind, namespace, name, got, want, hint)
}

// CacheKeyHash marshals v to JSON, sha256-hashes the bytes, and returns
// the hex of the first n bytes — the shared cache-key fingerprint used
// by the per-kind fetchers. Exported so subpackages (git, oci) can reuse
// one implementation; each caller picks its own width (n) and decides how
// to treat a marshal error.
func CacheKeyHash(v any, n int) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:n]), nil
}

// SecretGetter resolves a Secret CR by namespace + name. Fetchers
// that read authentication, TLS, proxy, or cosign-verify material
// from a Flux spec.*SecretRef accept one of these so they don't need
// a back-reference to the Store. Today: GitRepository (auth + TLS),
// OCIRepository (auth + TLS + cosign verify), Bucket (auth + TLS).
// The orchestrator wires it to Store.GetByName at construction time.
type SecretGetter func(namespace, name string) *manifest.Secret
