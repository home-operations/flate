// Package oci implements the source.Fetcher for KindOCIRepository
// via oras-go. Generic provider only — IRSA / Workload Identity is
// out of scope for offline flate.
//
// File map:
//
//	fetcher.go  — Fetcher type, Fetch entry, authIdentity
//	fetch.go    — fetch workhorse, cache-hit gate, artifact composer
//	auth.go     — TLS, registry-config, credential-store resolution
//	resolve.go  — OCI ref parsing, semver tag picking, revision shape
//	marker.go   — .flate-digest / .flate-verified slot markers
//	cosign.go   — cosign signature verification
//	layer.go    — spec.layerSelector copy/extract
package oci

import (
	"context"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher is the Fetcher implementation for KindOCIRepository.
// RegistryConfig is the global --registry-config docker-style
// config.json path used when no per-repo SecretRef is set. Secrets is
// the per-repo source.SecretGetter (typically the orchestrator-provided
// Store.GetByName), required when any OCIRepository has spec.secretRef
// pointing at a kubernetes.io/dockerconfigjson Secret.
type Fetcher struct {
	Cache          *source.Cache
	RegistryConfig string
	Secrets        source.SecretGetter
}

// Fetch implements source.TypedFetcher[*manifest.OCIRepository].
// The typed signature is wrapped via source.Wrap at orchestrator
// registration — a payload mismatch returns ErrInput once at the
// adapter site rather than panicking here.
//
// Fetch resolves credentials, TLS, and proxy from the CR's *SecretRef
// fields, then hands off to fetch() — the workhorse in fetch.go that
// owns slot lifecycle, oras Copy, cosign verification, layer
// extraction, and marker writes.
func (f *Fetcher) Fetch(ctx context.Context, repo *manifest.OCIRepository) (*store.SourceArtifact, error) {
	if repo.Provider != "" && repo.Provider != sourcev1.GenericOCIProvider {
		return nil, source.ErrUnsupportedProvider("OCIRepository",
			repo.Namespace, repo.Name, repo.Provider, sourcev1.GenericOCIProvider,
			"SecretRef or --registry-config credentials")
	}
	configPath, cleanup, err := f.resolveRegistryConfig(repo)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	tlsCfg, err := f.resolveTLS(repo)
	if err != nil {
		return nil, err
	}
	proxy, err := source.ResolveProxy(f.Secrets, repo.Namespace, "OCIRepository",
		repo.Namespace+"/"+repo.Name, repo.ProxySecretRef)
	if err != nil {
		return nil, err
	}
	return fetch(ctx, f, repo, configPath, tlsCfg, proxy)
}

// authIdentity returns the cache-key auth tag for an OCIRepository.
// Combines SecretRef (registry creds), CertSecretRef (TLS material),
// ProxySecretRef, and verify.SecretRef (cosign keys) since each can
// change which artifact bytes a reconcile resolves to.
func authIdentity(repo *manifest.OCIRepository) string {
	var verifyRef *manifest.LocalObjectReference
	if repo.Verify != nil {
		verifyRef = repo.Verify.SecretRef
	}
	return source.AuthIdentityFromRefs(repo.Namespace,
		repo.SecretRef, repo.CertSecretRef, repo.ProxySecretRef, verifyRef)
}
