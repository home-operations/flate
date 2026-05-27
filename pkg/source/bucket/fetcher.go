// Package bucket implements the source.Fetcher for KindBucket
// (S3-compatible object storage via minio-go).
package bucket

import (
	"context"
	"fmt"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/minio/minio-go/v7"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher pulls a Flux Bucket CR into the on-disk cache. Only the
// "generic" provider (any S3-API-compatible storage) is wired up
// today via minio-go. The aws/gcp/azure providers parse and route
// here but return a clear "not implemented" error rather than silently
// falling back.
type Fetcher struct {
	Cache   *source.Cache
	Secrets source.SecretGetter
}

// Fetch implements source.TypedFetcher[*manifest.Bucket]. The typed
// signature is wrapped via source.Wrap at orchestrator registration.
func (f *Fetcher) Fetch(ctx context.Context, b *manifest.Bucket) (*store.SourceArtifact, error) {
	if b.Provider != "" && b.Provider != sourcev1.BucketProviderGeneric {
		return nil, source.ErrUnsupportedProvider("Bucket",
			b.Namespace, b.Name, b.Provider, sourcev1.BucketProviderGeneric,
			"S3-compatible")
	}

	creds, err := f.resolveCredentials(b)
	if err != nil {
		return nil, err
	}

	endpoint, secure, err := normalizeEndpoint(b.Endpoint, b.Insecure)
	if err != nil {
		return nil, err
	}

	transport, err := f.resolveTransport(b)
	if err != nil {
		return nil, err
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:     creds,
		Secure:    secure,
		Region:    b.Region,
		Transport: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("bucket %s/%s: minio client: %w", b.Namespace, b.Name, err)
	}

	// Cache key: bucket+prefix. Unlike OCI / Git, the bucket fetcher
	// has no "is the cached slot still valid?" check — walkBucket is
	// write-only, so any file present in the slot from a previous run
	// that was DELETED upstream between runs would be served as a
	// ghost file. The revision hash is computed from the upstream
	// listing, so it correctly reflects "no change" while the slot
	// still holds the dead file. We treat every fetch as a miss:
	// write to a fresh staging dir, then atomic-rename into the final
	// slot on success.
	slot, err := f.Cache.Slot(ctx, endpoint+"/"+b.BucketName, b.Prefix, authIdentity(b))
	if err != nil {
		return nil, fmt.Errorf("bucket %s/%s cache slot: %w", b.Namespace, b.Name, err)
	}
	defer slot.Release()
	if slot.Exists {
		// Drop the prior committed slot and re-stage so the upcoming
		// walk writes into an empty staging dir.
		if err := slot.Refresh(); err != nil {
			return nil, fmt.Errorf("bucket %s/%s refresh: %w", b.Namespace, b.Name, err)
		}
	}

	keys, revHash, err := walkBucket(ctx, client, b.BucketName, b.Prefix, slot.Path)
	if err != nil {
		return nil, fmt.Errorf("bucket %s/%s walk: %w", b.Namespace, b.Name, err)
	}
	// Bucket uses the no-defaults ignore variant — matches upstream
	// source-controller's bucket_controller.go, which constructs an
	// ignore Matcher without VCS / extension defaults. Buckets are
	// object stores with no VCS semantics; .jpg / .flux.yaml / etc.
	// are legitimate content and must reach the artifact.
	if err := source.ApplyIgnoreNoDefaults(slot.Path, b.Ignore); err != nil {
		return nil, fmt.Errorf("bucket %s/%s: %w", b.Namespace, b.Name, err)
	}
	if err := slot.Commit(); err != nil {
		return nil, fmt.Errorf("bucket %s/%s: commit slot: %w", b.Namespace, b.Name, err)
	}

	return &store.SourceArtifact{
		Kind:      manifest.KindBucket,
		URL:       fmt.Sprintf("%s://%s/%s", schemeFor(secure), endpoint, b.BucketName),
		LocalPath: slot.Path,
		Revision:  revHash,
		Metadata: map[string]string{
			"objectCount": fmt.Sprintf("%d", len(keys)),
		},
	}, nil
}

// authIdentity returns the cache-key auth tag for a Bucket. Combines
// SecretRef (S3 access creds), CertSecretRef (TLS), and
// ProxySecretRef. Returns "" when all are unset.
func authIdentity(b *manifest.Bucket) string {
	return source.AuthIdentityFromRefs(b.Namespace,
		b.SecretRef, b.CertSecretRef, b.ProxySecretRef)
}
