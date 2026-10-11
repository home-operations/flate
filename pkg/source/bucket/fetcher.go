// Package bucket implements the source.Fetcher for KindBucket
// (S3-compatible object storage via minio-go).
package bucket

import (
	"context"
	"fmt"
	"io"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/minio/minio-go/v7"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher acquires a Flux Bucket into an immutable filesystem. Only the
// "generic" provider (any S3-API-compatible storage) is wired up
// today via minio-go. The aws/gcp/azure providers parse and route
// here but return a clear "not implemented" error rather than silently
// falling back, unless ForceGeneric sends them down the generic path.
type Fetcher struct {
	Cache   *cache.Cache
	Secrets source.SecretGetter

	// ForceGeneric skips the provider gate; the generic path then needs
	// static credentials (a SecretRef) or it fails.
	ForceGeneric bool
}

// Fetch implements source.TypedFetcher[*manifest.Bucket]. The typed
// signature is wrapped via source.Wrap at orchestrator registration.
func (f *Fetcher) Fetch(ctx context.Context, b *manifest.Bucket) (*store.SourceArtifact, error) {
	if !f.ForceGeneric && b.Provider != "" && b.Provider != sourcev1.BucketProviderGeneric {
		return nil, source.ErrUnsupportedProvider("Bucket",
			b.Namespace, b.Name, b.Provider, sourcev1.BucketProviderGeneric,
			"S3-compatible")
	}

	client, endpoint, secure, err := f.newMinioClient(b)
	if err != nil {
		return nil, err
	}

	ignore := ""
	if b.Ignore != nil {
		ignore = *b.Ignore
	}
	h, err := source.CacheKeyHash([]any{endpoint, secure, b.BucketName, b.Prefix, b.Region, authIdentity(b), ignore, sourceignore.RulesVersion}, 32)
	if err != nil {
		return nil, err
	}
	key := cache.ArchiveKey{ID: "source-bucket-" + h, Ignore: ignore}
	input, desc, err := f.Cache.Acquire(ctx, key, b.Interval.Duration, func(w io.Writer) (ocispec.Descriptor, error) {
		return walkBucket(ctx, client, b, w)
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", bucketID(b), err)
	}

	return &store.SourceArtifact{
		Kind:     manifest.KindBucket,
		URL:      fmt.Sprintf("%s://%s/%s", schemeFor(secure), endpoint, b.BucketName),
		FS:       input,
		Key:      key.ID + "@" + desc.Annotations["flate.revision"],
		Revision: desc.Annotations["flate.revision"],
		Metadata: map[string]string{
			"objectCount": desc.Annotations["flate.object-count"],
		},
	}, nil
}

// newMinioClient resolves credentials, endpoint, and TLS/proxy transport for b
// and constructs the minio client. Returns the resolved endpoint + secure flag
// too, since the cache key and artifact URL both need them.
func (f *Fetcher) newMinioClient(b *manifest.Bucket) (client *minio.Client, endpoint string, secure bool, err error) {
	creds, err := f.resolveCredentials(b)
	if err != nil {
		return nil, "", false, err
	}
	endpoint, secure, err = normalizeEndpoint(b.Endpoint, b.Insecure)
	if err != nil {
		return nil, "", false, err
	}
	transport, err := f.resolveTransport(b)
	if err != nil {
		return nil, "", false, err
	}
	client, err = minio.New(endpoint, &minio.Options{
		Creds:     creds,
		Secure:    secure,
		Region:    b.Region,
		Transport: transport,
	})
	if err != nil {
		return nil, "", false, fmt.Errorf("%s: minio client: %w", bucketID(b), err)
	}
	return client, endpoint, secure, nil
}

// bucketID is the "Bucket <namespace>/<name>" error prefix, via the shared
// source.QualifiedName so every fetcher formats kind/ns/name identically.
func bucketID(b *manifest.Bucket) string {
	return source.QualifiedName("Bucket", b.Namespace, b.Name)
}

// authIdentity returns the cache-key auth tag for a Bucket. Combines
// SecretRef (S3 access creds), CertSecretRef (TLS), and
// ProxySecretRef. Returns "" when all are unset.
func authIdentity(b *manifest.Bucket) string {
	return source.AuthIdentityFromRefs(b.Namespace,
		b.SecretRef, b.CertSecretRef, b.ProxySecretRef)
}
