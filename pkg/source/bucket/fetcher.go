// Package bucket implements the source.Fetcher for KindBucket
// (S3-compatible object storage via minio-go).
package bucket

import (
	"context"
	"fmt"
	"net/url"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/minio/minio-go/v7"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
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
	// Network is the required non-nil invocation owner, borrowed by this fetcher.
	Network *source.Network

	// ForceGeneric skips the provider gate; the generic path then needs
	// static credentials (a SecretRef) or it fails.
	ForceGeneric bool
}

// Fetch implements source.TypedFetcher[*manifest.Bucket].
func (f *Fetcher) Fetch(ctx context.Context, b *manifest.Bucket, policy cache.Policy) (*store.SourceArtifact, error) {
	if !f.ForceGeneric && b.Provider != "" && b.Provider != sourcev1.BucketProviderGeneric {
		return nil, source.ErrUnsupportedProvider(b.Named(), b.Provider, sourcev1.BucketProviderGeneric,
			"S3-compatible")
	}

	client, material, err := f.newMinioClient(b)
	if err != nil {
		return nil, err
	}

	endpoint, secure := client.EndpointURL().Host, client.EndpointURL().Scheme == "https"
	ignore := ""
	if b.Ignore != nil {
		ignore = *b.Ignore
	}
	key := cache.ArchiveKey{Content: struct{ Source, Transport any }{b.Content(endpoint, secure), material}, Ignore: ignore, Required: []string{"revision", "object-count"}}
	input, desc, id, err := f.Cache.Acquire(ctx, key, policy, func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
		desc, err := walkBucket(ctx, client, b, w)
		return cache.ProducerResult{Descriptor: desc}, err
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", source.QualifiedName(manifest.KindBucket, b.Namespace, b.Name), err)
	}

	return &store.SourceArtifact{
		Kind:     manifest.KindBucket,
		URL:      fmt.Sprintf("%s://%s/%s", schemeFor(secure), endpoint, b.BucketName),
		FS:       input,
		Key:      id,
		Revision: desc.Annotations["flate.revision"],
		Metadata: map[string]string{
			"objectCount": desc.Annotations["flate.object-count"],
		},
	}, nil
}

func (f *Fetcher) newMinioClient(b *manifest.Bucket) (*minio.Client, any, error) {
	prepared := *f
	prepared.Secrets = source.SnapshotSecrets(f.Secrets)
	creds, err := prepared.resolveCredentials(b)
	if err != nil {
		return nil, nil, err
	}
	endpoint, secure, err := normalizeEndpoint(b.Endpoint, b.Insecure)
	if err != nil {
		return nil, nil, err
	}
	owner := b.Named()
	owner.Name = b.Namespace + "/" + b.Name
	resolved, err := source.ResolveTransport(prepared.Secrets, owner, []source.TransportInput{
		{Reference: b.ProxySecretRef, Use: source.TransportProxy},
		{Reference: b.CertSecretRef, Use: source.TransportCert},
	})
	if err != nil {
		return nil, nil, err
	}
	if _, err := resolved.ProxyURL(&url.URL{Scheme: schemeFor(secure), Host: endpoint}); err != nil {
		return nil, nil, fmt.Errorf("%w: resolve bucket proxy: invalid proxy configuration", manifest.ErrInput)
	}
	value, _ := creds.GetWithContext(nil)
	var credentials any
	if value.AccessKeyID != "" || value.SecretAccessKey != "" {
		credentials = value
	}
	material := resolved.Identity(credentials)
	tr, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Material: resolved, Credentials: credentials})
	if err != nil {
		return nil, nil, err
	}
	client, err := minio.New(endpoint, &minio.Options{
		MaxRetries: 1,
		Creds:      creds,
		Secure:     secure,
		Region:     b.Region,
		Transport:  tr,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("%s: minio client: %w", source.QualifiedName(manifest.KindBucket, b.Namespace, b.Name), err)
	}
	return client, material, nil
}
