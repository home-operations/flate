// Package bucket implements the source.Fetcher for KindBucket
// (S3-compatible object storage via minio-go).
package bucket

import (
	"context"
	"fmt"
	"net/url"

	"golang.org/x/net/http/httpproxy"

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
func (f *Fetcher) Fetch(ctx context.Context, b *manifest.Bucket, retry cache.RetryConfig) (*store.SourceArtifact, error) {
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
	input, desc, id, err := f.Cache.Acquire(ctx, key, cache.Policy{MaxAge: b.Interval.Duration, Retry: retry}, func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
		desc, err := walkBucket(ctx, client, b, w)
		return cache.ProducerResult{Descriptor: desc}, err
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", bucketID(b), err)
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
	var certMaterial any
	prepared := *f
	if f.Secrets != nil {
		captured := make(map[string]*manifest.Secret)
		prepared.Secrets = func(ns, name string) *manifest.Secret {
			sec, ok := captured[name]
			if !ok {
				sec = f.Secrets(ns, name)
				captured[name] = sec
			}
			if b.CertSecretRef != nil && name == b.CertSecretRef.Name && sec != nil {
				certMaterial = [2]any{sec.Data, sec.StringData}
			}
			return sec
		}
	}
	creds, err := prepared.resolveCredentials(b)
	if err != nil {
		return nil, nil, err
	}
	endpoint, secure, err := normalizeEndpoint(b.Endpoint, b.Insecure)
	if err != nil {
		return nil, nil, err
	}
	proxyPolicy := httpproxy.FromEnvironment()
	transport, err := prepared.resolveTransport(b)
	if err != nil {
		return nil, nil, err
	}
	transport.ProxyEnvironment = proxyPolicy
	value, _ := creds.GetWithContext(nil)
	if value.AccessKeyID != "" || value.SecretAccessKey != "" {
		transport.Credentials = value
	}
	selected, err := proxyPolicy.ProxyFunc()(&url.URL{Scheme: schemeFor(secure), Host: endpoint})
	if transport.Proxy != nil {
		selected, err = transport.Proxy.URL()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("%w: resolve bucket proxy: %w", manifest.ErrInput, err)
	}
	var proxyIdentity any
	if b.ProxySecretRef == nil && (proxyPolicy.HTTPProxy != "" || proxyPolicy.HTTPSProxy != "") {
		proxyIdentity = proxyPolicy
	} else if selected != nil {
		proxyIdentity = selected.String()
	}
	var material any
	if value.AccessKeyID != "" || value.SecretAccessKey != "" || certMaterial != nil || proxyIdentity != nil {
		material = [3]any{value, certMaterial, proxyIdentity}
	}
	tr, err := f.Network.HTTPTransport(transport)
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
		return nil, nil, fmt.Errorf("%s: minio client: %w", bucketID(b), err)
	}
	return client, material, nil
}

// bucketID is the "Bucket <namespace>/<name>" error prefix, via the shared
// source.QualifiedName so every fetcher formats kind/ns/name identically.
func bucketID(b *manifest.Bucket) string {
	return source.QualifiedName("Bucket", b.Namespace, b.Name)
}
