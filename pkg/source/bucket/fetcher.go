// Package bucket implements the source.Fetcher for KindBucket
// (S3-compatible object storage via minio-go).
package bucket

import (
	"context"
	"errors"
	"fmt"

	"github.com/minio/minio-go/v7"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher acquires S3-compatible content using captured credentials.
type Fetcher struct {
	Cache *cache.Cache

	// Network is the required non-nil invocation owner, borrowed by this fetcher.
	Network *source.Network
}

type operation struct {
	*Fetcher
	reads   *store.ReadSet
	secrets source.SecretGetter
}

// Fetch acquires content using one execution's captured credentials and declarations.
func (f *Fetcher) Fetch(ctx context.Context, b *manifest.Bucket, reads *store.ReadSet, policy cache.Policy) (*store.SourceArtifact, error) {
	operation := operation{Fetcher: f, reads: reads, secrets: source.SecretLookup(reads)}
	return operation.fetchSource(ctx, b, policy)
}

func (f *operation) fetchSource(ctx context.Context, b *manifest.Bucket, policy cache.Policy) (*store.SourceArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request, err := f.prepare(b)
	if err != nil {
		if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && request.bucket != nil {
			missing.RetryAmbient = func(ctx context.Context) (*store.SourceArtifact, error) {
				return f.fetch(ctx, request, policy)
			}
		}
		return nil, err
	}
	return f.fetch(ctx, request, policy)
}

func (f *operation) fetch(ctx context.Context, request bucketAcquisition, policy cache.Policy) (*store.SourceArtifact, error) {
	b := request.bucket
	ignore := ""
	if b.Ignore != nil {
		ignore = *b.Ignore
	}
	key := cache.ArchiveKey{Content: struct{ Source, Transport any }{b.Content(request.endpoint, request.options.Secure), request.material}, Ignore: ignore, Policy: "", Required: []string{"revision", "object-count"}}
	timeout := b.OperationTimeout()
	policy.Timeout = &timeout
	input, desc, id, err := f.Cache.Acquire(ctx, key, policy, func(ctx context.Context, w cache.ProducerInput) (result cache.ProducerResult, err error) {
		defer func() {
			if request.providers != nil && err != nil {
				err = source.MachineCredentialErr(err)
			}
		}()
		opts := request.options
		if request.providers != nil {
			opts.Creds, err = request.machineCredentials(ctx)
			if err != nil {
				return cache.ProducerResult{}, err
			}
		}
		client, err := minio.New(request.endpoint, &opts)
		if err != nil {
			return cache.ProducerResult{}, fmt.Errorf("minio client: %w", err)
		}
		desc, err := walkBucket(ctx, client, b, w)
		return cache.ProducerResult{Descriptor: desc}, err
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", source.QualifiedName(manifest.KindBucket, b.Namespace, b.Name), err)
	}
	artifact := &store.SourceArtifact{
		Kind: manifest.KindBucket,
		URL:  fmt.Sprintf("%s://%s/%s", schemeFor(request.options.Secure), request.endpoint, b.BucketName),
		FS:   input, Content: cache.AcquisitionIdentity(input), Key: id,
		Revision: desc.Annotations["flate.revision"],
		Metadata: map[string]string{"objectCount": desc.Annotations["flate.object-count"]},
	}
	return artifact, nil
}
