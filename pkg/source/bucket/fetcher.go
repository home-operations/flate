// Package bucket implements the source.Fetcher for KindBucket
// (S3-compatible object storage via minio-go).
package bucket

import (
	"context"
	"errors"
	"fmt"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

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

	work manifest.AcquisitionWork

	reads   *store.ReadSet
	secrets source.SecretGetter
}

// Fetch acquires content using one execution's captured credentials and declarations.
func (f *Fetcher) Fetch(
	ctx context.Context,
	b *manifest.Bucket,
	reads *store.ReadSet,
	policyValue *cache.Policy,
) (*store.SourceArtifact, error) {
	policy := *policyValue

	innerOperation := operation{Fetcher: f, reads: reads, secrets: source.SecretLookup(reads)}
	return innerOperation.fetchSource(ctx, b, &policy)
}

func (f *operation) fetchSource(
	ctx context.Context,
	b *manifest.Bucket,
	policyValue *cache.Policy,
) (*store.SourceArtifact, error) {
	policy := *policyValue

	f.work.Scope = f.reads.Work().Scope().Source(b.Named())
	if policy.Work != nil {
		f.work.Scope = policy.Work.Attribution().Source(b.Named())
	}
	policy.Work = &f.work
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	request, err := f.prepare(b)
	if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && request.bucket != nil {
		missing.RetryAmbient = func(ctx context.Context) (*store.SourceArtifact, error) {
			return f.machineFetch(ctx, &request, &policy)
		}
	}
	if err != nil {
		return nil, err
	}
	if b.Provider == sourcev1.BucketProviderAmazon && b.SecretRef == nil {
		return f.machineFetch(ctx, &request, &policy)
	}
	return f.fetch(ctx, &request, &policy)
}

func (f *operation) machineFetch(
	ctx context.Context, requestValue *bucketAcquisition,
	policyValue *cache.Policy,
) (*store.SourceArtifact, error) {
	policy := *policyValue

	request := *requestValue
	timeout := request.bucket.OperationTimeout()
	policy.Timeout = &timeout
	return source.AcquireMachine(ctx, &policy, func(ctx context.Context) error {
		if err := request.machineCredentials(
			ctx,
			[]credentials.Provider{&credentials.EnvAWS{}, &credentials.FileAWSCredentials{}, &credentials.IAM{}},
		); err != nil {
			return err
		}
		value, _ := request.options.Creds.GetWithContext(nil)
		tr, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Material: request.resolved, Credentials: value})
		request.options.Transport = tr
		return err
	}, func(ctx context.Context, policyValue *cache.Policy) (*store.SourceArtifact, error) {
		policy := *policyValue

		return f.fetch(ctx, &request, &policy)
	})
}

func (f *operation) fetch(
	ctx context.Context, requestValue *bucketAcquisition,
	policyValue *cache.Policy,
) (*store.SourceArtifact, error) {
	policy := *policyValue

	request := *requestValue
	request.work.Scope = f.work.Scope
	policy.Work = &request.work
	b := request.bucket
	ignore := ""
	if b.Ignore != nil {
		ignore = *b.Ignore
	}
	key := cache.ArchiveKey{
		Content: struct{ Source, Transport any }{
			b.Content(request.endpoint, request.options.Secure),
			request.material,
		},
		Ignore:   ignore,
		Policy:   "",
		Required: []string{"revision", "object-count"},
	}
	timeout := b.OperationTimeout()
	policy.Timeout = &timeout
	input, desc, id, err := f.Cache.Acquire(
		ctx,
		key,
		&policy,
		func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			client, err := minio.New(request.endpoint, &request.options)
			if err != nil {
				return cache.ProducerResult{}, fmt.Errorf("minio client: %w", err)
			}
			desc, err := walkBucket(ctx, client, b, bucketOutput{Writer: w, work: w.Work().Physical()})
			return cache.ProducerResult{Descriptor: desc}, err
		},
	)
	if err != nil {
		return nil, err
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
