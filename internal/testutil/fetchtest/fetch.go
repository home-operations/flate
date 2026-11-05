// Package fetchtest preserves the two acquisition phases in source tests.
package fetchtest

import (
	"context"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// Fetch copies the policy because preparation retains it for deferred acquisition.
//
//nolint:revive // The adapter preserves the fetch signature and adds the admitted artifact.
func Fetch[T manifest.BaseManifest](
	ctx context.Context,
	fetch func(context.Context, T, *store.ReadSet, *cache.Policy) (source.PreparedFetch, error),
	object T,
	reads *store.ReadSet,
	policyValue *cache.Policy,
	local *store.SourceArtifact,
) (*store.SourceArtifact, error) {
	policy := *policyValue
	prepared, err := fetch(ctx, object, reads, &policy)
	if prepared.Close != nil {
		defer prepared.Close()
	}
	if err != nil {
		return nil, err
	}
	return prepared.Fetch(ctx, local)
}

// Retry prepares the next acquisition before it executes the returned fetch.
func Retry(
	ctx context.Context,
	retry func(context.Context) (source.PreparedFetch, error),
) (*store.SourceArtifact, error) {
	prepared, err := retry(ctx)
	if prepared.Close != nil {
		defer prepared.Close()
	}
	if err != nil {
		return nil, err
	}
	return prepared.Fetch(ctx, nil)
}

// Machine supplies current reads to the machine retry before acquisition.
func Machine(
	ctx context.Context,
	missing *source.MissingSecretError,
	reads *store.ReadSet,
) (*store.SourceArtifact, error) {
	return Retry(
		ctx,
		func(ctx context.Context) (source.PreparedFetch, error) { return missing.RetryMachine(ctx, reads) },
	)
}

// Prepared keeps admitted local content authoritative in fake fetchers.
func Prepared(artifact *store.SourceArtifact, err error) (source.PreparedFetch, error) {
	if err != nil {
		return source.PreparedFetch{}, err
	}
	return source.PreparedFetch{
		Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			if local != nil {
				return local, ctx.Err()
			}
			return artifact, ctx.Err()
		},
	}, nil
}
