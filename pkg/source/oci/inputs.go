package oci

import (
	"context"
	"encoding/json"
	"fmt"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"oras.land/oras-go/v2/registry"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// FetchInputs acquires evaluated registry tag inputs as an inputs.json artifact.
func (f *Fetcher) FetchInputs(ctx context.Context, provider *manifest.ResourceSetInputProvider, reads *store.ReadSet, policy cache.Policy) (*store.SourceArtifact, error) {
	plan, err := provider.InputPlan()
	if err != nil {
		return nil, err
	}
	if plan.Registry == nil {
		return nil, manifest.NewError(manifest.ErrInput, nil, "provider does not supply registry inputs")
	}
	op := operation{Fetcher: f, reads: reads, secrets: source.SecretLookup(reads), allowAnonymousDenied: true}
	op.produce = func(ctx context.Context, request registryAcquisition, policy cache.Policy) (*store.SourceArtifact, error) {
		content := struct {
			Provider       manifest.NamedResource
			Spec           fluxopv1.ResourceSetInputProviderSpec
			Transport      any
			RegistryConfig string
		}{provider.Named(), provider.ResourceSetInputProviderSpec, request.material, f.RegistryConfig}
		input, _, key, err := f.Cache.Blob(ctx, cache.BlobKey{Content: content}, policy, func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			tags, err := registry.Tags(ctx, request.client)
			if err != nil {
				return cache.ProducerResult{}, fmt.Errorf("list tags: %w", orasError(err))
			}
			selected, err := provider.SelectTags(tags)
			if err != nil {
				return cache.ProducerResult{}, err
			}
			inputs := make([]fluxopv1.ResourceSetInput, 0, len(selected))
			for _, tag := range selected {
				descriptor, err := request.client.Resolve(ctx, tag)
				if err != nil {
					return cache.ProducerResult{}, fmt.Errorf("resolve %s: %w", tag, orasError(err))
				}
				input, err := provider.TagInputs(tag, descriptor.Digest.String())
				if err != nil {
					return cache.ProducerResult{}, err
				}
				inputs = append(inputs, input)
			}
			return cache.ProducerResult{}, json.NewEncoder(w).Encode(inputs)
		})
		if err != nil {
			return nil, err
		}
		input = cache.NameBlob("inputs.json", input)
		return &store.SourceArtifact{Kind: manifest.KindResourceSetInputProvider, URL: provider.URL, FS: input, Content: cache.AcquisitionIdentity(input), Key: key}, nil
	}
	return op.acquire(ctx, *plan.Registry, policy)
}
