package oci

import (
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type registryAcquisition struct {
	client    *remote.Repository
	material  any
	resolved  source.TransportMaterial
	anonymous bool
}

func (f *operation) fetchLayer(
	ctx context.Context,
	repo *manifest.OCIRepository,
	requestValue *registryAcquisition,
	policy cache.Policy,
) (*store.SourceArtifact, error) {
	request := *requestValue
	var ref manifest.OCIRepositoryRef
	if repo.Reference != nil {
		ref = manifest.OCIRepositoryRef(*repo.Reference)
	}
	ignore := ""
	if repo.Ignore != nil {
		ignore = *repo.Ignore
	}
	contentKey := struct {
		Source, Transport any
		RegistryConfig    string
		MediaTypes        []string
	}{repo.Content(sourcev1.OCIRepositoryRef(ref)), request.material, f.RegistryConfig, f.mediaTypes}
	var id string
	if ref.Pinned() {
		policy.MaxAge = cache.Forever
	}
	timeout := repo.OperationTimeout()
	policy.Timeout = &timeout
	var input fs.FS
	var descriptor ocispec.Descriptor
	produce := func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
		selected, err := resolveRef(ctx, request.client, repo, ref)
		if err != nil {
			return cache.ProducerResult{}, err
		}
		selectedRepo := *repo
		selectedRepo.Reference = (*sourcev1.OCIRepositoryRef)(&selected)
		return f.produceLayer(ctx, request.client, &selectedRepo, w)
	}
	switch repo.LayerOperation() {
	case sourcev1.OCILayerCopy:
		generation, desc, key, err := f.Cache.Blob(
			ctx,
			cache.BlobKey{Content: contentKey, Required: []string{"upstream-digest", "revision"}},
			policy,
			produce,
		)
		if err != nil {
			return nil, err
		}
		input, descriptor, id = cache.NameBlob(source.OCICopiedLayerFilename, generation), desc, key
	case sourcev1.OCILayerExtract:
		var err error
		input, descriptor, id, err = f.Cache.Acquire(
			ctx,
			cache.ArchiveKey{
				Content:  contentKey,
				Ignore:   ignore,
				Policy:   "source",
				Required: []string{"upstream-digest", "revision"},
			},
			policy,
			produce,
		)
		if err != nil {
			return nil, err
		}
	default:
		return nil, manifest.NewError(
			manifest.ErrInput,
			nil,
			fmt.Sprintf("unsupported layer operation %q", repo.LayerOperation()),
		)
	}
	return &store.SourceArtifact{
		Kind: manifest.KindOCIRepository, URL: repo.URL, FS: input, Content: cache.AcquisitionIdentity(input),
		Key: id, Revision: descriptor.Annotations["flate.revision"],
		Digest: descriptor.Digest.String(), Size: descriptor.Size, Metadata: maps.Clone(descriptor.Annotations),
	}, nil
}

func resolveRef(
	ctx context.Context,
	client *remote.Repository,
	repo *manifest.OCIRepository,
	ref manifest.OCIRepositoryRef,
) (manifest.OCIRepositoryRef, error) {
	if ref.Digest != "" || ref.SemVer == "" {
		return ref, nil
	}
	tag, err := resolveOCISemver(ctx, client, ref, repo.LayerMediaType())
	if err != nil {
		return ref, fmt.Errorf("semver: %w", err)
	}
	return manifest.OCIRepositoryRef{Tag: tag}, nil
}

func (f *operation) produceLayer(
	ctx context.Context,
	client *remote.Repository,
	repo *manifest.OCIRepository,
	w cache.ProducerInput,
) (cache.ProducerResult, error) {
	ref := manifest.OCIRepositoryRef(*repo.Reference)
	tag := cmp.Or(versionTag(ref), latestTag)
	desc, err := client.Resolve(ctx, tag)
	if err != nil {
		return cache.ProducerResult{}, fmt.Errorf("resolve %s: %w", tag, orasError(err))
	}
	previous, err := w.Previous(ctx)
	if err != nil {
		return cache.ProducerResult{}, err
	}
	unchanged := previous != nil && previous.Digest == desc.Digest
	var m ocispec.Manifest
	if unchanged {
		desc = *previous
	} else {
		data, fetchAllErr := content.FetchAll(ctx, client, desc)
		if fetchAllErr != nil {
			return cache.ProducerResult{}, fmt.Errorf("fetch manifest: %w", orasError(fetchAllErr))
		}
		if unmarshalErr := json.Unmarshal(data, &m); unmarshalErr != nil {
			return cache.ProducerResult{}, fmt.Errorf("decode OCI manifest: %w", unmarshalErr)
		}
		desc.Annotations = maps.Clone(m.Annotations)
	}
	if desc.Annotations == nil {
		desc.Annotations = make(map[string]string)
	}
	desc.Annotations["flate.revision"] = ociRevision(ref, desc.Digest.String())
	desc.Annotations["flate.resolved-tag"] = ref.Tag
	if unchanged {
		return cache.ProducerResult{Descriptor: desc, Unchanged: true}, nil
	}
	result := cache.ProducerResult{Descriptor: desc}
	layer, ok := pickLayer(m.Layers, f.mediaTypes)
	if !ok {
		if len(m.Layers) == 0 {
			return result, manifest.NewError(manifest.ErrInput, nil, "no layers found in artifact")
		}
		return result, fmt.Errorf(
			"no layer matched mediaTypes %q (manifest has %d layer(s))",
			f.mediaTypes,
			len(m.Layers),
		)
	}
	selected := selectedLayer{descriptor: layer, extract: repo.LayerOperation() == sourcev1.OCILayerExtract}
	if err := selected.write(ctx, client, w); err != nil {
		return result, err
	}
	return result, nil
}

type selectedLayer struct {
	descriptor ocispec.Descriptor
	extract    bool
}

func (l *selectedLayer) write(ctx context.Context, client *remote.Repository, w cache.ProducerInput) error {
	r, err := client.Fetch(ctx, l.descriptor)
	if err != nil {
		return fmt.Errorf("fetch layer: %w", orasError(err))
	}
	defer func() { _ = r.Close() }()
	verified := content.NewVerifyReader(r, l.descriptor)
	var reader io.Reader = verified
	var compressed *gzip.Reader
	if l.extract {
		compressed, err = gzip.NewReader(verified)
		if err != nil {
			return fmt.Errorf("gzip: %w", orasError(err))
		}
		reader = compressed
	}
	_, copyErr := io.Copy(w, reader)
	var closeErr error
	if compressed != nil {
		closeErr = compressed.Close()
	}
	if copyErr != nil {
		return orasError(copyErr)
	}
	if closeErr != nil {
		return closeErr //nolint:wrapcheck // The layer adapter preserves the upstream compression error contract.
	}
	if _, err := io.Copy(io.Discard, verified); err != nil {
		return fmt.Errorf("copy source content: %w", err)
	}
	return verified.Verify() //nolint:wrapcheck // Preserve the verifier's upstream error contract.
}

func orasError(err error) error {
	if _, ok := errors.AsType[*fetcherr.ResponseError](err); ok {
		return err
	}
	if response, ok := errors.AsType[*errcode.ErrorResponse](err); ok {
		return &fetcherr.ResponseError{StatusCode: response.StatusCode, Op: response.Method, Err: err}
	}
	return err
}
