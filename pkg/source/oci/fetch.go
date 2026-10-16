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
	"oras.land/oras-go/v2/registry/remote/credentials"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func (f *Fetcher) fetchWithCredentials(ctx context.Context, repo *manifest.OCIRepository, credStore credentials.Store, retry cache.RetryConfig) (*store.SourceArtifact, error) {
	if repo.URL == "" {
		return nil, fmt.Errorf("%w: %s missing url", manifest.ErrInput, ociID(repo))
	}
	client, material, err := f.newRepoClient(ctx, repo, credStore)
	if err != nil {
		return nil, err
	}
	return f.fetch(ctx, repo, registryAcquisition{client, material}, retry)
}

type registryAcquisition struct {
	client   *remote.Repository
	material any
}

func (f *Fetcher) fetch(ctx context.Context, repo *manifest.OCIRepository, request registryAcquisition, retry cache.RetryConfig) (*store.SourceArtifact, error) {
	var ref manifest.OCIRepositoryRef
	if repo.Reference != nil {
		ref = *repo.Reference
	}
	ignore := ""
	if repo.Ignore != nil {
		ignore = *repo.Ignore
	}
	content := struct {
		Source, Transport any
		RegistryConfig    string
	}{repo.Content(ref), request.material, f.RegistryConfig}
	var id string
	maxAge := repo.Interval.Duration
	if ref.Digest != "" {
		maxAge = cache.Forever
	}
	var input fs.FS
	var descriptor ocispec.Descriptor
	produce := func(ctx context.Context, w io.Writer) (ocispec.Descriptor, error) {
		selected, err := resolveRef(ctx, request.client, repo, ref)
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		selectedRepo := *repo
		selectedRepo.Reference = &selected
		return produceLayer(ctx, request.client, &selectedRepo, w)
	}
	switch effectiveLayerOperation(repo.LayerSelector) {
	case sourcev1.OCILayerCopy:
		r, desc, key, err := f.Cache.Blob(ctx, cache.BlobKey{Content: content, Required: []string{"upstream-digest", "revision"}}, cache.Policy{MaxAge: maxAge, Retry: retry}, produce)
		if err != nil {
			return nil, fmt.Errorf("%s %w", ociID(repo), err)
		}
		input, descriptor, id = cache.FileFS(source.OCICopiedLayerFilename, r), desc, key
		if err := r.Close(); err != nil {
			return nil, fmt.Errorf("%s %w", ociID(repo), err)
		}
	case sourcev1.OCILayerExtract:
		var err error
		input, descriptor, id, err = f.Cache.Acquire(ctx, cache.ArchiveKey{Content: content, Ignore: ignore, WithDefaults: true, Required: []string{"upstream-digest", "revision"}}, cache.Policy{MaxAge: maxAge, Retry: retry}, produce)
		if err != nil {
			return nil, fmt.Errorf("%s %w", ociID(repo), err)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported layer operation %q", manifest.ErrInput, effectiveLayerOperation(repo.LayerSelector))
	}
	return &store.SourceArtifact{Kind: manifest.KindOCIRepository, URL: repo.URL, FS: input,
		Key: id, Revision: descriptor.Annotations["flate.revision"],
		Digest: descriptor.Digest.String(), Size: descriptor.Size, Metadata: maps.Clone(descriptor.Annotations)}, nil
}

func resolveRef(ctx context.Context, client *remote.Repository, repo *manifest.OCIRepository, ref manifest.OCIRepositoryRef) (manifest.OCIRepositoryRef, error) {
	if ref.Digest != "" || ref.SemVer == "" {
		return ref, nil
	}
	tag, err := resolveOCISemver(ctx, client, ref, layerMediaType(repo.LayerSelector))
	if err != nil {
		return ref, fmt.Errorf("semver: %w", err)
	}
	return manifest.OCIRepositoryRef{Tag: tag}, nil
}

func produceLayer(ctx context.Context, client *remote.Repository, repo *manifest.OCIRepository, w io.Writer) (ocispec.Descriptor, error) {
	ref, selector := *repo.Reference, repo.LayerSelector
	tag := cmp.Or(versionTag(ref), latestTag)
	desc, err := client.Resolve(ctx, tag)
	if err != nil {
		return desc, fmt.Errorf("resolve %s: %w", tag, orasError(err))
	}
	data, err := content.FetchAll(ctx, client, desc)
	if err != nil {
		return desc, fmt.Errorf("fetch manifest: %w", orasError(err))
	}
	var m ocispec.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return desc, err
	}
	desc.Annotations = maps.Clone(m.Annotations)
	if desc.Annotations == nil {
		desc.Annotations = make(map[string]string)
	}
	desc.Annotations["flate.revision"] = ociRevision(ref, desc.Digest.String())
	desc.Annotations["flate.resolved-tag"] = ref.Tag
	layer, ok := pickLayer(m.Layers, selector)
	if !ok {
		if len(m.Layers) == 0 {
			return desc, nil
		}
		return desc, fmt.Errorf("no layer matched mediaType %q (manifest has %d layer(s))", layerMediaType(selector), len(m.Layers))
	}
	r, err := client.Fetch(ctx, layer)
	if err != nil {
		return desc, fmt.Errorf("fetch layer: %w", orasError(err))
	}
	defer func() { _ = r.Close() }()
	verified := content.NewVerifyReader(r, layer)
	if effectiveLayerOperation(selector) == sourcev1.OCILayerExtract {
		gz, err := gzip.NewReader(verified)
		if err != nil {
			return desc, fmt.Errorf("gzip: %w", orasError(err))
		}
		_, copyErr := io.Copy(w, gz) //nolint:gosec // The archive consumer caps expanded regular payloads before publication.
		closeErr := gz.Close()
		if copyErr != nil {
			return desc, orasError(copyErr)
		}
		if closeErr != nil {
			return desc, closeErr
		}
	} else {
		if _, err := io.Copy(w, verified); err != nil {
			return desc, orasError(err)
		}
	}
	if _, err := io.Copy(io.Discard, verified); err != nil {
		return desc, err
	}
	return desc, verified.Verify()
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
