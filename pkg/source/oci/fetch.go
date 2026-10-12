package oci

import (
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"maps"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/registry/remote"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

func (f *Fetcher) fetch(ctx context.Context, repo *manifest.OCIRepository, client *remote.Repository) (*store.SourceArtifact, error) {
	if repo.URL == "" {
		return nil, fmt.Errorf("%w: %s missing url", manifest.ErrInput, ociID(repo))
	}
	var ref manifest.OCIRepositoryRef
	if repo.Reference != nil {
		ref = *repo.Reference
	}
	ignore := ""
	if repo.Ignore != nil {
		ignore = *repo.Ignore
	}
	h, err := source.CacheKeyHash([]any{repo.URL, ref, authIdentity(repo), f.RegistryConfig, repo.Insecure, repo.LayerSelector, ignore, sourceignore.RulesVersion}, 32)
	if err != nil {
		return nil, err
	}
	id := "source-oci-" + h
	maxAge := repo.Interval.Duration
	if ref.Digest != "" {
		maxAge = cache.Forever
	}
	var input fs.FS
	var descriptor ocispec.Descriptor
	produce := func(w io.Writer) (ocispec.Descriptor, error) {
		selected, err := resolveRef(ctx, client, repo, ref)
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		selectedRepo := *repo
		selectedRepo.Reference = &selected
		return produceLayer(ctx, client, &selectedRepo, w)
	}
	switch effectiveLayerOperation(repo.LayerSelector) {
	case sourcev1.OCILayerCopy:
		id = "blob-oci-" + h
		r, desc, err := f.Cache.Blob(ctx, cache.BlobKey{ID: id}, maxAge, produce)
		if err != nil {
			return nil, fmt.Errorf("%s %w", ociID(repo), err)
		}
		input, descriptor = cache.FileFS(source.OCICopiedLayerFilename, r), desc
		if err := r.Close(); err != nil {
			return nil, fmt.Errorf("%s %w", ociID(repo), err)
		}
	case sourcev1.OCILayerExtract:
		input, descriptor, err = f.Cache.Acquire(ctx, cache.ArchiveKey{ID: id, Ignore: ignore, WithDefaults: true}, maxAge, produce)
		if err != nil {
			return nil, fmt.Errorf("%s %w", ociID(repo), err)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported layer operation %q", manifest.ErrInput, effectiveLayerOperation(repo.LayerSelector))
	}
	return &store.SourceArtifact{Kind: manifest.KindOCIRepository, URL: repo.URL, FS: input,
		Key: id + "@" + descriptor.Digest.String(), Revision: descriptor.Annotations["flate.revision"],
		Digest: descriptor.Digest.String(), Size: descriptor.Size, Metadata: maps.Clone(descriptor.Annotations)}, nil
}

func resolveRef(ctx context.Context, client *remote.Repository, repo *manifest.OCIRepository, ref manifest.OCIRepositoryRef) (manifest.OCIRepositoryRef, error) {
	if !shouldResolveOCISemver(ref) {
		return ref, nil
	}
	tag, err := resolveOCISemver(ctx, client, ref.SemVer, ref.SemverFilter, layerMediaType(repo.LayerSelector))
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
		return desc, fmt.Errorf("resolve %s: %w", tag, err)
	}
	data, err := content.FetchAll(ctx, client, desc)
	if err != nil {
		return desc, fmt.Errorf("fetch manifest: %w", err)
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
		return desc, fmt.Errorf("fetch layer: %w", err)
	}
	defer func() { _ = r.Close() }()
	verified := content.NewVerifyReader(r, layer)
	if effectiveLayerOperation(selector) == sourcev1.OCILayerExtract {
		gz, err := gzip.NewReader(verified)
		if err != nil {
			return desc, fmt.Errorf("gzip: %w", err)
		}
		_, copyErr := io.Copy(w, gz) //nolint:gosec // The archive consumer caps expanded regular payloads before publication.
		closeErr := gz.Close()
		if copyErr != nil {
			return desc, copyErr
		}
		if closeErr != nil {
			return desc, closeErr
		}
	} else {
		if _, err := io.Copy(w, verified); err != nil {
			return desc, err
		}
	}
	if _, err := io.Copy(io.Discard, verified); err != nil {
		return desc, err
	}
	return desc, verified.Verify()
}
