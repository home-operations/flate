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
	"net/http"
	"net/url"

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

func (f *operation) fetchWithCredentials(ctx context.Context, repo *manifest.OCIRepository, policy cache.Policy) (*store.SourceArtifact, error) {
	if repo.URL == "" {
		return nil, fmt.Errorf("%w: %s missing url", manifest.ErrInput, manifest.DiagnosticFor(repo.Named(), repo).Label())
	}
	request, err := f.newRepoClient(ctx, repo, policy)
	if err != nil {
		return nil, err
	}
	if repo.SecretRef == nil {
		return f.machineFetch(ctx, repo, request, policy)
	}
	return f.fetchPrepared(ctx, repo, request, policy)
}

func (f *operation) fetchPrepared(ctx context.Context, repo *manifest.OCIRepository, request registryAcquisition, policy cache.Policy) (*store.SourceArtifact, error) {
	artifact, err := f.fetch(ctx, repo, request, policy)
	if repo.Insecure && ctx.Err() == nil && errors.Is(err, http.ErrSchemeMismatch) {
		if requestErr, ok := errors.AsType[*url.Error](err); ok {
			target, parseErr := url.Parse(requestErr.URL)
			if parseErr == nil && target.Host == request.client.Reference.Host() {
				request.client.PlainHTTP = true
				artifact, err = f.fetch(ctx, repo, request, policy)
			}
		}
	}

	return artifact, err
}

type registryAcquisition struct {
	client   *remote.Repository
	material any
	resolved source.TransportMaterial
}

func (f *operation) fetch(ctx context.Context, repo *manifest.OCIRepository, request registryAcquisition, policy cache.Policy) (*store.SourceArtifact, error) {
	var ref manifest.OCIRepositoryRef
	if repo.Reference != nil {
		ref = manifest.OCIRepositoryRef(*repo.Reference)
	}
	ignore := ""
	if repo.Ignore != nil {
		ignore = *repo.Ignore
	}
	content := struct {
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
		generation, desc, key, err := f.Cache.Blob(ctx, cache.BlobKey{Content: content, Required: []string{"upstream-digest", "revision"}}, policy, produce)
		if err != nil {
			return nil, fmt.Errorf("%s %w", manifest.DiagnosticFor(repo.Named(), repo).Label(), err)
		}
		input, descriptor, id = cache.NameBlob(source.OCICopiedLayerFilename, generation), desc, key
	case sourcev1.OCILayerExtract:
		var err error
		input, descriptor, id, err = f.Cache.Acquire(ctx, cache.ArchiveKey{Content: content, Ignore: ignore, Policy: "source", Required: []string{"upstream-digest", "revision"}}, policy, produce)
		if err != nil {
			return nil, fmt.Errorf("%s %w", manifest.DiagnosticFor(repo.Named(), repo).Label(), err)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported layer operation %q", manifest.ErrInput, repo.LayerOperation())
	}
	return &store.SourceArtifact{Kind: manifest.KindOCIRepository, URL: repo.URL, FS: input, Content: cache.AcquisitionIdentity(input),
		Key: id, Revision: descriptor.Annotations["flate.revision"],
		Digest: descriptor.Digest.String(), Size: descriptor.Size, Metadata: maps.Clone(descriptor.Annotations)}, nil
}

func resolveRef(ctx context.Context, client *remote.Repository, repo *manifest.OCIRepository, ref manifest.OCIRepositoryRef) (manifest.OCIRepositoryRef, error) {
	if ref.Digest != "" || ref.SemVer == "" {
		return ref, nil
	}
	tag, err := resolveOCISemver(ctx, client, ref, repo.LayerMediaType())
	if err != nil {
		return ref, fmt.Errorf("semver: %w", err)
	}
	return manifest.OCIRepositoryRef{Tag: tag}, nil
}

func (f *operation) produceLayer(ctx context.Context, client *remote.Repository, repo *manifest.OCIRepository, w cache.ProducerInput) (cache.ProducerResult, error) {
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
		data, err := content.FetchAll(ctx, client, desc)
		if err != nil {
			return cache.ProducerResult{}, fmt.Errorf("fetch manifest: %w", orasError(err))
		}
		if err := json.Unmarshal(data, &m); err != nil {
			return cache.ProducerResult{}, err
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
			return result, fmt.Errorf("%w: no layers found in artifact", manifest.ErrInput)
		}
		return result, fmt.Errorf("no layer matched mediaTypes %q (manifest has %d layer(s))", f.mediaTypes, len(m.Layers))
	}
	r, err := client.Fetch(ctx, layer)
	if err != nil {
		return result, fmt.Errorf("fetch layer: %w", orasError(err))
	}
	defer func() { _ = r.Close() }()
	verified := content.NewVerifyReader(r, layer)
	if repo.LayerOperation() == sourcev1.OCILayerExtract {
		gz, err := gzip.NewReader(verified)
		if err != nil {
			return result, fmt.Errorf("gzip: %w", orasError(err))
		}
		_, copyErr := io.Copy(w, gz) //nolint:gosec // Flux source extraction is uncapped; the cache validates and confines archive entries.
		closeErr := gz.Close()
		if copyErr != nil {
			return result, orasError(copyErr)
		}
		if closeErr != nil {
			return result, closeErr
		}
	} else {
		if _, err := io.Copy(w, verified); err != nil {
			return result, orasError(err)
		}
	}
	if _, err := io.Copy(io.Discard, verified); err != nil {
		return result, err
	}
	return result, verified.Verify()
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
