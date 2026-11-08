package helmchart

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	repo "helm.sh/helm/v4/pkg/repo/v1"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (f *Fetcher) fetchHTTPChart(
	ctx context.Context,
	r *httpRepository,
	hc *manifest.HelmChartSource,
	policyValue *cache.Policy,
) (*store.SourceArtifact, error) {
	policy := *policyValue

	chartName, version := hc.Chart, hc.Version
	key := cache.BlobKey{
		Content:  struct{ Repository, Chart, Transport any }{r.Content(), hc.Content(), r.material},
		Required: []string{"chart-version", "chart-url"},
	}
	timeout := r.OperationTimeout()
	policy.Timeout = &timeout
	var releaseIndex func()
	policy.Admit = func(ctx context.Context) (func(), error) {
		select {
		case f.indexDecodes <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		held := true
		releaseIndex = func() {
			if held {
				<-f.indexDecodes
				held = false
			}
		}
		return releaseIndex, nil
	}
	generation, desc, id, err := f.cache.Blob(
		ctx,
		key,
		&policy,
		func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			idx, err := f.fetchIndex(ctx, r, &policy)
			releaseIndex()
			if err != nil {
				return cache.ProducerResult{}, err
			}
			cv, err := idx.Get(chartName, version)
			if err != nil {
				return cache.ProducerResult{}, manifest.NewError(
					manifest.ErrObjectNotFound,
					err,
					fmt.Sprintf("chart %s %s not found in %s", chartName, version, r.URL),
				)
			}
			if len(cv.URLs) == 0 {
				return cache.ProducerResult{}, manifest.NewError(
					manifest.ErrObjectNotFound,
					nil,
					fmt.Sprintf("chart %s@%s in %s has no URLs", chartName, version, r.URL),
				)
			}
			chartURL, err := absChartURL(r.URL, cv.URLs[0])
			if err != nil {
				return cache.ProducerResult{}, err
			}
			desc, err := r.get(ctx, chartURL, w, w.Work().Physical())
			if err != nil {
				return cache.ProducerResult{Descriptor: desc}, err
			}
			expected := normalizeChartDigest(cv.Digest)
			if expected != "" && desc.Digest.Encoded() != expected {
				return cache.ProducerResult{
					Descriptor: desc,
				}, fmt.Errorf(
					"chart %s@%s digest mismatch: index has %s, downloaded %s",
					chartName,
					cv.Version,
					expected,
					desc.Digest.Encoded(),
				)
			}
			desc.Annotations = map[string]string{
				"flate.chart-version": cv.Version,
				"flate.chart-url":     chartURL,
				"flate.chart-digest":  expected,
			}
			return cache.ProducerResult{Descriptor: desc}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	input := cache.NameBlob(source.HelmChartArchiveFilename, generation)
	artifact := &store.SourceArtifact{
		Kind:     manifest.KindHelmChart,
		FS:       input,
		Content:  cache.AcquisitionIdentity(input),
		Key:      id,
		URL:      desc.Annotations["flate.chart-url"],
		Revision: desc.Annotations["flate.chart-version"],
		Digest:   desc.Digest.Encoded(),
		Size:     desc.Size,
	}
	return artifact, nil
}

func (f *Fetcher) fetchIndex(
	ctx context.Context,
	r *httpRepository,
	policyValue *cache.Policy,
) (*repo.IndexFile, error) {
	policy := *policyValue

	indexWork := policy.HistoryWork
	if indexWork == nil {
		indexWork = policy.Work
	}

	indexURL := strings.TrimSuffix(r.URL, "/") + "/index.yaml"
	key := cache.BlobKey{Content: struct{ Repository, Transport any }{r.Content(), r.material}}
	indexPolicy := cache.Policy{
		MaxAge:  r.Interval.Duration,
		Retry:   policy.Retry,
		Machine: policy.Machine,
		Work:    indexWork,
	}
	generation, desc, _, err := f.cache.Blob(ctx, key, &indexPolicy,
		func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			_, descriptor, err := f.loadIndex(ctx, indexURL, func(input io.Writer) (ocispec.Descriptor, error) {
				return r.get(ctx, indexURL, io.MultiWriter(w, input), w.Work().Physical())
			}, indexWork.Attribution())
			return cache.ProducerResult{Descriptor: descriptor}, err
		})
	if err != nil {
		return nil, err
	}
	if v, ok := f.indexCache.Load(desc.Digest.String()); ok {
		return v.(*repo.IndexFile), nil //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
	}
	candidate, _, err := f.loadIndex(ctx, indexURL, func(w io.Writer) (ocispec.Descriptor, error) {
		input, openErr := generation.Open("content")
		if openErr != nil {
			return desc, openErr //nolint:wrapcheck // Filesystem content adapters retain the original fs.PathError and io sentinel contracts.
		}
		_, openErr = io.Copy(w, input)
		return desc, errors.Join(openErr, input.Close())
	}, indexWork.Attribution())
	return candidate, err
}

func (f *Fetcher) loadIndex(
	ctx context.Context,
	indexURL string,
	produce func(io.Writer) (ocispec.Descriptor, error),
	scope manifest.WorkScope,
) (*repo.IndexFile, ocispec.Descriptor, error) {
	var data bytes.Buffer
	desc, err := produce(&data)
	if err != nil {
		return nil, desc, fmt.Errorf("fetch %s: %w", indexURL, err)
	}
	release, err := f.indexLocks.Acquire(ctx, desc.Digest.String())
	if err != nil {
		return nil, desc, err
	}
	defer release()
	if v, ok := f.indexCache.Load(desc.Digest.String()); ok {
		return v.(*repo.IndexFile), desc, nil //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
	}
	idx := new(repo.IndexFile)
	work := scope.Record(manifest.WorkIndexDecode, desc.Digest.String())
	work.Add(&manifest.WorkCounts{Attempts: 1})
	switch {
	case data.Len() == 0:
		err = repo.ErrEmptyIndexYaml
	case json.Valid(data.Bytes()):
		err = json.Unmarshal(data.Bytes(), idx)
	default:
		err = yaml.UnmarshalStrict(data.Bytes(), idx)
	}
	if err == nil && idx.APIVersion == "" {
		err = repo.ErrNoAPIVersion
	}
	work.Finish(err)
	if err != nil {
		return nil, desc, manifest.NewError(manifest.ErrFlux, fmt.Errorf("parse %s: %w", indexURL, err), "")
	}
	for name, entries := range idx.Entries {
		idx.Entries[name] = slices.DeleteFunc(entries, func(v *repo.ChartVersion) bool {
			if v == nil {
				return true
			}
			if v.Metadata == nil {
				v.Metadata = &chart.Metadata{}
			}
			v.APIVersion = cmp.Or(v.APIVersion, chart.APIVersionV1)
			err := v.Validate()
			if validation, ok := errors.AsType[chart.ValidationError](
				err,
			); ok &&
				strings.HasPrefix(validation.Error(), "validation: more than one dependency with name or alias") {
				return false
			}
			return err != nil
		})
	}
	idx.SortEntries()
	f.indexCache.Store(desc.Digest.String(), idx)
	return idx, desc, nil
}

func absChartURL(base, target string) (string, error) {
	u, err := url.Parse(target)
	if err != nil {
		return "", fmt.Errorf("resolve chart URL: %w", err)
	}
	if u.IsAbs() {
		return target, nil
	}
	parent, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("resolve chart URL: %w", err)
	}
	parent.Path = strings.TrimSuffix(parent.Path, "/") + "/"
	return parent.ResolveReference(u).String(), nil
}

func normalizeChartDigest(d string) string {
	return strings.TrimPrefix(strings.TrimSpace(d), "sha256:")
}

func (r *httpRepository) get(
	ctx context.Context,
	target string,
	w io.Writer,
	work *manifest.WorkRecord,
) (ocispec.Descriptor, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, http.NoBody)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("create source HTTP request: %w", err)
	}
	base, err := url.Parse(r.URL)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("resolve chart URL: %w", err)
	}
	if (r.PassCredentials || base.Scheme == req.URL.Scheme && base.Host == req.URL.Host) && r.username != "" &&
		r.password != "" {
		req.SetBasicAuth(r.username, r.password)
	}
	response, err := r.client.Do(req)
	if err != nil {
		return ocispec.Descriptor{}, err //nolint:wrapcheck // The HTTP client already returns a URL error identifying method and target, including net.Error behavior.
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return ocispec.Descriptor{}, &fetcherr.ResponseError{
			StatusCode: response.StatusCode,
			Op:         http.MethodGet,
			Message: fmt.Sprintf(
				"failed to fetch %s : %d %s",
				target,
				response.StatusCode,
				http.StatusText(response.StatusCode),
			),
		}
	}
	work.Add(&manifest.WorkCounts{RemoteTransfers: 1})
	h := digest.SHA256.Digester()
	size, err := io.Copy(io.MultiWriter(w, h.Hash()), response.Body)
	if err != nil {
		err = &url.Error{Op: "Get", URL: target, Err: err}
	}
	return ocispec.Descriptor{Digest: h.Digest(), Size: size, MediaType: response.Header.Get("Content-Type")}, err
}
