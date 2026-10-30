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
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func (f *Fetcher) fetchHTTPChart(ctx context.Context, r *httpRepository, hc *manifest.HelmChartSource, policy cache.Policy) (*store.SourceArtifact, error) {
	chartName, version := hc.Chart, hc.Version
	key := cache.BlobKey{Content: struct{ Repository, Chart, Transport any }{r.Content(), hc.Content(), r.material}, Required: []string{"chart-version", "chart-url"}}
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
	generation, desc, id, err := f.cache.Blob(ctx, key, policy, func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
		idx, err := f.fetchIndex(ctx, r, policy)
		releaseIndex()
		if err != nil {
			return cache.ProducerResult{}, err
		}
		cv, err := idx.Get(chartName, version)
		if err != nil {
			return cache.ProducerResult{}, fmt.Errorf("%w: chart %s@%s not found in %s: %w", manifest.ErrObjectNotFound, chartName, version, r.URL, err)
		}
		if len(cv.URLs) == 0 {
			return cache.ProducerResult{}, fmt.Errorf("%w: chart %s@%s in %s has no URLs", manifest.ErrObjectNotFound, chartName, version, r.URL)
		}
		chartURL, err := absChartURL(r.URL, cv.URLs[0])
		if err != nil {
			return cache.ProducerResult{}, err
		}
		desc, err := r.get(ctx, chartURL, w)
		if err != nil {
			return cache.ProducerResult{Descriptor: desc}, err
		}
		expected := normalizeChartDigest(cv.Digest)
		if expected != "" && desc.Digest.Encoded() != expected {
			return cache.ProducerResult{Descriptor: desc}, fmt.Errorf("chart %s@%s digest mismatch: index has %s, downloaded %s", chartName, cv.Version, expected, desc.Digest.Encoded())
		}
		desc.Annotations = map[string]string{"flate.chart-version": cv.Version, "flate.chart-url": chartURL, "flate.chart-digest": expected}
		return cache.ProducerResult{Descriptor: desc}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", manifest.DiagnosticFor(r.Named(), r).Label(), err)
	}
	input := cache.NameBlob(source.HelmChartArchiveFilename, generation)
	artifact := &store.SourceArtifact{Kind: manifest.KindHelmChart, FS: input, Content: cache.AcquisitionIdentity(input), Key: id,
		URL: desc.Annotations["flate.chart-url"], Revision: desc.Annotations["flate.chart-version"], Digest: desc.Digest.Encoded(), Size: desc.Size}
	return artifact, nil
}

func (f *Fetcher) fetchIndex(ctx context.Context, r *httpRepository, policy cache.Policy) (*repo.IndexFile, error) {
	indexURL := strings.TrimSuffix(r.URL, "/") + "/index.yaml"
	generation, desc, _, err := f.cache.Blob(ctx, cache.BlobKey{Content: struct{ Repository, Transport any }{r.Content(), r.material}}, cache.Policy{MaxAge: r.Interval.Duration, Retry: policy.Retry, Machine: policy.Machine}, func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
		_, descriptor, err := f.loadIndex(ctx, indexURL, func(input io.Writer) (ocispec.Descriptor, error) {
			return r.get(ctx, indexURL, io.MultiWriter(w, input))
		})
		return cache.ProducerResult{Descriptor: descriptor}, err
	})
	if err != nil {
		return nil, err
	}
	if v, ok := f.indexCache.Load(desc.Digest.String()); ok {
		return v.(*repo.IndexFile), nil
	}
	candidate, _, err := f.loadIndex(ctx, indexURL, func(w io.Writer) (ocispec.Descriptor, error) {
		input, err := generation.Open("content")
		if err != nil {
			return desc, err
		}
		_, err = io.Copy(w, input)
		return desc, errors.Join(err, input.Close())
	})
	return candidate, err
}

func (f *Fetcher) loadIndex(ctx context.Context, indexURL string, produce func(io.Writer) (ocispec.Descriptor, error)) (*repo.IndexFile, ocispec.Descriptor, error) {
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
		return v.(*repo.IndexFile), desc, nil
	}
	idx := &repo.IndexFile{}
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
	if err != nil {
		return nil, desc, fmt.Errorf("%w: parse %s: %w", manifest.ErrFlux, indexURL, err)
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
			if validation, ok := errors.AsType[chart.ValidationError](err); ok && strings.HasPrefix(validation.Error(), "validation: more than one dependency with name or alias") {
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
		return "", err
	}
	if u.IsAbs() {
		return target, nil
	}
	parent, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	parent.Path = strings.TrimSuffix(parent.Path, "/") + "/"
	return parent.ResolveReference(u).String(), nil
}
func normalizeChartDigest(d string) string {
	return strings.TrimPrefix(strings.TrimSpace(d), "sha256:")
}

func (r *httpRepository) get(ctx context.Context, target string, w io.Writer) (ocispec.Descriptor, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, http.NoBody)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	base, err := url.Parse(r.URL)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	if (r.PassCredentials || base.Scheme == req.URL.Scheme && base.Host == req.URL.Host) && r.username != "" && r.password != "" {
		req.SetBasicAuth(r.username, r.password)
	}
	response, err := r.client.Do(req)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return ocispec.Descriptor{}, &fetcherr.ResponseError{StatusCode: response.StatusCode, Op: http.MethodGet, Message: fmt.Sprintf("failed to fetch %s : %d %s", target, response.StatusCode, http.StatusText(response.StatusCode))}
	}
	h := digest.SHA256.Digester()
	size, err := io.Copy(io.MultiWriter(w, h.Hash()), response.Body)
	if err != nil {
		err = &url.Error{Op: "Get", URL: target, Err: err}
	}
	return ocispec.Descriptor{Digest: h.Digest(), Size: size, MediaType: response.Header.Get("Content-Type")}, err
}
