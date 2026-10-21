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

func (f *Fetcher) fetchHTTPChart(ctx context.Context, r *httpRepository, hc *manifest.HelmChartSource, retry cache.RetryConfig) (*store.SourceArtifact, error) {
	chartName, version := hc.Chart, hc.Version
	key := cache.BlobKey{Content: struct{ Repository, Chart, Transport any }{r.Content(), hc.Content(), r.material}, Required: []string{"chart-version", "chart-url"}}
	maxAge := r.Interval.Duration
	if f.disableChartDigestTracking && isExactSemver(version) {
		maxAge = cache.Forever
	}
	reader, desc, id, err := f.cache.Blob(ctx, key, cache.Policy{MaxAge: maxAge, Retry: retry}, func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
		idx, err := f.fetchIndex(ctx, r, retry)
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
		return nil, fmt.Errorf("%s: %w", helmID(r.HelmRepository), err)
	}
	input := cache.FileFS(source.HelmChartArchiveFilename, reader)
	if err := reader.Close(); err != nil {
		return nil, err
	}
	return &store.SourceArtifact{Kind: manifest.KindHelmChart, FS: input, Key: id,
		URL: desc.Annotations["flate.chart-url"], Revision: desc.Annotations["flate.chart-version"], Digest: desc.Digest.Encoded(), Size: desc.Size}, nil
}

func (f *Fetcher) fetchIndex(ctx context.Context, r *httpRepository, retry cache.RetryConfig) (*repo.IndexFile, error) {
	indexURL := strings.TrimSuffix(r.URL, "/") + "/index.yaml"
	var candidate *repo.IndexFile
	var candidateDigest digest.Digest
	reader, desc, _, err := f.cache.Blob(ctx, cache.BlobKey{Content: struct{ Repository, Transport any }{r.Content(), r.material}}, cache.Policy{MaxAge: r.Interval.Duration, Retry: retry}, func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
		candidate = nil
		var buf bytes.Buffer
		desc, err := r.get(ctx, indexURL, io.MultiWriter(w, &buf))
		if err != nil {
			return cache.ProducerResult{Descriptor: desc}, err
		}
		candidateDigest = desc.Digest
		if v, ok := f.indexCache.Load(desc.Digest.String()); ok {
			candidate = v.(*repo.IndexFile)
			return cache.ProducerResult{Descriptor: desc}, nil
		}
		candidate, err = indexFromBytes(buf.Bytes())
		if err != nil {
			return cache.ProducerResult{Descriptor: desc}, fmt.Errorf("parse %s: %w", indexURL, err)
		}
		return cache.ProducerResult{Descriptor: desc}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", indexURL, err)
	}
	release, err := f.indexLocks.Acquire(ctx, desc.Digest.String())
	if err != nil {
		return nil, errors.Join(err, reader.Close())
	}
	defer release()
	if v, ok := f.indexCache.Load(desc.Digest.String()); ok {
		return v.(*repo.IndexFile), reader.Close()
	}
	if candidateDigest != desc.Digest || candidate == nil {
		data, readErr := io.ReadAll(reader)
		if err := errors.Join(readErr, reader.Close()); err != nil {
			return nil, err
		}
		candidate, err = indexFromBytes(data)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", indexURL, err)
		}
	} else if err := reader.Close(); err != nil {
		return nil, err
	}
	f.indexCache.Store(desc.Digest.String(), candidate)
	return candidate, nil
}

// indexFromBytes follows Flux's IndexFromBytes policy, including skippable chart validation.
func indexFromBytes(data []byte) (*repo.IndexFile, error) {
	idx := &repo.IndexFile{}
	if len(data) == 0 {
		return idx, repo.ErrEmptyIndexYaml
	}
	var err error
	if json.Valid(data) {
		err = json.Unmarshal(data, idx)
	} else {
		err = yaml.UnmarshalStrict(data, idx)
	}
	if err != nil {
		return idx, err
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
	if idx.APIVersion == "" {
		return idx, repo.ErrNoAPIVersion
	}
	return idx, nil
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
		return ocispec.Descriptor{}, &fetcherr.ResponseError{StatusCode: response.StatusCode, Op: http.MethodGet, Message: fmt.Sprintf("failed to fetch %s : %s", target, response.Status)}
	}
	h := digest.SHA256.Digester()
	size, err := io.Copy(io.MultiWriter(w, h.Hash()), response.Body)
	if err != nil {
		err = &url.Error{Op: "Get", URL: target, Err: err}
	}
	return ocispec.Descriptor{Digest: h.Digest(), Size: size, MediaType: response.Header.Get("Content-Type")}, err
}
