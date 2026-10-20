package helm

import (
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	chart "helm.sh/helm/v4/pkg/chart/v2"
)

func TestLoadChart_ValidationAcrossSources(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindHelmChart, manifest.KindGitRepository} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			st, hr, dir := ociRenderFixture(t)
			hr = hr.Clone()
			hr.Chart.Source.Kind = kind
			if kind != manifest.KindOCIRepository {
				hr.ChartRef = nil
			}
			writeChartFiles(t, dir, "podinfo", "invalid")
			st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: kind, FS: os.DirFS(dir), LocalPath: dir})
			cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
			if err != nil {
				t.Fatal(err)
			}
			cli.SetSourceResolver(NewStoreSourceResolver(st))
			_, err = cli.loadChart(t.Context(), hr)
			_, typed := errors.AsType[chart.ValidationError](err)
			if !typed || !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("validation chain = %v", err)
			}
		})
	}
}

func TestLoadChart_ValuesContentWithMemoryOwner(t *testing.T) {
	t.Parallel()
	st, hr, first := ociRenderFixture(t)
	hr = hr.Clone()
	hr.ChartRef = nil
	hr.Chart.Source.Kind = manifest.KindGitRepository
	second := t.TempDir()
	writeChartFiles(t, second, "podinfo", "6.15.0")
	testutil.WriteFile(t, second, "prod.yaml", "marker: second\n")
	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatal(err)
	}
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	var previous string
	for i, dir := range []string{first, second} {
		st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: manifest.KindGitRepository, FS: os.DirFS(dir), LocalPath: dir})
		loaded, err := cli.loadChart(t.Context(), hr)
		if err != nil {
			t.Fatal(err)
		}
		if chartValuesCacheKey(loaded, hr.ChartValuesFiles, false, nil) == "" || chartValuesCacheKey(loaded, hr.ChartValuesFiles, false, nil) == previous {
			t.Fatal("different chart content shared a fingerprint")
		}
		merged, err := cli.mergeChartValuesFiles(t.Context(), loaded, hr.ChartValuesFiles, false)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, merged["marker"], any([]string{"original", "second"}[i]))
		previous = chartValuesCacheKey(loaded, hr.ChartValuesFiles, false, nil)
	}
}

func TestLoadChart_SharedChartRetainsValuesFingerprint(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			st, hr, dir := ociRenderFixture(t)
			hr = hr.Clone()
			hr.Chart.Source.Kind = kind
			if kind == manifest.KindGitRepository {
				hr.ChartRef = nil
			}
			st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir})
			cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
			if err != nil {
				t.Fatal(err)
			}
			cli.SetSourceResolver(NewStoreSourceResolver(st))
			withoutFiles := hr.Clone()
			withoutFiles.ChartValuesFiles = nil
			loaded, err := cli.loadChart(t.Context(), withoutFiles)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, loaded.fingerprint == "", false)
			var results [2]chartLoadResult
			var failures [2]error
			var wg sync.WaitGroup
			for i := range results {
				wg.Go(func() {
					results[i], failures[i] = cli.loadChart(t.Context(), hr)
				})
			}
			wg.Wait()
			for i, result := range results {
				if failures[i] != nil {
					t.Fatal(failures[i])
				}
				assert.Equal(t, chartValuesCacheKey(result, hr.ChartValuesFiles, false, nil), chartValuesCacheKey(loaded, hr.ChartValuesFiles, false, nil))
			}
			loaded = results[0]
			merged, err := cli.mergeChartValuesFiles(t.Context(), loaded, hr.ChartValuesFiles, false)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, merged["marker"], any("original"))
		})
	}
}
