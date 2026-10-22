package helm

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	chartutil "helm.sh/helm/v4/pkg/chart/v2/util"
	"sigs.k8s.io/yaml"
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
			cli := NewClient(cachetest.New(t, "", sourcecache.New))
			cliSources := NewSources(st)
			_, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
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
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	cliSources := NewSources(st)
	var previous string
	for i, dir := range []string{first, second} {
		st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: manifest.KindGitRepository, FS: os.DirFS(dir), LocalPath: dir})
		loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		if err != nil {
			t.Fatal(err)
		}
		if chartValuesCacheKey(loaded, hr.ChartValuesFiles, false, "") == "" || chartValuesCacheKey(loaded, hr.ChartValuesFiles, false, "") == previous {
			t.Fatal("different chart content shared a fingerprint")
		}
		merged, err := cli.mergeChartValuesFiles(t.Context(), loaded, hr.ChartValuesFiles, false)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, merged["marker"], any([]string{"original", "second"}[i]))
		previous = chartValuesCacheKey(loaded, hr.ChartValuesFiles, false, "")
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
			cli := NewClient(cachetest.New(t, "", sourcecache.New))
			cliSources := NewSources(st)
			withoutFiles := hr.Clone()
			withoutFiles.ChartValuesFiles = nil
			loaded, err := loadDecodedChart(t.Context(), cli, withoutFiles, cliSources)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, loaded.fingerprint == "", false)
			var results [2]chartDescription
			var failures [2]error
			var wg sync.WaitGroup
			for i := range results {
				wg.Go(func() {
					results[i], failures[i] = loadDecodedChart(t.Context(), cli, hr, cliSources)
				})
			}
			wg.Wait()
			for i, result := range results {
				if failures[i] != nil {
					t.Fatal(failures[i])
				}
				assert.Equal(t, chartValuesCacheKey(result, hr.ChartValuesFiles, false, ""), chartValuesCacheKey(loaded, hr.ChartValuesFiles, false, ""))
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

func loadDecodedChart(ctx context.Context, c *Client, hr *manifest.HelmRelease, sources Sources) (chartDescription, error) {
	d, err := c.describeChart(ctx, hr, sources)
	if err != nil {
		return chartDescription{}, err
	}
	if err := c.decodeChart(ctx, &d); err != nil {
		return chartDescription{}, err
	}
	return d, nil
}

func chartFixtureDescription(t *testing.T, ch *chart.Chart) chartDescription {
	t.Helper()
	root := t.TempDir()
	assert.NoError(t, chartutil.SaveDir(ch, root))
	data, err := yaml.Marshal(ch.Values)
	assert.NoError(t, err)
	testutil.WriteFile(t, root, filepath.Join(ch.Name(), "values.yaml"), string(data))
	captured, _, err := captureChart(os.DirFS(root), ch.Name())
	assert.NoError(t, err)
	fingerprint := captured.fingerprint()
	return chartDescription{chart: ch, captured: captured, fingerprint: fingerprint, key: fingerprint, location: ch.Name(), analysis: &chartAnalysis{Templates: chartTemplateText(ch), Allowed: knownTopLevelKeys(ch), RequiresLoader: requiresChartLoader(ch)}}
}

func TestSourceResolver_SharedClientDeclarations(t *testing.T) {
	t.Parallel()
	st := store.New()
	parent := &manifest.Kustomization{Name: "materializer", Namespace: "apps"}
	raw := &manifest.HelmRepository{Name: "covered", Namespace: "apps", URL: "https://raw.example"}
	st.AddObjects([]manifest.BaseManifest{parent, raw})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		raw.Named(): {Productions: []manifest.Production{{Owner: parent.Named(), Source: manifest.BootstrapSourceID}}},
	}})
	resolver := NewSources(st)
	if resolver.HelmRepository(raw.Namespace, raw.Name) != nil {
		t.Fatal("provisional source reached shared resolver")
	}
	accepted := *raw
	accepted.URL = "https://accepted.example"
	_, _, next := st.GetEmission(parent.Named())
	defer next.Finish()
	next.Children = []store.Child{{Obj: &accepted}}
	st.RecordEmitted(parent.Named(), &next, nil)
	st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "other children pending"})
	if resolver.HelmRepository(raw.Namespace, raw.Name) != &accepted {
		t.Fatal("shared resolver lost effective accepted source")
	}
}
