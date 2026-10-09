package helm

import (
	"errors"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"testing"
)

func TestLoadChart_ValidationAcrossSources(t *testing.T) {
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindHelmChart, manifest.KindGitRepository} {
		t.Run(kind, func(t *testing.T) {
			st, hr, dir := ociRenderFixture(t)
			hr = hr.Clone()
			hr.Chart.RepoKind = kind
			if kind != manifest.KindOCIRepository {
				hr.ChartRef = nil
			}
			writeChartFiles(t, dir, "podinfo", "invalid")
			st.SetArtifact(manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: kind, LocalPath: dir})
			cli, err := NewClient(cacheroot.New(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			cli.SetSourceResolver(NewStoreSourceResolver(st))
			_, err = cli.LoadChart(t.Context(), hr)
			_, typed := errors.AsType[chart.ValidationError](err)
			if !typed || !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("validation chain = %v", err)
			}
		})
	}
}

func TestLoadChart_ValuesContentWithoutTemplateCache(t *testing.T) {
	st, hr, first := ociRenderFixture(t)
	hr = hr.Clone()
	hr.ChartRef = nil
	hr.Chart.RepoKind = manifest.KindGitRepository
	second := t.TempDir()
	writeChartFiles(t, second, "podinfo", "6.15.0")
	testutil.WriteFile(t, second, "prod.yaml", "marker: second\n")
	cli, err := NewClientWithOptions(cacheroot.New(t.TempDir()), ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	var previous string
	for i, dir := range []string{first, second} {
		st.SetArtifact(manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: dir})
		loaded, err := cli.LoadChart(t.Context(), hr)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Fingerprint == "" || loaded.Fingerprint == previous {
			t.Fatal("different chart content shared a fingerprint")
		}
		merged, err := cli.mergeChartValuesFiles(loaded, hr.ChartValuesFiles, false)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, merged["marker"], any([]string{"original", "second"}[i]))
		previous = loaded.Fingerprint
	}
}
