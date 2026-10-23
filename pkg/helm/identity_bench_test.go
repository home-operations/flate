package helm

import (
	"fmt"
	"os"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkLoadChart_Mode(b *testing.B) {
	for _, disk := range []bool{false, true} {
		for _, warm := range []bool{false, true} {
			b.Run(fmt.Sprintf("disk_%t/warm_%t", disk, warm), func(b *testing.B) {
				dir := stageBenchChartDir(b)
				root := ""
				if disk {
					root = b.TempDir()
				}
				owner := cachetest.New(b, root, sourcecache.New)
				resolver := benchChartResolver(b, dir, os.DirFS(dir))
				hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Chart: manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Name: "chart-repo", Namespace: "flux-system", Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}}}
				newClient := func() *Client {
					cli, err := NewClient(owner)
					if err != nil {
						b.Fatal(err)
					}
					cli.SetSourceResolver(resolver)
					return cli
				}
				cli := newClient()
				if warm {
					if _, err := loadDecodedChart(b.Context(), cli, hr); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				for b.Loop() {
					if !warm {
						cli = newClient()
					}
					if _, err := loadDecodedChart(b.Context(), cli, hr); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkTemplate_OCIIdentity(b *testing.B) {
	for _, disk := range []bool{false, true} {
		for _, disabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("disk_%t/disabled_%t", disk, disabled), func(b *testing.B) {
				st, hr, dir := ociRenderFixture(b)
				const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
				st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Digest: digest, Revision: "6.15.0@" + digest})
				root := ""
				if disk {
					root = b.TempDir()
				}
				cli, err := NewClient(cachetest.New(b, root, sourcecache.New))
				if err != nil {
					b.Fatal(err)
				}
				cli.SetSourceResolver(NewStoreSourceResolver(st))
				opts := Options{DisableChartDigestTracking: disabled}
				if _, err := cli.Template(b.Context(), hr, nil, opts); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := cli.Template(b.Context(), hr, nil, opts); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkChartValues_Cached(b *testing.B) {
	st, hr, dir := ociRenderFixture(b)
	st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir})
	cli, err := NewClient(cachetest.New(b, "", sourcecache.New))
	if err != nil {
		b.Fatal(err)
	}
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	loaded, err := loadDecodedChart(b.Context(), cli, hr)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := cli.mergeChartValuesFiles(b.Context(), loaded, hr.ChartValuesFiles, false); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := cli.mergeChartValuesFiles(b.Context(), loaded, hr.ChartValuesFiles, false); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChartValues_PinnedSource(b *testing.B) {
	dir := stageBenchChartDir(b)
	names := make([]string, 10)
	for i := range names {
		names[i] = fmt.Sprintf("config/values-%d.yaml", i)
		testutil.WriteFile(b, dir, names[i], fmt.Sprintf("layer-%d:\n  key: value-%d\nshared:\n  nested-%d: %d\n", i, i, i, i))
	}
	owner := cachetest.New(b, "", sourcecache.New)
	cli, err := NewClient(owner)
	if err != nil {
		b.Fatal(err)
	}
	cli.SetSourceResolver(benchChartResolver(b, dir, acquireChartFS(b, owner, os.DirFS(dir))))
	hr := newHR()
	hr.ChartValuesFiles = names
	loaded, err := loadDecodedChart(b.Context(), cli, hr)
	if err != nil || loaded.valuesPin == "" {
		b.Fatalf("source values are not pinned: %v", err)
	}
	merged, err := cli.mergeChartValuesFiles(b.Context(), loaded, hr.ChartValuesFiles, false)
	if err != nil || len(merged) != 11 {
		b.Fatalf("prime selected values: %v, got %v", err, merged)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := cli.mergeChartValuesFiles(b.Context(), loaded, hr.ChartValuesFiles, false); err != nil {
			b.Fatal(err)
		}
	}
}
