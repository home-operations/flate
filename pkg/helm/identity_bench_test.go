package helm

import (
	"fmt"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
	"testing"
)

func BenchmarkLoadChart_Mode(b *testing.B) {
	for _, cache := range []bool{false, true} {
		for _, warm := range []bool{false, true} {
			b.Run(fmt.Sprintf("cache_%t/warm_%t", cache, warm), func(b *testing.B) {
				dir := stageBenchChartDir(b)
				layout := cacheroot.New(b.TempDir())
				resolver := benchLocalChartResolver(b, "chart-repo", "flux-system", dir)
				hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Chart: manifest.HelmChart{Name: "mychart", RepoName: "chart-repo", RepoNamespace: "flux-system", RepoKind: manifest.KindGitRepository}}
				newClient := func() *Client {
					opts := ClientOptions{}
					if cache {
						opts.TemplateCacheBytes = 1 << 20
					}
					cli, err := NewClientWithOptions(layout, opts)
					if err != nil {
						b.Fatal(err)
					}
					cli.SetSourceResolver(resolver)
					return cli
				}
				cli := newClient()
				if warm {
					if _, err := cli.LoadChart(b.Context(), hr); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				for b.Loop() {
					if !warm {
						cli = newClient()
					}
					if _, err := cli.LoadChart(b.Context(), hr); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkTemplate_OCIIdentity(b *testing.B) {
	for _, cache := range []bool{false, true} {
		for _, disabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("cache_%t/disabled_%t", cache, disabled), func(b *testing.B) {
				st, hr, dir := ociRenderFixture(b)
				const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
				st.SetArtifact(manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: dir, Digest: digest, Revision: "6.15.0@" + digest})
				clientOpts := ClientOptions{}
				if cache {
					clientOpts.TemplateCacheBytes = 1 << 20
				}
				cli, err := NewClientWithOptions(cacheroot.New(b.TempDir()), clientOpts)
				if err != nil {
					b.Fatal(err)
				}
				cli.SetSourceResolver(NewStoreSourceResolver(st))
				opts := Options{DisableChartDigestTracking: &disabled}
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
	st.SetArtifact(manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: dir})
	cli, err := NewClient(cacheroot.New(b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	loaded, err := cli.LoadChart(b.Context(), hr)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := cli.mergeChartValuesFiles(loaded, hr.ChartValuesFiles, false); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := cli.mergeChartValuesFiles(loaded, hr.ChartValuesFiles, false); err != nil {
			b.Fatal(err)
		}
	}
}
