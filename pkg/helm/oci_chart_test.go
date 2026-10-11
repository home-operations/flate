package helm

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestOCIChartVersion_RevisionSemantics(t *testing.T) {
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	for _, tc := range []struct {
		name, version, revision, want string
	}{
		{"tag", "6.15.0", "6.15.0@" + digest, "6.15.0+ff3d3e14728f"},
		{"digest pin", "6.15.0", digest, "6.15.0+ff3d3e14728f"},
		{"metadata replacement", "6.15.0+old", "6.15.0@" + digest, "6.15.0+ff3d3e14728f"},
		{"prerelease", "6.15.0-rc.1+old", "6.15.0-rc.1_old@" + digest, "6.15.0-rc.1+ff3d3e14728f"},
		{"underscore tag", "6.15.0+build.1", "6.15.0_build.1@" + digest, "6.15.0+ff3d3e14728f"},
		{"metadata equality", "6.15.0+old", "6.15.0_other@" + digest, "6.15.0+ff3d3e14728f"},
		{"normalized version", "v6.15", "v6.15@" + digest, "6.15.0+ff3d3e14728f"},
		{"non-semver tag", "6.15.0", "latest@" + digest, "6.15.0+ff3d3e14728f"},
		{"invalid version", "invalid", digest, ""},
		{"mismatched tag", "6.15.0", "6.14.0@" + digest, ""},
		{"mismatched prerelease", "6.15.0-rc.1", "6.15.0@" + digest, ""},
		{"missing digest", "6.15.0", "6.15.0", ""},
		{"missing revision", "6.15.0", "", ""},
		{"missing separator", "6.15.0", "sha256ff3d3e14728f", ""},
		{"short digest", "6.15.0", "sha256:ff3d3e14728", ""},
		{"multiple separators", "6.15.0", "sha256:ff3d3e14728f:extra", ""},
		{"multiple tags", "6.15.0", "6.15.0@extra@" + digest, ""},
		{"invalid metadata", "6.15.0", "sha256:ff3d3e14728_", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ociChartVersion(tc.version, tc.revision)
			if tc.want == "" {
				if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("validation error = %v, want ErrInput and ErrFlux", err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("version = %q, err = %v, want %q", got, err, tc.want)
			}
		})
	}
}

func TestTemplate_OCIConcurrentIdentities(t *testing.T) {
	for _, mode := range []struct {
		name    string
		disable bool
	}{{"tracking", false}, {"tracking off", true}} {
		t.Run(mode.name, func(t *testing.T) {

			st, hr, dir := ociRenderFixture(t)
			cli, err := NewClientWithOptions(cacheroot.New(t.TempDir()), ClientOptions{})
			if err != nil {
				t.Fatal(err)
			}
			cli.SetSourceResolver(NewStoreSourceResolver(st))
			hrB := hr.Clone()
			hrB.Name = "second"
			hrB.Chart.RepoName = "second-source"
			hrB.ChartRef.Name = "second-source"
			st.AddObject(&manifest.OCIRepository{Name: "second-source", Namespace: "apps", URL: "oci://example.test/podinfo"})
			const digestA = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
			const digestB = "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
			for i, release := range []*manifest.HelmRelease{hr, hrB} {
				digest := []string{digestA, digestB}[i]
				st.SetArtifact(manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "apps", Name: release.Chart.RepoName},
					&store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: dir, Revision: "6.15.0@" + digest, Digest: digest})
			}
			originals := []*manifest.HelmRelease{hr.Clone(), hrB.Clone()}
			ts := task.NewBounded(2)
			var expected [2]string
			for round := range 3 {
				entered := make(chan struct{}, 2)
				start := make(chan struct{})
				var output [2]string
				var errs [2]error
				for i, release := range []*manifest.HelmRelease{hr, hrB} {
					ts.Go(t.Context(), release.Name, func(ctx context.Context) {
						entered <- struct{}{}
						select {
						case <-start:
						case <-ctx.Done():
							errs[i] = ctx.Err()
							return
						}
						output[i], errs[i] = cli.Template(ctx, release, nil, Options{DisableChartDigestTracking: mode.disable})
					})
				}
				<-entered
				<-entered
				close(start)
				ts.BlockTillDone()
				if ts.Failures() != 0 {
					t.Fatal("render task panicked")
				}
				for i := range output {
					version := []string{"6.15.0+ff3d3e14728f", "6.15.0+abcdef123456"}[i]
					if mode.disable {
						version = "6.15.0"
					}
					if errs[i] != nil || !strings.Contains(output[i], version) {
						t.Fatalf("parallel render %d = %q, err = %v", i, output[i], errs[i])
					}
					if round > 0 && output[i] != expected[i] {
						t.Fatalf("parallel render %d changed bytes", i)
					}
				}
				expected = output
			}
			for i, release := range []*manifest.HelmRelease{hr, hrB} {
				if diff := cmp.Diff(originals[i], release); diff != "" {
					t.Fatalf("release mutated (-want +got):\n%s", diff)
				}
			}
			canonical := cli.chartCache[dir].chart
			if canonical.Metadata.Version != "6.15.0" || canonical.Dependencies()[0].Metadata.Version != "1.2.3+child" {
				t.Fatal("parallel renders mutated canonical metadata")
			}

		})
	}
}

func TestTemplate_OCIInvalidChartVersion(t *testing.T) {
	for _, mode := range []struct {
		name    string
		disable bool
	}{{"tracking", false}, {"tracking off", true}} {
		t.Run(mode.name, func(t *testing.T) {

			st, hr, dir := ociRenderFixture(t)
			const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
			st.SetArtifact(manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"},
				&store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: dir, Revision: "6.15.0@" + digest, Digest: digest})
			cli, err := NewClient(cacheroot.New(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			cli.SetSourceResolver(NewStoreSourceResolver(st))
			if _, err := cli.Template(t.Context(), hr, nil, Options{DisableChartDigestTracking: mode.disable}); err != nil {
				t.Fatal(err)
			}
			writeChartFiles(t, dir, "podinfo", "invalid")
			if _, err := cli.Template(t.Context(), hr, nil, Options{DisableChartDigestTracking: mode.disable}); !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("invalid chart version bypassed validation: %v", err)
			}

		})
	}
}

func writeChartFiles(t testing.TB, root, name, version string) {
	t.Helper()
	testutil.WriteFile(t, root, "Chart.yaml",
		"apiVersion: v2\nname: "+name+"\nversion: "+version+"\n")
	testutil.WriteFile(t, root, "templates/_helpers.tpl", "")
}
