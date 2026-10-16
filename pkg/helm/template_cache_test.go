package helm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	chartcommon "helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"oras.land/oras-go/v2/errdef"
)

func renderPersistenceEligible(t testing.TB) bool {
	t.Helper()
	info, ok := debug.ReadBuildInfo()
	eligible := false
	if ok {
		const module = "github.com/home-operations/flate"
		eligible = info.Main.Path == module && info.Main.Version != "" && info.Main.Version != "(devel)"
		for _, dep := range info.Deps {
			if dep.Path == module && dep.Version != "" && dep.Version != "(devel)" {
				eligible = true
			}
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				eligible = true
			}
		}
		for _, dep := range info.Deps {
			if dep.Replace != nil && dep.Replace.Version == "" {
				eligible = false
			}
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.modified" && setting.Value == "true" {
				eligible = false
			}
		}
	}
	return eligible
}

func TestTemplateCache_OCIIdentity(t *testing.T) {
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	const next = "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	shared := digest[:len(digest)-1] + "9"
	initial := store.SourceArtifact{Digest: digest, Revision: "6.15.0@" + digest}
	for _, disable := range []bool{false, true} {
		for _, tc := range []struct {
			name                                   string
			next                                   store.SourceArtifact
			wantSameOutput, wantFingerprintChanged bool
			marker                                 string
		}{
			{name: "unchanged", next: initial, wantSameOutput: true},
			{name: "same suffix", next: store.SourceArtifact{Digest: shared, Revision: "6.15.0@" + shared}, wantSameOutput: true, wantFingerprintChanged: true},
			{name: "digest only", next: store.SourceArtifact{Digest: shared, Revision: initial.Revision}, wantSameOutput: true, wantFingerprintChanged: true},
			{name: "revision only", next: store.SourceArtifact{Digest: digest, Revision: "latest@" + digest}, wantSameOutput: true, wantFingerprintChanged: true},
			{name: "changed suffix", next: store.SourceArtifact{Digest: next, Revision: "6.15.0@" + next}, wantSameOutput: disable, wantFingerprintChanged: true},
			{name: "new values", next: store.SourceArtifact{Digest: shared, Revision: "6.15.0@" + shared}, marker: "updated", wantFingerprintChanged: true},
		} {
			t.Run(fmt.Sprintf("disabled_%t/%s", disable, tc.name), func(t *testing.T) {
				st, hr, dir := ociRenderFixture(t)
				original := hr.Clone()
				cli, err := NewClient(newTestCache(t, t.TempDir()))
				if err != nil {
					t.Fatal(err)
				}
				cli.SetSourceResolver(NewStoreSourceResolver(st))
				opts := Options{DisableChartDigestTracking: disable}
				render := func(identity store.SourceArtifact) (string, string, string) {
					t.Helper()
					identity.Kind, identity.LocalPath, identity.FS = manifest.KindOCIRepository, dir, os.DirFS(dir)
					st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &identity)
					out, err := cli.Template(t.Context(), hr, nil, opts)
					if err != nil {
						t.Fatal(err)
					}
					loaded, err := cli.loadChart(t.Context(), hr)
					if err != nil {
						t.Fatal(err)
					}
					assert.Equal(t, loaded.chart.Metadata.Version, "6.15.0")
					assert.Equal(t, loaded.chart.Dependencies()[0].Metadata.Version, "1.2.3+child")
					loaded.oci = &ociChartContent{Digest: identity.Digest, Revision: identity.Revision, DisableTracking: disable}
					valuesKey := chartValuesCacheKey(loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
					merged, ok := cli.chartValuesCache[valuesKey]
					if !ok || valuesKey == "" {
						t.Fatal("render did not cache values under its effective fingerprint")
					}
					key := computeTemplateKey(loaded, merged, opts, hr)
					repeated, err := cli.Template(t.Context(), hr, nil, opts)
					if err != nil || repeated != out {
						t.Fatalf("repeat render differs: %v", err)
					}
					return out, key, valuesKey
				}
				first, key, valuesKey := render(initial)
				if !strings.Contains(first, `marker: "original"`) {
					t.Fatal(first)
				}
				if tc.marker != "" {
					template, err := os.ReadFile(filepath.Join(dir, "templates/cm.yaml"))
					if err != nil {
						t.Fatal(err)
					}
					dir = t.TempDir()
					writeChartFiles(t, dir, "podinfo", "6.15.0")
					testutil.WriteFile(t, dir, "values.yaml", "marker: default\n")
					testutil.WriteFile(t, dir, "prod.yaml", "marker: "+tc.marker+"\n")
					testutil.WriteFile(t, dir, "charts/child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.2.3+child\n")
					testutil.WriteFile(t, dir, "templates/cm.yaml", string(template))
				}
				second, nextKey, nextValuesKey := render(tc.next)
				assert.Equal(t, first == second, tc.wantSameOutput)
				assert.Equal(t, key != nextKey, tc.wantFingerprintChanged)
				assert.Equal(t, valuesKey != nextValuesKey, tc.wantFingerprintChanged)
				if tc.marker != "" && !strings.Contains(second, `marker: "`+tc.marker+`"`) {
					t.Fatal(second)
				}
				assert.Diff(t, hr, original)
				for _, revision := range []string{"6.14.0@" + next, "invalid"} {
					st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Digest: next, Revision: revision})
					if _, err := cli.Template(t.Context(), hr, nil, opts); !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
						t.Fatalf("invalid revision bypassed validation: %v", err)
					}
				}
			})
		}
	}

	t.Run("one admitted artifact", func(t *testing.T) {
		st, hr, dir := ociRenderFixture(t)
		reads := map[string]int{}
		input := os.DirFS(dir)
		observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) { reads[name]++; return input.Open(name) }}
		first := &store.SourceArtifact{FS: observed, Key: "untrusted", Revision: "6.15.0@" + digest, Digest: digest}
		second := &store.SourceArtifact{Revision: "invalid", FS: fstest.MapFS{}}
		resolver := &changingChartResolver{SourceResolver: NewStoreSourceResolver(st), artifacts: []*store.SourceArtifact{first, second}}
		cli, err := NewClient(newTestCache(t, ""))
		assert.NoError(t, err)
		cli.SetSourceResolver(resolver)
		out, err := cli.Template(t.Context(), hr, nil, Options{})
		assert.NoError(t, err)
		assert.Equal(t, resolver.calls, 1)
		assert.Equal(t, reads[".helmignore"], 1)
		assert.Equal(t, reads["templates/cm.yaml"], 1)
		assert.Equal(t, strings.Contains(out, "6.15.0+ff3d3e14728f"), true)
		assert.Equal(t, first.Key, "untrusted")
	})
}

func TestTemplateCache_NonOCITrackingMode(t *testing.T) {
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindGitRepository, manifest.KindBucket, manifest.KindExternalArtifact, manifest.KindHelmChart} {
		t.Run(kind, func(t *testing.T) {
			st, hr, dir := ociRenderFixture(t)
			hr = hr.Clone()
			hr.ChartRef = nil
			hr.Chart.RepoKind = kind
			if kind == manifest.KindHelmChart {
				hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: kind, Name: "podinfo"}
			}
			st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: kind, FS: os.DirFS(dir), LocalPath: dir})
			cli, err := NewClient(newTestCache(t, ""))
			if err != nil {
				t.Fatal(err)
			}
			cli.SetSourceResolver(NewStoreSourceResolver(st))
			var first, key string
			for _, disabled := range []bool{false, true} {
				opts := Options{DisableChartDigestTracking: disabled}
				out, err := cli.Template(t.Context(), hr, nil, opts)
				if err != nil {
					t.Fatal(err)
				}
				loaded, err := cli.loadChart(t.Context(), hr)
				if err != nil {
					t.Fatal(err)
				}
				merged, err := cli.mergeChartValuesFiles(loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
				if err != nil {
					t.Fatal(err)
				}
				nextKey := computeTemplateKey(loaded, merged, opts, hr)
				if first != "" {
					assert.Equal(t, out, first)
					assert.Equal(t, nextKey, key)
				}
				if !strings.Contains(out, `version: "6.15.0"`) {
					t.Fatal(out)
				}
				first, key = out, nextKey
			}
		})
	}
}

func TestComputeTemplateKey_StableSameInputs(t *testing.T) {
	ch := minimalChart()
	hr := minimalHR()
	values1 := map[string]any{"a": 1, "b": 2}
	values2 := map[string]any{"b": 2, "a": 1} // different in-memory order
	opts := Options{KubeVersion: "1.33"}

	k1 := computeTemplateKey(chartLoadResult{fingerprint: manifest.Fingerprint(projectChart(ch)), chart: ch}, values1, opts, hr)
	k2 := computeTemplateKey(chartLoadResult{fingerprint: manifest.Fingerprint(projectChart(ch)), chart: ch}, values2, opts, hr)
	if k1 != k2 {
		t.Errorf("same logical values must produce same key:\nk1=%s\nk2=%s", k1, k2)
	}
}

func TestComputeTemplateKey_DifferingFieldsDiverge(t *testing.T) {
	ch, hr := minimalChart(), minimalHR()
	values := map[string]any{"k": "v"}
	base := computeTemplateKey(chartLoadResult{fingerprint: manifest.Fingerprint(projectChart(ch)), chart: ch}, values, Options{}, hr)
	if base == "" {
		t.Fatal("valid inputs produced an empty key")
	}
	excluded := map[string]string{
		"DisableChartDigestTracking": "OCI tracking is projected only for direct OCI releases",
		"SkipKinds":                  "resource filtering occurs downstream of Template",
	}
	typ := reflect.TypeFor[Options]()
	for i := range typ.NumField() {
		field := typ.Field(i)
		t.Run("Options/"+field.Name, func(t *testing.T) {
			var opts Options
			v := reflect.ValueOf(&opts).Elem().Field(i)
			switch v.Kind() {
			case reflect.Bool:
				v.SetBool(true)
			case reflect.String:
				v.SetString("different")
			case reflect.Slice:
				v.Set(reflect.ValueOf([]string{"different"}))
			default:
				t.Fatalf("new option %s requires an explicit cache-key decision", field.Name)
			}
			key := computeTemplateKey(chartLoadResult{fingerprint: manifest.Fingerprint(projectChart(ch)), chart: ch}, values, opts, hr)
			if key == "" {
				t.Fatal("valid option produced an empty key")
			}
			_, skip := excluded[field.Name]
			assert.Equal(t, key == base, skip)
		})
	}
	for _, field := range []string{
		"Name", "Namespace", "HelmReleaseSpec.ReleaseName", "TargetNamespace",
		"CRDsPolicy", "DisableSchemaValidation", "DisableOpenAPIValidation",
		"Install.DisableHooks", "Install.Replace", "Upgrade.DisableHooks", "Test.Enable",
		"Chart.Version", "ChartValuesFiles", "IgnoreMissingValuesFiles", "PostRenderers",
	} {
		t.Run("HelmRelease/"+field, func(t *testing.T) {
			alt := hr.Clone()
			v := reflect.ValueOf(alt).Elem()
			for name := range strings.SplitSeq(field, ".") {
				v = v.FieldByName(name)
				if v.Kind() == reflect.Pointer {
					v.Set(reflect.New(v.Type().Elem()))
					v = v.Elem()
				}
			}
			switch v.Kind() {
			case reflect.Bool:
				v.SetBool(true)
			case reflect.String:
				v.SetString("different")
			case reflect.Slice:
				v.Set(reflect.MakeSlice(v.Type(), 1, 1))
				if v.Type().Elem().Kind() == reflect.String {
					v.Index(0).SetString("different")
				}
			default:
				t.Fatalf("field %s requires an explicit cache-key decision", field)
			}
			key := computeTemplateKey(chartLoadResult{fingerprint: manifest.Fingerprint(projectChart(ch)), chart: ch}, values, Options{}, alt)
			if key == "" || key == base {
				t.Fatal("changed release input did not produce a distinct valid key")
			}
		})
	}
	t.Run("ChartContent", func(t *testing.T) {
		changed := minimalChart()
		changed.Metadata.Version = "9.0.0"
		assert.Equal(t, computeTemplateKey(chartLoadResult{fingerprint: manifest.Fingerprint(projectChart(changed)), chart: changed}, values, Options{}, hr) == base, false)
	})
	for _, tc := range []struct {
		name string
		edit func(*chart.Chart, *chart.Chart)
	}{
		{"template name", func(_, b *chart.Chart) { b.Templates[0].Name += "x" }},
		{"template bytes", func(_, b *chart.Chart) { b.Templates[0].Data = []byte("kind: Namespace\n") }},
		{"template order", func(a, b *chart.Chart) {
			a.Templates = append(a.Templates, &chartcommon.File{Name: "other", Data: []byte("other")})
			b.Templates = []*chartcommon.File{a.Templates[1], a.Templates[0]}
		}},
		{"nil template entry", func(a, b *chart.Chart) { a.Templates = []*chartcommon.File{nil}; b.Templates = []*chartcommon.File{{}} }},
		{"file tuple boundary", func(a, b *chart.Chart) {
			a.Files = []*chartcommon.File{{Name: "a", Data: []byte("bc")}}
			b.Files = []*chartcommon.File{{Name: "ab", Data: []byte("c")}}
		}},
		{"file bytes", func(a, b *chart.Chart) {
			a.Files = []*chartcommon.File{{Name: "a", Data: []byte("one")}}
			b.Files = []*chartcommon.File{{Name: "a", Data: []byte("two")}}
		}},
		{"nil file entry", func(a, b *chart.Chart) { a.Files = []*chartcommon.File{nil}; b.Files = []*chartcommon.File{{}} }},
		{"schema bytes", func(_, b *chart.Chart) { b.Schema = []byte("{}") }},
		{"defaults", func(_, b *chart.Chart) { b.Values["default"] = false }},
		{"dependency", func(a, b *chart.Chart) {
			a.AddDependency(minimalChart())
			child := minimalChart()
			child.Values["default"] = false
			b.AddDependency(child)
		}},
	} {
		t.Run("Chart/"+tc.name, func(t *testing.T) {
			a, b := minimalChart(), minimalChart()
			tc.edit(a, b)
			first, second := chartLoadResult{chart: a, fingerprint: manifest.Fingerprint(projectChart(a))}, chartLoadResult{chart: b, fingerprint: manifest.Fingerprint(projectChart(b))}
			key := computeTemplateKey(first, nil, Options{}, hr)
			valuesKey := chartValuesCacheKey(first, []string{"values.yaml"}, false)
			if key == "" || key == computeTemplateKey(second, nil, Options{}, hr) || valuesKey == "" || valuesKey == chartValuesCacheKey(second, []string{"values.yaml"}, false) {
				t.Fatal("distinct chart content shared a cache key")
			}
		})
	}
	t.Run("Chart/nil", func(t *testing.T) {
		first, second := chartLoadResult{}, chartLoadResult{fingerprint: manifest.Fingerprint(projectChart(&chart.Chart{}))}
		assert.Equal(t, computeTemplateKey(first, nil, Options{}, hr) == computeTemplateKey(second, nil, Options{}, hr), false)
		assert.Equal(t, chartValuesCacheKey(first, nil, false) == chartValuesCacheKey(second, nil, false), false)
	})

	t.Run("Values", func(t *testing.T) {
		assert.Equal(t, computeTemplateKey(chartLoadResult{fingerprint: manifest.Fingerprint(projectChart(ch)), chart: ch}, map[string]any{"k": "different"}, Options{}, hr) == base, false)
	})
}

// TestComputeTemplateKey_CommonMetadataExcluded pins the cache boundary:
// spec.commonMetadata is applied DOWNSTREAM of Template (the controller's
// helm.ApplyHRCommonMetadata, on both the fresh-render and dedup-replay
// paths), never reaches the cached render, and so must NOT participate in
// the key — otherwise every CommonMetadata-only change forces a spurious
// full re-render. The control subtest asserts PostRenderers (which DOES
// change the rendered bytes) still diverges the key, so this test cannot
// pass by accident if a genuinely render-affecting input were dropped.
func TestComputeTemplateKey_CommonMetadataExcluded(t *testing.T) {
	ch := minimalChart()
	values := map[string]any{"k": "v"}
	opts := Options{KubeVersion: "1.33"}
	base := minimalHR()
	baseKey := computeTemplateKey(chartLoadResult{fingerprint: manifest.Fingerprint(projectChart(ch)), chart: ch}, values, opts, base)

	t.Run("CommonMetadata does not change the key", func(t *testing.T) {
		alt := *base
		alt.CommonMetadata = &helmv2.CommonMetadata{
			Labels:      map[string]string{"team": "platform"},
			Annotations: map[string]string{"note": "x"},
		}
		if got := computeTemplateKey(chartLoadResult{fingerprint: manifest.Fingerprint(projectChart(ch)), chart: ch}, values, opts, &alt); got != baseKey {
			t.Errorf("CommonMetadata must NOT affect the cache key (applied downstream of Template):\nbase=%s\nalt =%s", baseKey, got)
		}
	})

	t.Run("control: PostRenderers still diverges", func(t *testing.T) {
		alt := *base
		alt.PostRenderers = []helmv2.PostRenderer{{Kustomize: &helmv2.Kustomize{}}}
		if got := computeTemplateKey(chartLoadResult{fingerprint: manifest.Fingerprint(projectChart(ch)), chart: ch}, values, opts, &alt); got == baseKey {
			t.Error("PostRenderers changes rendered output and must still diverge the key (guards against dropping a real input)")
		}
	})
}

func TestTemplateCache_TemplateIntegration(t *testing.T) {
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
			testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", `{{ if .Values.fail }}{{ fail "cache miss action" }}{{ end }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
data:
  greeting: {{ .Values.greeting }}
`)
			owner := newTestCache(t, root)
			cli, err := NewClient(owner)
			if err != nil {
				t.Fatal(err)
			}
			cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
			first, err := cli.Template(t.Context(), newHR(), map[string]any{"greeting": "hi"}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			second, err := cli.Template(t.Context(), newHR(), map[string]any{"greeting": "hi"}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, second, first)
			third, err := cli.Template(t.Context(), newHR(), map[string]any{"greeting": "different"}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, third == first, false)
			loaded, err := cli.loadChart(t.Context(), newHR())
			if err != nil {
				t.Fatal(err)
			}
			values := map[string]any{"greeting": "hi", "fail": true}
			key := cache.BlobKey{Content: computeTemplateKey(loaded, values, Options{}, newHR()), PersistOnly: true}
			r, _, _, err := owner.Blob(t.Context(), key, cache.Policy{MaxAge: 0}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
				_, err := io.WriteString(w, first)
				return ocispec.Descriptor{}, err
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			cli, err = NewClient(newTestCache(t, root))
			if err != nil {
				t.Fatal(err)
			}
			cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
			out, err := cli.Template(t.Context(), newHR(), values, Options{})
			if root != "" && renderPersistenceEligible(t) {
				if err != nil || out != first {
					t.Fatalf("completed hit: output=%q, error=%v", out, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "cache miss action") {
				t.Fatalf("ineligible output did not bypass persistence: %v", err)
			}
			values["greeting"] = "changed"
			if _, err := cli.Template(t.Context(), newHR(), values, Options{}); err == nil || !strings.Contains(err.Error(), "cache miss action") {
				t.Fatalf("changed input did not miss completed output: %v", err)
			}
		})
	}
}

// minimalChart returns a tiny *chart.Chart fixture for the key-derivation
// tests; it shares the same shape the real chart-loading path produces.
func minimalChart() *chart.Chart {
	return &chart.Chart{
		Metadata: &chart.Metadata{Name: "test", Version: "0.1.0", APIVersion: "v2"},
		Templates: []*chartcommon.File{
			{Name: "templates/cm.yaml", Data: []byte("kind: ConfigMap\n")},
		},
		Values: map[string]any{"default": true},
	}
}

// minimalHR returns a tiny *manifest.HelmRelease for the key-derivation
// tests. Mirrors the structural shape Template's call sites observe.
func minimalHR() *manifest.HelmRelease {
	return &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "default",
	}
}

func TestTemplateCache_ChartDigestTrackingOptions(t *testing.T) {
	st, hr, dir := ociRenderFixture(t)
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	st.SetArtifact(manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"},
		&store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Digest: digest, Revision: "6.15.0@" + digest})
	owner := newTestCache(t, "")
	newClient := func() *Client {
		cli, err := NewClient(owner)
		if err != nil {
			t.Fatal(err)
		}
		cli.SetSourceResolver(NewStoreSourceResolver(st))
		return cli
	}
	cli := newClient()
	for _, disable := range []bool{false, true, false, true, false} {
		opts := Options{DisableChartDigestTracking: disable}
		out, err := cli.Template(t.Context(), hr, nil, opts)
		if err != nil {
			t.Fatal(err)
		}
		version := "6.15.0"
		if !disable {
			version += "+ff3d3e14728f"
		}
		if !strings.Contains(out, `version: "`+version+`"`) {
			t.Fatalf("disable=%v returned incompatible output:\n%s", disable, out)
		}
		fresh, err := newClient().Template(t.Context(), hr, nil, opts)
		if err != nil || fresh != out {
			t.Fatalf("fresh disable=%v result differs: err=%v output=%s", disable, err, fresh)
		}
	}
}

func TestTemplateCache_StorageFailures(t *testing.T) {
	if !renderPersistenceEligible(t) {
		t.Skip("storage faults require an eligible build")
	}
	for _, fault := range []string{"lookup", "publication"} {
		t.Run(fault, func(t *testing.T) {
			chartDir := t.TempDir()
			testutil.WriteFile(t, chartDir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
			testutil.WriteFile(t, chartDir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fresh}\n")
			root := t.TempDir()
			owner := newTestCache(t, root)
			cli, err := NewClient(owner)
			if err != nil {
				t.Fatal(err)
			}
			cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", chartDir))
			hr := newHR()
			if fault == "lookup" {
				if _, err := cli.Template(t.Context(), hr, nil, Options{}); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.RemoveAll(root); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				out, err := cli.Template(t.Context(), hr, nil, Options{})
				if out != "" || !errors.Is(err, manifest.ErrFlux) || errors.Is(err, errdef.ErrNotFound) {
					t.Fatalf("%s fault returned output %q, error %v", fault, out, err)
				}
			}
		})
	}
}

type changingChartResolver struct {
	SourceResolver
	artifacts []*store.SourceArtifact
	calls     int
}

func (r *changingChartResolver) LocalSourceArtifact(manifest.NamedResource) *store.SourceArtifact {
	artifact := r.artifacts[min(r.calls, len(r.artifacts)-1)]
	r.calls++
	return artifact
}
