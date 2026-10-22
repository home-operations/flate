package helm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	chartcommon "helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"oras.land/oras-go/v2/errdef"
)

func renderPersistenceEligible(tb testing.TB) bool {
	tb.Helper()
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
	t.Parallel()
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
				t.Parallel()
				st, hr, dir := ociRenderFixture(t)
				original := hr.Clone()
				cli, err := NewClient(cachetest.New(t, t.TempDir(), cache.New))
				if err != nil {
					t.Fatal(err)
				}
				cli.SetSourceResolver(NewStoreSourceResolver(st))
				opts := Options{DisableChartDigestTracking: disable}
				held := acquireChartFS(t, cli.cache, os.DirFS(dir))
				var canonical *chart.Chart
				render := func(identity store.SourceArtifact) (string, string, string) {
					t.Helper()
					identity.Kind, identity.LocalPath, identity.FS = manifest.KindOCIRepository, dir, held
					st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &identity)
					out, err := cli.Template(t.Context(), hr, nil, opts)
					if err != nil {
						t.Fatal(err)
					}
					loaded, err := loadDecodedChart(t.Context(), cli, hr)
					if err != nil {
						t.Fatal(err)
					}
					if canonical == nil {
						canonical = loaded.chart
					}
					assert.Equal(t, loaded.chart == canonical, true)
					assert.Equal(t, loaded.artifact.Revision, identity.Revision)
					assert.Equal(t, loaded.artifact.Digest, identity.Digest)
					assert.Equal(t, loaded.chart.Metadata.Version, "6.15.0")
					assert.Equal(t, loaded.chart.Dependencies()[0].Metadata.Version, "1.2.3+child")
					loaded.oci = &ociChartContent{Digest: identity.Digest, Revision: identity.Revision, DisableTracking: disable}
					valuesKey := chartValuesCacheKey(loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles, nil)
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
					t.Run("updated acquisition", func(t *testing.T) { held = acquireChartFS(t, cli.cache, os.DirFS(dir)) })
					canonical = nil
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
					st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: held, LocalPath: dir, Digest: next, Revision: revision})
					if _, err := cli.Template(t.Context(), hr, nil, opts); !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
						t.Fatalf("invalid revision bypassed validation: %v", err)
					}
				}
			})
		}
	}

	t.Run("one admitted artifact", func(t *testing.T) {
		t.Parallel()
		st, hr, dir := ociRenderFixture(t)
		reads := map[string]int{}
		input := os.DirFS(dir)
		observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) { reads[name]++; return input.Open(name) }}
		first := &store.SourceArtifact{FS: observed, Key: "untrusted", Revision: "6.15.0@" + digest, Digest: digest}
		second := &store.SourceArtifact{Revision: "invalid", FS: fstest.MapFS{}}
		resolver := &changingChartResolver{SourceResolver: NewStoreSourceResolver(st), artifacts: []*store.SourceArtifact{first, second}}
		cli, err := NewClient(cachetest.New(t, "", cache.New))
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

func TestTemplateCache_SubchartPersistence(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build to exercise persisted renders")
	}
	t.Log("persistence eligible: true")
	st, hr, dir := ociRenderFixture(t)
	testutil.WriteFile(t, dir, "Chart.yaml", `apiVersion: v2
name: podinfo
version: 6.15.0
dependencies:
  - name: child
    version: 1.2.3+child
    condition: child.enabled
  - name: sibling
    version: 1.0.0
    condition: sibling.enabled
`)
	for _, name := range []string{"child", "sibling"} {
		version := "1.0.0"
		if name == "child" {
			version = "1.2.3+child"
		}
		testutil.WriteFile(t, dir, "charts/"+name+"/Chart.yaml", "apiVersion: v2\nname: "+name+"\nversion: "+version+"\n")
		testutil.WriteFile(t, dir, "charts/"+name+"/values.yaml", "enabled: true\nmarker: original\n")
		testutil.WriteFile(t, dir, "charts/"+name+"/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Chart.Name }}
data:
  marker: {{ .Values.marker | quote }}
`)
	}
	input := acquireChartFS(t, cachetest.New(t, t.TempDir(), cache.New), os.DirFS(dir))
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{
		Kind: manifest.KindOCIRepository, FS: input, Digest: digest, Revision: "6.15.0@" + digest,
	})
	newClient := func(t *testing.T, owner *cache.Cache, reverse bool) *Client {
		t.Helper()
		_, pin, err := source.OperationView(t.Context(), input)
		assert.NoError(t, err)
		if pin == "" {
			t.Fatal("fixture did not acquire an immutable operation pin")
		}
		selector := manifest.Fingerprint(struct {
			Pin, Path string
			Auto      bool
		}{Pin: pin, Auto: true})
		captured, _, err := captureChart(input, ".")
		assert.NoError(t, err)
		ch, err := captured.load()
		assert.NoError(t, err)
		dependencies := ch.Dependencies()
		slices.SortFunc(dependencies, func(a, b *chart.Chart) int { return strings.Compare(a.Name(), b.Name()) })
		if reverse {
			slices.Reverse(dependencies)
		}
		ch.SetDependencies(dependencies...)
		order := slices.Clone(ch.Dependencies())
		before := cloneChartForRender(ch)
		fingerprint := captured.fingerprint()
		t.Cleanup(func() {
			assert.Equal(t, slices.Equal(ch.Dependencies(), order), true)
			assert.Equal(t, reflect.DeepEqual(ch, before), true)
			assert.Equal(t, captured.fingerprint(), fingerprint)
		})
		cli, err := NewClient(owner)
		assert.NoError(t, err)
		cli.SetSourceResolver(NewStoreSourceResolver(st))
		// Canonical charts MUST remain immutable; choose the loader order before admission.
		cli.chartCache[selector] = chartDescription{chart: ch, captured: captured, key: selector, location: ".", fingerprint: captured.fingerprint(), analysis: &chartAnalysis{Templates: chartTemplateText(ch), Allowed: knownTopLevelKeys(ch), RequiresLoader: requiresChartLoader(ch)}}
		return cli
	}
	keyFor := func(t *testing.T, cli *Client) cache.BlobKey {
		t.Helper()
		loaded, err := loadDecodedChart(t.Context(), cli, hr)
		assert.NoError(t, err)
		loaded.oci = &ociChartContent{Digest: digest, Revision: "6.15.0@" + digest}
		_, err = cli.mergeChartValuesFiles(t.Context(), loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
		assert.NoError(t, err)
		key := computeTemplateKey(loaded, map[string]any{}, Options{}, hr)
		t.Logf("chart fingerprint=%s template key=%s", loaded.fingerprint, key)
		if key == "" {
			t.Fatal("subchart fixture produced an empty completed-output key")
		}
		return cache.BlobKey{Content: key, PersistOnly: true}
	}
	readBlob := func(t *testing.T, owner *cache.Cache, key cache.BlobKey) string {
		t.Helper()
		r, _, _, err := owner.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil)
		assert.NoError(t, err)
		data, err := io.ReadAll(r)
		assert.NoError(t, errors.Join(err, r.Close()))
		var record renderRecord
		assert.NoError(t, json.Unmarshal(data, &record))
		return record.Output
	}
	writeBlob := func(t *testing.T, owner *cache.Cache, key cache.BlobKey, data string) {
		t.Helper()
		captured, _, err := captureChart(input, ".")
		assert.NoError(t, err)
		ch, err := captured.load()
		assert.NoError(t, err)
		analysis := &chartAnalysis{Templates: chartTemplateText(ch), Allowed: knownTopLevelKeys(ch), RequiresLoader: requiresChartLoader(ch)}
		r, _, _, err := owner.Blob(t.Context(), key, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			err := json.NewEncoder(w).Encode(renderRecord{Output: data, Analysis: analysis})
			return cache.ProducerResult{}, err
		})
		assert.NoError(t, err)
		assert.NoError(t, r.Close())
	}
	root := t.TempDir()
	owner := cachetest.New(t, root, cache.New)
	cli := newClient(t, owner, false)
	key := keyFor(t, cli)
	_, _, _, err := owner.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil)
	if !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("cold lookup: got %v, want not found", err)
	}
	t.Log("direct cold lookup: not found")
	cold, err := cli.Template(t.Context(), hr, nil, Options{})
	assert.NoError(t, err)
	for _, name := range []string{"child", "sibling"} {
		if !strings.Contains(cold, "name: "+name+"\n") {
			t.Fatalf("cold render omitted enabled dependency %s", name)
		}
	}
	assert.Equal(t, readBlob(t, owner, key), cold)
	t.Log("direct cold publication: exact render bytes")
	second, err := cli.Template(t.Context(), hr, nil, Options{})
	assert.NoError(t, err)
	assert.Equal(t, second, cold)
	marked := cold + "\n# persisted-subchart-hit\n"
	writeBlob(t, owner, key, marked)
	warm, err := cli.Template(t.Context(), hr, nil, Options{})
	assert.NoError(t, err)
	assert.Equal(t, warm, marked)
	t.Log("same-client marked hit: verified")
	assert.NoError(t, owner.Close())
	owner = cachetest.New(t, root, cache.New)
	cli = newClient(t, owner, true)
	reopenedKey := keyFor(t, cli)
	reader, _, _, lookupErr := owner.Blob(t.Context(), reopenedKey, cache.Policy{MaxAge: cache.Forever}, nil)
	if lookupErr == nil {
		assert.NoError(t, reader.Close())
	}
	t.Logf("fresh-owner direct lookup: %v", lookupErr)
	reopened, err := cli.Template(t.Context(), hr, nil, Options{})
	assert.NoError(t, err)
	if reopened != marked {
		t.Fatalf("fresh owner/client rerendered unchanged subcharts: marker absent=%v, fresh bytes equal cold=%v", !strings.Contains(reopened, "# persisted-subchart-hit"), reopened == cold)
	}
	writeBlob(t, owner, reopenedKey, cold)
	restored, err := cli.Template(t.Context(), hr, nil, Options{})
	assert.NoError(t, err)
	assert.Equal(t, restored, cold)
	assert.Equal(t, reopenedKey.Content, key.Content)
	assert.NoError(t, lookupErr)
	t.Log("verified persisted subchart publication, marked hit, reopen, and restored byte equality")
	writeBlob(t, owner, key, marked)
	for _, field := range []string{"template", "defaults"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			files := fstest.MapFS{}
			assert.NoError(t, fs.WalkDir(os.DirFS(dir), ".", func(name string, entry fs.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				data, err := fs.ReadFile(os.DirFS(dir), name)
				files[name] = &fstest.MapFile{Data: data}
				return err
			}))
			if field == "template" {
				files["charts/child/templates/cm.yaml"].Data = []byte(strings.ReplaceAll(string(files["charts/child/templates/cm.yaml"].Data), "marker:", "edited-template:"))
			} else {
				files["charts/child/values.yaml"].Data = []byte("enabled: true\nmarker: edited-default\n")
			}
			updated := acquireChartFS(t, cachetest.New(t, t.TempDir(), cache.New), files)
			nextStore := store.New()
			nextStore.AddObject(&manifest.OCIRepository{Name: "podinfo", Namespace: "apps"})
			nextStore.SetArtifact(hr.ChartSource(), &store.SourceArtifact{
				Kind: manifest.KindOCIRepository, FS: updated, Digest: digest, Revision: "6.15.0@" + digest,
			})
			next, err := NewClient(owner)
			assert.NoError(t, err)
			next.SetSourceResolver(NewStoreSourceResolver(nextStore))
			nextKey := keyFor(t, next)
			if nextKey.Content == key.Content {
				t.Fatal("edited child reused the original completed-output key")
			}
			_, _, _, err = owner.Blob(t.Context(), nextKey, cache.Policy{MaxAge: cache.Forever}, nil)
			if !errors.Is(err, errdef.ErrNotFound) {
				t.Fatalf("edited child lookup: got %v, want not found", err)
			}
			out, err := next.Template(t.Context(), hr, nil, Options{})
			assert.NoError(t, err)
			if out == cold || strings.Contains(out, "# persisted-subchart-hit") || !strings.Contains(out, "edited-") {
				t.Fatal("edited child reused stale output or omitted its new content")
			}
			assert.Equal(t, readBlob(t, owner, nextKey), out)
			assert.Equal(t, readBlob(t, owner, key), marked)
			t.Log("reacquired child content invalidated the key and rendered fresh bytes with fixed parent/release/OCI identity")
		})
	}
}

func TestTemplateCache_NonOCITrackingMode(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindGitRepository, manifest.KindBucket, manifest.KindExternalArtifact, manifest.KindHelmChart} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			st, hr, dir := ociRenderFixture(t)
			hr = hr.Clone()
			hr.ChartRef = nil
			hr.Chart.Source.Kind = kind
			if kind == manifest.KindHelmChart {
				hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: kind, Name: "podinfo"}
			}
			st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: kind, FS: os.DirFS(dir), LocalPath: dir})
			cli, err := NewClient(cachetest.New(t, "", cache.New))
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
				loaded, err := loadDecodedChart(t.Context(), cli, hr)
				if err != nil {
					t.Fatal(err)
				}
				merged, err := cli.mergeChartValuesFiles(t.Context(), loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
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
	t.Parallel()
	ch := minimalChart()
	hr := minimalHR()
	values1 := map[string]any{"a": 1, "b": 2}
	values2 := map[string]any{"b": 2, "a": 1} // different in-memory order
	opts := Options{KubeVersion: "1.33"}

	k1 := computeTemplateKey(chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch}, values1, opts, hr)
	k2 := computeTemplateKey(chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch}, values2, opts, hr)
	if k1 != k2 {
		t.Errorf("same logical values must produce same key:\nk1=%s\nk2=%s", k1, k2)
	}
}

func TestComputeTemplateKey_DifferingFieldsDiverge(t *testing.T) {
	t.Parallel()
	ch, hr := minimalChart(), minimalHR()
	values := map[string]any{"k": "v"}
	base := computeTemplateKey(chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch}, values, Options{}, hr)
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
			t.Parallel()
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
			key := computeTemplateKey(chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch}, values, opts, hr)
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
			t.Parallel()
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
			key := computeTemplateKey(chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch}, values, Options{}, alt)
			if key == "" || key == base {
				t.Fatal("changed release input did not produce a distinct valid key")
			}
		})
	}
	t.Run("ChartContent", func(t *testing.T) {
		t.Parallel()
		changed := minimalChart()
		changed.Metadata.Version = "9.0.0"
		assert.Equal(t, computeTemplateKey(chartDescription{fingerprint: chartFixtureDescription(t, changed).fingerprint, chart: changed}, values, Options{}, hr) == base, false)
	})

	t.Run("Values", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, computeTemplateKey(chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch}, map[string]any{"k": "different"}, Options{}, hr) == base, false)
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
	t.Parallel()
	ch := minimalChart()
	values := map[string]any{"k": "v"}
	opts := Options{KubeVersion: "1.33"}
	base := minimalHR()
	baseKey := computeTemplateKey(chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch}, values, opts, base)

	t.Run("CommonMetadata does not change the key", func(t *testing.T) {
		t.Parallel()
		alt := *base
		alt.CommonMetadata = &helmv2.CommonMetadata{
			Labels:      map[string]string{"team": "platform"},
			Annotations: map[string]string{"note": "x"},
		}
		if got := computeTemplateKey(chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch}, values, opts, &alt); got != baseKey {
			t.Errorf("CommonMetadata must NOT affect the cache key (applied downstream of Template):\nbase=%s\nalt =%s", baseKey, got)
		}
	})

	t.Run("control: PostRenderers still diverges", func(t *testing.T) {
		t.Parallel()
		alt := *base
		alt.PostRenderers = []helmv2.PostRenderer{{Kustomize: &helmv2.Kustomize{}}}
		if got := computeTemplateKey(chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch}, values, opts, &alt); got == baseKey {
			t.Error("PostRenderers changes rendered output and must still diverge the key (guards against dropping a real input)")
		}
	})
}

func TestTemplateCache_TemplateIntegration(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk", "acquired"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			root := ""
			if backing != "memory" {
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
			owner := cachetest.New(t, root, cache.New)
			cli, err := NewClient(owner)
			if err != nil {
				t.Fatal(err)
			}
			reads := map[string]int{}
			input := os.DirFS(dir)
			chartRoot := ""
			if backing == "acquired" {
				chartRoot = t.TempDir()
				input = acquireChartFS(t, cachetest.New(t, chartRoot, cache.New), input)
			} else {
				base := input
				input = chartOpenFS{FS: base, open: func(name string) (fs.File, error) {
					reads[name]++
					return base.Open(name)
				}}
			}
			st := store.New()
			st.SetArtifact(newHR().ChartSource(), &store.SourceArtifact{FS: input})
			resolver := NewStoreSourceResolver(st)
			cli.SetSourceResolver(resolver)
			first, err := cli.Template(t.Context(), newHR(), map[string]any{"greeting": "hi"}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			canonical, err := loadDecodedChart(t.Context(), cli, newHR())
			assert.NoError(t, err)
			beforeReads := reads["mychart/templates/configmap.yaml"]
			second, err := cli.Template(t.Context(), newHR(), map[string]any{"greeting": "hi"}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, second, first)
			if backing != "acquired" {
				assert.Equal(t, reads["mychart/templates/configmap.yaml"], beforeReads+1)
			}
			warm, err := loadDecodedChart(t.Context(), cli, newHR())
			assert.NoError(t, err)
			assert.Equal(t, warm.chart == canonical.chart, true)
			assert.Equal(t, warm.fingerprint, canonical.fingerprint)
			third, err := cli.Template(t.Context(), newHR(), map[string]any{"greeting": "different"}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, third == first, false)
			loaded, err := loadDecodedChart(t.Context(), cli, newHR())
			if err != nil {
				t.Fatal(err)
			}
			values := map[string]any{"greeting": "hi", "fail": true}
			loaded.inputs, loaded.valuesErr = loaded.observeValuesFiles(t.Context(), newHR().ChartValuesFiles, newHR().IgnoreMissingValuesFiles)
			assert.NoError(t, loaded.valuesErr)
			key := cache.BlobKey{Content: computeTemplateKey(loaded, values, Options{}, newHR()), PersistOnly: true}
			r, _, _, err := owner.Blob(t.Context(), key, cache.Policy{MaxAge: 0}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				err := json.NewEncoder(w).Encode(renderRecord{Output: first, Analysis: loaded.analyze()})
				return cache.ProducerResult{}, err
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
			cli, err = NewClient(cachetest.New(t, root, cache.New))
			if err != nil {
				t.Fatal(err)
			}
			cli.SetSourceResolver(resolver)
			loaded, err = loadDecodedChart(t.Context(), cli, newHR())
			assert.NoError(t, err)
			if backing == "acquired" {
				// Remove chart backing alone so completed output remains readable.
				assert.NoError(t, os.RemoveAll(filepath.Join(chartRoot, "oci", "blobs")))
				_, err := fs.ReadFile(input, "mychart/templates/configmap.yaml")
				if err == nil {
					t.Fatal("backing removal did not prevent chart reads")
				}
				repeated, err := cli.Template(t.Context(), newHR(), map[string]any{"greeting": "hi"}, Options{})
				assert.NoError(t, err)
				assert.Equal(t, repeated, first)
			}
			out, err := cli.Template(t.Context(), newHR(), values, Options{})
			if root != "" && renderPersistenceEligible(t) {
				t.Log("verified persisted completed-output hit")
				if err != nil || out != first {
					t.Fatalf("completed hit: output=%q, error=%v", out, err)
				}
				initial, err := cli.describeChart(t.Context(), newHR())
				assert.NoError(t, err)
				_, err = cli.Template(t.Context(), newHR(), values, Options{})
				assert.NoError(t, err)
				repeated, err := cli.describeChart(t.Context(), newHR())
				assert.NoError(t, err)
				if initial.record == nil || repeated.record != initial.record {
					t.Fatal("repeated hit did not share its typed record")
				}
				alternate := maps.Clone(values)
				alternate["variant"] = true
				loaded.inputs, loaded.valuesErr = loaded.observeValuesFiles(t.Context(), newHR().ChartValuesFiles, newHR().IgnoreMissingValuesFiles)
				assert.NoError(t, loaded.valuesErr)
				alternateKey := cache.BlobKey{Content: computeTemplateKey(loaded, alternate, Options{}, newHR()), PersistOnly: true}
				r, _, _, err := cli.cache.Blob(t.Context(), alternateKey, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
					err := json.NewEncoder(w).Encode(initial.record)
					return cache.ProducerResult{}, err
				})
				assert.NoError(t, err)
				assert.Equal(t, cache.AcquisitionIdentity(r), initial.record.identity)
				assert.NoError(t, r.Close())
				_, err = cli.Template(t.Context(), newHR(), alternate, Options{})
				assert.NoError(t, err)
				distinct, err := cli.describeChart(t.Context(), newHR())
				assert.NoError(t, err)
				if distinct.record == initial.record || distinct.record.key != alternateKey.Content {
					t.Fatal("distinct release key reused a typed record")
				}
				for _, replacement := range []string{"changed output", "{", `{"Output":"missing analysis"}`} {
					r, _, _, err := cli.cache.Blob(t.Context(), alternateKey, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
						var err error
						if replacement == "changed output" {
							err = json.NewEncoder(w).Encode(renderRecord{Output: replacement, Analysis: loaded.analyze()})
						} else {
							_, err = io.WriteString(w, replacement)
						}
						return cache.ProducerResult{}, err
					})
					assert.NoError(t, err)
					assert.NoError(t, r.Close())
					out, err := cli.Template(t.Context(), newHR(), alternate, Options{})
					switch replacement {
					case "changed output":
						assert.NoError(t, err)
						if out != replacement {
							t.Fatal("same-key replacement served stale output")
						}
					case "{":
						if _, ok := errors.AsType[*json.SyntaxError](err); !ok || !errors.Is(err, manifest.ErrFlux) {
							t.Fatalf("malformed replacement was concealed: %v", err)
						}
					default:
						if !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "analysis missing") {
							t.Fatalf("missing analysis was accepted: %v", err)
						}
					}
				}
			} else if err == nil || !strings.Contains(err.Error(), "cache miss action") {
				t.Fatalf("ineligible output did not bypass persistence: %v", err)
			}
			warm, err = loadDecodedChart(t.Context(), cli, newHR())
			assert.NoError(t, err)
			assert.Equal(t, warm.chart == loaded.chart, true)
			assert.Equal(t, warm.fingerprint, loaded.fingerprint)
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
	t.Parallel()
	st, hr, dir := ociRenderFixture(t)
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	st.SetArtifact(manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"},
		&store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: os.DirFS(dir), LocalPath: dir, Digest: digest, Revision: "6.15.0@" + digest})
	owner := cachetest.New(t, "", cache.New)
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
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("storage faults require an eligible build")
	}
	for _, fault := range []string{"lookup", "publication"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			chartDir := t.TempDir()
			testutil.WriteFile(t, chartDir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
			testutil.WriteFile(t, chartDir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fresh}\n")
			root := t.TempDir()
			owner := cachetest.New(t, root, cache.New)
			cli, err := NewClient(owner)
			if err != nil {
				t.Fatal(err)
			}
			cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", chartDir))
			hr := newHR()
			if fault == "lookup" {
				for range 2 {
					_, err := cli.Template(t.Context(), hr, nil, Options{})
					assert.NoError(t, err)
				}
				d, err := cli.describeChart(t.Context(), hr)
				assert.NoError(t, err)
				if d.record == nil {
					t.Fatal("lookup fault fixture did not warm a typed record")
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

func TestTemplateCache_ActionHookPolicy(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Log("render persistence ineligible; running warm/fresh functional assertions without claiming disk hits")
	}
	resolver := localChartResolver(t, "chart-repo", "flux-system", helmChartDirectory(t))
	warm, err := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	assert.NoError(t, err)
	warm.SetSourceResolver(resolver)
	for i, state := range []struct {
		isUpgrade, install, upgrade, noHooks bool
		wantHook                             bool
	}{
		{false, false, true, false, true},
		{true, false, true, false, false},
		{true, false, false, false, true},
		{true, false, false, true, false},
		{false, false, false, true, false},
		{false, false, false, false, true},
		{false, true, false, false, false},
		{false, false, true, false, true},
	} {
		opts := Options{IsUpgrade: state.isUpgrade, NoHooks: state.noHooks}
		want := []string{"ConfigMap/demo-cm"}
		if state.wantHook {
			want = append(want, "ConfigMap/demo-pre")
		}
		var first string
		for repeat := range 2 {
			hr := newHR()
			hr.Install = &helmv2.Install{DisableHooks: state.install}
			hr.Upgrade = &helmv2.Upgrade{DisableHooks: state.upgrade}
			out, err := warm.Template(t.Context(), hr, nil, opts)
			assert.NoError(t, err)
			assert.Diff(t, templateResources(t, out), want)
			fresh, err := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			assert.NoError(t, err)
			fresh.SetSourceResolver(resolver)
			freshHR := newHR()
			freshHR.Install = &helmv2.Install{DisableHooks: state.install}
			freshHR.Upgrade = &helmv2.Upgrade{DisableHooks: state.upgrade}
			uncached, err := fresh.Template(t.Context(), freshHR, nil, opts)
			assert.NoError(t, err)
			assert.Diff(t, templateResources(t, uncached), want)
			assert.Equal(t, out, uncached)
			if repeat == 0 {
				first = out
			} else if out != first {
				t.Errorf("state %d repeated output differs: %q != %q", i, out, first)
			}
		}
	}
}

func sourceValuesCacheFixture(t *testing.T) (string, *store.Store, *manifest.HelmRelease) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "charts/demo/values.yaml", "marker: default\n")
	testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  marker: {{ .Values.marker | quote }}\n")
	testutil.WriteFile(t, root, "config/prod.yaml", "marker: first\n")
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	admitted, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	hr := &manifest.HelmRelease{Name: "demo", Namespace: "apps", Chart: manifest.HelmChart{Name: "charts/demo", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}}, ChartValuesFiles: []string{"config/prod.yaml"}, IgnoreMissingValuesFiles: true}
	st := store.New()
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: admitted.FS})
	return root, st, hr
}

func TestTemplateCache_SourceRootValuesFiles(t *testing.T) {
	t.Parallel()
	root, st, hr := sourceValuesCacheFixture(t)
	cacheRoot := t.TempDir()
	owner := cachetest.New(t, cacheRoot, cache.New)
	cli, err := NewClient(owner)
	assert.NoError(t, err)
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	first, err := cli.Template(t.Context(), hr, nil, Options{})
	assert.NoError(t, err)
	if !strings.Contains(first, `marker: "first"`) {
		t.Fatal(first)
	}
	t.Run("persistent reuse", func(t *testing.T) {
		t.Parallel()
		if !renderPersistenceEligible(t) {
			t.Skipf("requires clean VCS-stamped test binary: %s", cache.RenderPersistenceDisabledReason())
		}
		root, st, hr := sourceValuesCacheFixture(t)
		cacheRoot := t.TempDir()
		owner := cachetest.New(t, cacheRoot, cache.New)
		cli, err := NewClient(owner)
		assert.NoError(t, err)
		cli.SetSourceResolver(NewStoreSourceResolver(st))
		first, err := cli.Template(t.Context(), hr, nil, Options{})
		assert.NoError(t, err)
		initial, err := loadDecodedChart(t.Context(), cli, hr)
		assert.NoError(t, err)
		initial.inputs, initial.valuesErr = initial.observeValuesFiles(t.Context(), hr.ChartValuesFiles, true)
		assert.NoError(t, initial.valuesErr)
		key := computeTemplateKey(initial, map[string]any{}, Options{}, hr)
		if key == "" {
			t.Fatal("fixture has no completed-output key")
		}
		r, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: key, PersistOnly: true}, cache.Policy{MaxAge: cache.Forever}, nil)
		assert.NoError(t, err)
		data, err := io.ReadAll(r)
		assert.NoError(t, errors.Join(err, r.Close()))
		var record renderRecord
		assert.NoError(t, json.Unmarshal(data, &record))
		assert.Equal(t, record.Output, first)
		cached := first + "# persistent hit witness\n"
		r, _, _, err = owner.Blob(t.Context(), cache.BlobKey{Content: key, PersistOnly: true}, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			err := json.NewEncoder(w).Encode(renderRecord{Output: cached, Analysis: initial.analyze()})
			return cache.ProducerResult{}, err
		})
		assert.NoError(t, err)
		assert.NoError(t, r.Close())
		fresh, err := NewClient(cachetest.New(t, cacheRoot, cache.New))
		assert.NoError(t, err)
		fresh.SetSourceResolver(NewStoreSourceResolver(st))
		hit, err := fresh.Template(t.Context(), hr, nil, Options{})
		assert.NoError(t, err)
		assert.Equal(t, hit, cached)
		assertSourceValuesChanges(t, fresh, hr, root)
	})
	t.Run("packaged cache cannot mask missing source values", func(t *testing.T) {
		t.Parallel()
		if !renderPersistenceEligible(t) {
			t.Skipf("requires clean VCS-stamped test binary: %s", cache.RenderPersistenceDisabledReason())
		}
		root, st, hr := sourceValuesCacheFixture(t)
		artifact := st.GetArtifact(hr.ChartSource())
		testutil.WriteFile(t, root, "charts/demo/prod.yaml", "marker: packaged\n")
		hr.Chart.Source.Kind = manifest.KindExternalArtifact
		hr.ChartValuesFiles = []string{"prod.yaml"}
		hr.IgnoreMissingValuesFiles = false
		st.SetArtifact(hr.ChartSource(), artifact)
		cacheRoot := t.TempDir()
		owner := cachetest.New(t, cacheRoot, cache.New)
		cli, err := NewClient(owner)
		assert.NoError(t, err)
		cli.SetSourceResolver(NewStoreSourceResolver(st))
		first, err := cli.Template(t.Context(), hr, nil, Options{})
		assert.NoError(t, err)
		if !strings.Contains(first, `marker: "packaged"`) {
			t.Fatal(first)
		}
		initial, err := cli.describeChart(t.Context(), hr)
		assert.NoError(t, err)
		key := computeTemplateKey(initial, map[string]any{}, Options{}, hr)
		if key == "" {
			t.Fatal("fixture has no completed-output key")
		}
		r, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: key, PersistOnly: true}, cache.Policy{MaxAge: cache.Forever}, nil)
		assert.NoError(t, err)
		data, err := io.ReadAll(r)
		assert.NoError(t, errors.Join(err, r.Close()))
		var record renderRecord
		assert.NoError(t, json.Unmarshal(data, &record))
		assert.Equal(t, record.Output, first)
		if record.Analysis == nil {
			t.Fatal("persisted record has no chart analysis")
		}
		sourceHR := hr.Clone()
		sourceHR.Chart.Source.Kind = manifest.KindGitRepository
		st.SetArtifact(sourceHR.ChartSource(), artifact)
		fresh, err := NewClient(cachetest.New(t, cacheRoot, cache.New))
		assert.NoError(t, err)
		fresh.SetSourceResolver(NewStoreSourceResolver(st))
		out, err := fresh.Template(t.Context(), sourceHR, nil, Options{})
		assert.Equal(t, out, "")
		if !errors.Is(err, fs.ErrNotExist) || !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
			t.Fatalf("missing source values error = %v, want fs.ErrNotExist, ErrInput and ErrFlux", err)
		}
		if !strings.Contains(err.Error(), `read values file "prod.yaml"`) {
			t.Fatal(err)
		}
		missing, err := fresh.describeChart(t.Context(), sourceHR)
		assert.NoError(t, err)
		missing.inputs, missing.valuesErr = missing.observeValuesFiles(t.Context(), sourceHR.ChartValuesFiles, false)
		if !errors.Is(missing.valuesErr, fs.ErrNotExist) {
			t.Fatal(missing.valuesErr)
		}
		assert.Equal(t, computeTemplateKey(missing, map[string]any{}, Options{}, sourceHR), key)
	})
	assertSourceValuesChanges(t, cli, hr, root)
}

func assertSourceValuesChanges(t *testing.T, cli *Client, hr *manifest.HelmRelease, root string) {
	t.Helper()
	initial, err := loadDecodedChart(t.Context(), cli, hr)
	assert.NoError(t, err)
	for _, marker := range []string{"changed", "", "added"} {
		if marker == "" {
			assert.NoError(t, os.Remove(filepath.Join(root, "config/prod.yaml")))
		} else {
			testutil.WriteFile(t, root, "config/prod.yaml", "marker: "+marker+"\n")
		}
		out, err := cli.Template(t.Context(), hr, nil, Options{})
		assert.NoError(t, err)
		want := marker
		if want == "" {
			want = "default"
		}
		if !strings.Contains(out, `marker: "`+want+`"`) {
			t.Fatal(out)
		}
		repeated, err := cli.Template(t.Context(), hr, nil, Options{})
		assert.NoError(t, err)
		assert.Equal(t, repeated, out)
		loaded, err := loadDecodedChart(t.Context(), cli, hr)
		assert.NoError(t, err)
		assert.Equal(t, loaded.chart == initial.chart, true)
	}
}
