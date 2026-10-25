package helm

import (
	"context"
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
	"sync"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/kustomize"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/opencontainers/go-digest"
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
				cli := NewClient(cachetest.New(t, t.TempDir(), cache.New))
				cliSources := NewSources(st)
				opts := Options{DisableChartDigestTracking: disable}
				held := acquireChartFS(t, cli.cache, chartFixtureFS(hr, os.DirFS(dir)))
				var canonical *chart.Chart
				render := func(identity store.SourceArtifact) ([]map[string]any, string, string) {
					t.Helper()
					identity.Kind, identity.LocalPath, identity.FS = manifest.KindOCIRepository, dir, held
					st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &identity)
					out, receipt1, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources})
					if err == nil {
						if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt1, emptyRenderInventory); recordErr != nil {
							t.Fatal(recordErr)
						}
					}

					if err != nil {
						t.Fatal(err)
					}
					loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
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
					valuesKey := chartValuesCacheKey(loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles, "")
					merged, ok := cli.chartValuesCache[valuesKey]
					if !ok || valuesKey == "" {
						t.Fatal("render did not cache values under its effective fingerprint")
					}
					key := computeTemplateKey(loaded, merged.merged, opts, hr)
					repeated, receipt2, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources})
					if err == nil {
						if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt2, emptyRenderInventory); recordErr != nil {
							t.Fatal(recordErr)
						}
					}

					if err != nil || !reflect.DeepEqual(repeated, out) {
						t.Fatalf("repeat render differs: %v", err)
					}
					return out, key, valuesKey
				}
				first, key, valuesKey := render(initial)
				if !hasRenderField(first, "marker", "original") {
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
					t.Run("updated acquisition", func(t *testing.T) { held = acquireChartFS(t, cli.cache, chartFixtureFS(hr, os.DirFS(dir))) })
					canonical = nil
				}
				second, nextKey, nextValuesKey := render(tc.next)
				assert.Equal(t, reflect.DeepEqual(first, second), tc.wantSameOutput)
				assert.Equal(t, key != nextKey, tc.wantFingerprintChanged)
				assert.Equal(t, valuesKey != nextValuesKey, tc.wantFingerprintChanged)
				if tc.marker != "" && !hasRenderField(second, "marker", tc.marker) {
					t.Fatal(second)
				}
				assert.Diff(t, hr, original)
				for _, revision := range []string{"6.14.0@" + next, "invalid"} {
					st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: held, LocalPath: dir, Digest: next, Revision: revision})
					_, receipt3, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources})
					if err == nil {
						if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt3, emptyRenderInventory); recordErr != nil {
							t.Fatal(recordErr)
						}
					}
					if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
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
		input := chartFixtureFS(hr, os.DirFS(dir))
		observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) { reads[name]++; return input.Open(name) }}
		first := &store.SourceArtifact{FS: observed, Key: "untrusted", Revision: "6.15.0@" + digest, Digest: digest}
		second := &store.SourceArtifact{Revision: "invalid", FS: fstest.MapFS{}}
		resolver := &changingChartReader{sourceReader: st, artifacts: []*store.SourceArtifact{first, second}}
		cli := NewClient(cachetest.New(t, "", cache.New))
		cliSources := NewSources(resolver)
		out, receipt4, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
		if err == nil {
			if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt4, emptyRenderInventory); recordErr != nil {
				t.Fatal(recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Equal(t, resolver.calls, 1)
		assert.Equal(t, reads[".helmignore"], 0)
		assert.Equal(t, reads["chart/templates/cm.yaml"], 1)
		assert.Equal(t, hasRenderField(out, "version", "6.15.0+ff3d3e14728f"), true)
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
	input := acquireChartFS(t, cachetest.New(t, t.TempDir(), cache.New), chartFixtureFS(hr, os.DirFS(dir)))
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{
		Kind: manifest.KindOCIRepository, FS: input, Digest: digest, Revision: "6.15.0@" + digest,
	})
	newClient := func(t *testing.T, owner *cache.Cache, reverse bool) (*Client, Sources) {
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
		captured, _, err := captureChart(t.Context(), input, ".", true)
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
		cli := NewClient(owner)
		cliSources := NewSources(st)
		// Canonical charts MUST remain immutable; choose the loader order before admission.
		cli.chartCache[selector] = chartDescription{chart: ch, captured: captured, key: selector, location: ".", fingerprint: captured.fingerprint(), analysis: &chartAnalysis{Version: ch.Metadata.Version, Templates: chartTemplateText(ch), Allowed: knownTopLevelKeys(ch), RequiresLoader: requiresChartLoader(ch)}}
		return cli, cliSources
	}
	keyFor := func(t *testing.T, cli *Client, cliSources Sources) cache.BlobKey {
		t.Helper()
		loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		assert.NoError(t, err)
		loaded.oci = &ociChartContent{Digest: digest, Revision: "6.15.0@" + digest}
		_, err = cli.mergeChartValuesFiles(t.Context(), loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
		assert.NoError(t, err)
		key := computeTemplateKey(loaded, map[string]any{}, Options{}, hr)
		t.Logf("chart fingerprint=%s template key=%s", loaded.fingerprint, key)
		if key == "" {
			t.Fatal("subchart fixture produced an empty completed-output key")
		}
		return cache.BlobKey{Content: key, Render: cache.RenderAccepted}
	}
	readBlob := func(t *testing.T, owner *cache.Cache, key cache.BlobKey) []map[string]any {
		t.Helper()
		r, _, _, err := owner.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil)
		assert.NoError(t, err)
		data, err := fs.ReadFile(r, "content")
		assert.NoError(t, err)

		var record renderRecord
		assert.NoError(t, decodeTestRecord(data, &record))
		docs, err := manifest.DecodeRender(record.Output)
		assert.NoError(t, err)
		return docs
	}
	writeBlob := func(t *testing.T, owner *cache.Cache, key cache.BlobKey, data []map[string]any) {
		t.Helper()
		captured, _, err := captureChart(t.Context(), input, ".", true)
		assert.NoError(t, err)
		ch, err := captured.load()
		assert.NoError(t, err)
		analysis := &chartAnalysis{Version: ch.Metadata.Version, Templates: chartTemplateText(ch), Allowed: knownTopLevelKeys(ch), RequiresLoader: requiresChartLoader(ch)}
		_, _, _, err = owner.Blob(t.Context(), key, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			err := writeTestRecord(w, key, renderRecord{Output: encodedRender(t, data), Analysis: analysis})
			return cache.ProducerResult{}, err
		})
		assert.NoError(t, err)
	}
	root := t.TempDir()
	owner := cachetest.New(t, root, cache.New)
	cli, cliSources := newClient(t, owner, false)
	key := keyFor(t, cli, cliSources)
	_, _, _, err := owner.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil)
	if !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("cold lookup: got %v, want not found", err)
	}
	t.Log("direct cold lookup: not found")
	cold, receipt5, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
	if err == nil {
		if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt5, emptyRenderInventory); recordErr != nil {
			t.Fatal(recordErr)
		}
	}

	assert.NoError(t, err)
	for _, name := range []string{"child", "sibling"} {
		if !hasRenderField(cold, "name", name) {
			t.Fatalf("cold render omitted enabled dependency %s", name)
		}
	}
	assert.Diff(t, readBlob(t, owner, key), cold)
	t.Log("direct cold publication: exact constructed documents")
	second, receipt6, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
	if err == nil {
		if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt6, emptyRenderInventory); recordErr != nil {
			t.Fatal(recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Diff(t, second, cold)
	marked := append(slices.Clone(cold), map[string]any{"witness": "persisted-subchart-hit"})
	writeBlob(t, owner, key, marked)
	warm, receipt7, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
	if err == nil {
		if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt7, emptyRenderInventory); recordErr != nil {
			t.Fatal(recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Diff(t, warm, marked)
	t.Log("same-client marked hit: verified")
	assert.NoError(t, owner.Close())
	owner = cachetest.New(t, root, cache.New)
	cli, cliSources = newClient(t, owner, true)
	reopenedKey := keyFor(t, cli, cliSources)
	_, _, _, lookupErr := owner.Blob(t.Context(), reopenedKey, cache.Policy{MaxAge: cache.Forever}, nil)
	t.Logf("fresh-owner direct lookup: %v", lookupErr)
	reopened, receipt8, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
	if err == nil {
		if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt8, emptyRenderInventory); recordErr != nil {
			t.Fatal(recordErr)
		}
	}

	assert.NoError(t, err)
	if !reflect.DeepEqual(reopened, marked) {
		t.Fatalf("fresh owner/client rerendered unchanged subcharts: marker absent=%v, fresh bytes equal cold=%v", !hasRenderField(reopened, "witness", "persisted-subchart-hit"), reflect.DeepEqual(reopened, cold))
	}
	writeBlob(t, owner, reopenedKey, cold)
	restored, receipt9, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
	if err == nil {
		if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt9, emptyRenderInventory); recordErr != nil {
			t.Fatal(recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Diff(t, restored, cold)
	assert.Equal(t, reopenedKey.Content, key.Content)
	assert.NoError(t, lookupErr)
	t.Log("verified persisted subchart publication, marked hit, reopen, and restored document equality")
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
			updated := acquireChartFS(t, cachetest.New(t, t.TempDir(), cache.New), chartFixtureFS(hr, files))
			nextStore := store.New()
			nextStore.AddObject(&manifest.OCIRepository{Name: "podinfo", Namespace: "apps"})
			nextStore.SetArtifact(hr.ChartSource(), &store.SourceArtifact{
				Kind: manifest.KindOCIRepository, FS: updated, Digest: digest, Revision: "6.15.0@" + digest,
			})
			next := NewClient(owner)
			nextSources := NewSources(nextStore)
			nextKey := keyFor(t, next, nextSources)
			if nextKey.Content == key.Content {
				t.Fatal("edited child reused the original completed-output key")
			}
			_, _, _, err := owner.Blob(t.Context(), nextKey, cache.Policy{MaxAge: cache.Forever}, nil)
			if !errors.Is(err, errdef.ErrNotFound) {
				t.Fatalf("edited child lookup: got %v, want not found", err)
			}
			out, receipt10, err := next.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: nextSources})
			if err == nil {
				if recordErr := next.RecordRender(t.Context(), (hr).Named(), receipt10, emptyRenderInventory); recordErr != nil {
					t.Fatal(recordErr)
				}
			}

			assert.NoError(t, err)
			if reflect.DeepEqual(out, cold) || hasRenderField(out, "witness", "persisted-subchart-hit") || !hasRenderField(out, "edited-template", "original") && !hasRenderField(out, "marker", "edited-default") {
				t.Fatal("edited child reused stale output or omitted its new content")
			}
			assert.Diff(t, readBlob(t, owner, nextKey), out)
			assert.Diff(t, readBlob(t, owner, key), marked)
			t.Log("reacquired child content invalidated the key and rendered fresh documents with fixed parent/release/OCI identity")
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
			st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: kind, FS: chartFixtureFS(hr, os.DirFS(dir)), LocalPath: dir})
			cli := NewClient(cachetest.New(t, "", cache.New))
			cliSources := NewSources(st)
			var first []map[string]any
			var key string
			for _, disabled := range []bool{false, true} {
				opts := Options{DisableChartDigestTracking: disabled}
				out, receipt11, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources})
				if err == nil {
					if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt11, emptyRenderInventory); recordErr != nil {
						t.Fatal(recordErr)
					}
				}

				if err != nil {
					t.Fatal(err)
				}
				loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
				if err != nil {
					t.Fatal(err)
				}
				merged, err := cli.mergeChartValuesFiles(t.Context(), loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
				if err != nil {
					t.Fatal(err)
				}
				nextKey := computeTemplateKey(loaded, merged, opts, hr)
				if first != nil {
					assert.Diff(t, out, first)
					assert.Equal(t, nextKey, key)
				}
				if !hasRenderField(out, "version", "6.15.0") {
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

// Common metadata MUST remain outside the cached render and its key.
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

type templateActionProbe chan struct{}

func (p templateActionProbe) MarshalJSON() ([]byte, error) { return []byte(`"fresh"`), nil }

func (p templateActionProbe) String() string { p <- struct{}{}; return "fresh" }

func TestTemplateCache_TemplateIntegration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, input string
		invalid     bool
	}{
		{"list scalars", `apiVersion: v1
kind: List
items:
- apiVersion: example.test/v1
  kind: Example
  metadata: {name: nested-keys}
  spec:
    mapping: {true: value}
    string: 'yes'
    date: '2001-01-01'
    number: 9007199254740993
`, false},
		{"list partial identity", "apiVersion: v1\nkind: ConfigMapList\nitems: [{apiVersion: v1, metadata: {name: partial}}]\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir, root := t.TempDir(), t.TempDir()
			testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
			testutil.WriteFile(t, dir, "mychart/templates/list.yaml", "{{ if .Values.fail }}{{ fail \"cache miss action\" }}{{ end }}\n"+tc.input)
			owner := cachetest.New(t, root, cache.New)
			cli := NewClient(owner)
			sources := localChartResolver(t, "chart-repo", "flux-system", dir)
			first, receipt, firstErr := cli.Template(t.Context(), newHR(), nil, RenderOptions{Sources: sources})
			if tc.invalid {
				if !errors.Is(firstErr, manifest.ErrInput) || first != nil {
					t.Fatalf("cold invalid list = %#v, %v", first, firstErr)
				}
			} else {
				assert.NoError(t, firstErr)
				assert.NoError(t, cli.RecordRender(t.Context(), newHR().Named(), receipt, emptyRenderInventory))
			}
			t.Run("persisted replay", func(t *testing.T) {
				t.Parallel()
				if !renderPersistenceEligible(t) {
					t.Skipf("requires identified test binary: %s", cache.RenderPersistenceDisabledReason())
				}
				loaded, err := loadDecodedChart(t.Context(), cli, newHR(), sources)
				assert.NoError(t, err)
				loaded.values, loaded.valuesErr = cli.observeValuesFiles(t.Context(), loaded, newHR().ChartValuesFiles, newHR().IgnoreMissingValuesFiles)
				assert.NoError(t, loaded.valuesErr)
				key := cache.BlobKey{Content: computeTemplateKey(loaded, map[string]any{}, Options{}, newHR()), Render: cache.RenderAccepted}
				if tc.invalid {
					raw, err := manifest.SplitDocs([]byte(tc.input))
					assert.NoError(t, err)
					record := renderRecord{Output: encodedRender(t, raw), Analysis: loaded.analyze()}
					_, _, _, err = owner.Blob(t.Context(), key, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
						return cache.ProducerResult{}, writeTestRecord(w, key, record)
					})
					assert.NoError(t, err)

				}
				r, _, _, err := owner.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil)
				assert.NoError(t, err)
				var record renderRecord
				assert.NoError(t, readTestRecord(r, &record))

				raw, err := manifest.DecodeRender(record.Output)
				assert.NoError(t, err)
				wantRaw, err := manifest.SplitDocs([]byte(tc.input))
				assert.NoError(t, err)
				assert.Diff(t, raw, wantRaw)
				values := map[string]any{"fail": true}
				key.Content = computeTemplateKey(loaded, values, Options{}, newHR())
				_, _, _, err = owner.Blob(t.Context(), key, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
					return cache.ProducerResult{}, writeTestRecord(w, key, record)
				})
				assert.NoError(t, err)
				assert.NoError(t, owner.Close())
				fresh := NewClient(cachetest.New(t, root, cache.New))
				out, _, err := fresh.Template(t.Context(), newHR(), values, RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)})
				if tc.invalid {
					if !errors.Is(err, manifest.ErrInput) || err.Error() != firstErr.Error() || out != nil {
						t.Fatalf("persisted invalid list = %#v, %v; cold = %v", out, err, firstErr)
					}
				} else {
					assert.NoError(t, err)
					assert.Diff(t, out, first)
				}
				t.Log("verified persisted raw-list hit; Helm action would fail on a miss")
			})
		})
	}
	t.Run("cold coalescing", func(t *testing.T) {
		t.Parallel()
		for _, disk := range []bool{false, true} {
			t.Run(fmt.Sprintf("disk_%t", disk), func(t *testing.T) {
				t.Parallel()
				root := ""
				if disk {
					root = t.TempDir()
				}
				dir := t.TempDir()
				testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
				testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata:\n  action: {{ .Values.probe | quote }}\n")
				cli := NewClient(cachetest.New(t, root, cache.New))
				probe := make(templateActionProbe, 4)
				var outputs [2][]map[string]any
				var wg sync.WaitGroup
				start := make(chan struct{})
				for i := range outputs {
					wg.Go(func() {
						<-start
						var err error
						outputs[i], _, err = cli.Template(t.Context(), newHR(), map[string]any{"probe": probe}, RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)})
						assert.NoError(t, err)
					})
				}
				close(start)
				wg.Wait()
				assert.Equal(t, len(probe), 1)
				assert.Diff(t, outputs[0], outputs[1])
				outputs[0][0]["data"].(map[string]any)["action"] = "mutated"
				again, _, err := cli.Template(t.Context(), newHR(), map[string]any{"probe": probe}, RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)})
				assert.NoError(t, err)
				assert.Diff(t, again, outputs[1])
				assert.Equal(t, len(probe), 1)
			})
		}
	})
	for _, failure := range []string{"template", "post-renderer"} {
		t.Run(failure+" diagnostics", func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
			body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata:\n  action: {{ .Values.probe | quote }}\n"
			hr := newHR()
			hr.DisableSchemaValidation = true
			want := "producer diagnostic"
			if failure == "template" {
				body += `{{ fail "producer diagnostic" }}`
			} else {
				want = "error while running post render"
				hr.PostRenderers = []helmv2.PostRenderer{{Kustomize: &helmv2.Kustomize{Patches: []kustomize.Patch{{
					Patch:  "- op: replace\n  path: /data/missing/child\n  value: changed\n",
					Target: &kustomize.Selector{Kind: "ConfigMap", Name: "demo"},
				}}}}}
			}
			testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", body)
			cli := NewClient(cachetest.New(t, "", cache.New))
			cliSources := localChartResolver(t, "chart-repo", "flux-system", dir)
			probe := make(templateActionProbe, 4)
			var previous error
			for i, keyed := range []bool{false, true} {
				values := map[string]any{"probe": probe}
				if !keyed {
					values["unused"] = func() {}
				}
				var key string
				out, _, err := cli.Template(t.Context(), hr, values, RenderOptions{Sources: cliSources, Admit: func(value string) (bool, error) {
					key = value
					return false, nil
				}})
				assert.Equal(t, key != "", keyed)
				assert.Equal(t, len(probe), i+1)
				if len(out) != 0 || err == nil || !strings.HasPrefix(err.Error(), "helm template default/demo: ") || !strings.Contains(err.Error(), want) {
					t.Fatalf("keyed=%t: output %v, error %v", keyed, out, err)
				}
				if _, storage := errors.AsType[*cache.CacheError](err); storage {
					t.Fatalf("keyed=%t: producer failure classified as storage: %v", keyed, err)
				}
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), keyed)
				if previous != nil {
					assert.Equal(t, err.Error(), previous.Error())
				}
				previous = err
			}
		})
	}
	t.Run("empty stream", func(t *testing.T) {
		t.Parallel()
		dir, root := t.TempDir(), t.TempDir()
		testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
		for range 2 {
			cli := NewClient(cachetest.New(t, root, cache.New))
			cliSources := localChartResolver(t, "chart-repo", "flux-system", dir)
			docs, receipt12, err := cli.Template(t.Context(), newHR(), nil, RenderOptions{Sources: cliSources})
			if err == nil {
				if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt12, emptyRenderInventory); recordErr != nil {
					t.Fatal(recordErr)
				}
			}

			assert.NoError(t, err)
			assert.Equal(t, len(docs), 0)
			if renderPersistenceEligible(t) {
				loaded, err := cli.describeChart(t.Context(), newHR(), cliSources)
				assert.NoError(t, err)
				loaded.values, loaded.valuesErr = cli.observeValuesFiles(t.Context(), loaded, newHR().ChartValuesFiles, newHR().IgnoreMissingValuesFiles)
				assert.NoError(t, loaded.valuesErr)
				key := cache.BlobKey{Content: computeTemplateKey(loaded, map[string]any{}, Options{}, newHR()), Render: cache.RenderAccepted}
				r, _, _, err := cli.cache.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil)
				assert.NoError(t, err)
				var record renderRecord
				assert.NoError(t, readTestRecord(r, &record))

				decoded, err := manifest.DecodeRender(record.Output)
				assert.NoError(t, err)
				assert.Equal(t, len(decoded), 0)
			}
		}
	})
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
extra: {sequence: [{value: original}]}
`)
			owner := cachetest.New(t, root, cache.New)
			cli := NewClient(owner)
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
			resolver := NewSources(st)
			cliSources := resolver
			first, receipt13, err := cli.Template(t.Context(), newHR(), map[string]any{"greeting": "hi"}, RenderOptions{Options: Options{}, Sources: cliSources})
			if err == nil {
				if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt13, emptyRenderInventory); recordErr != nil {
					t.Fatal(recordErr)
				}
			}

			if err != nil {
				t.Fatal(err)
			}
			canonical, err := loadDecodedChart(t.Context(), cli, newHR(), cliSources)
			assert.NoError(t, err)
			beforeReads := reads["mychart/templates/configmap.yaml"]
			second, receipt14, err := cli.Template(t.Context(), newHR(), map[string]any{"greeting": "hi"}, RenderOptions{Options: Options{}, Sources: cliSources})
			if err == nil {
				if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt14, emptyRenderInventory); recordErr != nil {
					t.Fatal(recordErr)
				}
			}

			if err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, second, first)
			admittedKey := ""
			blocked, receipt15, err := cli.Template(t.Context(), newHR(), map[string]any{"greeting": "hi"}, RenderOptions{
				Sources: cliSources,
				Admit: func(key string) (bool, error) {
					admittedKey = key
					return true, nil
				},
			})
			if err == nil {
				if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt15, emptyRenderInventory); recordErr != nil {
					t.Fatal(recordErr)
				}
			}

			assert.NoError(t, err)
			assert.Diff(t, blocked, []map[string]any(nil))
			if admittedKey == "" {
				t.Fatal("completed output bypassed admission")
			}
			if backing != "acquired" {
				assert.Equal(t, reads["mychart/templates/configmap.yaml"], beforeReads+2)
			}
			warm, err := loadDecodedChart(t.Context(), cli, newHR(), cliSources)
			assert.NoError(t, err)
			assert.Equal(t, warm.chart == canonical.chart, true)
			assert.Equal(t, warm.fingerprint, canonical.fingerprint)
			third, receipt16, err := cli.Template(t.Context(), newHR(), map[string]any{"greeting": "different"}, RenderOptions{Options: Options{}, Sources: cliSources})
			if err == nil {
				if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt16, emptyRenderInventory); recordErr != nil {
					t.Fatal(recordErr)
				}
			}

			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, reflect.DeepEqual(third, first), false)
			loaded, err := loadDecodedChart(t.Context(), cli, newHR(), cliSources)
			if err != nil {
				t.Fatal(err)
			}
			values := map[string]any{"greeting": "hi", "fail": true}
			loaded.values, loaded.valuesErr = cli.observeValuesFiles(t.Context(), loaded, newHR().ChartValuesFiles, newHR().IgnoreMissingValuesFiles)
			assert.NoError(t, loaded.valuesErr)
			key := cache.BlobKey{Content: computeTemplateKey(loaded, values, Options{}, newHR()), Render: cache.RenderAccepted}
			_, _, _, err = owner.Blob(t.Context(), key, cache.Policy{MaxAge: 0}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				err := writeTestRecord(w, key, renderRecord{Output: encodedRender(t, first), Analysis: loaded.analyze()})
				return cache.ProducerResult{}, err
			})
			if err != nil {
				t.Fatal(err)
			}

			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			cli = NewClient(cachetest.New(t, root, cache.New))
			cliSources = resolver
			loaded, err = loadDecodedChart(t.Context(), cli, newHR(), cliSources)
			assert.NoError(t, err)
			out, receipt17, err := cli.Template(t.Context(), newHR(), values, RenderOptions{Options: Options{}, Sources: cliSources})
			if err == nil {
				if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt17, emptyRenderInventory); recordErr != nil {
					t.Fatal(recordErr)
				}
			}

			if root != "" && renderPersistenceEligible(t) {
				t.Log("verified persisted completed-output hit")
				if err != nil || !reflect.DeepEqual(out, first) {
					t.Fatalf("completed hit: output=%q, error=%v", out, err)
				}
				out[0]["data"].(map[string]any)["greeting"] = "mutated"
				out[0]["extra"].(map[string]any)["sequence"].([]any)[0].(map[string]any)["value"] = "mutated"
				again, receipt18, err := cli.Template(t.Context(), newHR(), values, RenderOptions{Sources: cliSources})
				if err == nil {
					if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt18, emptyRenderInventory); recordErr != nil {
						t.Fatal(recordErr)
					}
				}

				assert.NoError(t, err)
				assert.Diff(t, again, first)
				fresh := NewClient(cachetest.New(t, root, cache.New))
				var receipt19 *manifest.RenderReceipt
				again, receipt19, err = fresh.Template(t.Context(), newHR(), values, RenderOptions{Sources: cliSources})
				if err == nil {
					if recordErr := fresh.RecordRender(t.Context(), (newHR()).Named(), receipt19, emptyRenderInventory); recordErr != nil {
						t.Fatal(recordErr)
					}
				}

				assert.NoError(t, err)
				assert.Diff(t, again, first)
				initial, err := cli.describeChart(t.Context(), newHR(), cliSources)
				assert.NoError(t, err)
				var receipt20 *manifest.RenderReceipt
				_, receipt20, err = cli.Template(t.Context(), newHR(), values, RenderOptions{Options: Options{}, Sources: cliSources})
				if err == nil {
					if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt20, emptyRenderInventory); recordErr != nil {
						t.Fatal(recordErr)
					}
				}

				assert.NoError(t, err)
				repeated, err := cli.describeChart(t.Context(), newHR(), cliSources)
				assert.NoError(t, err)
				if initial.record == nil || repeated.record != initial.record {
					t.Fatal("repeated hit did not share its typed record")
				}
				alternate := maps.Clone(values)
				alternate["variant"] = true
				loaded.values, loaded.valuesErr = cli.observeValuesFiles(t.Context(), loaded, newHR().ChartValuesFiles, newHR().IgnoreMissingValuesFiles)
				assert.NoError(t, loaded.valuesErr)
				alternateKey := cache.BlobKey{Content: computeTemplateKey(loaded, alternate, Options{}, newHR()), Render: cache.RenderAccepted}
				r, _, _, err := cli.cache.Blob(t.Context(), alternateKey, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
					err := writeTestRecord(w, alternateKey, *initial.record)
					return cache.ProducerResult{}, err
				})
				assert.NoError(t, err)
				assert.Equal(t, cache.AcquisitionIdentity(r) != initial.record.identity, true)

				var receipt21 *manifest.RenderReceipt
				_, receipt21, err = cli.Template(t.Context(), newHR(), alternate, RenderOptions{Options: Options{}, Sources: cliSources})
				if err == nil {
					if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt21, emptyRenderInventory); recordErr != nil {
						t.Fatal(recordErr)
					}
				}

				assert.NoError(t, err)
				distinct, err := cli.describeChart(t.Context(), newHR(), cliSources)
				assert.NoError(t, err)
				if distinct.record == initial.record || distinct.record.key != alternateKey.Content {
					t.Fatal("distinct release key reused a typed record")
				}
				for _, replacement := range []string{"changed output", "corrupt payload", "{", `{"Output":"bWlzc2luZyBhbmFseXNpcw=="}`} {
					replacementDocs := []map[string]any{{"witness": replacement}}
					_, _, _, err := cli.cache.Blob(t.Context(), alternateKey, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
						var err error
						switch replacement {
						case "corrupt payload":
							err = writeTestRecord(w, alternateKey, renderRecord{Output: []byte("corrupt"), Analysis: loaded.analyze()})
						case "changed output":
							err = writeTestRecord(w, alternateKey, renderRecord{Output: encodedRender(t, replacementDocs), Analysis: loaded.analyze()})
						default:
							_, err = io.WriteString(w, replacement)
						}
						return cache.ProducerResult{}, err
					})
					assert.NoError(t, err)

					out, receipt22, err := cli.Template(t.Context(), newHR(), alternate, RenderOptions{Options: Options{}, Sources: cliSources})
					if err == nil {
						if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt22, emptyRenderInventory); recordErr != nil {
							t.Fatal(recordErr)
						}
					}

					switch replacement {
					case "changed output":
						assert.NoError(t, err)
						if !reflect.DeepEqual(out, replacementDocs) {
							t.Fatal("same-key replacement served stale output")
						}
					case "corrupt payload":
						if err == nil || !strings.Contains(err.Error(), "cache miss action") || len(out) != 0 {
							t.Fatalf("corrupt payload: %v, %v", out, err)
						}
					case "{":
						if err == nil || !strings.Contains(err.Error(), "cache miss action") || len(out) != 0 {
							t.Fatalf("malformed replacement was concealed: %v", err)
						}
					default:
						if err == nil || !strings.Contains(err.Error(), "cache miss action") || len(out) != 0 {
							t.Fatalf("missing analysis was accepted: %v", err)
						}
					}
				}
				recoveryValues := map[string]any{"greeting": "hi"}
				recoveryKey := cache.BlobKey{Content: computeTemplateKey(loaded, recoveryValues, Options{}, newHR()), Render: cache.RenderAccepted}
				_, _, _, err = cli.cache.Blob(t.Context(), recoveryKey, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
					err := writeTestRecord(w, recoveryKey, renderRecord{Output: []byte("corrupt"), Analysis: loaded.analyze()})
					return cache.ProducerResult{}, err
				})
				assert.NoError(t, err)

				beforeRecovery, err := cli.describeChart(t.Context(), newHR(), cliSources)
				assert.NoError(t, err)
				recovered, receipt23, err := cli.Template(t.Context(), newHR(), recoveryValues, RenderOptions{Sources: cliSources})
				if err == nil {
					if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt23, emptyRenderInventory); recordErr != nil {
						t.Fatal(recordErr)
					}
				}

				assert.NoError(t, err)
				assert.Diff(t, recovered, first)
				afterRecovery, err := cli.describeChart(t.Context(), newHR(), cliSources)
				assert.NoError(t, err)
				if afterRecovery.record == beforeRecovery.record || afterRecovery.record == nil {
					t.Fatal("recovered generation did not replace the typed record")
				}
				recoveredOutput, err := manifest.DecodeRender(afterRecovery.record.Output)
				assert.NoError(t, err)
				assert.Diff(t, recoveredOutput, first)
				r, _, _, err = cli.cache.Blob(t.Context(), recoveryKey, cache.Policy{MaxAge: cache.Forever}, nil)
				assert.NoError(t, err)
				var replacement renderRecord
				assert.NoError(t, readTestRecord(r, &replacement))

				decoded, err := manifest.DecodeRender(replacement.Output)
				assert.NoError(t, err)
				assert.Diff(t, decoded, first)
				var receipt24 *manifest.RenderReceipt
				again, receipt24, err = cli.Template(t.Context(), newHR(), recoveryValues, RenderOptions{Sources: cliSources})
				if err == nil {
					if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt24, emptyRenderInventory); recordErr != nil {
						t.Fatal(recordErr)
					}
				}

				assert.NoError(t, err)
				assert.Diff(t, again, first)
				reopened := NewClient(cachetest.New(t, root, cache.New))
				var receipt25 *manifest.RenderReceipt
				again, receipt25, err = reopened.Template(t.Context(), newHR(), recoveryValues, RenderOptions{Sources: cliSources})
				if err == nil {
					if recordErr := reopened.RecordRender(t.Context(), (newHR()).Named(), receipt25, emptyRenderInventory); recordErr != nil {
						t.Fatal(recordErr)
					}
				}

				assert.NoError(t, err)
				assert.Diff(t, again, first)
				t.Log("verified corrupt payload recovery, replacement, subsequent and reopened hits")
			} else if err == nil || !strings.Contains(err.Error(), "cache miss action") {
				t.Fatalf("ineligible output did not bypass persistence: %v", err)
			}
			warm, err = loadDecodedChart(t.Context(), cli, newHR(), cliSources)
			assert.NoError(t, err)
			assert.Equal(t, warm.chart == loaded.chart, true)
			assert.Equal(t, warm.fingerprint, loaded.fingerprint)
			values["greeting"] = "changed"
			_, receipt26, err := cli.Template(t.Context(), newHR(), values, RenderOptions{Options: Options{}, Sources: cliSources})
			if err == nil {
				if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt26, emptyRenderInventory); recordErr != nil {
					t.Fatal(recordErr)
				}
			}
			if err == nil || !strings.Contains(err.Error(), "cache miss action") {
				t.Fatalf("changed input did not miss completed output: %v", err)
			}
			if backing == "acquired" {
				// Remove chart backing alone so completed output remains readable.
				assert.NoError(t, os.RemoveAll(filepath.Join(chartRoot, "oci", "blobs")))
				_, err := fs.ReadFile(input, "mychart/templates/configmap.yaml")
				if err == nil {
					t.Fatal("backing removal did not prevent chart reads")
				}
				repeated, receipt27, err := cli.Template(t.Context(), newHR(), map[string]any{"greeting": "hi"}, RenderOptions{Sources: cliSources})
				if err == nil {
					if recordErr := cli.RecordRender(t.Context(), (newHR()).Named(), receipt27, emptyRenderInventory); recordErr != nil {
						t.Fatal(recordErr)
					}
				}

				assert.NoError(t, err)
				assert.Diff(t, repeated, first)
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
		&store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: chartFixtureFS(hr, os.DirFS(dir)), LocalPath: dir, Digest: digest, Revision: "6.15.0@" + digest})
	owner := cachetest.New(t, "", cache.New)
	newClient := func() (*Client, Sources) {
		cli := NewClient(owner)
		cliSources := NewSources(st)
		return cli, cliSources
	}
	cli, cliSources := newClient()
	for _, disable := range []bool{false, true, false, true, false} {
		opts := Options{DisableChartDigestTracking: disable}
		out, receipt28, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources})
		if err == nil {
			if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt28, emptyRenderInventory); recordErr != nil {
				t.Fatal(recordErr)
			}
		}

		if err != nil {
			t.Fatal(err)
		}
		version := "6.15.0"
		if !disable {
			version += "+ff3d3e14728f"
		}
		if !hasRenderField(out, "version", version) {
			t.Fatalf("disable=%v returned incompatible output:\n%s", disable, out)
		}
		freshClient, freshSources := newClient()
		fresh, receipt29, err := freshClient.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: freshSources})
		if err == nil {
			if recordErr := freshClient.RecordRender(t.Context(), (hr).Named(), receipt29, emptyRenderInventory); recordErr != nil {
				t.Fatal(recordErr)
			}
		}

		if err != nil || !reflect.DeepEqual(fresh, out) {
			t.Fatalf("fresh disable=%v result differs: err=%v output=%s", disable, err, fresh)
		}
	}
}

func TestTemplateCache_StorageFailures(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"lookup", "read", "publication", "construction"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			if fault != "construction" && !renderPersistenceEligible(t) {
				t.Skip("storage faults require an eligible build")
			}
			chartDir := t.TempDir()
			testutil.WriteFile(t, chartDir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
			testutil.WriteFile(t, chartDir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fresh}\n")
			if fault == "construction" {
				testutil.WriteFile(t, chartDir, "mychart/crds/a-valid.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: prefix}\n")
				testutil.WriteFile(t, chartDir, "mychart/crds/b-invalid.yaml", "apiVersion: [\n")
			}
			root := t.TempDir()
			owner := cachetest.New(t, root, cache.New)
			cli := NewClient(owner)
			cliSources := localChartResolver(t, "chart-repo", "flux-system", chartDir)
			hr := newHR()
			if fault == "lookup" || fault == "read" {
				for range 2 {
					_, receipt30, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
					if err == nil {
						if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt30, emptyRenderInventory); recordErr != nil {
							t.Fatal(recordErr)
						}
					}

					assert.NoError(t, err)
				}
				d, err := cli.describeChart(t.Context(), hr, cliSources)
				assert.NoError(t, err)
				if d.record == nil {
					t.Fatal("lookup fault fixture did not warm a typed record")
				}
				if fault == "read" {
					r, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: d.record.key, Render: cache.RenderAccepted}, cache.Policy{MaxAge: cache.Forever}, nil)
					assert.NoError(t, err)
					data, err := fs.ReadFile(r, "content")
					assert.NoError(t, err)

					sum := digest.FromBytes(data)
					blob := filepath.Join(root, "oci", "blobs", sum.Algorithm().String(), sum.Encoded())
					assert.NoError(t, os.Remove(blob))
					assert.NoError(t, os.Mkdir(blob, 0o750))
					cli = NewClient(owner)
					cliSources = localChartResolver(t, "chart-repo", "flux-system", chartDir)
				}
			}
			if fault != "read" {
				assert.NoError(t, os.RemoveAll(root))
			}
			for range 2 {
				out, receipt31, err := cli.Template(t.Context(), hr, nil, RenderOptions{Sources: cliSources})
				if err == nil {
					err = cli.RecordRender(t.Context(), (hr).Named(), receipt31, emptyRenderInventory)
				}

				if fault == "read" {
					if pathErr, ok := errors.AsType[*fs.PathError](err); !ok || pathErr.Op != "read" {
						t.Fatalf("read fault did not precede decoding: %v", err)
					}
				}
				if fault == "construction" && !errors.Is(err, manifest.ErrInput) {
					t.Fatalf("construction did not precede publication: %v", err)
				}
				wantDocuments := 0
				if fault == "publication" {
					wantDocuments = 1
				}
				if len(out) != wantDocuments || !errors.Is(err, manifest.ErrFlux) || errors.Is(err, errdef.ErrNotFound) {
					t.Fatalf("%s fault returned output %q, error %v", fault, out, err)
				}
			}
		})
	}
}

type changingChartReader struct {
	sourceReader
	artifacts []*store.SourceArtifact
	calls     int
}

func (r *changingChartReader) GetArtifact(manifest.NamedResource) store.Artifact {
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
	warm := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	warmSources := resolver
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
		var first []map[string]any
		for repeat := range 2 {
			hr := newHR()
			hr.Install = &helmv2.Install{DisableHooks: state.install}
			hr.Upgrade = &helmv2.Upgrade{DisableHooks: state.upgrade}
			out, receipt32, err := warm.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: warmSources})
			if err == nil {
				if recordErr := warm.RecordRender(t.Context(), (hr).Named(), receipt32, emptyRenderInventory); recordErr != nil {
					t.Fatal(recordErr)
				}
			}

			assert.NoError(t, err)
			assert.Diff(t, templateResources(t, out), want)
			fresh := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			freshSources := resolver
			freshHR := newHR()
			freshHR.Install = &helmv2.Install{DisableHooks: state.install}
			freshHR.Upgrade = &helmv2.Upgrade{DisableHooks: state.upgrade}
			uncached, receipt33, err := fresh.Template(t.Context(), freshHR, nil, RenderOptions{Options: opts, Sources: freshSources})
			if err == nil {
				if recordErr := fresh.RecordRender(t.Context(), (freshHR).Named(), receipt33, emptyRenderInventory); recordErr != nil {
					t.Fatal(recordErr)
				}
			}

			assert.NoError(t, err)
			assert.Diff(t, templateResources(t, uncached), want)
			assert.Diff(t, out, uncached)
			if repeat == 0 {
				first = out
			} else if !reflect.DeepEqual(out, first) {
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
	cli := NewClient(owner)
	cliSources := NewSources(st)
	first, receipt34, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
	if err == nil {
		if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt34, emptyRenderInventory); recordErr != nil {
			t.Fatal(recordErr)
		}
	}

	assert.NoError(t, err)
	if !hasRenderField(first, "marker", "first") {
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
		cli := NewClient(owner)
		cliSources := NewSources(st)
		first, receipt35, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
		if err == nil {
			if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt35, emptyRenderInventory); recordErr != nil {
				t.Fatal(recordErr)
			}
		}

		assert.NoError(t, err)
		initial, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		assert.NoError(t, err)
		initial.values, initial.valuesErr = cli.observeValuesFiles(t.Context(), initial, hr.ChartValuesFiles, true)
		assert.NoError(t, initial.valuesErr)
		key := computeTemplateKey(initial, map[string]any{}, Options{}, hr)
		if key == "" {
			t.Fatal("fixture has no completed-output key")
		}
		r, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: key, Render: cache.RenderAccepted}, cache.Policy{MaxAge: cache.Forever}, nil)
		assert.NoError(t, err)
		data, err := fs.ReadFile(r, "content")
		assert.NoError(t, err)

		var record renderRecord
		assert.NoError(t, decodeTestRecord(data, &record))
		decoded, err := manifest.DecodeRender(record.Output)
		assert.NoError(t, err)
		assert.Diff(t, decoded, first)
		cached := append(slices.Clone(first), map[string]any{"witness": "persistent hit witness"})
		_, _, _, err = owner.Blob(t.Context(), cache.BlobKey{Content: key, Render: cache.RenderAccepted}, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			err := writeTestRecord(w, cache.BlobKey{Content: key, Render: cache.RenderAccepted}, renderRecord{Output: encodedRender(t, cached), Analysis: initial.analyze()})
			return cache.ProducerResult{}, err
		})
		assert.NoError(t, err)

		fresh := NewClient(cachetest.New(t, cacheRoot, cache.New))
		freshSources := NewSources(st)
		hit, receipt36, err := fresh.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: freshSources})
		if err == nil {
			if recordErr := fresh.RecordRender(t.Context(), (hr).Named(), receipt36, emptyRenderInventory); recordErr != nil {
				t.Fatal(recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Diff(t, hit, cached)
		fresh.assertSourceValuesChanges(t, hr, root, freshSources)
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
		cli := NewClient(owner)
		cliSources := NewSources(st)
		first, receipt37, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
		if err == nil {
			if recordErr := cli.RecordRender(t.Context(), (hr).Named(), receipt37, emptyRenderInventory); recordErr != nil {
				t.Fatal(recordErr)
			}
		}

		assert.NoError(t, err)
		if !hasRenderField(first, "marker", "packaged") {
			t.Fatal(first)
		}
		initial, err := cli.describeChart(t.Context(), hr, cliSources)
		assert.NoError(t, err)
		key := computeTemplateKey(initial, map[string]any{}, Options{}, hr)
		if key == "" {
			t.Fatal("fixture has no completed-output key")
		}
		r, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: key, Render: cache.RenderAccepted}, cache.Policy{MaxAge: cache.Forever}, nil)
		assert.NoError(t, err)
		data, err := fs.ReadFile(r, "content")
		assert.NoError(t, err)

		var record renderRecord
		assert.NoError(t, decodeTestRecord(data, &record))
		decoded, err := manifest.DecodeRender(record.Output)
		assert.NoError(t, err)
		assert.Diff(t, decoded, first)
		if record.Analysis == nil {
			t.Fatal("persisted record has no chart analysis")
		}
		sourceHR := hr.Clone()
		sourceHR.Chart.Source.Kind = manifest.KindGitRepository
		st.SetArtifact(sourceHR.ChartSource(), artifact)
		fresh := NewClient(cachetest.New(t, cacheRoot, cache.New))
		freshSources := NewSources(st)
		out, receipt38, err := fresh.Template(t.Context(), sourceHR, nil, RenderOptions{Options: Options{}, Sources: freshSources})
		if err == nil {
			if recordErr := fresh.RecordRender(t.Context(), (sourceHR).Named(), receipt38, emptyRenderInventory); recordErr != nil {
				t.Fatal(recordErr)
			}
		}

		assert.Diff(t, out, []map[string]any(nil))
		if !errors.Is(err, fs.ErrNotExist) || !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
			t.Fatalf("missing source values error = %v, want fs.ErrNotExist, ErrInput and ErrFlux", err)
		}
		if !strings.Contains(err.Error(), `read values file "prod.yaml"`) {
			t.Fatal(err)
		}
		missing, err := fresh.describeChart(t.Context(), sourceHR, freshSources)
		assert.NoError(t, err)
		missing.values, missing.valuesErr = fresh.observeValuesFiles(t.Context(), missing, sourceHR.ChartValuesFiles, false)
		if !errors.Is(missing.valuesErr, fs.ErrNotExist) {
			t.Fatal(missing.valuesErr)
		}
		assert.Equal(t, computeTemplateKey(missing, map[string]any{}, Options{}, sourceHR), key)
	})
	cli.assertSourceValuesChanges(t, hr, root, cliSources)
}

func (c *Client) assertSourceValuesChanges(t *testing.T, hr *manifest.HelmRelease, root string, cliSources Sources) {
	t.Helper()
	initial, err := loadDecodedChart(t.Context(), c, hr, cliSources)
	assert.NoError(t, err)
	for _, marker := range []string{"changed", "", "added"} {
		if marker == "" {
			assert.NoError(t, os.Remove(filepath.Join(root, "config/prod.yaml")))
		} else {
			testutil.WriteFile(t, root, "config/prod.yaml", "marker: "+marker+"\n")
		}
		out, receipt39, err := c.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
		if err == nil {
			if recordErr := c.RecordRender(t.Context(), (hr).Named(), receipt39, emptyRenderInventory); recordErr != nil {
				t.Fatal(recordErr)
			}
		}

		assert.NoError(t, err)
		want := marker
		if want == "" {
			want = "default"
		}
		if !hasRenderField(out, "marker", want) {
			t.Fatal(out)
		}
		repeated, receipt40, err := c.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
		if err == nil {
			if recordErr := c.RecordRender(t.Context(), (hr).Named(), receipt40, emptyRenderInventory); recordErr != nil {
				t.Fatal(recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Diff(t, repeated, out)
		loaded, err := loadDecodedChart(t.Context(), c, hr, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, loaded.chart == initial.chart, true)
	}
}
