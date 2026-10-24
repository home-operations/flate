package helm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	chartcommon "helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"oras.land/oras-go/v2/errdef"
)

func TestDescriptionProjection(t *testing.T) {
	t.Parallel()
	files := fstest.MapFS{
		"Chart.yaml":                         {Data: []byte("apiVersion: v2\nname: test\nversion: 1.0.0\ndependencies: [{name: child, alias: childAlias, version: 1.0.0, condition: feature.enabled}]\n")},
		"values.yaml":                        {Data: []byte("used: first\n")},
		"templates/cm.yaml":                  {Data: []byte("{{ .Values.used }}")},
		"values.schema.json":                 {Data: []byte(`{"type":"object"}`)},
		"charts/child/Chart.yaml":            {Data: []byte("apiVersion: v2\nname: child\nversion: 1.0.0\ndependencies: [{name: grand, version: 1.0.0, tags: [group]}]\n")},
		"charts/child/templates/unused.yaml": {Data: []byte("{{ .Values.nested }}")},
	}
	initial, _, err := captureChart(files, ".")
	assert.NoError(t, err)
	for name := range files {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changed := fstest.MapFS{}
			for path, file := range files {
				cloned := *file
				changed[path] = &cloned
			}
			changed[name].Data = append(bytes.Clone(changed[name].Data), '\n')
			next, _, err := captureChart(changed, ".")
			assert.NoError(t, err)
			if initial.fingerprint() == next.fingerprint() {
				t.Fatal("changed captured bytes shared an identity")
			}
		})
	}
	ch, err := initial.load()
	assert.NoError(t, err)
	assert.Equal(t, strings.Contains(chartTemplateText(ch), ".Values.nested"), true)
	assert.Equal(t, requiresChartLoader(ch), false)
	analysis := &chartAnalysis{Templates: chartTemplateText(ch), Allowed: knownTopLevelKeys(ch), RequiresLoader: requiresChartLoader(ch)}
	assert.Diff(t, analysis.Allowed, map[string]struct{}{"child": {}, "childAlias": {}, "feature": {}, "global": {}, "tags": {}})
	data, err := json.Marshal(renderRecord{Output: []byte("rendered"), Analysis: analysis})
	assert.NoError(t, err)
	var restored renderRecord
	assert.NoError(t, json.Unmarshal(data, &restored))
	assert.Diff(t, restored.Output, []byte("rendered"))
	assert.Diff(t, restored.Analysis, analysis)
	values := map[string]any{"used": 1, "nested": 1, "child": 1, "childAlias": 1, "feature": 1, "global": 1, "tags": 1, "unused": 1}
	assert.Diff(t, analysis.unreferencedValuePaths(values), []string{"unused"})
	assert.Diff(t, restored.Analysis.unreferencedValuePaths(values), []string{"unused"})
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "opaque root", true: "opaque subchart"}[nested], func(t *testing.T) {
			t.Parallel()
			ch, err := initial.load()
			assert.NoError(t, err)
			ch.Templates = append(ch.Templates, &chartcommon.File{Data: []byte(strings.Repeat("unused analysis payload\n", 1<<14))})
			target := ch
			if nested {
				target = ch.Dependencies()[0]
			}
			target.Templates = append(target.Templates, &chartcommon.File{Data: []byte("{{ toYaml .Values }}")})
			analysis := (chartDescription{chart: ch}).analyze()
			if analysis.Templates != "" {
				t.Fatal("opaque chart retained unused template analysis")
			}
			assert.Diff(t, analysis.unreferencedValuePaths(values), []string(nil))
			data, err := json.Marshal(renderRecord{Output: []byte("rendered"), Analysis: analysis})
			assert.NoError(t, err)
			if len(data) > 1024 {
				t.Fatalf("opaque analysis payload is %d bytes", len(data))
			}
		})
	}
}

func TestDescription_SourceObservation(t *testing.T) {
	t.Parallel()
	root, st, hr := sourceValuesCacheFixture(t)
	input := os.DirFS(root)
	reads := 0
	observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
		if name == "config/prod.yaml" {
			reads++
			if reads > 1 {
				return fstest.MapFS{"value": {Data: []byte("marker: second\n")}}.Open("value")
			}
		}
		return input.Open(name)
	}}
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: observed})
	hr = hr.Clone()
	hr.ChartValuesFiles = []string{"config/prod.yaml", "config/../config/prod.yaml"}
	cli, err := NewClient(cachetest.New(t, "", cache.New))
	assert.NoError(t, err)
	cliSources := NewSources(st)
	out, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
	assert.NoError(t, err)
	assert.Equal(t, reads, 1)
	if !hasRenderField(out, "marker", "first") {
		t.Fatalf("key and merge observed different input: %s", out)
	}
}

func TestDescription_HitHydration(t *testing.T) {
	t.Parallel()
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory miss", true: "fresh persisted hit"}[persistent], func(t *testing.T) {
			t.Parallel()
			if persistent && !renderPersistenceEligible(t) {
				t.Skip("requires clean stamped persistence; item 2 must execute this proof")
			}
			_, st, hr := sourceValuesCacheFixture(t)
			root := ""
			if persistent {
				root = t.TempDir()
			}
			var want []map[string]any
			for i := range 2 {
				cli, err := NewClient(cachetest.New(t, root, cache.New))
				assert.NoError(t, err)
				cliSources := NewSources(st)
				out, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
				assert.NoError(t, err)
				if i == 0 {
					want = out
				}
				assert.Diff(t, out, want)
				d, err := cli.describeChart(t.Context(), hr, cliSources)
				assert.NoError(t, err)
				assert.Equal(t, d.chart == nil, persistent && i == 1)
				if !persistent || i == 0 {
					if d.analysis != nil || d.captured.files != nil || d.captured.archive != nil {
						t.Fatal("decoded miss retained captured input or derived analysis")
					}
				}
				assert.Diff(t, cli.StaleValuePaths(t.Context(), hr, map[string]any{"marker": 1, "unused": 1}, cliSources), []string{"unused"})
				if persistent {
					_, err = cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
					assert.NoError(t, err)
					hit, err := cli.describeChart(t.Context(), hr, cliSources)
					assert.NoError(t, err)
					_, err = cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
					assert.NoError(t, err)
					repeated, err := cli.describeChart(t.Context(), hr, cliSources)
					assert.NoError(t, err)
					if hit.record == nil || hit.record != repeated.record || hit.record.Analysis != repeated.record.Analysis {
						t.Fatal("same record did not share its immutable typed projection")
					}
					if hit.analysis != hit.record.Analysis {
						t.Fatal("record did not share canonical analysis")
					}
					if i == 1 && hit.chart != nil {
						t.Fatal("fresh hit decoded the chart")
					}
				} else if d.record != nil {
					t.Fatal("disabled persistence retained a render record")
				}
				after, err := cli.describeChart(t.Context(), hr, cliSources)
				assert.NoError(t, err)
				assert.Equal(t, after.chart == d.chart, true)
			}
		})
	}
}

func TestDescription_HydrationKeepsDecoded(t *testing.T) {
	t.Parallel()
	for _, packaged := range []bool{false, true} {
		t.Run(map[bool]string{false: "files", true: "archive"}[packaged], func(t *testing.T) {
			t.Parallel()
			root, st, hr := sourceValuesCacheFixture(t)
			if packaged {
				input, err := fs.Sub(os.DirFS(root), "charts/demo")
				assert.NoError(t, err)
				st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: fstest.MapFS{"chart.tgz": {Data: chartArchive(t, input)}}})
				hr = hr.Clone()
				hr.Chart.Name = "chart.tgz"
			}
			cli, err := NewClient(cachetest.New(t, "", cache.New))
			assert.NoError(t, err)
			cliSources := NewSources(st)
			d, err := cli.describeChart(t.Context(), hr, cliSources)
			assert.NoError(t, err)
			ch, err := d.captured.load()
			assert.NoError(t, err)
			fresh := &renderRecord{Output: []byte("hit"), Analysis: &chartAnalysis{Templates: chartTemplateText(ch), Allowed: knownTopLevelKeys(ch)}}
			start := make(chan struct{})
			var wg sync.WaitGroup
			decoded := d
			var decodeErr error
			wg.Go(func() { <-start; decodeErr = cli.decodeChart(t.Context(), &decoded) })
			wg.Go(func() { <-start; cli.rememberDescription(d, fresh) })
			close(start)
			wg.Wait()
			assert.NoError(t, decodeErr)
			if decoded.captured.files != nil || decoded.captured.archive != nil {
				t.Fatal("decode retained captured input")
			}
			late := &renderRecord{Output: []byte("late"), Analysis: &chartAnalysis{Templates: chartTemplateText(ch), Allowed: knownTopLevelKeys(ch)}}
			cli.rememberDescription(d, late)
			cli.rememberDescription(d, nil)
			current, err := cli.describeChart(t.Context(), hr, cliSources)
			assert.NoError(t, err)
			assert.Equal(t, current.chart != nil && current.chart == decoded.chart, true)
			assert.Equal(t, current.record == late, true)
			if fresh.Analysis != current.analysis || late.Analysis != current.analysis {
				t.Fatal("publication did not share canonical analysis")
			}
			if current.captured.files != nil || current.captured.archive != nil {
				t.Fatal("stale publication restored captured input after decode")
			}
			reused := d
			err = cli.decodeChart(t.Context(), &reused)
			assert.NoError(t, err)
			if reused.chart != current.chart || reused.captured.files != nil || reused.captured.archive != nil {
				t.Fatal("decode reuse retained stale captured input")
			}
		})
	}
}

func TestReview_ByteIdentity(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires clean stamped persistence; item 2 must execute the output guard mutation")
	}
	root, st, hr := sourceValuesCacheFixture(t)
	testutil.WriteFile(t, root, "charts/demo/crds/raw.yaml", "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name: fixture}\n# raw: "+string([]byte{0xff})+"\n")
	assertDescriptionIneligible(t, st, hr, false)
}

func TestReview_TemplateBytesIneligible(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires clean stamped persistence; item 2 must execute the analysis guard mutation")
	}
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "root", true: "disabled nested chart"}[nested], func(t *testing.T) {
			t.Parallel()
			root, st, hr := sourceValuesCacheFixture(t)
			location := "charts/demo"
			if nested {
				testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\ndependencies: [{name: child, version: 1.0.0, condition: child.enabled}]\n")
				testutil.WriteFile(t, root, "charts/demo/values.yaml", "marker: default\nchild: {enabled: false}\n")
				location += "/charts/child"
				testutil.WriteFile(t, root, location+"/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.0.0\n")
			}
			testutil.WriteFile(t, root, location+"/templates/_unused.tpl", "{{/* unused .Values "+string([]byte{0xff})+" */}}")
			assertDescriptionIneligible(t, st, hr, true)
		})
	}
}

func assertDescriptionIneligible(t *testing.T, st *store.Store, hr *manifest.HelmRelease, validOutput bool) {
	t.Helper()
	root := t.TempDir()
	var want []map[string]any
	for i := range 2 {
		owner := cachetest.New(t, root, cache.New)
		cli, err := NewClient(owner)
		assert.NoError(t, err)
		cliSources := NewSources(st)
		out, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
		if validOutput {
			assert.NoError(t, err)
		} else if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || len(out) != 0 {
			t.Fatalf("invalid UTF-8 render: %v, %v", out, err)
		}
		if i == 0 {
			want = out
		}
		assert.Diff(t, out, want)
		d, err := cli.describeChart(t.Context(), hr, cliSources)
		assert.NoError(t, err)
		if d.chart == nil {
			t.Fatal("ineligible input hydrated without a real decode")
		}
		if validOutput && validChartTemplates(d.chart) {
			t.Fatal("invalid template bytes were changed")
		}
		d.values, d.valuesErr = cli.observeValuesFiles(t.Context(), d, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
		assert.NoError(t, d.valuesErr)
		r, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: computeTemplateKey(d, map[string]any{}, Options{}, hr), PersistOnly: true}, cache.Policy{MaxAge: cache.Forever}, nil)
		if err == nil {
			assert.NoError(t, r.Close())
			t.Fatal("invalid UTF-8 was published")
		}
		if !errors.Is(err, errdef.ErrNotFound) {
			t.Fatal(err)
		}
	}
}

func TestReview_NestedWarnings(t *testing.T) {
	if !renderPersistenceEligible(t) {
		t.Skip("requires clean stamped persistence; item 2 must execute recursive diagnostic mutations")
	}
	for _, name := range []string{"requirements.yaml", "requirements.lock"} {
		t.Run(name, func(t *testing.T) {
			root, st, hr := sourceValuesCacheFixture(t)
			testutil.WriteFile(t, root, "charts/demo/charts/child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.0.0\n")
			testutil.WriteFile(t, root, "charts/demo/charts/child/"+name, "dependencies: []\n")
			cacheRoot := t.TempDir()
			var diagnostics bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&diagnostics)
			defer log.SetOutput(previous)
			for range 2 {
				diagnostics.Reset()
				cli, err := NewClient(cachetest.New(t, cacheRoot, cache.New))
				assert.NoError(t, err)
				cliSources := NewSources(st)
				_, err = cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
				assert.NoError(t, err)
				if !strings.Contains(diagnostics.String(), "Warning:") {
					t.Fatal("real nested loader diagnostics were suppressed")
				}
				d, err := cli.describeChart(t.Context(), hr, cliSources)
				assert.NoError(t, err)
				if d.chart == nil || !requiresChartLoader(d.chart) {
					t.Fatal("nested warning did not require the real loader")
				}
				diagnostics.Reset()
				_, err = cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
				assert.NoError(t, err)
				assert.Equal(t, diagnostics.Len(), 0)
			}
		})
	}
}

func TestReview_ErrorPrecedence(t *testing.T) {
	t.Parallel()
	t.Run("chart before capability and values", func(t *testing.T) {
		t.Parallel()
		root, st, hr := sourceValuesCacheFixture(t)
		testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: invalid\n")
		hr = hr.Clone()
		hr.ChartValuesFiles = []string{"absent.yaml"}
		hr.IgnoreMissingValuesFiles = false
		cli, err := NewClient(cachetest.New(t, "", cache.New))
		assert.NoError(t, err)
		cliSources := NewSources(st)
		_, err = cli.Template(t.Context(), hr, nil, RenderOptions{KubeVersion: "invalid", Sources: cliSources})
		if !errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), "load chart") {
			t.Fatalf("chart validation lost precedence: %v", err)
		}
	})
	for _, disabled := range []bool{false, true} {
		t.Run("OCI before capability/disabled="+map[bool]string{false: "false", true: "true"}[disabled], func(t *testing.T) {
			t.Parallel()
			st, hr, dir := ociRenderFixture(t)
			st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: os.DirFS(dir), Revision: "invalid"})
			cli, err := NewClient(cachetest.New(t, "", cache.New))
			assert.NoError(t, err)
			cliSources := NewSources(st)
			_, err = cli.Template(t.Context(), hr, nil, RenderOptions{KubeVersion: "invalid", DisableChartDigestTracking: disabled, Sources: cliSources})
			if !errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), "track oci chart version") {
				t.Fatalf("OCI validation lost precedence: %v", err)
			}
		})
	}
	for _, failure := range []string{"chart lookup", "chart malformed", "OCI tracked", "OCI disabled", "packaged malformed", "packaged missing"} {
		t.Run(failure+" before persisted fault", func(t *testing.T) {
			t.Parallel()
			if !renderPersistenceEligible(t) {
				t.Skip("combined faults require clean stamped persistence")
			}
			st, original, dir := ociRenderFixture(t)
			hr := original.Clone()
			art := &store.SourceArtifact{FS: os.DirFS(dir), LocalPath: dir, Revision: "6.15.0@sha256:0123456789abcdef"}
			opts := Options{}
			want := "load chart " + dir
			switch failure {
			case "chart lookup", "chart malformed":
				testutil.WriteFile(t, dir, "Chart.yaml", "apiVersion: v2\nname: podinfo\nversion: invalid\n")
				hr.ChartValuesFiles = nil
			case "OCI tracked", "OCI disabled":
				art.Revision = "invalid"
				opts.DisableChartDigestTracking = failure == "OCI disabled"
				want = "track oci chart version"
			case "packaged malformed", "packaged missing":
				hr.ChartRef = nil
				hr.Chart.Source.Kind = manifest.KindHelmChart
				hr.ChartValuesFiles = []string{"prod.yaml"}
				if failure == "packaged malformed" {
					testutil.WriteFile(t, dir, "prod.yaml", "marker: [\n")
				} else {
					hr.ChartValuesFiles = []string{"absent.yaml"}
				}
				want = "helm chart valuesFiles apps/podinfo"
			}
			st.SetArtifact(hr.ChartSource(), art)
			root := t.TempDir()
			owner := cachetest.New(t, root, cache.New)
			cli, err := NewClient(owner)
			assert.NoError(t, err)
			cliSources := NewSources(st)
			d, err := cli.describeChart(t.Context(), hr, cliSources)
			assert.NoError(t, err)
			if hr.UsesDirectOCIRepository() {
				d.oci = &ociChartContent{Digest: art.Digest, Revision: art.Revision, DisableTracking: opts.DisableChartDigestTracking}
			}
			d.values, d.valuesErr = cli.observeValuesFiles(t.Context(), d, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
			assert.NoError(t, d.valuesErr)
			key := cache.BlobKey{Content: computeTemplateKey(d, map[string]any{}, opts, hr), PersistOnly: true}
			r, _, _, err := owner.Blob(t.Context(), key, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				_, err := io.WriteString(w, "{")
				return cache.ProducerResult{}, err
			})
			assert.NoError(t, err)
			assert.NoError(t, r.Close())
			if failure == "chart lookup" {
				assert.NoError(t, os.RemoveAll(root))
			}
			r, _, _, err = owner.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil)
			if failure == "chart lookup" {
				if _, ok := errors.AsType[*cache.CacheError](err); !ok || !errors.Is(err, manifest.ErrFlux) || errors.Is(err, errdef.ErrNotFound) {
					t.Fatalf("fixture did not reach lookup fault: %v", err)
				}
			} else {
				assert.NoError(t, err)
				data, err := io.ReadAll(r)
				assert.NoError(t, err)
				assert.NoError(t, r.Close())
				var record renderRecord
				if _, ok := errors.AsType[*json.SyntaxError](json.Unmarshal(data, &record)); !ok {
					t.Fatal("fixture did not reach malformed persisted record")
				}
			}
			_, err = cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources})
			if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), want) {
				t.Fatalf("preparation lost precedence over persisted fault: %v", err)
			}
			if strings.HasPrefix(failure, "chart") {
				if _, ok := errors.AsType[chart.ValidationError](err); !ok {
					t.Fatalf("chart validation sentinel lost: %v", err)
				}
			}
			if strings.HasPrefix(failure, "packaged") && !strings.Contains(err.Error(), hr.ChartValuesFiles[0]) {
				t.Fatalf("values file context lost: %v", err)
			}
			if failure == "packaged missing" && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("missing values sentinel lost: %v", err)
			}
		})
	}
}

func TestReview_ErrorPrecedenceDiagnostics(t *testing.T) {
	t.Run("record decode before action", func(t *testing.T) {
		if !renderPersistenceEligible(t) {
			t.Skip("requires clean stamped persistence for storage-error precedence")
		}
		root, st, hr := sourceValuesCacheFixture(t)
		testutil.WriteFile(t, root, "charts/demo/templates/fail.yaml", `{{ fail "action must wait for cache error" }}`)
		testutil.WriteFile(t, root, "charts/demo/requirements.yaml", "dependencies: []\n")
		owner := cachetest.New(t, t.TempDir(), cache.New)
		cli, err := NewClient(owner)
		assert.NoError(t, err)
		cliSources := NewSources(st)
		d, err := cli.describeChart(t.Context(), hr, cliSources)
		assert.NoError(t, err)
		d.values, d.valuesErr = cli.observeValuesFiles(t.Context(), d, hr.ChartValuesFiles, true)
		assert.NoError(t, d.valuesErr)
		r, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: computeTemplateKey(d, map[string]any{}, Options{}, hr), PersistOnly: true}, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			_, err := io.WriteString(w, "{")
			return cache.ProducerResult{}, err
		})
		assert.NoError(t, err)
		assert.NoError(t, r.Close())
		var diagnostics bytes.Buffer
		previous := log.Writer()
		log.SetOutput(&diagnostics)
		defer log.SetOutput(previous)
		_, err = cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
		_, syntax := errors.AsType[*json.SyntaxError](err)
		if !errors.Is(err, manifest.ErrFlux) || !syntax || strings.Contains(err.Error(), "action must wait") || !strings.Contains(diagnostics.String(), "Warning:") {
			t.Fatalf("deferred cache error lost precedence: %v", err)
		}
	})
}

func TestDescription_CurrentChartLocation(t *testing.T) {
	t.Parallel()
	input := fstest.MapFS{
		"first/Chart.yaml":  {Data: []byte("apiVersion: v2\nname: demo\nversion: invalid\n")},
		"second/Chart.yaml": {Data: []byte("apiVersion: v2\nname: demo\nversion: invalid\n")},
	}
	st := store.New()
	hr := minimalHR()
	hr.Chart = manifest.HelmChart{Name: "first", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "repo", Namespace: "default"}}
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input})
	cli, err := NewClient(cachetest.New(t, "", cache.New))
	assert.NoError(t, err)
	cliSources := NewSources(st)
	for _, location := range []string{"first", "second"} {
		request := hr.Clone()
		request.Chart.Name = location
		_, err := cli.Template(t.Context(), request, nil, RenderOptions{Options: Options{}, Sources: cliSources})
		if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "load chart "+location+":") {
			t.Fatalf("location %q: got %v", location, err)
		}
	}
}
