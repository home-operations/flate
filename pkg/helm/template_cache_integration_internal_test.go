package helm

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/kustomize"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestTemplateCache_TemplateIntegration(t *testing.T) {
	t.Parallel()
	t.Run(
		"fresh analysis handoff",
		func(t *testing.T) { t.Parallel(); testTemplateCacheTemplateIntegrationFreshAnalysisHandoff(t) },
	)
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
			testTemplateCacheTemplateIntegrationCase(t, &tc)
		},
		)
	}
	t.Run("cold coalescing", func(t *testing.T) { t.Parallel(); testTemplateCacheTemplateIntegrationColdCoalescing(t) })
	for _, failure := range []string{"template", "post-renderer"} {
		t.Run(failure+" diagnostics", func(t *testing.T) {
			t.Parallel()
			testTemplateCacheTemplateIntegrationCase2(
				t,
				&failure,
			)
		},
		)
	}
	t.Run("empty stream", func(t *testing.T) { t.Parallel(); testTemplateCacheTemplateIntegrationEmptyStream(t) })
	for _, backing := range []string{"memory", "disk", "acquired"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			testTemplateCacheTemplateIntegrationCase3(
				t,
				&backing,
			)
		},

		// Remove chart backing alone so completed output remains readable.

		)
	}
}

func testTemplateCacheTemplateIntegrationFreshAnalysisHandoff(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		dir,
		"mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: example}\ndata: {marker: {{ .Values.marker | quote }}}\n",
	)
	cli := NewClient(cachetest.New(t, "", cache.New))
	sources := localChartResolver(t, "chart-repo", "flux-system", dir)
	for _, marker := range []string{"first", "second"} {
		hr := newHR()
		values := map[string]any{"marker": marker}
		input := RenderOptions{Sources: sources}
		prepared, err := cli.prepareTemplate(t.Context(), hr, values, &input)
		assert.NoError(t, err)
		docs, receipt, err := testutil.RenderMaps(cli.Template(t.Context(), hr, values, input))
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			len(docs) != 1 || receipt == nil,
			"fresh render %s returned %d documents and receipt %v",
			marker,
			len(docs),
			receipt,
		)
		cli.chartMu.RLock()
		canonical := cli.chartCache[prepared.loaded.key].analysis
		cli.chartMu.RUnlock()
		frame := receipt.Value().(renderFrame)
		assert.FatalIf(
			t,
			frame.header.Analysis != canonical,
			"fresh render %s cloned immutable chart analysis",
			marker,
		)
		assert.FatalIf(
			t,
			frame.chart != nil || frame.stored != nil,
			"%s",
			"fresh receipt retained noncompact backing",
		)
		again, reused, err := testutil.RenderMaps(cli.Template(t.Context(), hr, values, input))
		assert.NoError(t, err)
		assert.Diff(t, docs, again)
		cached := reused.Value().(renderFrame)
		assert.FatalIf(
			t,
			cached.header.Analysis != canonical,
			"%s",
			"cached render reconstructed canonical analysis",
		)
		assert.Diff(t, frame.output, cached.output)
		docs[0]["data"].(map[string]any)["marker"] = "mutated"
		assert.Equal(t, again[0]["data"].(map[string]any)["marker"].(string), marker)
	}
}

func testTemplateCacheTemplateIntegrationCase(
	t *testing.T,
	testCaseInput *struct {
		name    string
		input   string
		invalid bool
	},
) {
	t.Helper()

	dir, root := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		dir,
		"mychart/templates/list.yaml",
		"{{ if .Values.fail }}{{ fail \"cache miss action\" }}{{ end }}\n"+testCaseInput.input,
	)
	owner := cachetest.New(t, root, cache.New)
	cli := NewClient(owner)
	sources := localChartResolver(t, "chart-repo", "flux-system", dir)
	first, receipt, firstErr := testutil.RenderMaps(
		cli.Template(t.Context(), newHR(), nil, RenderOptions{Sources: sources}),
	)
	if testCaseInput.invalid {
		assert.FatalIf(
			t,
			!errors.Is(firstErr, manifest.ErrInput) || first != nil,
			"cold invalid list = %#v, %v",
			first,
			firstErr,
		)
	} else {
		assert.NoError(t, firstErr)
		assert.NoError(
			t,
			cli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   newHR().Named(),
					Receipt: receipt,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			),
		)
	}
	t.Run("persisted replay", func(t *testing.T) {
		t.Parallel()
		if !renderPersistenceEligible(t) {
			t.Skipf("requires identified test binary: %s", cache.RenderPersistenceDisabledReason())
		}
		loaded, err := loadDecodedChart(t.Context(), cli, newHR(), sources)
		assert.NoError(t, err)
		loaded.values, loaded.valuesErr = cli.observeValuesFiles(
			t.Context(), &loaded, newHR().ChartValuesFiles,
			newHR().IgnoreMissingValuesFiles,
		)
		assert.NoError(t, loaded.valuesErr)
		key := cache.BlobKey{
			Content: computeTemplateKey(&loaded, map[string]any{}, Options{}, newHR()),
			Render:  cache.RenderAccepted,
		}
		if testCaseInput.invalid {
			raw, splitDocsErr := testutil.SplitDocs([]byte(testCaseInput.input))
			assert.NoError(t, splitDocsErr)
			record := renderRecord{Output: encodedRender(t, raw), Analysis: loaded.analyze()}
			_, _, _, splitDocsErr = owner.Blob(
				t.Context(),
				key,
				&cache.Policy{},
				func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
					return cache.ProducerResult{}, writeTestRecord(t.Context(), w, key, &record)
				},
			)
			assert.NoError(t, splitDocsErr)
		}
		r, _, _, err := owner.Blob(t.Context(), key, &cache.Policy{MaxAge: cache.Forever}, nil)
		assert.NoError(t, err)
		var record renderRecord
		assert.NoError(t, readTestRecord(t.Context(), r, &record))

		raw, err := testutil.DecodeRender(record.Output)
		assert.NoError(t, err)
		wantRaw, err := testutil.SplitDocs([]byte(testCaseInput.input))
		assert.NoError(t, err)
		assert.Diff(t, raw, wantRaw)
		values := map[string]any{"fail": true}
		key.Content = computeTemplateKey(&loaded, values, Options{}, newHR())
		_, _, _, err = owner.Blob(
			t.Context(),
			key,
			&cache.Policy{},
			func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				return cache.ProducerResult{}, writeTestRecord(t.Context(), w, key, &record)
			},
		)
		assert.NoError(t, err)
		assert.NoError(t, owner.Close())
		fresh := NewClient(cachetest.New(t, root, cache.New))
		out, _, err := testutil.RenderMaps(
			fresh.Template(
				t.Context(),
				newHR(),
				values,
				RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)},
			),
		)
		if testCaseInput.invalid {
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || err.Error() != firstErr.Error() || out != nil,
				"persisted invalid list = %#v, %v; cold = %v",
				out,
				err,
				firstErr,
			)
		} else {
			assert.NoError(t, err)
			assert.Diff(t, out, first)
		}
		t.Log("verified persisted raw-list hit; Helm action would fail on a miss")
	})
}

func testTemplateCacheTemplateIntegrationColdCoalescing(t *testing.T) {
	t.Helper()

	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk_%t", disk), func(t *testing.T) {
			t.Parallel()
			root := ""
			if disk {
				root = t.TempDir()
			}
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
			testutil.WriteFile(
				t,
				dir,
				"mychart/templates/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata:\n  action: {{ .Values.probe | quote }}\n",
			)
			cli := NewClient(cachetest.New(t, root, cache.New))
			probe := make(templateActionProbe, 4)
			var outputs [2][]map[string]any
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range outputs {
				wg.Go(func() {
					<-start
					var err error
					outputs[i], _, err = testutil.RenderMaps(
						cli.Template(
							t.Context(),
							newHR(),
							map[string]any{"probe": probe},
							RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)},
						),
					)
					assert.NoError(t, err)
				})
			}
			close(start)
			wg.Wait()
			assert.Equal(t, len(probe), 1)
			assert.Diff(t, outputs[0], outputs[1])
			outputs[0][0]["data"].(map[string]any)["action"] = "mutated"
			again, _, err := testutil.RenderMaps(
				cli.Template(
					t.Context(),
					newHR(),
					map[string]any{"probe": probe},
					RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)},
				),
			)
			assert.NoError(t, err)
			assert.Diff(t, again, outputs[1])
			assert.Equal(t, len(probe), 1)
		})
	}
}

func testTemplateCacheTemplateIntegrationCase2(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata:\n  action: {{ .Values.probe | quote }}\n"
	hr := newHR()
	hr.DisableSchemaValidation = true
	want := "producer diagnostic"
	if (*testCaseInput) == "template" {
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
		out, _, err := testutil.RenderMaps(
			cli.Template(
				t.Context(),
				hr,
				values,
				RenderOptions{Sources: cliSources, Admit: func(admission RenderAdmission) (bool, error) {
					value := admission.Key
					key = value
					return false, nil
				}},
			),
		)
		assert.Equal(t, key != "", keyed)
		assert.Equal(t, len(probe), i+1)
		assert.FatalIf(
			t,
			len(out) != 0 || err == nil || !strings.HasPrefix(err.Error(), "helm template: ") ||
				!strings.Contains(err.Error(), want),
			"keyed=%t: output %v, error %v",
			keyed,
			out,
			err,
		)
		{
			_, storage := errors.AsType[*cache.CacheError](err)
			assert.FatalIf(t, storage, "keyed=%t: producer failure classified as storage: %v", keyed, err)
		}
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), keyed)
		if previous != nil {
			assert.Equal(t, err.Error(), previous.Error())
		}
		previous = err
	}
}

func testTemplateCacheTemplateIntegrationEmptyStream(t *testing.T) {
	t.Helper()

	dir, root := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	for range 2 {
		cli := NewClient(cachetest.New(t, root, cache.New))
		cliSources := localChartResolver(t, "chart-repo", "flux-system", dir)
		docs, receipt12, err := testutil.RenderMaps(
			cli.Template(t.Context(), newHR(), nil, RenderOptions{Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{
						Owner:   newHR().Named(),
						Receipt: receipt12,
						Records: emptyRenderInventory,
						Sources: []manifest.SourceEvidence{},
					},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Equal(t, len(docs), 0)
		if renderPersistenceEligible(t) {
			loaded, err := cli.describeChart(t.Context(), newHR(), cliSources.chartInput(newHR()))
			assert.NoError(t, err)
			loaded.values, loaded.valuesErr = cli.observeValuesFiles(
				t.Context(), &loaded, newHR().ChartValuesFiles,
				newHR().IgnoreMissingValuesFiles,
			)
			assert.NoError(t, loaded.valuesErr)
			key := cache.BlobKey{
				Content: computeTemplateKey(&loaded, map[string]any{}, Options{}, newHR()),
				Render:  cache.RenderAccepted,
			}
			r, _, _, err := cli.cache.Blob(t.Context(), key, &cache.Policy{MaxAge: cache.Forever}, nil)
			assert.NoError(t, err)
			var record renderRecord
			assert.NoError(t, readTestRecord(t.Context(), r, &record))

			decoded, err := testutil.DecodeRender(record.Output)
			assert.NoError(t, err)
			assert.Equal(t, len(decoded), 0)
		}
	}
}

func testTemplateCacheTemplateIntegrationCase3(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := ""
	if (*testCaseInput) != "memory" {
		root = t.TempDir()
	}
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		dir,
		"mychart/templates/configmap.yaml",
		`{{ if .Values.fail }}{{ fail "cache miss action" }}{{ end }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
data:
  greeting: {{ .Values.greeting }}
extra: {sequence: [{value: original}]}
`,
	)
	owner := cachetest.New(t, root, cache.New)
	cli := NewClient(owner)
	reads := map[string]int{}
	input := os.DirFS(dir)
	chartRoot := ""
	if (*testCaseInput) == "acquired" {
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
	first, receipt13, err := testutil.RenderMaps(
		cli.Template(
			t.Context(),
			newHR(),
			map[string]any{"greeting": "hi"},
			RenderOptions{Options: Options{}, Sources: cliSources},
		),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   newHR().Named(),
					Receipt: receipt13,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	canonical, err := loadDecodedChart(t.Context(), cli, newHR(), cliSources)
	assert.NoError(t, err)
	beforeReads := reads["mychart/templates/configmap.yaml"]
	second, receipt14, err := testutil.RenderMaps(
		cli.Template(
			t.Context(),
			newHR(),
			map[string]any{"greeting": "hi"},
			RenderOptions{Options: Options{}, Sources: cliSources},
		),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   newHR().Named(),
					Receipt: receipt14,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Diff(t, second, first)
	admittedKey := ""
	blocked, receipt15, err := testutil.RenderMaps(
		cli.Template(t.Context(), newHR(), map[string]any{"greeting": "hi"}, RenderOptions{
			Sources: cliSources,
			Admit: func(admission RenderAdmission) (bool, error) {
				key := admission.Key
				assert.Equal(t, admission.Receipt != nil, true)
				admittedKey = key
				return true, nil
			},
		}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   newHR().Named(),
					Receipt: receipt15,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Diff(t, blocked, []map[string]any(nil))
	assert.FatalIf(t, admittedKey == "", "%s", "completed output bypassed admission")
	if (*testCaseInput) != "acquired" {
		assert.Equal(t, reads["mychart/templates/configmap.yaml"], beforeReads+2)
	}
	warm, err := loadDecodedChart(t.Context(), cli, newHR(), cliSources)
	assert.NoError(t, err)
	assert.Equal(t, warm.chart == canonical.chart, true)
	assert.Equal(t, warm.fingerprint, canonical.fingerprint)
	third, receipt16, err := testutil.RenderMaps(
		cli.Template(
			t.Context(),
			newHR(),
			map[string]any{"greeting": "different"},
			RenderOptions{Options: Options{}, Sources: cliSources},
		),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   newHR().Named(),
					Receipt: receipt16,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Equal(t, reflect.DeepEqual(third, first), false)
	loaded, err := loadDecodedChart(t.Context(), cli, newHR(), cliSources)
	assert.NoError(t, err)
	values := map[string]any{"greeting": "hi", "fail": true}
	loaded.values, loaded.valuesErr = cli.observeValuesFiles(
		t.Context(), &loaded, newHR().ChartValuesFiles,
		newHR().IgnoreMissingValuesFiles,
	)
	assert.NoError(t, loaded.valuesErr)
	key := cache.BlobKey{
		Content: computeTemplateKey(&loaded, values, Options{}, newHR()),
		Render:  cache.RenderAccepted,
	}
	_, _, _, err = owner.Blob(
		t.Context(),
		key,
		&cache.Policy{MaxAge: 0},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			writeTestRecordErr := writeTestRecord(t.Context(),
				w,
				key,
				&renderRecord{Output: encodedRender(t, first), Analysis: loaded.analyze()},
			)
			return cache.ProducerResult{}, writeTestRecordErr
		},
	)
	assert.NoError(t, err)

	{
		closeErr := owner.Close()
		assert.NoError(t, closeErr)
	}
	cli = NewClient(cachetest.New(t, root, cache.New))
	cliSources = resolver
	loaded, err = loadDecodedChart(t.Context(), cli, newHR(), cliSources)
	assert.NoError(t, err)
	out, receipt17, err := testutil.RenderMaps(
		cli.Template(t.Context(), newHR(), values, RenderOptions{Options: Options{}, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   newHR().Named(),
					Receipt: receipt17,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	if root != "" && renderPersistenceEligible(t) {
		testTemplateCachePersistedIntegration(
			t,
			&templateCacheIntegrationInputs{
				root:       &root,
				cli:        &cli,
				cliSources: &cliSources,
				loaded:     &loaded,
				values:     &values,
				first:      &first,
				out:        &out,
				err:        &err,
			},
		)
	} else {
		assert.FatalIf(
			t,
			err == nil || !strings.Contains(err.Error(), "cache miss action"),
			"ineligible output did not bypass persistence: %v",
			err,
		)
	}
	warm, err = loadDecodedChart(t.Context(), cli, newHR(), cliSources)
	assert.NoError(t, err)
	assert.Equal(t, warm.chart == loaded.chart, true)
	assert.Equal(t, warm.fingerprint, loaded.fingerprint)
	values["greeting"] = "changed"
	_, receipt26, err := testutil.RenderMaps(
		cli.Template(t.Context(), newHR(), values, RenderOptions{Options: Options{}, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   newHR().Named(),
					Receipt: receipt26,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}
	assert.FatalIf(
		t,
		err == nil || !strings.Contains(err.Error(), "cache miss action"),
		"changed input did not miss completed output: %v",
		err,
	)
	testTemplateCacheTemplateIntegrationCase3Loop(t, struct {
		testCaseInput **string
		chartRoot     *string
		input         *fs.FS
		cli           **Client
		cliSources    *Sources
		first         *[]map[string]any
	}{testCaseInput: &testCaseInput, chartRoot: &chartRoot, input: &input, cli: &cli, cliSources: &cliSources, first: &first})
}

func testTemplateCacheTemplateIntegrationCase3Loop(t *testing.T, loopInputs struct {
	testCaseInput **string
	chartRoot     *string
	input         *fs.FS
	cli           **Client
	cliSources    *Sources
	first         *[]map[string]any
},
) {
	t.Helper()

	if (*(*loopInputs.testCaseInput)) == "acquired" {
		assert.NoError(t, os.RemoveAll(filepath.Join((*loopInputs.chartRoot), "oci", "blobs")))
		_, err := fs.ReadFile((*loopInputs.input), "mychart/templates/configmap.yaml")
		assert.FatalIf(t, err == nil, "%s", "backing removal did not prevent chart reads")
		repeated, receipt27, err := testutil.RenderMaps(
			(*loopInputs.cli).Template(
				t.Context(),
				newHR(),
				map[string]any{"greeting": "hi"},
				RenderOptions{Sources: (*loopInputs.cliSources)},
			),
		)
		if err == nil {
			{
				recordErr := (*loopInputs.cli).RecordRender(
					t.Context(),
					RenderAcceptance{
						Owner:   newHR().Named(),
						Receipt: receipt27,
						Records: emptyRenderInventory,
						Sources: []manifest.SourceEvidence{},
					},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Diff(t, repeated, (*loopInputs.first))
	}
}

type templateCacheIntegrationInputs struct {
	root       *string
	cli        **Client
	cliSources *Sources
	loaded     *chartDescription
	values     *map[string]any
	first      *[]map[string]any
	out        *[]map[string]any
	err        *error
}
