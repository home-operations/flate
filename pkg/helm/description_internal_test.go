package helm

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"log" //nolint:depguard // The fixture must capture standard-library logs emitted by Helm dependencies.
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	chartcommon "helm.sh/helm/v4/pkg/chart/common"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestDescriptionProjection(t *testing.T) {
	t.Parallel()
	files := fstest.MapFS{
		"Chart.yaml": {
			Data: []byte(
				"apiVersion: v2\nname: test\nversion: 1.0.0\ndependencies: [{name: child, alias: childAlias, version: 1.0.0, condition: feature.enabled}]\n",
			),
		},
		"values.yaml":        {Data: []byte("used: first\n")},
		"templates/cm.yaml":  {Data: []byte("{{ .Values.used }}")},
		"values.schema.json": {Data: []byte(`{"type":"object"}`)},
		"charts/child/Chart.yaml": {
			Data: []byte(
				"apiVersion: v2\nname: child\nversion: 1.0.0\ndependencies: [{name: grand, version: 1.0.0, tags: [group]}]\n",
			),
		},
		"charts/child/templates/unused.yaml": {Data: []byte("{{ .Values.nested }}")},
	}
	initial, _, err := captureChart(t.Context(), files, ".", false)
	assert.NoError(t, err)
	for name := range files {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testDescriptionProjectionCase(
				t,
				&testDescriptionProjectionCaseInputs{files: &files, initial: &initial, name: &name},
			)
		},
		)
	}
	ch, err := initial.load()
	assert.NoError(t, err)
	assert.Equal(t, strings.Contains(chartTemplateText(ch), ".Values.nested"), true)
	assert.Equal(t, requiresChartLoader(ch), false)
	analysis := &chartAnalysis{
		Version:        ch.Metadata.Version,
		Templates:      chartTemplateText(ch),
		Allowed:        knownTopLevelKeys(ch),
		RequiresLoader: requiresChartLoader(ch),
	}
	assert.Diff(
		t,
		analysis.Allowed,
		map[string]struct{}{"child": {}, "childAlias": {}, "feature": {}, "global": {}, "tags": {}},
	)
	data, err := json.Marshal(renderRecord{Output: []byte("rendered"), Analysis: analysis})
	assert.NoError(t, err)
	var restored renderRecord
	assert.NoError(t, json.Unmarshal(data, &restored))
	assert.Diff(t, restored.Output, []byte("rendered"))
	assert.Diff(t, restored.Analysis, analysis)
	values := map[string]any{
		"used":       1,
		"nested":     1,
		"child":      1,
		"childAlias": 1,
		"feature":    1,
		"global":     1,
		"tags":       1,
		"unused":     1,
	}
	assert.Diff(t, analysis.unreferencedValuePaths(values), []string{"unused"})
	assert.Diff(t, restored.Analysis.unreferencedValuePaths(values), []string{"unused"})
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "opaque root", true: "opaque subchart"}[nested], func(t *testing.T) {
			t.Parallel()
			testDescriptionProjectionCase2(
				t,
				&testDescriptionProjectionCase2Inputs{initial: &initial, values: &values, nested: &nested},
			)
		},
		)
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
	cli := NewClient(cachetest.New(t, "", cache.New))
	cliSources := NewSources(st)
	out, receipt1, err := testutil.RenderMaps(
		cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: hr.Named(), Receipt: receipt1, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Equal(t, reads, 1)
	assert.FatalIf(t, !hasRenderField(out, "marker", "first"), "key and merge observed different input: %s", out)
}

func TestDescription_HitHydration(t *testing.T) {
	t.Parallel()
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory miss", true: "fresh persisted hit"}[persistent], func(t *testing.T) {
			t.Parallel()
			testDescriptionHitHydrationCase(t, &persistent)
		},
		)
	}
}

func TestDescription_HydrationKeepsDecoded(t *testing.T) {
	t.Parallel()
	for _, packaged := range []bool{false, true} {
		t.Run(map[bool]string{false: "files", true: "archive"}[packaged], func(t *testing.T) {
			t.Parallel()
			testDescriptionHydrationKeepsDecodedCase(
				t,
				&packaged,
			)
		},
		)
	}
}

func TestReview_ByteIdentity(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires clean stamped persistence; item 2 must execute the output guard mutation")
	}
	root, st, hr := sourceValuesCacheFixture(t)
	testutil.WriteFile(
		t,
		root,
		"charts/demo/crds/raw.yaml",
		"apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name: fixture}\n# raw: "+string(
			[]byte{0xff},
		)+"\n",
	)
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
				testutil.WriteFile(
					t,
					root,
					"charts/demo/Chart.yaml",
					"apiVersion: v2\nname: demo\nversion: 1.0.0\ndependencies: [{name: child, version: 1.0.0, condition: child.enabled}]\n",
				)
				testutil.WriteFile(t, root, "charts/demo/values.yaml", "marker: default\nchild: {enabled: false}\n")
				location += "/charts/child"
				testutil.WriteFile(t, root, location+"/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.0.0\n")
			}
			testutil.WriteFile(
				t,
				root,
				location+"/templates/_unused.tpl",
				"{{/* unused .Values "+string([]byte{0xff})+" */}}",
			)
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
		cli := NewClient(owner)
		cliSources := NewSources(st)
		out, receipt5, err := testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt5, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		if validOutput {
			assert.NoError(t, err)
		} else {
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || len(out) != 0,
				"invalid UTF-8 render: %v, %v",
				out,
				err,
			)
		}
		if i == 0 {
			want = out
		}
		assert.Diff(t, out, want)
		d, err := cli.describeChart(t.Context(), hr, cliSources)
		assert.NoError(t, err)
		assert.FatalIf(t, d.chart == nil, "%s", "ineligible input hydrated without a real decode")
		assert.FatalIf(t, validOutput && validChartTemplates(d.chart), "%s", "invalid template bytes were changed")
		d.values, d.valuesErr = cli.observeValuesFiles(
			t.Context(),
			&d,
			hr.ChartValuesFiles,
			hr.IgnoreMissingValuesFiles,
		)
		assert.NoError(t, d.valuesErr)
		_, _, _, err = owner.Blob(
			t.Context(),
			cache.BlobKey{
				Content: computeTemplateKey(&d, map[string]any{}, Options{}, hr),
				Render:  cache.RenderAccepted,
			},
			cache.Policy{MaxAge: cache.Forever},
			nil,
		)
		assert.FatalIf(t, err == nil, "%s", "invalid UTF-8 was published")
		assert.FatalIf(t, !errors.Is(err, errdef.ErrNotFound), "%v", err)
	}
}

//nolint:paralleltest // test changes process-wide logging.
func TestReview_NestedWarnings(t *testing.T) {
	if !renderPersistenceEligible(t) {
		t.Skip("requires clean stamped persistence; item 2 must execute recursive diagnostic mutations")
	}
	for _, name := range []string{"requirements.yaml", "requirements.lock"} {
		t.Run(
			name,
			func(t *testing.T) { testReviewNestedWarningsCase(t, &name) },
		)
	}
}

type testDescriptionProjectionCaseInputs struct {
	files   *fstest.MapFS
	initial *capturedChart
	name    *string
}

func testDescriptionProjectionCase(t *testing.T, testCaseInput *testDescriptionProjectionCaseInputs) {
	t.Helper()

	changed := fstest.MapFS{}
	for path, file := range *testCaseInput.files {
		cloned := *file
		changed[path] = &cloned
	}
	changed[(*testCaseInput.name)].Data = append(bytes.Clone(changed[(*testCaseInput.name)].Data), '\n')
	next, _, captureChartErr := captureChart(t.Context(), changed, ".", false)
	assert.NoError(t, captureChartErr)
	assert.FatalIf(
		t,
		testCaseInput.initial.fingerprint() == next.fingerprint(),
		"%s",
		"changed captured bytes shared an identity",
	)
}

type testDescriptionProjectionCase2Inputs struct {
	initial *capturedChart
	values  *map[string]any
	nested  *bool
}

func testDescriptionProjectionCase2(t *testing.T, testCaseInput *testDescriptionProjectionCase2Inputs) {
	t.Helper()

	ch, err := testCaseInput.initial.load()
	assert.NoError(t, err)
	ch.Templates = append(
		ch.Templates,
		&chartcommon.File{Data: []byte(strings.Repeat("unused analysis payload\n", 1<<14))},
	)
	target := ch
	if *testCaseInput.nested {
		target = ch.Dependencies()[0]
	}
	target.Templates = append(target.Templates, &chartcommon.File{Data: []byte("{{ toYaml .Values }}")})
	analysis := (&chartDescription{chart: ch}).analyze()
	assert.FatalIf(t, analysis.Templates != "", "%s", "opaque chart retained unused template analysis")
	assert.Diff(t, analysis.unreferencedValuePaths((*testCaseInput.values)), []string(nil))
	data, err := json.Marshal(renderRecord{Output: []byte("rendered"), Analysis: analysis})
	assert.NoError(t, err)
	assert.FatalIf(t, len(data) > 1024, "opaque analysis payload is %d bytes", len(data))
}

func testDescriptionHitHydrationCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	if (*testCaseInput) && !renderPersistenceEligible(t) {
		t.Skip("requires clean stamped persistence; item 2 must execute this proof")
	}
	_, st, hr := sourceValuesCacheFixture(t)
	root := ""
	if *testCaseInput {
		root = t.TempDir()
	}
	var want []map[string]any
	for i := range 2 {
		cli := NewClient(cachetest.New(t, root, cache.New))
		cliSources := NewSources(st)
		out, receipt2, err := testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt2, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		if i == 0 {
			want = out
		}
		assert.Diff(t, out, want)
		d, err := cli.describeChart(t.Context(), hr, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, d.chart == nil, (*testCaseInput) && i == 1)
		if !(*testCaseInput) || i == 0 {
			assert.FatalIf(
				t,
				d.analysis == nil || d.captured.files != nil || d.captured.archive != nil,
				"%s",
				"decoded generation lost analysis or retained captured input",
			)
		}
		assert.Diff(
			t,
			cli.StaleValuePaths(t.Context(), hr, map[string]any{"marker": 1, "unused": 1}, cliSources),
			[]string{"unused"},
		)
		{
			var receipt3 *manifest.RenderReceipt
			_, receipt3, err = testutil.RenderMaps(
				cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
			)
			if err == nil {
				{
					recordErr := cli.RecordRender(
						t.Context(),
						RenderAcceptance{Owner: hr.Named(), Receipt: receipt3, Records: emptyRenderInventory},
					)
					assert.NoError(t, recordErr)
				}
			}

			assert.NoError(t, err)
			hit, describeChartErr := cli.describeChart(t.Context(), hr, cliSources)
			assert.NoError(t, describeChartErr)
			var receipt4 *manifest.RenderReceipt
			_, receipt4, describeChartErr = testutil.RenderMaps(
				cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
			)
			if describeChartErr == nil {
				{
					recordErr := cli.RecordRender(
						t.Context(),
						RenderAcceptance{Owner: hr.Named(), Receipt: receipt4, Records: emptyRenderInventory},
					)
					assert.NoError(t, recordErr)
				}
			}

			assert.NoError(t, describeChartErr)
			repeated, describeChartErr := cli.describeChart(t.Context(), hr, cliSources)
			assert.NoError(t, describeChartErr)
			assert.FatalIf(t, hit.record == nil || hit.record != repeated.record ||
				hit.record.Analysis != repeated.record.Analysis, "%s", "same record did not share its immutable typed projection")
			assert.FatalIf(t, hit.analysis != hit.record.Analysis, "%s", "record did not share canonical analysis")
			assert.FatalIf(t, (*testCaseInput) && i == 1 && hit.chart != nil, "%s", "fresh hit decoded the chart")
		}
		after, err := cli.describeChart(t.Context(), hr, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, after.chart == d.chart, true)
	}
}

func testDescriptionHydrationKeepsDecodedCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	root, st, hr := sourceValuesCacheFixture(t)
	if *testCaseInput {
		input, err := fs.Sub(os.DirFS(root), "charts/demo")
		assert.NoError(t, err)
		st.SetArtifact(
			hr.ChartSource(),
			&store.SourceArtifact{FS: fstest.MapFS{"chart.tgz": {Data: chartArchive(t, input)}}},
		)
		hr = hr.Clone()
		hr.Chart.Name = "chart.tgz"
	}
	cli := NewClient(cachetest.New(t, "", cache.New))
	cliSources := NewSources(st)
	d, err := cli.describeChart(t.Context(), hr, cliSources)
	assert.NoError(t, err)
	ch, err := d.captured.load()
	assert.NoError(t, err)
	fresh := &renderRecord{
		Output: []byte("hit"),
		Analysis: &chartAnalysis{
			Version:   ch.Metadata.Version,
			Templates: chartTemplateText(ch),
			Allowed:   knownTopLevelKeys(ch),
		},
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	decoded := d
	var decodeErr error
	wg.Go(func() { <-start; decodeErr = cli.decodeChart(t.Context(), &decoded) })
	wg.Go(func() { <-start; cli.rememberDescription(&d, fresh) })
	close(start)
	wg.Wait()
	assert.NoError(t, decodeErr)
	assert.FatalIf(
		t,
		decoded.captured.files != nil || decoded.captured.archive != nil,
		"%s",
		"decode retained captured input",
	)
	late := &renderRecord{
		Output: []byte("late"),
		Analysis: &chartAnalysis{
			Version:   ch.Metadata.Version,
			Templates: chartTemplateText(ch),
			Allowed:   knownTopLevelKeys(ch),
		},
	}
	cli.rememberDescription(&d, late)
	cli.rememberDescription(&d, nil)
	current, err := cli.describeChart(t.Context(), hr, cliSources)
	assert.NoError(t, err)
	assert.Equal(t, current.chart != nil && current.chart == decoded.chart, true)
	assert.Equal(t, current.record == late, true)
	assert.FatalIf(
		t,
		fresh.Analysis != current.analysis || late.Analysis != current.analysis,
		"%s",
		"publication did not share canonical analysis",
	)
	assert.FatalIf(
		t,
		current.captured.files != nil || current.captured.archive != nil,
		"%s",
		"stale publication restored captured input after decode",
	)
	reused := d
	err = cli.decodeChart(t.Context(), &reused)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		reused.chart != current.chart || reused.captured.files != nil || reused.captured.archive != nil,
		"%s",
		"decode reuse retained stale captured input",
	)
}

func testReviewNestedWarningsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()
	root, st, hr := sourceValuesCacheFixture(t)
	testutil.WriteFile(
		t,
		root,
		"charts/demo/charts/child/Chart.yaml",
		"apiVersion: v2\nname: child\nversion: 1.0.0\n",
	)
	testutil.WriteFile(t, root, "charts/demo/charts/child/"+(*testCaseInput), "dependencies: []\n")
	cacheRoot := t.TempDir()
	var diagnostics bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&diagnostics)
	defer log.SetOutput(previous)
	for range 2 {
		diagnostics.Reset()
		cli := NewClient(cachetest.New(t, cacheRoot, cache.New))
		cliSources := NewSources(st)
		var receipt6 *manifest.RenderReceipt
		var err error
		_, receipt6, err = testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt6, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		assert.FatalIf(
			t,
			!strings.Contains(diagnostics.String(), "Warning:"),
			"%s",
			"real nested loader diagnostics were suppressed",
		)
		d, err := cli.describeChart(t.Context(), hr, cliSources)
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			d.chart == nil || !requiresChartLoader(d.chart),
			"%s",
			"nested warning did not require the real loader",
		)
		diagnostics.Reset()
		var receipt7 *manifest.RenderReceipt
		_, receipt7, err = testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt7, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Equal(t, diagnostics.Len(), 0)
	}
}
