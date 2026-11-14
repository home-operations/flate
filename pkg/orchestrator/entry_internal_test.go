package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_EntryDeclarationQualifiesOpaqueApplier(t *testing.T) {
	t.Run("settled_candidates", testRenderChangedLiteralEntryDrainsSettledCandidates)
	t.Run("supplied applications", testSuppliedApplications)
	for _, scenario := range []string{"declaration", "templated_group", "mutable_chart", "nested_chart", "nested_unchanged", "incomplete_inputs", "content", "owned_source"} {
		t.Run(scenario, func(t *testing.T) {
			testRenderEntryDeclarationQualifiesOpaqueApplierCase(
				t,
				&scenario,
			)
		},
		)
	}
}

func testRenderEntryDeclarationQualifiesOpaqueApplierCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()
	declaration := (*testCaseInput) != "content" && (*testCaseInput) != "owned_source"
	nested := (*testCaseInput) == "nested_chart" || (*testCaseInput) == "nested_unchanged"
	workload := "workload"
	if (*testCaseInput) == "owned_source" {
		workload = "entry/workload"
	}
	root, original, chart := t.TempDir(), t.TempDir(), t.TempDir()
	const url = "https://example.test/entry/repo"
	for _, dir := range []string{root, original} {
		testutil.GitInit(t, dir)
		testutil.WriteFile(
			t,
			dir,
			"entry/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: self, namespace: apps}\nspec: {url: "+url+"}\n",
		)
		testutil.WriteFile(
			t,
			dir,
			"entry/chart.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart, namespace: apps}\nspec: {url: 'oci://fixture.invalid/chart', ref: {tag: 1.0.0}}\n",
		)
		testutil.WriteFile(
			t,
			dir,
			"entry/generator.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: generator, namespace: apps}\nspec: {interval: 1h, chartRef: {kind: OCIRepository, name: chart}}\n",
		)
		testutil.WriteFile(
			t,
			dir,
			"entry/child.yaml",
			testutil.KustomizationYAML(
				"child",
				"apps",
				"  interval: 1h\n  path: ./"+workload+"\n  sourceRef: {kind: GitRepository, name: self}\n",
			),
		)
		testutil.WriteFile(t, dir, workload+"/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			dir,
			workload+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\ndata: {value: old}\n",
		)
		if (*testCaseInput) == "owned_source" {
			testutil.WriteFile(t, dir, workload+"/kustomization.yaml", "resources: [cm.yaml, input.yaml]\n")
			testutil.WriteFile(
				t,
				dir,
				workload+"/input.yaml",
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: input, namespace: apps}\nspec: {url: oci://fixture.invalid/input, ref: {tag: old}}\n",
			)
		}
	}
	testutil.WriteFile(t, chart, "fixture/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		chart,
		"fixture/templates/application.yaml",
		"apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: applies-entry, namespace: apps}\nspec:\n  sync: {name: self, kind: GitRepository, url: "+url+", ref: main, path: ./entry}\n",
	)
	if (*testCaseInput) == "templated_group" {
		testutil.WriteFile(
			t,
			chart,
			"fixture/templates/application.yaml",
			"apiVersion: '{{ printf \"%s.%s\" \"fluxcd\" \"controlplane.io/v1\" }}'\nkind: FluxInstance\nmetadata: {name: applies-entry, namespace: apps}\nspec:\n  sync: {name: self, kind: GitRepository, url: "+url+", ref: main, path: ./entry}\n",
		)
	}
	var charts map[manifest.NamedResource]string
	if nested {
		child := t.TempDir()
		charts = map[manifest.NamedResource]string{
			{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "nested-chart"}: child,
		}
		for _, dir := range []string{root, original} {
			testutil.WriteFile(
				t,
				dir,
				"entry/nested-chart.yaml",
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: nested-chart, namespace: apps}\nspec: {url: oci://fixture.invalid/nested, ref: {tag: 1.0.0}}\n",
			)
		}
		testutil.WriteFile(t, child, "fixture/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
		testutil.WriteFile(
			t,
			child,
			"fixture/templates/application.yaml",
			"apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: '{{ .Values.applier }}', namespace: apps}\nspec:\n  sync: {name: self, kind: GitRepository, url: "+url+", ref: main, path: ./entry}\n",
		)
		testutil.WriteFile(
			t,
			chart,
			"fixture/templates/application.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: nested, namespace: apps}\nspec: {interval: 1h, chartRef: {kind: OCIRepository, name: nested-chart}, values: {applier: applies-entry}}\n",
		)
	}
	before := testDirectoryInput(t, original)
	file := workload + "/cm.yaml"
	switch {
	case declaration:
		file = "entry/child.yaml"
		testutil.WriteFile(
			t,
			root,
			file,
			testutil.KustomizationYAML(
				"child",
				"apps",
				"  interval: 1h\n  timeout: 2m\n  path: ./workload\n  sourceRef: {kind: GitRepository, name: self}\n",
			),
		)
	case (*testCaseInput) == "owned_source":
		file = workload + "/input.yaml"
		testutil.WriteFile(
			t,
			root,
			file,
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: input, namespace: apps}\nspec: {url: oci://fixture.invalid/input, ref: {tag: new}}\n",
		)
	default:
		testutil.WriteFile(
			t,
			root,
			file,
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\ndata: {value: new}\n",
		)
	}
	testRenderEntryDeclarationQualifiesOpaqueApplierCaseTail(
		t,
		&testRenderEntryDeclarationQualifiesOpaqueApplierCaseTailInputs{
			root:          &root,
			url:           url,
			before:        &before,
			file:          &file,
			testCaseInput: &testCaseInput,
			chart:         &chart,
			charts:        &charts,
			nested:        &nested,
			declaration:   &declaration,
		},
	)
}

func testRenderEntryDeclarationQualifiesOpaqueApplierCaseTail(
	t *testing.T,
	tailCaseInput *testRenderEntryDeclarationQualifiesOpaqueApplierCaseTailInputs,
) {
	t.Helper()
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	cfg := Config{CacheDir: t.TempDir(), Concurrency: 2}
	tree := Tree{
		Path:            filepath.Join((*tailCaseInput.root), "entry"),
		RepoRoot:        (*tailCaseInput.root),
		SelfURLs:        []string{tailCaseInput.url},
		BaseInput:       (*tailCaseInput.before),
		ExternalChanges: change.NewSet([]string{(*tailCaseInput.file)}),
	}
	for run := range 2 {
		applier := tailCaseInput.entryApplicationApplier(t, run)
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		t.Cleanup(func() { slog.SetDefault(previous) })
		o, err := newTestOrchestrator(t, tree, cfg)
		assert.NoError(t, err)
		fetcher := &countedChartFetcher{
			chart:      (*tailCaseInput.chart),
			charts:     (*tailCaseInput.charts),
			incomplete: (*(*tailCaseInput.testCaseInput)) == "incomplete_inputs",
		}
		o.src.Fetchers[manifest.KindOCIRepository] = fetcher
		assert.NoError(t, o.bootstrap(t.Context()))
		id := testutil.HelmReleaseID("apps", "generator")
		calls, admitted := map[manifest.NamedResource]*atomic.Int64{
			id: {},
		}, map[manifest.NamedResource]*atomic.Int64{
			id: {},
		}
		if *tailCaseInput.nested {
			nested := testutil.HelmReleaseID("apps", "nested")
			calls[nested], admitted[nested] = new(atomic.Int64), new(atomic.Int64)
		}
		o.controllers.HelmRelease = countedNodeController{o.hrc, calls, admitted}
		assert.NoError(t, o.run(t.Context()))
		entryApplicationWorkResult1 := entryApplicationWork(t, &logs)
		renders, reused := entryApplicationWorkResult1.renders, entryApplicationWorkResult1.reused
		slog.SetDefault(previous)
		if *tailCaseInput.declaration {
			tailCaseInput.assertEntryApplicationDeclaration(t, o, applier, &logs)
			if run == 0 || (*(*tailCaseInput.testCaseInput)) == "mutable_chart" ||
				(*(*tailCaseInput.testCaseInput)) == "nested_chart" ||
				(*(*tailCaseInput.testCaseInput)) ==
					"incomplete_inputs" ||
				!persistent {
				tailCaseInput.assertEntryApplicationRendered(t, fetcher, renders, run)
				assert.Equal(t, o.selection.Snapshot().Reconcile(id), true)
			} else {
				tailCaseInput.assertEntryApplicationReused(t, fetcher, renders, admitted[id])
			}
		} else {
			fetches := int64(0)
			if (*(*tailCaseInput.testCaseInput)) == "owned_source" {
				fetches = 1
			}
			assert.Equal(t, renders, 0)
			assert.Equal(t, fetcher.calls.Load(), fetches)
			assert.Equal(t, admitted[id].Load(), int64(0))
		}
		wantReuse := 0
		if run == 1 &&
			((*(*tailCaseInput.testCaseInput)) == "declaration" || (*(*tailCaseInput.testCaseInput)) == "templated_group" || (*(*tailCaseInput.testCaseInput)) == "nested_unchanged") &&
			persistent {
			wantReuse = 1
		}
		assert.Equal(t, reused, wantReuse)
		t.Logf(
			"run=%d declaration=%t renders=%d fetches=%d admissions=%d reused=%d persist=%t",
			run, (*tailCaseInput.declaration), renders,
			fetcher.calls.Load(),
			admitted[id].Load(),
			reused,
			persistent,
		)
		o.stop()
		assert.NoError(t, o.cache.Close())
	}
}

type testRenderEntryDeclarationQualifiesOpaqueApplierCaseTailInputs struct {
	root *string

	url string

	before **store.
		SourceArtifact

	file *string

	testCaseInput **string

	chart *string

	charts *map[manifest.
		NamedResource]string

	nested *bool

	declaration *bool
}

type entryWork struct {
	renders int
	reused  int
}

func entryApplicationWork(t *testing.T, logs *bytes.Buffer) entryWork {
	t.Helper()
	renders, reused := 0, 0
	for line := range bytes.Lines(logs.Bytes()) {
		var event struct {
			Message string `json:"msg"`
		}
		assert.NoError(t, json.Unmarshal(line, &event))
		if event.Message == "helm: render" {
			renders++
		}
		if event.Message == "discovery: entry application reused" {
			reused++
		}
	}
	return entryWork{renders: renders, reused: reused}
}

func (i *testRenderEntryDeclarationQualifiesOpaqueApplierCaseTailInputs) assertEntryApplicationDeclaration(
	t *testing.T, o *Orchestrator, applier string, logs *bytes.Buffer,
) {
	t.Helper()
	assert.Equal(
		t,
		o.diagnostic(o.entryBinding.Producer).Resource,
		manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      "FluxInstance",
			Namespace: "apps",
			Name:      applier,
		},
	)
	_, _, output := o.store.GetEmission(o.entryBinding.Producer)
	found := false
	for _, occurrence := range output.Manifests {
		doc := occurrence.Document
		if doc["kind"] == manifest.KindKustomization &&
			doc["metadata"].(map[string]any)["name"] == "child" {
			assert.Equal(t, doc["spec"].(map[string]any)["timeout"], any("2m"))
			found = true
		}
	}
	output.Finish()
	assert.Equal(t, found, true)
	_, ok := o.store.GetObject(o.entryBinding.Producer).(*manifest.Kustomization)
	if !ok {
		t.Fatalf("entry producer absent after qualification: %v\n%s", o.entryBinding, logs.String())
	}
	assert.Equal(t, o.entryBinding.Entry == nil, (*(*i.testCaseInput)) == "incomplete_inputs")
	if o.entryBinding.Entry == nil && (*(*i.testCaseInput)) != "incomplete_inputs" {
		t.Log(logs.String())
	}
}

func (i *testRenderEntryDeclarationQualifiesOpaqueApplierCaseTailInputs) assertEntryApplicationRendered(
	t *testing.T, fetcher *countedChartFetcher, renders, run int,
) {
	t.Helper()
	expected := 1
	if *i.nested {
		expected = 2
	}
	assert.Equal(t, fetcher.calls.Load(), int64(expected))
	if (*(*i.testCaseInput)) != "incomplete_inputs" || run == 0 {
		assert.Equal(t, renders, expected)
	}
}

func (i *testRenderEntryDeclarationQualifiesOpaqueApplierCaseTailInputs) assertEntryApplicationReused(
	t *testing.T, fetcher *countedChartFetcher, renders int, admission *atomic.Int64,
) {
	t.Helper()
	assert.Equal(t, renders, 0)
	expected := int64(1)
	if *i.nested {
		expected = 2
	}
	assert.Equal(t, fetcher.calls.Load(), expected)
	assert.Equal(t, admission.Load(), int64(0))
}

func (i *testRenderEntryDeclarationQualifiesOpaqueApplierCaseTailInputs) entryApplicationApplier(
	t *testing.T,
	run int,
) string {
	t.Helper()
	applier := "applies-entry"
	if (*(*i.testCaseInput)) == "mutable_chart" && run == 1 {
		applier = "changed-applier"
		testutil.WriteFile(
			t,
			(*i.chart),
			"fixture/templates/application.yaml",
			"apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: changed-applier, namespace: apps}\nspec:\n  sync: {name: self, kind: GitRepository, url: "+i.url+", ref: main, path: ./entry}\n",
		)
	}
	if (*(*i.testCaseInput)) == "nested_chart" && run == 1 {
		applier = "changed-applier"
		testutil.WriteFile(
			t,
			(*i.chart),
			"fixture/templates/application.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: nested, namespace: apps}\nspec: {interval: 1h, chartRef: {kind: OCIRepository, name: nested-chart}, values: {applier: changed-applier}}\n",
		)
	}
	return applier
}

func TestRenderTrees_ChangedLiteralEntryQualifiesOpaqueApplier(t *testing.T) {
	t.Parallel()
	for _, depth := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("depth-%d", depth), func(t *testing.T) {
			t.Parallel()
			trees := changedLiteralEntryTrees(t, depth)
			delta := func(runs []Rendered) []byte {
				t.Helper()
				projectedResults0, projectionErrors0 := Select(
					runs,
					nil,
					[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")},
					nil,
				)
				selected, selectedErr, err := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
				assert.NoError(t, err)
				comparison1, preparationErr1 := diff.Compare([]diff.Group{{Orig: diff.Snapshot{
					Subjects:     selected[0].Subjects,
					Manifests:    selected[0].Manifests,
					ProducerKeys: selected[0].ProducerKeys,
					Unavailable:  selected[0].Unavailable,
				}, Current: diff.Snapshot{
					Subjects:     selected[1].Subjects,
					Manifests:    selected[1].Manifests,
					ProducerKeys: selected[1].ProducerKeys,
					Unavailable:  selected[1].Unavailable,
				}, SelectedErr: selectedErr}}, diff.Options{Format: diff.FormatJSON})
				var comparisonOutput1 bytes.Buffer
				writeErr1 := comparison1.Write(&comparisonOutput1, nil)
				body, warnings, suppressed, err := comparisonOutput1.Bytes(), comparison1.Groups[0].Warnings, comparison1.Groups[0].Suppressed, errors.Join(
					preparationErr1,
					writeErr1,
				)
				assert.NoError(t, err)
				assert.Equal(t, len(warnings), 0)
				assert.Equal(t, len(suppressed), 0)
				return body
			}
			full := make([]Rendered, 2)
			for side, tree := range trees {
				runs, err := renderTreesForTest(
					t.Context(),
					t,
					[]Tree{tree},
					Config{Concurrency: 2, CacheDir: t.TempDir()},
				)
				assert.NoError(t, err)
				full[side] = runs[0]
			}
			want := delta(full)
			var expected struct {
				Changes []diff.Change `json:"changes"`
			}
			assert.NoError(t, json.Unmarshal(want, &expected))
			assert.Equal(t, slices.ContainsFunc(expected.Changes, func(c diff.Change) bool {
				return c.Kind == manifest.KindConfigMap && c.Name == "literal" && c.Namespace == "apps" &&
					c.Status == diff.StatusChanged
			}), true)
			t.Logf("independent full delta: %s", want)
			for side := range trees {
				trees[side].ExternalChanges = change.NewSet([]string{"entry/cm.yaml"})
			}
			cfg := Config{Concurrency: 2, CacheDir: t.TempDir()}
			for range 2 {
				runs, err := renderTreesForTest(t.Context(), t, trees, cfg)
				assert.NoError(t, err)
				assert.Equal(t, string(delta(runs)), string(want))
			}
		})
	}
}

func changedLiteralEntryTrees(t *testing.T, depth int) []Tree {
	t.Helper()
	roots := [2]string{t.TempDir(), t.TempDir()}
	trees := make([]Tree, 2)
	const url = "https://example.test/entry/repo"
	for side, root := range roots {
		testutil.GitInit(t, root)
		testutil.WriteFile(t, root, "entry/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: self, namespace: apps}
spec: {url: `+url+"}\n")
		testutil.WriteFile(t, root, "entry/generator.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: generator, namespace: apps}
spec:
  interval: 1h
  chart: {spec: {chart: charts/fixture-0, sourceRef: {kind: GitRepository, name: self}}}
  values: {entry: ./entry}
`)
		testutil.WriteFile(t, root, "entry/cm.yaml", fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata: {name: literal, namespace: apps}
data: {value: side-%d}
`, side))
		for level := range depth {
			chart := fmt.Sprintf("charts/fixture-%d", level)
			testutil.WriteFile(t, root, chart+"/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
			application := `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: applies-entry, namespace: apps}
spec:
  sync: {name: self, kind: GitRepository, url: ` + url + ", ref: main, path: {{ .Values.entry | quote }}}\n"
			if level+1 < depth {
				application = fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: generator-%d, namespace: apps}
spec:
  interval: 1h
  chart: {spec: {chart: charts/fixture-%d, sourceRef: {kind: GitRepository, name: self}}}
  values: {entry: {{ .Values.entry | quote }}}
`, level+1, level+1)
			}
			testutil.WriteFile(t, root, chart+"/templates/application.yaml", application)
		}
		trees[side] = Tree{
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
			Path:     filepath.Join(root, "entry"),
			RepoRoot: root,
			SelfURLs: []string{url},
		}
	}
	return trees
}
