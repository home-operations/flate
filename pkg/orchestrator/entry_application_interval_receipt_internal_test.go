package orchestrator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_EntryApplicationIntervalReceipt(t *testing.T) {
	t.Run("cold-self-applier-substituteFrom", testEntrySelfApplier)
	for _, kind := range []string{"fluxinstance", "kustomization"} {
		t.Run(kind, func(t *testing.T) {
			testEntryApplicationIntervalReceipt(t, kind)
		})
	}
	t.Run("cold-self-applier-substituteFrom", testColdSelfApplierSubstituteFrom)
}

func testEntryApplicationIntervalReceipt(t *testing.T, kind string) {
	t.Helper()
	root, original, cache := t.TempDir(), t.TempDir(), t.TempDir()
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
			"entry/generator.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: generator, namespace: apps}\nspec:\n  interval: 10m\n  valuesFrom: [{kind: ConfigMap, name: settings}]\n  chart:\n    spec:\n      chart: charts/bootstrap\n      sourceRef: {kind: GitRepository, name: self, namespace: apps}\n",
		)
		testutil.WriteFile(t, dir, "charts/bootstrap/Chart.yaml", "apiVersion: v2\nname: bootstrap\nversion: 1.0.0\n")
		application := `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: apps}
spec:
  sync: {name: self, kind: GitRepository, url: ` + url + ", ref: refs/heads/main, path: {{ .Values.syncPath | default \"./entry\" }}}\n"
		if kind == "kustomization" {
			application = `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: sync, namespace: apps}
spec:
  interval: 1h
  path: {{ .Values.syncPath | default "./entry" }}
  sourceRef: {kind: GitRepository, name: self, namespace: apps}
`
		}
		testutil.WriteFile(t, dir, "charts/bootstrap/templates/application.yaml", application)
		testutil.WriteFile(
			t,
			dir,
			"entry/child.yaml",
			testutil.KustomizationYAML(
				"child",
				"apps",
				"  interval: 1h\n  path: ./workload\n  sourceRef: {kind: GitRepository, name: self}\n",
			),
		)
		testutil.WriteFile(t, dir, "workload/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, dir, "workload/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\ndata: {value: before}\n")
		testutil.WriteFile(t, dir, "unapplied/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, dir, "unapplied/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unapplied}\n")
		testutil.WriteFile(t, dir, "elsewhere/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, dir, "elsewhere/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: alternate-application}\n")
	}
	before := testDirectoryInput(t, original)
	before.LocalPath, before.LocalRoot, before.URL = root, root, "file://"+root
	tree := Tree{Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: []string{url}}
	settings := &manifest.ConfigMap{
		Name:      "settings",
		Namespace: "apps",
		Data:      map[string]any{"values.yaml": "value: original\n"},
	}
	cfg := Config{
		CacheDir:    cache,
		Concurrency: 2,
		Supply: []store.Declaration{
			{Obj: settings, Doc: testutil.ObjectDocument(settings), Origin: manifest.SuppliedDeclaration},
		},
	}
	prime, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	primed, err := prime.render(t.Context(), t)
	assert.NoError(t, err)
	assertEntryReceiptInterval(t, primed, "1h")
	testutil.WriteFile(
		t,
		root,
		"entry/child.yaml",
		testutil.KustomizationYAML(
			"child",
			"apps",
			"  interval: 2h\n  path: ./workload\n  sourceRef: {kind: GitRepository, name: self}\n",
		),
	)
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	assert.Equal(t, prime.cache.CanPersistRenders(), persistent)
	fixture := entryReceiptFixture{
		root:     root,
		tree:     tree,
		cfg:      cfg,
		before:   before,
		after:    testDirectoryInput(t, root),
		persists: persistent,
	}
	for _, mode := range []string{"cold", "warm"} {
		t.Run(mode, func(t *testing.T) { testEntryReceiptInterval(t, &fixture, mode) })
	}
	full, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	complete, err := full.render(t.Context(), t)
	assert.NoError(t, err)
	assertEntryReceiptInterval(t, complete, "2h")
	evidence := full.ksc.Trees.PreviousEntry(t.Context(), kustomize.RenderRequest{
		Source: full.cfg.Input, ApplyIgnore: true, SubPath: "entry",
		Spec: manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}),
	})
	assert.Equal(t, evidence.Application != nil, fixture.persists)
	assert.Equal(t, evidence.Receipt != nil, fixture.persists)
	fixture.assertEntryReceiptSuppliedInputs(t, full.ksc.Trees, &evidence, settings)

	t.Run("warm_workload", func(t *testing.T) { testEntryReceiptWorkload(t, &fixture) })
	testEntryReceiptInvalidation(t, &fixture)
}

type entryReceiptFixture struct {
	root          string
	tree          Tree
	cfg           Config
	before, after *store.SourceArtifact
	persists      bool
}

func assertEntryReceiptInterval(t *testing.T, result *Result, want string) {
	t.Helper()
	for _, batch := range result.Manifests {
		for _, doc := range batch.Documents {
			metadata, _ := doc["metadata"].(map[string]any)
			if doc["kind"] == "Kustomization" && metadata["name"] == "child" {
				spec, _ := doc["spec"].(map[string]any)
				if spec["interval"] != want {
					t.Errorf("interval=%v, want %s", spec["interval"], want)
				}
				return
			}
		}
	}
	t.Error("root-layer Kustomization interval edit was omitted")
}

func testEntryReceiptInterval(t *testing.T, fixture *entryReceiptFixture, mode string) {
	t.Helper()

	var oracle [2][]byte
	for run := range 2 {
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		var admissions [2]atomic.Int64
		id := testutil.HelmReleaseID("apps", "generator")
		selected := Config{
			CacheDir: fixture.cfg.CacheDir, Concurrency: 2, Selection: selection.Request{Changed: true},
			Supply: fixture.cfg.Supply,
			StatusListener: func(tree manifest.TreeID, subject manifest.DiagnosticIdentity, info store.StatusInfo) {
				if subject.ID == id && info.String() == "rendering" {
					admissions[tree.Side].Add(1)
				}
			},
		}
		if mode == "cold" {
			selected.CacheDir = t.TempDir()
		}
		changes := change.NewSet([]string{"entry/child.yaml"})
		results, err := renderTreesForTest(t.Context(), t, []Tree{
			{
				ID:   manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				Path: filepath.Join(fixture.root, "entry"), RepoRoot: fixture.root, SelfURLs: fixture.tree.SelfURLs,
				Input: fixture.before, BaseInput: fixture.after, ExternalChanges: changes,
			},
			{
				ID:   manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Path: filepath.Join(fixture.root, "entry"), RepoRoot: fixture.root, SelfURLs: fixture.tree.SelfURLs,
				Input: fixture.after, BaseInput: fixture.before, ExternalChanges: changes,
			},
		}, selected)
		slog.SetDefault(previous)
		if err != nil {
			t.Fatal(err)
		}
		templates := strings.Count(logs.String(), `"msg":"helm: render"`)
		t.Logf(
			"run=%d actual applier templates=%d admissions base=%d head=%d root builds=%d",
			run,
			templates,
			admissions[0].Load(),
			admissions[1].Load(),
			strings.Count(logs.String(), `"msg":"kustomize: build"`),
		)
		want := int64(1)
		if mode == "warm" {
			want = 0
		}
		wantAdmissions := int64(1)
		if mode == "warm" && fixture.persists {
			wantAdmissions = 0
		}
		if templates != int(want) || admissions[0].Load() != wantAdmissions || admissions[1].Load() != wantAdmissions {
			t.Errorf(
				"actual templates=%d admissions=%d/%d, want templates=%d admissions=%d",
				templates,
				admissions[0].Load(),
				admissions[1].Load(),
				want, wantAdmissions,
			)
		}
		if len(results) != 2 {
			t.Fatalf("results=%d", len(results))
		}
		assertEntryReceiptIntervals(t, results, &oracle, run)
	}
}

func testEntryReceiptWorkload(t *testing.T, fixture *entryReceiptFixture) {
	t.Helper()

	original := t.TempDir()
	assert.NoError(t, os.CopyFS(original, os.DirFS(fixture.root)))
	before := testDirectoryInput(t, original)
	before.LocalPath, before.LocalRoot, before.URL = fixture.root, fixture.root, "file://"+fixture.root
	testutil.WriteFile(t, fixture.root, "workload/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\ndata: {value: after}\n")
	t.Cleanup(func() {
		testutil.WriteFile(t, fixture.root, "workload/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\ndata: {value: before}\n")
	})
	after := testDirectoryInput(t, fixture.root)
	var oracle [2][]byte
	for run := range 2 {
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		var admissions [2]atomic.Int64
		id := testutil.HelmReleaseID("apps", "generator")
		selected := Config{
			CacheDir: fixture.cfg.CacheDir, Concurrency: 2, Selection: selection.Request{Changed: true},
			Supply: fixture.cfg.Supply,
			StatusListener: func(tree manifest.TreeID, subject manifest.DiagnosticIdentity, info store.StatusInfo) {
				if subject.ID == id && info.String() == "rendering" {
					admissions[tree.Side].Add(1)
				}
			},
		}
		changes := change.NewSet([]string{"workload/cm.yaml"})
		results, err := renderTreesForTest(t.Context(), t, []Tree{
			{
				ID:   manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				Path: filepath.Join(fixture.root, "entry"), RepoRoot: fixture.root, SelfURLs: fixture.tree.SelfURLs,
				Input: before, BaseInput: after, ExternalChanges: changes,
			},
			{
				ID:   manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Path: filepath.Join(fixture.root, "entry"), RepoRoot: fixture.root, SelfURLs: fixture.tree.SelfURLs,
				Input: after, BaseInput: before, ExternalChanges: changes,
			},
		}, selected)
		slog.SetDefault(previous)
		assert.NoError(t, err)
		literalBuilds, siblingBuilds := 0, 0
		for line := range strings.SplitSeq(logs.String(), "\n") {
			var event struct {
				Msg  string `json:"msg"`
				Path string `json:"path"`
			}
			if json.Unmarshal([]byte(line), &event) == nil && event.Msg == "kustomize: build" {
				switch manifest.NormalizeClaimBase(event.Path) {
				case "entry":
					literalBuilds++
				case "unapplied":
					siblingBuilds++
				}
			}
		}
		templates := strings.Count(logs.String(), `"msg":"helm: render"`)
		reused := strings.Count(logs.String(), `"msg":"discovery: entry application reused"`)
		t.Logf("run=%d literal root builds=%d templates=%d admissions=%d/%d receipts reused=%d",
			run, literalBuilds, templates, admissions[0].Load(), admissions[1].Load(), reused)
		assert.Equal(t, literalBuilds, 0)
		assert.Equal(t, siblingBuilds, 0)
		assert.Equal(t, templates, 0)
		wantReused := 0
		if fixture.persists {
			wantReused = 2
		}
		assert.Equal(t, reused, wantReused)
		assert.Equal(t, len(results), 2)
		for side := range results {
			assert.Equal(t, admissions[side].Load(), int64(0))
		}
		fixture.assertEntryReceiptWorkload(t, results, &oracle, run)
	}
}

type entryInvalidationCase struct{ name, file, old, next string }

func testEntryReceiptInvalidation(t *testing.T, fixture *entryReceiptFixture) {
	t.Helper()

	for _, tc := range []entryInvalidationCase{
		{"chart", "charts/bootstrap/templates/application.yaml", "./entry", "./elsewhere"},
		{"values", "entry/generator.yaml", "  interval: 10m\n", "  interval: 10m\n  values: {syncPath: ./elsewhere}\n"},
		{"source", "entry/generator.yaml", "name: self, namespace: apps", "name: alternate, namespace: apps"},
	} {
		t.Run("invalidated/"+tc.name, func(t *testing.T) {
			testEntryReceiptInvalidationCase(t, fixture, tc)
		})
	}
}

func assertEntryReceiptIntervals(t *testing.T, results []Rendered, oracle *[2][]byte, run int) {
	t.Helper()

	for side, result := range results {
		assertEntryReceiptInterval(t, result.Result, []string{"1h", "2h"}[side])
		output, err := json.Marshal(result.Result.Manifests)
		if err != nil {
			t.Fatal(err)
		}
		if run == 0 {
			oracle[side] = output
		} else if !bytes.Equal(output, oracle[side]) {
			t.Errorf("side %d changed output between runs", side)
		}
	}
}

func (f *entryReceiptFixture) assertEntryReceiptWorkload(t *testing.T, results []Rendered, oracle *[2][]byte, run int) {
	t.Helper()

	for side, result := range results {
		assert.Equal(t, result.entryBinding.Producer == (manifest.NamedResource{}), !f.persists)
		assert.Diff(t, configMapNames(result.Result), map[string]bool{"selected": true})
		for _, batch := range result.Result.Manifests {
			for _, doc := range batch.Documents {
				if doc["kind"] == "ConfigMap" {
					assert.Diff(t, doc["data"], any(map[string]any{"value": []string{"before", "after"}[side]}))
				}
			}
		}
		output, err := json.Marshal(result.Result.Manifests)
		assert.NoError(t, err)
		if run == 0 {
			oracle[side] = output
		} else if !bytes.Equal(output, oracle[side]) {
			t.Errorf("side %d changed output between runs", side)
		}
	}
}

func (f *entryReceiptFixture) assertEntryReceiptSuppliedInputs(
	t *testing.T,
	trees *kustomize.TreeCache,
	evidence *kustomize.EntryEvidence,
	settings *manifest.ConfigMap,
) {
	t.Helper()
	for _, scenario := range []string{"unchanged", "changed", "removed", "repository precedence"} {
		t.Run(scenario, func(t *testing.T) {
			current := store.New()
			if scenario != "removed" {
				data := *settings
				if scenario != "unchanged" {
					data.Data = map[string]any{"values.yaml": "value: changed\n"}
				}
				current.AddDeclaration(store.Declaration{Obj: &data, Origin: manifest.SuppliedDeclaration})
			}
			if scenario == "repository precedence" {
				current.AddObject(settings, testutil.ObjectDocument(settings))
			}
			want := f.persists && (scenario == "unchanged" || scenario == "repository precedence")
			if got := trees.EntryValid(t.Context(), *evidence, current); got != want {
				t.Fatalf("discovered application receipt valid = %t, want %t", got, want)
			}
		})
	}
}

func entryReceiptTemplateCount(logs string, generator manifest.NamedResource) int {
	templates := 0
	for line := range strings.SplitSeq(logs, "\n") {
		var event struct {
			Msg     string `json:"msg"`
			Release string `json:"release"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.Msg == "helm: render" &&
			event.Release == generator.String() {
			templates++
		}
	}
	return templates
}

func assertEntryReceiptInvalidatedRender(
	t *testing.T,
	invalidated *Orchestrator,
	logs *bytes.Buffer,
	admissions *atomic.Int64,
) {
	t.Helper()
	assert.NoError(t, invalidated.run(t.Context()))
	assert.Equal(t, entryReceiptTemplateCount(logs.String(), testutil.HelmReleaseID("apps", "generator")), 1)
	assert.Equal(t, admissions.Load(), int64(1))
}

func testEntryReceiptInvalidationCase(t *testing.T, fixture *entryReceiptFixture, tc entryInvalidationCase) {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(fixture.root, tc.file))
	if err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, fixture.root, tc.file, strings.ReplaceAll(string(content), tc.old, tc.next))
	t.Cleanup(func() { testutil.WriteFile(t, fixture.root, tc.file, string(content)) })
	if tc.name == "source" {
		testutil.WriteFile(t, fixture.root, "entry/alternate.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: alternate, namespace: apps}
spec: {url: `+fixture.tree.SelfURLs[0]+"}\n")
	}
	current := fixture.tree
	current.BaseInput, current.ExternalChanges = fixture.before, change.NewSet(
		[]string{"entry/child.yaml", tc.file},
	)
	cfg := fixture.cfg
	var admissions atomic.Int64
	generator := testutil.HelmReleaseID("apps", "generator")
	cfg.StatusListener = func(_ manifest.TreeID, subject manifest.DiagnosticIdentity, info store.StatusInfo) {
		if subject.ID == generator && info.String() == "rendering" {
			admissions.Add(1)
		}
	}
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	invalidated, err := newTestOrchestrator(t, current, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := invalidated.bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	if invalidated.entryBinding.Producer != (manifest.NamedResource{}) {
		t.Fatal("changed applier input retained stale application evidence")
	}
	if tc.name == "chart" {
		assertEntryReceiptInvalidatedRender(t, invalidated, &logs, &admissions)
	}
}

func testColdSelfApplierSubstituteFrom(t *testing.T) {
	t.Helper()
	owners := []manifest.NamedResource{
		testutil.HelmReleaseID("apps", "typed"),
		testutil.KustomizationID("apps", "volsync-a"),
		testutil.KustomizationID("apps", "volsync-b"),
	}
	var full [2]Rendered
	var measured [2][2]Rendered
	var finalErrors [2][2]error
	for side := range full {
		reference := selfApplierTrees(t)[side]
		reference.BaseInput, reference.ExternalChanges = nil, nil
		runs, err := renderTreesForTest(
			t.Context(),
			t,
			[]Tree{reference},
			Config{CacheDir: t.TempDir(), Concurrency: 2},
		)
		assert.NoError(t, err)
		assert.Equal(t, len(runs), 1)
		full[side] = runs[0]
	}
	for repeat := range measured {
		for side := range measured[repeat] {
			tree := selfApplierTrees(t)[side]
			runs, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{
				CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true,
				Selection: selection.Request{Changed: true},
			})
			assert.Equal(t, len(runs), 1)
			measured[repeat][side], finalErrors[repeat][side] = runs[0], err
		}
	}
	t.Run("output", func(t *testing.T) {
		for repeat := range measured {
			for side, run := range measured[repeat] {
				t.Run(fmt.Sprintf("run-%d/%s", repeat, []string{"base", "head"}[run.ID.Side]), func(t *testing.T) {
					assert.Diff(t, visibleWitnessOutput(run.Result), visibleWitnessOutput(full[side].Result))
					assertSelfApplierOutput(t, run, owners, []string{"before", "after"}[side])
				})
			}
		}
	})
	t.Run("final-failures", func(t *testing.T) {
		for repeat := range measured {
			for side, run := range measured[repeat] {
				t.Run(fmt.Sprintf("run-%d/%s", repeat, []string{"base", "head"}[run.ID.Side]), func(t *testing.T) {
					assert.NoError(t, finalErrors[repeat][side])
					assert.NoError(t, run.Err)
					assert.NoError(t, full[side].Err)
					assert.Diff(t, run.Result.Failed, full[side].Result.Failed)
					assert.Equal(t, len(run.Result.Failed), 0)
				})
			}
		}
	})
	t.Run("determinism", func(t *testing.T) {
		for side := range full {
			t.Run([]string{"base", "head"}[side], func(t *testing.T) {
				before, err := json.Marshal(measured[0][side].Result.Manifests)
				assert.NoError(t, err)
				after, err := json.Marshal(measured[1][side].Result.Manifests)
				assert.NoError(t, err)
				assert.Equal(t, string(after), string(before))
				assert.Diff(t, measured[1][side].Result.Failed, measured[0][side].Result.Failed)
			})
		}
	})
	for repeat := range measured {
		for _, run := range measured[repeat] {
			t.Run(fmt.Sprintf("run-%d/%s", repeat, []string{"base", "head"}[run.ID.Side]), func(t *testing.T) {
				assertSelfApplierWork(t, run, owners)
			})
		}
	}
}

func selfApplierTrees(t *testing.T) [2]Tree {
	t.Helper()
	const url = "https://example.test/self-applier/repo"
	files := map[string]string{
		"entry/kustomization.yaml": "resources: [application.yaml, source.yaml, vars.yaml, typed.yaml, volsync-a.yaml, volsync-b.yaml]\n",
		"entry/source.yaml": "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n" +
			"metadata: {name: self, namespace: apps}\nspec:\n  url: " + url + "\n  ignore: |\n    /*\n    !/entry\n    !/charts\n    !/shared\n",
		"entry/application.yaml": `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: sync, namespace: apps}
spec:
  interval: 1h
  path: ./entry
  sourceRef: {kind: GitRepository, name: self}
  decryption: {provider: sops}
  postBuild:
    substitute: {COUNT: "7"}
    substituteFrom: [{kind: Secret, name: vars, optional: false}]
  patches:
    - target: {kind: HelmRelease, name: typed}
      patch: |-
        - op: replace
          path: /spec/values/marker
          value: accepted
`,
		"entry/vars.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: vars, namespace: apps}\n" +
			"stringData: {TOKEN: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n",
		"entry/typed.yaml": `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: typed, namespace: apps}
spec:
  interval: 1h
  chart:
    spec:
      chart: charts/typed
      sourceRef: {kind: GitRepository, name: self}
  values:
    count: ${COUNT}
    marker: raw
`,
		"charts/typed/Chart.yaml":         "apiVersion: v2\nname: typed\nversion: 1.0.0\n",
		"charts/typed/values.schema.json": `{"type":"object","required":["count"],"properties":{"count":{"type":"integer"}}}`,
		"charts/typed/templates/cm.yaml": `apiVersion: v1
kind: ConfigMap
metadata: {name: typed-output, namespace: apps}
data:
  count: {{ .Values.count | quote }}
  marker: {{ .Values.marker | quote }}
  revision: before
`,
		"shared/kustomization.yaml": "resources: [cm.yaml]\n",
		"shared/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '${WORKLOAD_NAME}', namespace: apps}\n" +
			"data: {workload: '${WORKLOAD_NAME}', revision: before}\n",
	}
	for _, name := range []string{"a", "b"} {
		files["entry/volsync-"+name+".yaml"] = testutil.KustomizationYAML("volsync-"+name, "apps",
			"  interval: 1h\n  path: ./shared\n  sourceRef: {kind: GitRepository, name: self}\n"+
				"  postBuild:\n    substitute: {WORKLOAD_NAME: volsync-"+name+"}\n")
	}
	roots := [2]string{t.TempDir(), t.TempDir()}
	for side, root := range roots {
		for _, name := range slices.Sorted(maps.Keys(files)) {
			body := files[name]
			if side == 1 && (name == "charts/typed/templates/cm.yaml" || name == "shared/cm.yaml") {
				body = strings.ReplaceAll(body, "revision: before", "revision: after")
			}
			if side == 1 && name == "entry/volsync-a.yaml" {
				body = strings.ReplaceAll(body, "interval: 1h", "interval: 2h")
			}
			testutil.WriteFile(t, root, name, body)
		}
	}
	inputs := [2]*store.SourceArtifact{testDirectoryInput(t, roots[0]), testDirectoryInput(t, roots[1])}
	inputs[0].LocalPath, inputs[0].LocalRoot, inputs[0].URL = roots[1], roots[1], "file://"+roots[1]
	var trees [2]Tree
	for side := range trees {
		trees[side] = Tree{
			ID: manifest.TreeID{
				Cluster: ".",
				Side:    []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead}[side],
			},
			Path:      filepath.Join(roots[1], "entry"),
			RepoRoot:  roots[1],
			SelfURLs:  []string{url},
			Input:     inputs[side],
			BaseInput: inputs[1-side],
			ExternalChanges: change.NewSet(
				[]string{"charts/typed/templates/cm.yaml", "shared/cm.yaml", "entry/volsync-a.yaml"},
			),
		}
	}
	return trees
}

func assertSelfApplierOutput(t *testing.T, run Rendered, owners []manifest.NamedResource, revision string) {
	t.Helper()
	output := visibleWitnessOutput(run.Result)
	assert.Equal(t, len(output), len(owners)+1)
	for index, owner := range owners {
		docs := output[owner]
		assert.Equal(t, len(docs), 1)
		if len(docs) != 1 {
			continue
		}
		metadata, _ := docs[0]["metadata"].(map[string]any)
		name := "typed-output"
		data := map[string]any{"count": "7", "marker": "accepted", "revision": revision}
		if index != 0 {
			name = owner.Name
			data = map[string]any{"workload": name, "revision": revision}
		}
		assert.Equal(t, metadata["name"], any(name))
		assert.Equal(t, metadata["namespace"], any("apps"))
		assert.Diff(t, docs[0]["data"], data)
	}
	entry := output[testutil.KustomizationID("apps", "sync")]
	assert.Equal(t, len(entry), 6)
	for _, doc := range entry {
		metadata, _ := doc["metadata"].(map[string]any)
		if doc["kind"] != "HelmRelease" || metadata["name"] != "typed" {
			continue
		}
		spec, _ := doc["spec"].(map[string]any)
		values, err := json.Marshal(spec["values"])
		assert.NoError(t, err)
		assert.Equal(t, string(values), `{"count":7,"marker":"accepted"}`)
	}
}

func assertSelfApplierWork(t *testing.T, run Rendered, owners []manifest.NamedResource) {
	t.Helper()
	engines := make(map[manifest.NamedResource]manifest.WorkSummary)
	t.Logf("side=%d binding=%+v kustomize=%+v helm=%+v", run.ID.Side, run.entryBinding,
		workCounts(run.Result.Work, manifest.NamedResource{}, manifest.WorkKustomize),
		workCounts(run.Result.Work, manifest.NamedResource{}, manifest.WorkHelm))
	for index := range run.Result.Work {
		record := &run.Result.Work[index]
		assert.Equal(t, record.Key.Tree, run.ID)
		assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
		if record.Key.Phase == manifest.WorkHelm || record.Key.Phase == manifest.WorkKustomize {
			assert.Equal(t, record.Key.Input != "", true)
			engines[record.Key.Owner] = append(engines[record.Key.Owner], *record)
			t.Logf("engine: %+v", record)
		}
	}
	t.Run("provisional-work", func(t *testing.T) {
		for _, owner := range owners {
			for index := range engines[owner] {
				record := &engines[owner][index]
				assert.Equal(t, record.Counts.Failures, uint64(0))
				assert.Equal(t, record.Counts.Attempts, uint64(1))
			}
			assert.Equal(t, len(engines[owner]), 1)
		}
	})
	t.Run("entry-application", func(t *testing.T) {
		assert.Equal(t, workCounts(run.Result.Work, manifest.NamedResource{}, manifest.WorkKustomize),
			manifest.WorkCounts{Attempts: 3, Completed: 3})
		assert.Equal(t, workCounts(run.Result.Work, manifest.NamedResource{}, manifest.WorkHelm),
			manifest.WorkCounts{Attempts: 1, Completed: 1})
		applier := testutil.KustomizationID("apps", "sync")
		synthetic := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", applier).Named()
		assert.Equal(t, run.entryBinding.Producer, applier)
		assert.Equal(t, len(engines[synthetic]), 0)
		assert.Equal(t, len(engines[applier]), 1)
		for index := range engines[applier] {
			record := &engines[applier][index]
			assert.Equal(t, record.Counts, manifest.WorkCounts{Attempts: 1, Completed: 1})
		}
		assert.Equal(
			t,
			workCounts(run.Result.Work, applier, manifest.WorkAdmission),
			manifest.WorkCounts{Attempts: 1, Completed: 1},
		)
	})
	t.Run("final-child-identities", func(t *testing.T) {
		assertSelfApplierChildWork(t, run, owners, engines)
	})
}

func assertSelfApplierChildWork(
	t *testing.T,
	run Rendered,
	owners []manifest.NamedResource,
	engines map[manifest.NamedResource]manifest.WorkSummary,
) {
	t.Helper()
	var rawInputs [2]string
	for index, owner := range owners {
		phase := manifest.WorkKustomize
		source := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindGitRepository,
			Namespace: "apps",
			Name:      "self",
		}
		if index == 0 {
			phase = manifest.WorkHelm
			source.Kind, source.Name = manifest.KindHelmChart, "apps-typed"
		}
		assert.Equal(t, len(engines[owner]), 1)
		for index := range engines[owner] {
			record := &engines[owner][index]
			assert.Equal(t, record.Key.Source, source)
			assert.Equal(t, record.Key.Phase, phase)
			assert.Equal(t, record.Counts, manifest.WorkCounts{Attempts: 1, Completed: 1})
		}
		if index == 0 {
			continue
		}
		if len(engines[owner]) == 1 {
			rawInputs[index-1] = engines[owner][0].Key.Input
		}
		assert.Equal(
			t,
			workCounts(run.Result.Work, owner, manifest.WorkSubstitution),
			manifest.WorkCounts{Attempts: 1, Completed: 1},
		)
	}
	assert.Equal(t, rawInputs[0] != rawInputs[1], true)
}
