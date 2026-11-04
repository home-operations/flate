package orchestrator

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_EntryApplicationIntervalReceipt(t *testing.T) {
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
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: generator, namespace: apps}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/bootstrap\n      sourceRef: {kind: GitRepository, name: self, namespace: apps}\n",
		)
		testutil.WriteFile(t, dir, "charts/bootstrap/Chart.yaml", "apiVersion: v2\nname: bootstrap\nversion: 1.0.0\n")
		application := `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: apps}
spec:
  sync: {name: self, kind: GitRepository, url: ` + url + ", ref: main, path: {{ .Values.syncPath | default \"./entry\" }}}\n"
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
	}
	before := testDirectoryInput(t, original)
	before.LocalPath, before.LocalRoot, before.URL = root, root, "file://"+root
	tree := Tree{Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: []string{url}}
	cfg := Config{CacheDir: cache, Concurrency: 2}
	prime, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	primed, err := prime.render(t.Context())
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
	fixture := entryReceiptFixture{
		root:     root,
		tree:     tree,
		cfg:      cfg,
		before:   before,
		after:    testDirectoryInput(t, root),
		persists: prime.cache.CanPersistRenders(),
	}
	for _, mode := range []string{"cold", "warm"} {
		t.Run(mode, func(t *testing.T) { testEntryReceiptInterval(t, &fixture, mode) })
	}
	full, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	complete, err := full.render(t.Context())
	assert.NoError(t, err)
	assertEntryReceiptInterval(t, complete, "2h")
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

	if mode == "warm" && !fixture.persists {
		t.Skip("warm receipt witness requires a clean VCS-stamped build")
	}
	var oracle [2][]byte
	for run := range 2 {
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		var admissions [2]atomic.Int64
		id := testutil.HelmReleaseID("apps", "generator")
		selected := Config{
			CacheDir: fixture.cfg.CacheDir, Concurrency: 2, Selection: manifest.Selection{Changed: true},
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
		results, err := RenderTrees(t.Context(), []Tree{
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
		if templates != int(want) || admissions[0].Load() != want || admissions[1].Load() != want {
			t.Errorf(
				"actual templates=%d admissions=%d/%d, want %d",
				templates,
				admissions[0].Load(),
				admissions[1].Load(),
				want,
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

	if !fixture.persists {
		t.Skip("warm receipt witness requires a clean VCS-stamped build")
	}
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
			CacheDir: fixture.cfg.CacheDir, Concurrency: 2, Selection: manifest.Selection{Changed: true},
			StatusListener: func(tree manifest.TreeID, subject manifest.DiagnosticIdentity, info store.StatusInfo) {
				if subject.ID == id && info.String() == "rendering" {
					admissions[tree.Side].Add(1)
				}
			},
		}
		changes := change.NewSet([]string{"workload/cm.yaml"})
		results, err := RenderTrees(t.Context(), []Tree{
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
		literalBuilds := 0
		for line := range strings.SplitSeq(logs.String(), "\n") {
			var event struct {
				Msg  string `json:"msg"`
				Path string `json:"path"`
			}
			if json.Unmarshal([]byte(line), &event) == nil && event.Msg == "kustomize: build" &&
				manifest.NormalizeClaimBase(event.Path) == "entry" {
				literalBuilds++
			}
		}
		templates := strings.Count(logs.String(), `"msg":"helm: render"`)
		reused := strings.Count(logs.String(), `"msg":"discovery: entry application reused"`)
		t.Logf("run=%d literal root builds=%d templates=%d admissions=%d/%d receipts reused=%d",
			run, literalBuilds, templates, admissions[0].Load(), admissions[1].Load(), reused)
		assert.Equal(t, literalBuilds, 0)
		assert.Equal(t, templates, 0)
		assert.Equal(t, reused, 2)
		assert.Equal(t, len(results), 2)
		for side := range results {
			assert.Equal(t, admissions[side].Load(), int64(0))
		}
		assertEntryReceiptWorkload(t, results, &oracle, run)
	}
}

func testEntryReceiptInvalidation(t *testing.T, fixture *entryReceiptFixture) {
	t.Helper()

	for _, tc := range []struct{ name, file, old, next string }{
		{"chart", "charts/bootstrap/templates/application.yaml", "./entry", "./elsewhere"},
		{"values", "entry/generator.yaml", "  interval: 10m\n", "  interval: 10m\n  values: {syncPath: ./elsewhere}\n"},
		{"source", "entry/generator.yaml", "name: self, namespace: apps", "name: alternate, namespace: apps"},
	} {
		t.Run("invalidated/"+tc.name, func(t *testing.T) {
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
			invalidated, err := newTestOrchestrator(t, current, fixture.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := invalidated.bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			if invalidated.entryBinding.Producer != (manifest.NamedResource{}) {
				t.Fatal("changed applier input retained stale application evidence")
			}
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

func assertEntryReceiptWorkload(t *testing.T, results []Rendered, oracle *[2][]byte, run int) {
	t.Helper()

	for side, result := range results {
		assert.Equal(t, result.entryBinding.Producer == (manifest.NamedResource{}), false)
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
