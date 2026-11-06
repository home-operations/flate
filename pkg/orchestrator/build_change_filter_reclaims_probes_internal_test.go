package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/schedule"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestBuildChangeFilter_ReclaimsProbes(t *testing.T) {
	for _, mode := range []string{"hit", "miss", "error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			testBuildChangeFilterReclaimsProbesCase(t, &mode)
		},
		)
	}
}

type failedProbeFS struct{ fs.FS }

func (f failedProbeFS) Open(name string) (fs.File, error) {
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_ChangedPathUsesStaticReaders(t *testing.T) {
	var logs historyLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: apps}
spec: {url: https://fixture.invalid/repo}
`)
	testutil.WriteFile(t, root, "flux/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: unrelated, namespace: apps}
spec:
  chart:
    spec:
      chart: ./charts/unrelated
      sourceRef: {kind: GitRepository, name: cluster}
`)
	testutil.WriteFile(t, root, "charts/unrelated/Chart.yaml", "apiVersion: v2\nname: unrelated\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		root,
		"charts/unrelated/templates/output.yaml",
		testutil.KustomizationYAML(
			"emitted",
			"apps",
			"  path: ./apps/unrelated\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		),
	)
	testutil.WriteFile(t, root, "apps/unrelated/kustomization.yaml", "resources: [output.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/unrelated/output.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: kustomize-output, namespace: apps}\n",
	)
	testutil.WriteFile(t, root, "unclaimed/notes.txt", "changed\n")
	testutil.WriteFile(
		t,
		root,
		"unclaimed/input.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unclaimed, namespace: apps}\ndata: {value: changed}\n",
	)
	counts := func() [2]int {
		var renders [2]int
		for line := range bytes.Lines(logs.Bytes()) {
			var event struct {
				Message string `json:"msg"`
				Path    string `json:"path"`
			}
			assert.NoError(t, json.Unmarshal(line, &event))
			if event.Message == "helm: render" {
				renders[0]++
			}
			if event.Message == "kustomize: build" && strings.HasSuffix(event.Path, "apps/unrelated") {
				renders[1]++
			}
		}
		return renders
	}
	tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/repo"}}
	full, err := newTestOrchestrator(t, tree, Config{CacheDir: t.TempDir(), Concurrency: 2})
	assert.NoError(t, err)
	assert.NoError(t, full.bootstrap(t.Context()))
	assert.NoError(t, full.run(t.Context()))
	full.stop()
	assert.Equal(t, counts(), [2]int{1, 1})
	for _, file := range []string{"unclaimed/notes.txt", "unclaimed/input.yaml", "apps/unrelated/output.yaml", "charts/unrelated/templates/output.yaml"} {
		t.Run(filepath.Base(file), func(t *testing.T) {
			testRenderChangedPathUsesStaticReadersCase(
				t,
				testRenderChangedPathUsesStaticReadersCaseInputs{
					logs:   &logs,
					root:   &root,
					counts: &counts,
					tree:   &tree,
					file:   &file,
				},
			)
		},
		)
	}
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestHistory_RemovedPathSourceQualification(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	var logs historyLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	for _, source := range []string{"bootstrap", "foreign"} {
		t.Run(source, func(t *testing.T) {
			testHistoryRemovedPathSourceQualificationCase(
				t,
				testHistoryRemovedPathSourceQualificationCaseInputs{logs: &logs, source: &source},
			)
		},
		)
	}
}

func TestRender_ChangedPathThroughGeneratedSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/producer.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: source.toolkit.fluxcd.io/v1
    kind: GitRepository
    metadata: {name: generated, namespace: apps}
    spec: {url: https://fixture.invalid/repo}
    ---
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: generated, namespace: apps}
    spec:
      path: ./apps/generated
      sourceRef: {kind: GitRepository, name: generated}
`)
	testutil.WriteFile(t, root, "apps/generated/kustomization.yaml", "resources: [payload.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/generated/payload.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: changed}\n",
	)
	tree := Tree{
		ID:       manifest.TreeID{Cluster: "."},
		Path:     filepath.Join(root, "flux"),
		RepoRoot: root,
		SelfURLs: []string{"https://fixture.invalid/repo"},
	}
	var expected map[string]any
	for _, changed := range []bool{false, true} {
		if changed {
			tree.ExternalChanges = change.NewSet([]string{"apps/generated/payload.yaml"})
		}
		results, err := RenderTrees(t.Context(), []Tree{tree}, Config{CacheDir: t.TempDir(), Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(results), 1)
		assert.NoError(t, results[0].Err)
		var payload map[string]any
		for _, batch := range results[0].Result.Manifests {
			for _, doc := range batch.Documents {
				metadata := manifest.DocMetadata(doc)
				name := metadata.Name
				if manifest.DocKind(doc) == manifest.KindConfigMap && name == "payload" {
					payload = doc
				}
			}
		}
		assert.Equal(t, payload != nil, true)
		if changed {
			assert.Diff(t, payload, expected)
		} else {
			expected = payload
		}
	}
}

func TestRender_ChangedPathInputFromProjectedComponent(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: https://fixture.invalid/repo}\n",
	)
	testutil.WriteFile(t, root, "flux/producer.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: flux-system}
spec:
  resourcesTemplate: |
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: consumer, namespace: flux-system}
    spec:
      path: ./apps/workload
      targetNamespace: workload
      components: [../../components/settings]
      sourceRef: {kind: GitRepository, name: cluster}
      postBuild:
        substituteFrom: [{kind: Secret, name: settings}]
    ---
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: provider, namespace: flux-system}
    spec:
      path: ./apps/configuration
      targetNamespace: flux-system
      components: [../../components/settings]
      sourceRef: {kind: GitRepository, name: cluster}
`)
	testutil.WriteFile(t, root, "apps/workload/kustomization.yaml", "resources: [payload.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/workload/payload.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\ndata: {value: '${SETTING}'}\n",
	)
	testutil.WriteFile(t, root, "apps/configuration/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(
		t,
		root,
		"components/settings/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [settings.yaml]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"components/settings/settings.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata: {name: settings}\ndata: {SETTING: dmFsdWU=}\n",
	)
	tree := Tree{
		ID:       manifest.TreeID{Cluster: "."},
		Path:     filepath.Join(root, "flux"),
		RepoRoot: root,
		SelfURLs: []string{"https://fixture.invalid/repo"},
	}
	var expected map[string]any
	for _, changed := range []bool{false, true} {
		if changed {
			tree.ExternalChanges = change.NewSet([]string{"apps/workload/payload.yaml"})
		}
		results, err := RenderTrees(t.Context(), []Tree{tree}, Config{CacheDir: t.TempDir(), Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(results), 1)
		if results[0].Err != nil {
			t.Fatalf("changed=%t: %v", changed, results[0].Err)
		}
		var payload map[string]any
		for _, batch := range results[0].Result.Manifests {
			for _, doc := range batch.Documents {
				metadata2 := manifest.DocMetadata(doc)
				name := metadata2.Name
				if manifest.DocKind(doc) == manifest.KindConfigMap && name == "payload" {
					payload = doc
				}
			}
		}
		assert.Equal(t, payload != nil, true)
		if changed {
			assert.Diff(t, payload, expected)
		} else {
			expected = payload
		}
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_ChangedSubstitutedChildPath(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	root, cacheDir := t.TempDir(), t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux/emitter.yaml",
		testutil.KustomizationYAML(
			"emitter",
			"apps",
			"  path: ./producers\n  postBuild:\n    substitute:\n      ROOT: apps/red\n",
		),
	)
	testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [child.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"producers/child.yaml",
		testutil.KustomizationYAML("child", "apps", "  path: ./${ROOT}\n"),
	)
	testutil.WriteFile(t, root, "apps/red/kustomization.yaml", "resources: [cm.yaml]\n")
	write := func(value string) {
		testutil.WriteFile(
			t,
			root,
			"apps/red/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: "+value+"}\n",
		)
	}
	tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}
	run := func(input Tree, dir string) *Result {
		t.Helper()
		o, err := newTestOrchestrator(t, input, Config{CacheDir: dir, Concurrency: 2})
		assert.FatalIf(t, err != nil, "%v", err)
		defer o.stop()
		{
			localErr := o.bootstrap(t.Context())
			assert.FatalIf(t, localErr != nil, "%v", localErr)
		}
		result, err := o.collectResult(o.run(t.Context()))
		assert.FatalIf(t, err != nil, "%v", err)
		return result
	}
	payload := func(result *Result) map[string]any {
		for _, batch := range result.Manifests {
			for _, doc := range batch.Documents {
				if manifest.DocIdentity(
					doc,
				) == (manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "payload", Namespace: "apps"}) {
					return doc
				}
			}
		}
		return nil
	}
	write("before")
	assert.FatalIf(t, payload(run(tree, cacheDir)) == nil, "%v", "warm-up omitted the child output")
	write("after")
	oracle := payload(run(tree, t.TempDir()))
	assert.FatalIf(t, oracle == nil, "%v", "full-build oracle omitted the child output")
	tree.ExternalChanges = change.NewSet([]string{"apps/red/cm.yaml"})
	actual := payload(run(tree, cacheDir))
	assert.FatalIf(
		t,
		!manifest.ValueEqual(actual, oracle),
		"changed-only output differs from full build: got=%v want=%v",
		actual,
		oracle,
	)
}

func testBuildChangeFilterReclaimsProbesCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	if (*testCaseInput) == "hit" && sourcecache.RenderPersistenceDisabledReason() != "" {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root, cacheDir := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [emitter.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux/emitter.yaml",
		testutil.KustomizationYAML("emitter", "apps", "  path: ./apps\n"),
	)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\n",
	)
	tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}
	cfg := Config{CacheDir: cacheDir, Concurrency: 2}
	if (*testCaseInput) == "hit" {
		warm, err := newTestOrchestrator(t, tree, cfg)
		assert.NoError(t, err)
		assert.NoError(t, warm.bootstrap(t.Context()))
		assert.NoError(t, warm.run(t.Context()))
		warm.stop()
	}
	tree.ExternalChanges = change.NewSet([]string{"flux/emitter.yaml"})
	current, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { current.stop() })
	err = current.prepareBootstrap(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	source := *current.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	if (*testCaseInput) == "error" {
		source.FS = failedProbeFS{source.FS}
	}
	if (*testCaseInput) == "cancel" {
		source.FS = cancelProbeFS{FS: source.FS, cancel: cancel}
	}
	current.store.SetArtifact(manifest.BootstrapSourceID, &source)
	owner := testutil.KustomizationID("apps", "emitter")
	current.store.AddObject(
		&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace, Path: "./apps"},
	)
	released := func() <-chan struct{} {
		declaration := current.store.GetObject(owner).(*manifest.Kustomization)
		done := make(chan struct{})
		runtime.AddCleanup(declaration, func(done chan struct{}) { close(done) }, done)
		err := current.buildChangeFilter(ctx)
		if (*testCaseInput) == "cancel" && current.cache.CanPersistRenders() {
			assert.Equal(t, errors.Is(err, context.Canceled), true)
			assert.Equal(t, errors.Is(ctx.Err(), context.Canceled), true)
			assert.Equal(t, len(current.filter.KeepNames()) == 0, true)
		} else {
			assert.NoError(t, err)
			assert.NoError(t, ctx.Err())
		}
		if !current.cache.CanPersistRenders() {
			reads := &store.ReadSet{}
			reads.Init(current.store, owner, nil)
			defer reads.Retire()
			preparedHistory449 := current.controllers.PreviousRender(ctx, declaration, schedule.Result{})
			if preparedHistory449.Assessment != nil {
				defer preparedHistory449.Assessment.Close()
			}
			records, narrow := preparedHistory449.Records, preparedHistory449.Complete
			assert.Equal(t, records == nil, true)
			assert.Equal(t, narrow, false)
			assert.NoError(t, ctx.Err())
		}
		current.store.DeleteObject(owner)
		current.sweepProbes(false)
		return done
	}()
	for id := range current.store.Sweep() {
		t.Fatalf("preselection retained a live input for %s", id)
	}
	runtime.GC()
	wait, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	select {
	case <-released:
	case <-wait.Done():
		t.Fatal("preselection retained a retired probe declaration before Run")
	}
	runtime.KeepAlive(current)
}

type testRenderChangedPathUsesStaticReadersCaseInputs struct {
	logs   *historyLogBuffer
	root   *string
	counts *func() [2]int
	tree   *Tree
	file   *string
}

func testRenderChangedPathUsesStaticReadersCase(
	t *testing.T,
	testCaseInput testRenderChangedPathUsesStaticReadersCaseInputs,
) {
	t.Helper()
	testCaseInput.tree.ExternalChanges = change.NewSet([]string{(*testCaseInput.file)})
	if strings.HasSuffix((*testCaseInput.file), ".yaml") {
		assert.NoError(
			t,
			testCaseInput.tree.ExternalChanges.CollectInputs(
				t.Context(),
				os.DirFS((*testCaseInput.root)),
				(*testCaseInput.file),
				true,
			),
		)
	}
	for range 2 {
		testCaseInput.logs.Reset()
		o, err := newTestOrchestrator(t, (*testCaseInput.tree), Config{CacheDir: t.TempDir(), Concurrency: 2})
		assert.NoError(t, err)
		assert.NoError(t, o.bootstrap(t.Context()))
		result, err := o.collectResult(o.run(t.Context()))
		assert.NoError(t, err)
		o.stop()
		if strings.HasPrefix((*testCaseInput.file), "apps/") || strings.HasPrefix((*testCaseInput.file), "charts/") {
			assert.Equal(t, (*testCaseInput.counts)(), [2]int{1, 1})
			assert.Equal(t, len(result.Manifests) > 0, true)
		} else {
			assert.Equal(t, (*testCaseInput.counts)(), [2]int{})
			assert.Equal(t, len(result.Manifests), 0)
			assert.Equal(t, len(o.index.Readers), 0)
		}
	}
}

type testHistoryRemovedPathSourceQualificationCaseInputs struct {
	logs   *historyLogBuffer
	source *string
}

func testHistoryRemovedPathSourceQualificationCase(
	t *testing.T,
	testCaseInput testHistoryRemovedPathSourceQualificationCaseInputs,
) {
	t.Helper()
	roots := [2]string{t.TempDir(), t.TempDir()}
	contentURL := "https://fixture.invalid/repo"
	if (*testCaseInput.source) == "foreign" {
		foreign := t.TempDir()
		repo := testutil.GitInit(t, foreign)
		testutil.WriteFile(t, foreign, "apps/blue/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			foreign,
			"apps/blue/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: foreign}\n",
		)
		testutil.GitCommitAll(
			t,
			repo,
			"init",
			&gogit.CommitOptions{
				Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
			},
		)
		contentURL = "file://" + filepath.ToSlash(foreign)
	}
	cacheDir := t.TempDir()
	for _, root := range roots {
		testutil.WriteFile(t, root, "flux/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: emitter, namespace: apps}
spec:
  chart:
    spec:
      chart: ./charts/demo
      sourceRef: {kind: GitRepository, name: charts}
`)
		testutil.WriteFile(
			t,
			root,
			"flux/unrelated.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: unrelated, namespace: apps}\nspec: {chart: {spec: {chart: ./charts/unrelated, sourceRef: {kind: GitRepository, name: charts}}}}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"charts/unrelated/Chart.yaml",
			"apiVersion: v2\nname: unrelated\nversion: 0.1.0\n",
		)
		testutil.WriteFile(
			t,
			root,
			"charts/unrelated/templates/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated, namespace: apps}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"flux/charts.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: charts, namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"flux/content.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: content, namespace: apps}\nspec: {url: "+contentURL+"}\n",
		)
		testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
		testutil.WriteFile(
			t,
			root,
			"charts/demo/templates/child.yaml",
			testutil.KustomizationYAML(
				"child",
				"apps",
				"  path: ./apps/blue\n  sourceRef: {kind: GitRepository, name: content}\n",
			),
		)
		testutil.WriteFile(t, root, "apps/blue/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"apps/blue/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: before}\n",
		)
	}
	var trees [2]Tree
	var configs [2]Config
	for side, root := range roots {
		trees[side] = Tree{
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
			Path:     filepath.Join(root, "flux"),
			RepoRoot: root,
			SelfURLs: []string{"https://fixture.invalid/repo"},
		}
		configs[side] = Config{CacheDir: cacheDir, Concurrency: 2}
		warm, err := newTestOrchestrator(t, trees[side], configs[side])
		assert.NoError(t, err)
		_, err = warm.render(t.Context())
		assert.NoError(t, err)
	}
	assert.NoError(t, os.Remove(filepath.Join(roots[1], "apps", "blue", "cm.yaml")))
	testutil.WriteFile(t, roots[1], "apps/blue/kustomization.yaml", "resources: []\n")
	var current [2]*Orchestrator
	for side := range trees {
		trees[side].ExternalChanges = change.NewSet([]string{"apps/blue/cm.yaml"})
		var err error
		current[side], err = newTestOrchestrator(t, trees[side], configs[side])
		assert.NoError(t, err)
		assert.NoError(t, current[side].discover(t.Context()))
	}
	changes, err := change.Qualify(
		t.Context(),
		change.NewSet([]string{"apps/blue/cm.yaml"}),
		[2]*store.Store{current[0].store, current[1].store},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	changes, err = changes.WithReaders(
		t.Context(),
		[2]*manifest.DiscoveryIndex{current[0].index, current[1].index},
		[2]map[manifest.NamedResource]inputprojection.Projection{
			current[0].loader.InputContent,
			current[1].loader.InputContent,
		},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	owner := testutil.HelmReleaseID("apps", "emitter")
	child := testutil.KustomizationID("apps", "child")
	for _, o := range current {
		o.cfg.ExternalChanges = changes
		assert.NoError(t, o.bootstrap(t.Context()))
		assert.Equal(t, o.filter.ShouldReconcile(owner), (*testCaseInput.source) == "bootstrap")
		assert.Equal(t, o.filter.ShouldReconcile(child), (*testCaseInput.source) == "bootstrap")
	}
	var previousOutput [2][]byte
	for run := range 2 {
		testCaseInput.logs.Reset()
		results, err := RenderTrees(t.Context(), trees[:], Config{CacheDir: cacheDir, Concurrency: 2})
		assert.NoError(t, err)
		for side, result := range results {
			assert.NoError(t, result.Err)
			output, err := json.Marshal(result.Result.Manifests)
			assert.NoError(t, err)
			if run != 0 {
				assert.Equal(t, string(output), string(previousOutput[side]))
			}
			previousOutput[side] = output
		}
		for line := range bytes.Lines(testCaseInput.logs.Bytes()) {
			var event struct {
				Message string `json:"msg"`
				Release string `json:"release"`
			}
			assert.NoError(t, json.Unmarshal(line, &event))
			assert.FatalIf(t, event.Message == "helm: render" &&
				event.Release == testutil.HelmReleaseID("apps", "unrelated").
					String(),
				"%v", "paired removal rendered an unrelated producer")
		}
	}
}
