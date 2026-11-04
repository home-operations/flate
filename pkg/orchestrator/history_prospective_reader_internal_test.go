package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestHistory_ProspectiveReader(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/producer.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: templates, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: child, namespace: apps}
    spec:
      interval: 1m
      path: ./apps/child
      sourceRef: {kind: GitRepository, name: content}
      postBuild:
        substituteFrom: [{kind: ConfigMap, name: settings}]
`)
	testutil.WriteFile(t, root, "apps/child/kustomization.yaml", "resources: [output.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/child/output.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: '${VALUE}'}\n",
	)
	input := func(value string) {
		t.Helper()
		testutil.WriteFile(
			t,
			root,
			"flux/settings.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: '"+value+"'}\n",
		)
	}
	run := func(t *testing.T, tree Tree, cacheDir string) *Result {
		t.Helper()
		o, err := newTestOrchestrator(t, tree, Config{CacheDir: cacheDir, Concurrency: 2})
		assert.NoError(t, err)
		defer func() {
			o.stop()
			assert.NoError(t, o.cache.Close())
		}()
		assert.NoError(t, o.bootstrap(t.Context()))
		result, err := o.collectResult(o.run(t.Context()))
		assert.NoError(t, err)
		return result
	}
	payload := func(t *testing.T, result *Result) map[string]any {
		t.Helper()
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
	testutil.WriteFile(
		t,
		root,
		"flux/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: content, namespace: apps}\nspec: {url: https://fixture.invalid/repo, interval: 1m}\n",
	)
	tree := Tree{RepoRoot: root, Path: filepath.Join(root, "flux"), SelfURLs: []string{"https://fixture.invalid/repo"}}
	input("before")
	cacheDir := t.TempDir()
	before := payload(t, run(t, tree, cacheDir))
	assert.Equal(t, before != nil, true)
	input("after")
	oracle := payload(t, run(t, tree, t.TempDir()))
	assert.Equal(t, oracle != nil, true)
	assert.Equal(t, manifest.ValueEqual(before, oracle), false)
	tree.ExternalChanges = change.NewSet([]string{"flux/settings.yaml"})
	for _, history := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_history", true: "accepted_history"}[history], func(t *testing.T) {
			testHistoryProspectiveReaderCase(
				t,
				testHistoryProspectiveReaderCaseInputs{
					root:     &root,
					run:      &run,
					payload:  &payload,
					tree:     &tree,
					cacheDir: &cacheDir,
					oracle:   &oracle,
					history:  &history,
				},
			)
		},
		)
	}
}

type historyLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *historyLogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *historyLogBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buffer.Reset()
}

func (b *historyLogBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.buffer.Bytes())
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestBootstrap_AcceptedOrphanHelmChildRoot(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	var logs historyLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	for _, mode := range []string{"inline", "values_from", "substitute_from"} {
		t.Run(mode, func(t *testing.T) {
			testBootstrapAcceptedOrphanHelmChildRootCase(
				t,
				testBootstrapAcceptedOrphanHelmChildRootCaseInputs{logs: &logs, mode: &mode},
			)
		},
		)
	}
}

type cancelProbeFS struct {
	fs.FS

	cancel context.CancelFunc
}

func (f cancelProbeFS) Open(name string) (fs.File, error) {
	f.cancel()
	return f.FS.Open(name)
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestCompleteBootstrap_CanceledLookupMiss(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/emitter.yaml", testutil.KustomizationYAML("emitter", "apps", "  path: ./apps\n"))
	o, err := newTestOrchestrator(
		t,
		Tree{
			Path:            filepath.Join(root, "flux"),
			RepoRoot:        root,
			ExternalChanges: change.NewSet([]string{"flux/emitter.yaml"}),
		},
		Config{CacheDir: t.TempDir(), Concurrency: 2},
	)
	assert.NoError(t, err)
	t.Cleanup(func() { o.stop() })
	err = o.prepareBootstrap(t.Context())
	assert.NoError(t, err)
	owner := testutil.KustomizationID("apps", "emitter")
	o.store.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace, Path: "./apps"})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := *o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	input.FS = cancelProbeFS{FS: input.FS, cancel: cancel}
	o.store.SetArtifact(manifest.BootstrapSourceID, &input)
	o.completeBootstrap()
	// Clean builds exercise lookup cancellation; dirty builds must leave the probe unopened.
	if !o.cache.CanPersistRenders() {
		o.configureControllers()
		reads := &store.ReadSet{}
		reads.Init(o.store, owner, nil)
		defer reads.Retire()
		records, narrow := o.controllers.PreviousRender(ctx, o.store.GetObject(owner), reads)
		assert.Equal(t, records == nil, true)
		assert.Equal(t, narrow, false)
		assert.NoError(t, ctx.Err())
		return
	}
	o.cfg.ExternalChanges = change.NewSet(nil)
	err = o.buildChangeFilter(ctx)
	assert.Equal(t, errors.Is(ctx.Err(), context.Canceled), true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, len(o.filter.KeepNames()), 0)
	declaration := o.store.GetObject(owner).(*manifest.Kustomization).Clone()
	declaration.Path = "./different"
	o.store.AddObject(declaration)
	for id := range o.store.Sweep() {
		t.Fatalf("canceled probe retained a live input for %s", id)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestBuildChangeFilter_PreviousRenderNarrowing(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	for _, mode := range []string{"literal", "substitute_from", "helm_values_from"} {
		t.Run(mode, func(t *testing.T) {
			testBuildChangeFilterPreviousRenderNarrowingCase(
				t,
				&mode,
			)
		},
		)
	}
}

type testHistoryProspectiveReaderCaseInputs struct {
	root *string
	run  *func(t *testing.
		T, tree Tree, cacheDir string) *Result
	payload *func(t *testing.
		T, result *Result) map[string]any
	tree     *Tree
	cacheDir *string
	oracle   *map[string]any
	history  *bool
}

func testHistoryProspectiveReaderCase(t *testing.T, testCaseInput testHistoryProspectiveReaderCaseInputs) {
	t.Helper()

	if *testCaseInput.history {
		if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
			t.Skip(reason)
		}
		reopened := cachetest.New(t, (*testCaseInput.cacheDir), sourcecache.New)
		producer := manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      manifest.KindResourceSet,
			Namespace: "apps",
			Name:      "templates",
		}
		owners, err := reopened.ProducersForPaths(
			t.Context(),
			[]manifest.SourceEvidence{
				{Source: manifest.SourceIdentity{Root: (*testCaseInput.root)}, Path: "apps/child/output.yaml"},
			},
		)
		assert.NoError(t, err)
		assert.NoError(t, reopened.Close())
		{
			assert.FatalIf(
				t,
				!slices.Contains(owners, producer),
				"reopened history owners = %v, want ResourceSet %s",
				owners,
				producer,
			)
		}
	}
	var previous map[string]any
	for range 2 {
		selectedCache := (*testCaseInput.cacheDir)
		if !(*testCaseInput.history) {
			selectedCache = t.TempDir()
		}
		result := (*testCaseInput.run)(t, (*testCaseInput.tree), selectedCache)
		got := (*testCaseInput.payload)(t, result)
		assert.Diff(t, got, (*testCaseInput.oracle))
		if previous != nil {
			assert.Diff(t, got, previous)
		}
		previous = got
	}
}

type testBootstrapAcceptedOrphanHelmChildRootCaseInputs struct {
	logs *historyLogBuffer
	mode *string
}

func testBootstrapAcceptedOrphanHelmChildRootCase(
	t *testing.T,
	testCaseInput testBootstrapAcceptedOrphanHelmChildRootCaseInputs,
) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: emitter, namespace: apps}
spec:
  interval: 10m
  values: {root: ./apps/blue, marker: keep}
  chart:
    spec:
      chart: ./charts/demo
      sourceRef: {kind: GitRepository, name: charts}
`)
	testutil.WriteFile(t, root, "flux/unrelated.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: unrelated, namespace: apps}
spec:
  chart:
    spec:
      chart: ./charts/unrelated
      sourceRef: {kind: GitRepository, name: charts}
`)
	testutil.WriteFile(
		t,
		root,
		"charts/unrelated/Chart.yaml",
		"apiVersion: v2\nname: unrelated\nversion: 0.1.0\n",
	)
	testutil.WriteFile(
		t,
		root,
		"charts/unrelated/templates/output.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-output, namespace: apps}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"flux/unrelated-kustomize.yaml",
		testutil.KustomizationYAML(
			"unrelated",
			"apps",
			"  path: ./apps/unrelated\n  sourceRef: {kind: GitRepository, name: content}\n",
		),
	)
	testutil.WriteFile(t, root, "apps/unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/unrelated/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-kustomize, namespace: apps}\n",
	)
	for _, name := range []string{"charts", "content"} {
		testutil.WriteFile(
			t,
			root,
			"flux/"+name+".yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: "+name+", namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n",
		)
	}
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		root,
		"charts/demo/templates/child.yaml",
		strings.Replace(testutil.KustomizationYAML("child", "apps", `  path: {{ .Values.root }}
  sourceRef: {kind: GitRepository, name: content}
`), "metadata: {name: child, namespace: apps}", `metadata: {name: child, namespace: apps, labels: {witness: {{ .Values.marker }}}}`, 1),
	)
	if (*testCaseInput.mode) != "inline" {
		testutil.WriteFile(
			t,
			root,
			"flux/supplied.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: supplied, namespace: apps}\ndata:\n  values.yaml: 'root: ./apps/blue'\n  ROOT: ./apps/blue\n",
		)
	}
	if (*testCaseInput.mode) == "values_from" {
		release, err := os.ReadFile(filepath.Join(root, "flux/release.yaml"))
		assert.NoError(t, err)
		testutil.WriteFile(
			t,
			root,
			"flux/release.yaml",
			strings.Replace(
				string(release),
				"  values: {root: ./apps/blue, marker: keep}",
				"  values: {marker: keep}\n  valuesFrom: [{kind: ConfigMap, name: supplied}]",
				1,
			),
		)
	}
	if (*testCaseInput.mode) == "substitute_from" {
		testutil.WriteFile(
			t,
			root,
			"flux/supplied.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: supplied, namespace: apps}\ndata: {ROOT: ./apps/blue}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"flux/release.yaml",
			testutil.KustomizationYAML(
				"emitter",
				"apps",
				"  path: ./producers\n  sourceRef: {kind: GitRepository, name: content}\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: supplied}]}\n  patches:\n  - target: {kind: Kustomization, name: child}\n    patch: '[{\"op\":\"add\",\"path\":\"/spec/interval\",\"value\":\"11m\"}]'\n",
			),
		)
		testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [child.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"producers/child.yaml",
			testutil.KustomizationYAML(
				"child",
				"apps",
				"  path: '${ROOT}'\n  sourceRef: {kind: GitRepository, name: content}\n",
			),
		)
	}
	testutil.WriteFile(t, root, "apps/blue/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/blue/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: before}\n",
	)
	tree := Tree{
		Path:     filepath.Join(root, "flux"),
		RepoRoot: root,
		SelfURLs: []string{"https://fixture.invalid/repo"},
	}
	cfg := Config{CacheDir: t.TempDir(), Concurrency: 2}
	warm, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	assert.NoError(t, warm.bootstrap(t.Context()))
	assert.NoError(t, warm.run(t.Context()))
	owner := testutil.HelmReleaseID("apps", "emitter")
	if (*testCaseInput.mode) == "substitute_from" {
		owner = testutil.KustomizationID("apps", "emitter")
	}
	child := testutil.KustomizationID("apps", "child")
	assert.Equal(t, warm.store.GetObject(child).(*manifest.Kustomization).Path, "./apps/blue")
	if (*testCaseInput.mode) == "substitute_from" {
		assert.Equal(t, warm.store.GetObject(child).(*manifest.Kustomization).Interval.Duration, 11*time.Minute)
	}
	testutil.WriteFile(
		t,
		root,
		"apps/blue/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: after}\n",
	)
	testCaseInput.logs.Reset()
	tree.ExternalChanges = change.NewSet([]string{"apps/blue/cm.yaml"})
	fresh, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { fresh.stop() })
	assert.NoError(t, fresh.discover(t.Context()))
	if (*testCaseInput.mode) != "substitute_from" {
		assert.Equal(t, fresh.loader.IndexedObject(owner, fresh.index.Records[owner]).Obj == nil, true)
	}
	assert.Equal(t, fresh.store.GetDeclaration(owner) == nil, true)
	if (*testCaseInput.mode) != "inline" {
		supplied := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "supplied"}
		assert.Equal(t, fresh.store.GetObject(supplied) == nil, true)
	}
	assert.Equal(t, fresh.store.GetObject(child) == nil, true)
	assert.NoError(t, fresh.lookupHistory(t.Context()))
	assert.Equal(t, slices.Contains(fresh.loader.Options.HistoryOwners, owner), true)
	assert.NoError(t, fresh.prepareBootstrap(t.Context()))
	{
		assert.FatalIf(
			t,
			fresh.store.GetDeclaration(owner) == nil,
			"normal admission omitted the declaration of %s",
			owner,
		)
	}
	if (*testCaseInput.mode) == "substitute_from" {
		declaration := fresh.store.GetDeclaration(owner).(*manifest.Kustomization)
		assert.Equal(t, len(declaration.Patches), 1)
		assert.Equal(t, len(declaration.PostBuild.SubstituteFrom), 1)
	} else {
		declaration := fresh.store.GetDeclaration(owner).(*manifest.HelmRelease)
		assert.Equal(t, declaration.Values["marker"], any("keep"))
		if (*testCaseInput.mode) == "values_from" {
			assert.Equal(t, len(declaration.ValuesFrom), 1)
		}
	}
	if (*testCaseInput.mode) != "inline" {
		supplied := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "supplied"}
		assert.Equal(t, fresh.store.GetObject(supplied) != nil, true)
	}
	fresh.completeBootstrap()
	assert.NoError(t, fresh.buildChangeFilter(t.Context()))
	assert.NoError(t, fresh.installAdmission(t.Context()))
	assert.Equal(t, fresh.filter.ShouldReconcile(child), true)
	assert.Equal(t, fresh.filter.ShouldReconcile(owner), true)
	assert.Equal(t, fresh.store.GetObject(child) == nil, true)
	assert.Equal(t, fresh.store.GetArtifact(owner, store.ArtifactContent) == nil, true)
	assert.Equal(t, fresh.store.GetArtifact(child, store.ArtifactContent) == nil, true)
	unrelated := testutil.HelmReleaseID("apps", "unrelated")
	assert.Equal(t, fresh.filter.ShouldReconcile(unrelated), false)
	result, err := fresh.collectResult(fresh.run(t.Context()))
	assert.NoError(t, err)
	docs := outputByParent(result.Manifests)[child]
	{
		assert.FatalIf(t, len(docs) != 1, "changed generated child output: %v", docs)
	}
	assert.Equal(t, docs[0]["data"].(map[string]any)["value"], "after")
	var renders [3]int
	testBootstrapAcceptedOrphanHelmChildRootCaseSegment(t, &testCaseInput, &unrelated, &renders)
	assert.Equal(t, renders, [3]int{0, 1, 0})
	if (*testCaseInput.mode) == "substitute_from" {
		declaration := fresh.store.GetObject(owner).(*manifest.Kustomization).Clone()
		declaration.Path = "./missing-root"
		fresh.store.AddObject(declaration)
	} else {
		declaration := fresh.store.GetObject(owner).(*manifest.HelmRelease).Clone()
		declaration.Chart.Name = "./missing-chart"
		fresh.store.AddObject(declaration)
	}
	for id := range fresh.store.Sweep() {
		t.Fatalf("Bootstrap left a live probe for %s", id)
	}
}

func testBuildChangeFilterPreviousRenderNarrowingCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [emitter.yaml, consumer.yaml]\n")
	postBuild := "  postBuild:\n    substitute: {ROOT: apps/red}\n"
	if (*testCaseInput) == "substitute_from" {
		postBuild = "  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: supplied}]\n"
	}
	testutil.WriteFile(
		t,
		root,
		"flux/emitter.yaml",
		testutil.KustomizationYAML("emitter", "apps", "  path: ./producers\n"+postBuild),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"apps",
			"  path: ./apps\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: target}]\n",
		),
	)
	testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"producers/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: target, namespace: apps}\ndata: {value: rendered}\n",
	)
	if (*testCaseInput) == "helm_values_from" {
		testutil.WriteFile(
			t,
			root,
			"flux/kustomization.yaml",
			"resources: [emitter.yaml, consumer.yaml, charts.yaml, supplied.yaml]\n",
		)
		testutil.WriteFile(t, root, "flux/emitter.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: emitter, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: ./charts/demo
      sourceRef: {kind: GitRepository, name: charts}
  valuesFrom: [{kind: ConfigMap, name: supplied}]
`)
		testutil.WriteFile(
			t,
			root,
			"flux/charts.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: charts, namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"flux/supplied.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: supplied, namespace: apps}\ndata: {values.yaml: '{}'}\n",
		)
		testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
	}

	tree := Tree{
		Path:     filepath.Join(root, "flux"),
		RepoRoot: root,
		SelfURLs: []string{"https://fixture.invalid/repo"},
	}

	cfg := Config{
		CacheDir:    t.TempDir(),
		Concurrency: 2,
		Supply: []store.SuppliedObject{
			{
				Object: &manifest.ConfigMap{
					Name:      "supplied",
					Namespace: "apps",
					Data:      map[string]any{"ROOT": "apps/red"},
				},
			},
		},
		StrictSubstitutions: true,
	}
	warm, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	assert.NoError(t, warm.bootstrap(t.Context()))
	warm.configureControllers()
	owner := testutil.KustomizationID("apps", "emitter")
	if (*testCaseInput) == "helm_values_from" {
		owner = (&manifest.HelmRelease{Name: "emitter", Namespace: "apps"}).Named()
	}
	reads := &store.ReadSet{}
	reads.Init(warm.store, owner, nil)
	declaration := warm.index.Records[owner].Declaration
	records, narrow := warm.controllers.PreviousRender(t.Context(), declaration, reads)
	assert.Equal(t, records == nil, true)
	assert.Equal(t, narrow, false)
	if (*testCaseInput) == "helm_values_from" {
		supplied := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "supplied"}
		assert.Equal(t, warm.store.GetObject(supplied) == nil, true)
		reads.Retire()
		reconcileEntryForTest(t, warm)
		assert.Equal(t, warm.store.EmissionOwner(supplied), warm.entryBinding.Producer)
		assert.Equal(t, warm.store.GetDeclaration(supplied) != nil, true)
		reads.Init(warm.store, owner, nil)
		hr := declaration.(*manifest.HelmRelease)
		prepared, omitted, localErr := helm.Prepare(
			hr,
			helm.NewSources(reads).Declaration,
			values.NewStoreProvider(reads, cfg.WipeSecrets),
			warm.hrc.Helm.ValuesCache(),
		)
		assert.NoError(t, localErr)
		assert.Equal(t, len(omitted), 0)
		_, receipt, localErr := testutil.RenderMaps(
			warm.hrc.Helm.Template(
				t.Context(),
				prepared,
				prepared.Values,
				helm.RenderOptions{
					Options:                 warm.hrc.Options,
					DependencySubstitutions: warm.dependencySubstitutions,
					Sources:                 helm.NewSources(reads),
					WipeSecrets:             cfg.WipeSecrets,
				},
			),
		)
		assert.NoError(t, localErr)
		assert.NoError(
			t,
			warm.hrc.Helm.RecordRender(
				t.Context(),
				helm.RenderAcceptance{
					Owner:   owner,
					Receipt: receipt,
					Records: func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {},
					Sources: []manifest.SourceEvidence{},
				},
			),
		)
	} else {
		reconcileEntryForTest(t, warm)
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{
				Tasks:         warm.tasks,
				Store:         warm.store,
				ID:            owner,
				ReconcileNode: warm.ksc.ReconcileNode,
			},
		)
		assert.Equal(t, info.Status, store.StatusReady)
	}
	records, narrow = warm.controllers.PreviousRender(t.Context(), declaration, reads)
	assert.Equal(t, records != nil, true)
	assert.Equal(t, narrow, true)
	if (*testCaseInput) == "helm_values_from" {
		for range records {
			t.Fatal("accepted empty history gained a child")
		}
	} else {
		record, found := maps.Collect(records)[manifest.NamedResource{
			Kind: manifest.KindConfigMap, Name: "target", Namespace: "apps",
		}]
		assert.Equal(t, found, true)
		assert.Equal(t, record.Productions[0].Owner, owner)
	}
	reads.Retire()
	for range warm.store.Sweep() {
	}
	warm.stop()
	tree.ExternalChanges = change.NewSet([]string{"flux/consumer.yaml"})
	fresh, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { fresh.stop() })
	if (*testCaseInput) == "helm_values_from" {
		assert.NoError(t, fresh.prepareBootstrap(t.Context()))
		fresh.completeBootstrap()
		supplied := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "supplied"}
		var entryReads store.ReadSet
		entryReads.Init(fresh.store, fresh.entryBinding.Producer, nil)
		_, _, entry := entryReads.GetEmission(fresh.entryBinding.Producer)
		entry.Children = []store.Child{{Obj: declaration}, {Obj: warm.store.GetObject(supplied)}}
		entryReads.Bind(&entry)
		fresh.store.RecordEmitted(fresh.entryBinding.Producer, &entry, nil)
		entry.Finish()
		entryReads.Retire()
		assert.Equal(t, fresh.store.EmissionOwner(supplied), fresh.entryBinding.Producer)
		assert.NoError(t, fresh.buildChangeFilter(t.Context()))
		assert.NoError(t, fresh.installAdmission(t.Context()))
	} else {
		assert.NoError(t, fresh.bootstrap(t.Context()))
	}
	assert.Equal(t, fresh.filter.ShouldReconcile(owner), (*testCaseInput) != "helm_values_from")
	assert.Equal(t, fresh.filter.ShouldReconcile(testutil.KustomizationID("apps", "consumer")), true)
	assert.Equal(t, fresh.store.GetArtifact(owner, store.ArtifactContent) == nil, true)
}

func testBootstrapAcceptedOrphanHelmChildRootCaseSegment(
	t *testing.T,
	testCaseInput *testBootstrapAcceptedOrphanHelmChildRootCaseInputs,
	unrelated *manifest.NamedResource,
	renders *[3]int,
) {
	t.Helper()
	for line := range bytes.Lines(testCaseInput.logs.Bytes()) {
		var event struct {
			Message string `json:"msg"`
			Release string `json:"release"`
			Path    string `json:"path"`
		}
		assert.NoError(t, json.Unmarshal(line, &event))
		if event.Message == "helm: render" && event.Release == unrelated.String() {
			renders[0]++
		}
		if event.Message == "kustomize: build" && strings.HasSuffix(event.Path, "apps/blue") {
			renders[1]++
		}
		if event.Message == "kustomize: build" && strings.HasSuffix(event.Path, "apps/unrelated") {
			renders[2]++
		}
	}
}
