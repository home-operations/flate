package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_NetworkRunIsolation(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			testRenderTreesNetworkRunIsolationCase(t, &workers)
		},
		)
	}
	t.Run(
		"remote-generators",
		func(t *testing.T) { ; testRenderTreesNetworkRunIsolationRemoteGenerators(t) },
	)
}

func TestRenderTrees_ErrorProjectionRetainsCleanup(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeSubstitutionCluster(t, root, "values", "value")
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, healthy.yaml, values.yaml]\n")
	testutil.WriteFile(t, root, "healthy.yaml", substitutionKS("healthy", "healthy", ""))
	testutil.WriteFile(t, root, "healthy/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "healthy/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: partial}\n")
	testutil.WriteFile(
		t,
		root,
		"values.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: apps}\ndata: {VALUE: stable}\n",
	)
	assert.NoError(t, os.Remove(filepath.Join(root, "app", "cm.yaml")))
	runs, err := renderTreesForTest(t.Context(), t,
		[]Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: root, RepoRoot: root}},
		Config{Concurrency: 2},
	)

	assert.FatalIf(
		t,
		err == nil || len(runs) != 1 || runs[0].Orchestrator == nil || len(runs[0].Result.Manifests) == 0,
		"partial output lost: runs=%v error=%v",
		runs,
		err,
	)

	cleanupErr := errors.New("backing close failed")
	joined := errors.Join(err, cleanupErr)
	projectedResults0, projectionErrors0 := Select(
		runs,
		joined,
		[]manifest.NamedResource{testutil.KustomizationID("", "healthy")},
		nil,
	)
	_, selected, remainder := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
	for _, projected := range []error{selected, remainder} {
		assert.FatalIf(
			t,
			!errors.Is(projected, cleanupErr) || strings.Count(projected.Error(), cleanupErr.Error()) != 1,
			"cleanup error must survive once: %v",
			projected,
		)

		{
			_, ok := errors.AsType[*selection.FailuresError](projected)
			assert.FatalIf(t, ok, "unrelated resource failure retained: %v", projected)
		}
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_RunJoinsRemoteWorkers(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, backing := range []string{"memory", "disk"} {
			t.Run(fmt.Sprintf("%d/%s", workers, backing), func(t *testing.T) {
				testOrchestratorRunJoinsRemoteWorkersCase(
					t,
					testOrchestratorRunJoinsRemoteWorkersCaseInputs{workers: &workers, backing: &backing},
				)
			},
			)
		}
	}
}

func TestRSRawSink_DeterministicCollisionWinner(t *testing.T) {
	t.Parallel()

	parent := testutil.KustomizationID("flux-system", "apps")
	ownerA := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      manifest.KindResourceSet,
		Namespace: "flux-system",
		Name:      "rs-a",
	}
	ownerB := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      manifest.KindResourceSet,
		Namespace: "flux-system",
		Name:      "rs-b",
	}
	mkDoc := func(owner string) map[string]any {
		return map[string]any{
			"apiVersion": "example.com/v1",
			"kind":       "Widget",
			"metadata":   map[string]any{"name": "shared", "namespace": "default"},
			"spec":       map[string]any{"owner": owner},
		}
	}
	docA, docB := mkDoc("a-loses"), mkDoc("b-wins")
	other := mkDoc("other-group")
	other["apiVersion"] = "other.example/v1"
	version := mkDoc("version-loses")
	version["apiVersion"] = "example.com/v2"

	omitted := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "foo"},
		"data":       map[string]any{"value": "a-omitted"},
	}
	explicit := manifest.DeepCopyMap(omitted)
	explicit["metadata"].(map[string]any)["namespace"] = "flux-system"
	explicit["data"].(map[string]any)["value"] = "a-explicit"
	winningCM := manifest.DeepCopyMap(explicit)
	winningCM["data"].(map[string]any)["value"] = "b-wins"
	namespaceA := map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata":   map[string]any{"name": "shared"},
		"spec":       map[string]any{"owner": "a-loses"},
	}
	namespaceB := manifest.DeepCopyMap(namespaceA)
	namespaceB["spec"].(map[string]any)["owner"] = "b-wins"
	incomplete := []map[string]any{
		{"apiVersion": "example.com/v1", "kind": "Widget"},
		{"apiVersion": "example.com/v1", "metadata": map[string]any{"name": "kindless"}},
		{"kind": "Widget", "metadata": map[string]any{"name": "versionless"}},
	}
	malformed := map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "invalid", "namespace": "flux-system"},
		"spec":       map[string]any{"path": []any{"invalid"}},
	}

	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			t.Parallel()
			testRSRawSinkDeterministicCollisionWinnerCase(
				t,
				&testRSRawSinkDeterministicCollisionWinnerCaseInputs{
					parent:     &parent,
					ownerA:     &ownerA,
					ownerB:     &ownerB,
					docA:       &docA,
					docB:       &docB,
					other:      &other,
					version:    &version,
					omitted:    &omitted,
					explicit:   &explicit,
					winningCM:  &winningCM,
					namespaceA: &namespaceA,
					namespaceB: &namespaceB,
					incomplete: &incomplete,
					malformed:  &malformed,
					reverse:    &reverse,
				},
			)
		},
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_SourceValuesReadersRenderEmitter(t *testing.T) {
	for _, form := range []string{"inline", "chartRef"} {
		t.Run(form, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [ks.yaml]\n")
			testutil.WriteFile(t, root, "flux/ks.yaml", testutil.KustomizationYAML("demo", "flux-system",
				"  path: ./apps/demo\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
			resources := "resources: [hr.yaml, sibling.yaml]\n"
			spec := `  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
      valuesFiles: [./config/prod.yaml]
`
			if form == "chartRef" {
				resources = "resources: [hr.yaml, sibling.yaml, chart.yaml]\n"
				spec = "  chartRef: {kind: HelmChart, name: chart, namespace: flux-system}\n"
				testutil.WriteFile(t, root, "apps/demo/chart.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmChart
metadata: {name: chart, namespace: flux-system}
spec:
  chart: charts/demo
  sourceRef: {kind: GitRepository, name: flux-system}
  valuesFiles: [./config/prod.yaml]
`)
			}
			testutil.WriteFile(t, root, "apps/demo/kustomization.yaml", resources)
			testutil.WriteFile(
				t,
				root,
				"apps/demo/hr.yaml",
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec:\n  interval: 10m\n"+spec,
			)
			testutil.WriteFile(t, root, "apps/demo/sibling.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: sibling, namespace: apps}
spec:
  chartRef: {kind: OCIRepository, name: unavailable}
`)
			testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
			testutil.WriteFile(t, root, "charts/demo/values.yaml", "greeting: default\n")
			testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: rendered, namespace: apps}
data: {greeting: {{ .Values.greeting | quote }}}
`)
			testutil.WriteFile(t, root, "config/prod.yaml", "greeting: production\n")
			o, err := newTestOrchestrator(t, Tree{
				Path: filepath.Join(root, "flux"), RepoRoot: root,
				ExternalChanges: change.NewSet([]string{"config/prod.yaml"}),
			}, Config{CacheDir: t.TempDir(), Concurrency: 2},
			)

			assert.FatalIf(t, err != nil, "%v", err)

			t.Cleanup(func() { o.stop() })
			assert.NoError(t, o.bootstrap(t.Context()))
			release := testutil.HelmReleaseID("apps", "demo")
			owner := testutil.KustomizationID("flux-system", "demo")
			assert.Equal(t, o.store.GetObject(release) == nil, true)
			assert.Equal(t, o.selection.Snapshot().Reconcile(owner), true)
			result, err := o.collectResult(o.run(t.Context()))
			t.Cleanup(result.Close)

			assert.FatalIf(t, err != nil, "%v", err)

			assert.Equal(t, len(result.Failed), 0)
			assert.Equal(t, len(result.Blocked), 0)
			assert.Equal(t, o.store.GetArtifact(owner, store.ArtifactContent) != nil, true)
			rendered, ok := o.store.Get[*manifest.ConfigMap](
				manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "rendered"},
			)

			assert.FatalIf(t, !ok, "%v", "selected release did not render")

			assert.Equal(t, rendered.Data["greeting"], "production")
			assert.Equal(t, o.selection.Snapshot().Reconcile(testutil.HelmReleaseID("apps", "sibling")), false)
			assert.Equal(
				t,
				o.store.GetArtifact(testutil.HelmReleaseID("apps", "sibling"), store.ArtifactContent) == nil,
				true,
			)
		})
	}
}

func testRenderTreesNetworkRunIsolationCase(t *testing.T, testCaseInput *int,
) {
	t.Helper()

	c := cachetest.New(t, "", sourcecache.New)
	root := t.TempDir()
	testutil.WriteFile(t, root, "cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\n")
	tree := Tree{RepoRoot: root}
	cfg := treeConfig{Cache: c, Concurrency: (*testCaseInput), Tree: Tree{}}
	renderedTrees4, err := renderTreesForTest(t.Context(), t,
		[]Tree{
			{
				RepoRoot:        tree.RepoRoot,
				Path:            tree.Path,
				SelfURLs:        tree.SelfURLs,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				ExternalChanges: cfg.ExternalChanges,
				Input:           cfg.BaseInput,
				BaseInput:       cfg.Input,
			},
			{
				RepoRoot:        tree.RepoRoot,
				Path:            tree.Path,
				SelfURLs:        tree.SelfURLs,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Input:           cfg.Input,
				BaseInput:       cfg.BaseInput,
				ExternalChanges: cfg.ExternalChanges,
			},
		},
		cfg.Config,
	)
	baseline, head := renderedTrees4[0], renderedTrees4[1]

	assert.FatalIf(t, err != nil, "%v", err)

	network := baseline.cfg.Network

	assert.FatalIf(
		t,
		network == nil || head.cfg.Network != network,
		"%v",
		"diff sides did not share one Network",
	)

	var hits atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, _ *http.Request) { hits.Add(1); _, _ = io.WriteString(w, "new run") },
		),
	)
	t.Cleanup(srv.Close)
	var failing atomic.Bool
	failing.Store(true)
	testutil.StubDNS(t, func(context.Context, string) error {
		if failing.Load() {
			return os.ErrDeadlineExceeded
		}
		return nil
	})
	target := strings.Replace(srv.URL, "127.0.0.1", "example.com", 1)
	tr, err := network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
	assert.NoError(t, err)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, target, http.NoBody)
	_, err = (&http.Client{Transport: tr}).Do(req)
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)

	assert.FatalIf(t, !ok || !terminal.Attempted, "seed: %v", err)

	cause := terminal.Err
	failing.Store(false)
	for i, side := range []Rendered{baseline, head} {
		{
			_, localErr := side.inputs.Acquire(
				t.Context(),
				&source.HTTPRequest{URL: target, Retry: sourcecache.RetryConfig{}, HeldSlot: nil},
			)
			assert.FatalIf(t, !errors.Is(
				localErr,
				fs.ErrClosed,
			), "returned side %d is not terminal: %v", i, localErr)
		}

		trees := source.NewHTTPInputs(c, side.cfg.Network, task.NewBounded(2))
		t.Cleanup(trees.Close)
		_, innerLocalErr := trees.Acquire(
			t.Context(),
			&source.HTTPRequest{
				URL:      fmt.Sprintf("%s/%d", target, i),
				Retry:    sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour},
				HeldSlot: nil,
			},
		)
		terminal, ok := errors.AsType[*sourcecache.TerminalError](innerLocalErr)

		assert.FatalIf(
			t,
			!ok || terminal.Attempted || !errors.Is(innerLocalErr, cause),
			"side %d: %v",
			i,
			innerLocalErr,
		)
	}
	if hits.Load() != 0 {
		t.Fatalf("suppressed requests=%d", hits.Load())
	}
	renderedTrees5, err := renderTreesForTest(t.Context(), t,
		[]Tree{
			{
				RepoRoot:        tree.RepoRoot,
				Path:            tree.Path,
				SelfURLs:        tree.SelfURLs,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				Input:           cfg.BaseInput,
				BaseInput:       cfg.Input,
				ExternalChanges: cfg.ExternalChanges,
			},
			{
				RepoRoot:        tree.RepoRoot,
				Path:            tree.Path,
				SelfURLs:        tree.SelfURLs,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				BaseInput:       cfg.BaseInput,
				ExternalChanges: cfg.ExternalChanges,
				Input:           cfg.Input,
			},
		},
		cfg.Config,
	)
	next, other := renderedTrees5[0], renderedTrees5[1]

	assert.FatalIf(t, err != nil, "%v", err)

	assert.FatalIf(
		t,
		next.cfg.Network == network || other.cfg.Network != next.cfg.Network,
		"%v",
		"Network leaked across invocations sharing Cache",
	)

	for _, side := range []Rendered{next, other} {
		{
			_, innerInnerLocalErr := side.inputs.Acquire(
				t.Context(),
				&source.HTTPRequest{URL: target, Retry: sourcecache.RetryConfig{}, HeldSlot: nil},
			)
			assert.FatalIf(t, !errors.Is(
				innerInnerLocalErr,
				fs.ErrClosed,
			), "fresh returned side is not terminal: %v", innerInnerLocalErr)
		}
	}
	trees := source.NewHTTPInputs(c, next.cfg.Network, task.NewBounded(2))
	t.Cleanup(trees.Close)
	input, err := trees.Acquire(
		t.Context(),
		&source.HTTPRequest{URL: target + "/0", Retry: sourcecache.RetryConfig{}, HeldSlot: nil},
	)
	var body []byte
	if err == nil {
		body, err = fs.ReadFile(input, "content")
	}
	if err != nil || string(body) != "new run" || hits.Load() != 1 {
		t.Fatalf("fresh run: %q %v hits=%d", body, err, hits.Load())
	}
}

func testRenderTreesNetworkRunIsolationRemoteGenerators(t *testing.T) {
	t.Helper()

	var requests, version atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		fmt.Fprintf(w, "VALUE=run-%d\n", version.Load())
	}))
	defer server.Close()
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux.yaml",
		testutil.KustomizationYAML("owner", "apps", "  path: ./apps\n  interval: 5m\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"apps/kustomization.yaml",
		fmt.Sprintf(
			"namespace: apps\nconfigMapGenerator: [{name: remote, envs: [%s]}]\ngeneratorOptions: {disableNameSuffixHash: true}\n",
			server.URL,
		),
	)
	cfg := treeConfig{Cache: cachetest.New(t, "", sourcecache.New), Concurrency: 2, Tree: Tree{}}
	for _, v := range []int32{1, 2} {
		version.Store(v)
		before := requests.Load()
		renderedTrees6, err := renderTreesForTest(t.Context(), t,
			[]Tree{
				{
					RepoRoot:        root,
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
					ExternalChanges: cfg.ExternalChanges,
					Input:           cfg.BaseInput,
					BaseInput:       cfg.Input,
				},
				{
					RepoRoot:        root,
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					ExternalChanges: cfg.ExternalChanges,
					Input:           cfg.Input,
					BaseInput:       cfg.BaseInput,
				},
			},
			cfg.Config,
		)
		baseline, head := renderedTrees6[0], renderedTrees6[1]
		assert.NoError(t, err)
		for _, side := range []Rendered{baseline, head} {
			obj, ok := side.store.Get[*manifest.ConfigMap](
				manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "remote"},
			)

			assert.FatalIf(t, !ok, "%v", "snapshot lost bootstrap generator data")

			assert.Diff(t, obj.Data, map[string]any{"VALUE": fmt.Sprintf("run-%d", v)})
		}

		{
			fetched := requests.Load() - before
			assert.FatalIf(
				t,
				fetched < 1 || fetched > 2,
				"new invocation made %d requests, want one shared request or two independent waves",
				fetched,
			)
		}

		assert.FatalIf(
			t,
			baseline.cfg.Network != head.cfg.Network || &baseline.inputs == &head.inputs,
			"%v",
			"snapshots must share transport and retain independent input owners",
		)
	}
}

type testOrchestratorRunJoinsRemoteWorkersCaseInputs struct {
	workers *int
	backing *string
}

func testOrchestratorRunJoinsRemoteWorkersCase(
	t *testing.T,
	testCaseInput testOrchestratorRunJoinsRemoteWorkersCaseInputs,
) {
	t.Helper()

	root := ""
	if (*testCaseInput.backing) == "disk" {
		root = t.TempDir()
	}
	o, err := newTestOrchestrator(
		t,
		Tree{Path: t.TempDir()},
		Config{CacheDir: root, Concurrency: (*testCaseInput.workers)},
	)
	assert.NoError(t, err)
	entered, completed := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(completed)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { o.stop() })
	callers := make(chan error, (*testCaseInput.workers))
	for range *testCaseInput.workers {
		go func() {
			_, innerInnerInnerLocalErr := o.inputs.Acquire(
				t.Context(),
				&source.HTTPRequest{URL: srv.URL, Retry: sourcecache.RetryConfig{}, HeldSlot: nil},
			)
			callers <- innerInnerInnerLocalErr
		}()
	}
	<-entered
	result, err := o.render(t.Context(), t)
	assert.NoError(t, err)

	assert.FatalIf(t, result == nil, "%v", "cleanup discarded successful result")

	for range *testCaseInput.workers {
		{
			err := <-callers
			assert.FatalIf(
				t,
				!errors.Is(err, context.Canceled) && !errors.Is(err, fs.ErrClosed),
				"remote caller after Stop: %v",
				err,
			)
		}
	}
	<-completed

	{
		_, err := o.inputs.Acquire(
			t.Context(),
			&source.HTTPRequest{URL: srv.URL, Retry: sourcecache.RetryConfig{}, HeldSlot: nil},
		)
		assert.FatalIf(t, !errors.Is(
			err,
			fs.ErrClosed,
		), "run left TreeCache live: %v", err)
	}

	assert.NoError(t, o.cache.Close())
	if root != "" {
		res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})

		assert.FatalIf(t, err != nil || len(res.Errors) != 0, "run retained backing: %+v %v", res, err)
	}
}

type testRSRawSinkDeterministicCollisionWinnerCaseInputs struct {
	parent     *manifest.NamedResource
	ownerA     *manifest.NamedResource
	ownerB     *manifest.NamedResource
	docA       *map[string]any
	docB       *map[string]any
	other      *map[string]any
	version    *map[string]any
	omitted    *map[string]any
	explicit   *map[string]any
	winningCM  *map[string]any
	namespaceA *map[string]any
	namespaceB *map[string]any
	incomplete *[]map[string]any
	malformed  *map[string]any
	reverse    *bool
}

func testRSRawSinkDeterministicCollisionWinnerCase(
	t *testing.T,
	fixture *testRSRawSinkDeterministicCollisionWinnerCaseInputs,
) {
	t.Helper()
	testCaseInput := *fixture

	s := store.New()
	a := &manifest.ResourceSet{Name: testCaseInput.ownerA.Name, Namespace: testCaseInput.ownerA.Namespace}
	b := &manifest.ResourceSet{Name: testCaseInput.ownerB.Name, Namespace: testCaseInput.ownerB.Namespace}
	s.AddObjects(
		[]manifest.BaseManifest{
			a,
			b,
			&manifest.Kustomization{Name: testCaseInput.parent.Name, Namespace: testCaseInput.parent.Namespace},
		},
	)
	o := &Orchestrator{
		store: s,
		index: &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				(*testCaseInput.ownerA): {Parent: (*testCaseInput.parent)},
				(*testCaseInput.ownerB): {Parent: (*testCaseInput.parent)},
			},
		},
	}
	ids := []manifest.NamedResource{(*testCaseInput.ownerA), (*testCaseInput.ownerB)}
	if *testCaseInput.reverse {
		slices.Reverse(ids)
	}
	testRSRawSinkDeterministicCollisionWinnerCaseSegment(t, &testCaseInput, &s, &ids)
	assert.Equal(t, s.GetObject(testutil.KustomizationID("flux-system", "invalid")), manifest.BaseManifest(nil))
	want := append(
		slices.Clone((*testCaseInput.incomplete)),
		(*testCaseInput.docB),
		(*testCaseInput.other),
		(*testCaseInput.winningCM),
		(*testCaseInput.namespaceB),
	)
	assert.Diff(
		t,
		outputByParent(o.collectManifests()),
		map[manifest.NamedResource][]map[string]any{(*testCaseInput.parent): want},
	)
}

func testRSRawSinkDeterministicCollisionWinnerCaseSegment(
	t *testing.T,
	testCaseInput *testRSRawSinkDeterministicCollisionWinnerCaseInputs,
	s **store.Store,
	ids *[]manifest.NamedResource,
) {
	t.Helper()
	for _, id := range *ids {
		input := []map[string]any{
			(*testCaseInput.docB),
			(*testCaseInput.other),
			(*testCaseInput.winningCM),
			(*testCaseInput.namespaceB),
		}
		if id == (*testCaseInput.ownerA) {
			input = append(
				[]map[string]any{
					(*testCaseInput.docA),
					(*testCaseInput.version),
					(*testCaseInput.omitted),
					(*testCaseInput.explicit),
					(*testCaseInput.namespaceA),
					(*testCaseInput.malformed),
				},
				(*testCaseInput.incomplete)...)
		}
		parent := &manifest.RawObject{
			APIVersion: id.Group + "/v1",
			Kind:       id.Kind,
			Name:       id.Name,
			Namespace:  id.Namespace,
		}
		parseChildrenResult := (&base.Execution{Controller: &base.Controller{}}).ParseChildren(
			parent,
			testutil.DocumentOccurrences(input),
			manifest.ParseDocOptions{},
		)
		children, authoritative, failures := parseChildrenResult.Children, parseChildrenResult.Visible, parseChildrenResult.Failures
		assert.Diff(t, testutil.DocumentMaps(authoritative), input)
		mixedChildren := 0
		childValues := children
		for i := range childValues {
			child := childValues[i]
			if child.Obj.Named().Namespace == "" && manifest.KindOf(child.Obj).Namespaced() {
				children[i].Obj = child.Obj.WithNamespace(id.Namespace)
			}
			if children[i].Obj.Named() == (manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "foo"}) {
				mixedChildren++
			}

			assert.FatalIf(
				t,
				child.Obj.Named().Name == "invalid",
				"%v",
				"malformed typed document admitted",
			)
		}
		if id == (*testCaseInput.ownerA) {
			assert.Equal(t, mixedChildren, 2)
		}
		(*s).RecordEmitted(
			id,
			&store.Emission{Children: children, Manifests: authoritative, Failures: failures, Complete: true},
			nil,
		)
		_, _, admitted := (*s).GetEmission(id)
		admitted.Finish()
		assertCollisionEmission(t, admitted.Children)
	}
}

func assertCollisionEmission(t *testing.T, children []store.Child) {
	t.Helper()
	mixedChildren := 0
	childValues2 := children
	for childIndex := range childValues2 {
		child := childValues2[childIndex]
		if child.Obj.Named().Kind == manifest.KindConfigMap {
			mixedChildren++
		}
		if child.Obj.Named().Kind == "Namespace" {
			assert.Equal(t, child.Obj.Named().Namespace, "")
		}
	}
	assert.Equal(t, mixedChildren, 1)
}
