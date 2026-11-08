package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/schedule"
	src "github.com/home-operations/flate/internal/source"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/baseline"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/diff"
	"github.com/home-operations/flate/internal/discovery"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // Subtests share a synctest bubble and cannot run in parallel.
func TestRenderTrees_CancellationJoinsBeforeCacheClose(t *testing.T) {
	t.Run(
		"discovery_failure",
		func(t *testing.T) { testRenderTreesCancellationJoinsBeforeCacheCloseDiscoveryFailure(t) },
	)
	for _, workers := range []int{2, 16} {
		for _, headOnly := range []bool{false, true} {
			for _, tc := range []struct {
				backing  string
				injected bool
			}{{"memory", true}, {"disk", true}, {"disk", false}} {
				t.Run(
					fmt.Sprintf("workers_%d/head_only_%t/%s/injected_%t", workers, headOnly, tc.backing, tc.injected),
					func(t *testing.T) {
						testRenderTreesCancellationJoinsBeforeCacheCloseCase(
							t,
							testRenderTreesCancellationJoinsBeforeCacheCloseCaseInputs{
								workers:  &workers,
								headOnly: &headOnly,
								tc:       &tc,
							},
						)
					},
				)
			}
		}
	}
	t.Run("http-bootstrap", func(t *testing.T) { testRenderTreesCancellationJoinsBeforeCacheCloseHTTPBootstrap(t) })
}

type countedNodeController struct {
	reconcileController

	calls, admitted map[manifest.NamedResource]*atomic.Int64
}

type reconcileController interface {
	Owns(manifest.NamedResource) bool
	ReconcileNode(context.Context, manifest.NamedResource, manifest.DrainLevel, schedule.Result) *base.Execution
	PreviousRender(
		context.Context,
		manifest.BaseManifest,
		schedule.Result,
	) base.History
}

func (c countedNodeController) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
	previous schedule.Result,
) *base.Execution {
	c.calls[id].Add(1)
	execution := c.reconcileController.ReconcileNode(ctx, id, level, previous)
	if execution != nil && execution.Reads.Admitted() {
		c.admitted[id].Add(1)
	}
	return execution
}

type countedChartFetcher struct {
	chart      string
	charts     map[manifest.NamedResource]string
	incomplete bool
	calls      atomic.Int64
}

func (f *countedChartFetcher) Fetch(
	_ context.Context,
	object manifest.BaseManifest,
	_ *store.ReadSet,
	_ *sourcecache.Policy,
) (src.PreparedFetch, error) {
	return src.PreparedFetch{
		Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			if local != nil {
				return local, ctx.Err()
			}

			f.calls.Add(1)
			const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
			chart := f.chart
			if selected := f.charts[object.Named()]; selected != "" {
				chart = selected
			}
			artifact, _, err := baseline.AdmitDirectory(ctx, chart, chart, manifest.WorkScope{})
			if err != nil {
				return nil, err
			}
			artifact.Kind, artifact.Digest, artifact.Revision, artifact.Key = manifest.KindOCIRepository, digest, "1.0.0@"+digest, digest
			if f.incomplete {
				artifact.ContentFingerprint = nil
			}
			return artifact, nil
		},
	}, nil
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_DeclarationOnlyExcludesUnrelatedWork(t *testing.T) {
	roots := [2]string{t.TempDir(), t.TempDir()}
	for side, root := range roots {
		writeCluster(t, root, "same", "same")
		testutil.WriteFile(
			t,
			root,
			"ks-a.yaml",
			testutil.KustomizationYAML(
				"app-a",
				"flux-system",
				"  interval: "+[]string{"1h", "2h"}[side]+"\n  path: ./apps/app-a\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		)
		testutil.WriteFile(
			t,
			root,
			"kustomization.yaml",
			"resources: [ks-a.yaml, ks-b.yaml, chart.yaml, release.yaml]\n",
		)
		testutil.WriteFile(
			t,
			root,
			"chart.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: unrelated-chart, namespace: flux-system}\nspec: {url: 'oci://fixture.invalid/chart', ref: {tag: 1.0.0}}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"release.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: unrelated-release, namespace: flux-system}\nspec: {interval: 1h, chartRef: {kind: OCIRepository, name: unrelated-chart}}\n",
		)
	}
	chart := t.TempDir()
	testutil.WriteFile(t, chart, "fixture/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		chart,
		"fixture/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: from-chart}\n",
	)
	inputs := [2]*store.SourceArtifact{testDirectoryInput(t, roots[0]), testDirectoryInput(t, roots[1])}
	changes, err := change.Detect(t.Context(), inputs[0], inputs[1], manifest.WorkScope{})
	assert.NoError(t, err)
	assert.Diff(t, changes.Paths(), []string{"ks-a.yaml"})
	for side, root := range roots {
		for _, changedOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("side_%d/changed_%t", side, changedOnly), func(t *testing.T) {
				testRenderDeclarationOnlyExcludesUnrelatedWorkCase(
					t,
					testRenderDeclarationOnlyExcludesUnrelatedWorkCaseInputs{
						chart:       &chart,
						inputs:      &inputs,
						changes:     &changes,
						side:        &side,
						root:        &root,
						changedOnly: &changedOnly,
					},
				)
			},
			)
		}
	}
}

func TestProjectEntryReaders_ZeroBinding(t *testing.T) {
	t.Parallel()
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
	index := &manifest.DiscoveryIndex{
		Readers: map[string][]manifest.FileReader{"root.yaml": {{Reader: entry, Source: manifest.BootstrapSourceID}}},
	}
	for _, pair := range [][2]discovery.Binding{{{}, {Producer: entry}}, {{Producer: entry}, {}}, {{}, {}}} {
		assert.Equal(t, projectEntryReaders(index, pair[0].Producer, pair[1].Producer), index)
	}
}

func TestRenderTrees_EntryQualificationTransitions(t *testing.T) {
	t.Parallel()

	for _, pair := range [][2]string{{"discovery", "discovery"}, {"discovery", "package"}, {"package", "discovery"}, {"authored", "discovery"}, {"discovery", "authored"}} {
		t.Run(pair[0]+" to "+pair[1], func(t *testing.T) {
			t.Parallel()
			testRenderTreesEntryQualificationTransitionsCase(
				t,
				&pair,
			)
		},
		)
	}
}

func testRenderTreesCancellationJoinsBeforeCacheCloseDiscoveryFailure(t *testing.T) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		headEntered := make(chan struct{})
		failure := errors.New("baseline discovery failed")
		baseInput := &store.SourceArtifact{LocalPath: t.TempDir(), FS: &joinedTreeFS{acquire: func() error {
			<-release
			return failure
		}}, Warnings: []manifest.Warning{{Category: manifest.WarnBaselineStatusUnavailable, Message: "base warning"}}}
		headFailure := errors.New("head discovery failed")
		headInput := &store.SourceArtifact{LocalPath: t.TempDir(), FS: &joinedTreeFS{acquire: func() error {
			close(headEntered)
			return headFailure
		}}, Warnings: []manifest.Warning{{Category: manifest.WarnBaselineStatusUnavailable, Message: "head warning"}}}
		var baseRef, head Rendered
		done := make(chan error, 1)
		go func() {
			var err error
			runs, runErr := renderTreesForTest(t.Context(), t, []Tree{
				{
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
					Input:           baseInput,
					ExternalChanges: change.NewSet(nil),
				},
				{
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					Input:           headInput,
					ExternalChanges: change.NewSet(nil),
				},
			}, Config{Concurrency: 2, HeadOnly: true})
			err = runErr
			if len(runs) != 2 {
				t.Errorf("missing diagnostic sides: %v", err)
			} else {
				baseRef, head = runs[0], runs[1]
			}
			done <- err
		}()
		synctest.Wait()
		select {
		case <-headEntered:
		default:
			t.Error("head planning did not start while base planning waited")
		}
		close(release)
		err := <-done
		assert.FatalIf(
			t,
			!errors.Is(err, failure) || strings.Count(err.Error(), failure.Error()) != 1,
			"discovery cause lost or duplicated: %v",
			err,
		)
		for i, side := range []Rendered{baseRef, head} {
			assert.Equal(t, errors.Is(side.Err, []error{failure, headFailure}[i]), true)
			assert.FatalIf(t, side.Orchestrator != nil || side.Result == nil ||
				len(
					side.Result.Manifests,
				)+len(
					side.Result.Failed,
				) != 0, "failed preparation exposed content: %+v", side)
		}
	})
}

type testRenderTreesCancellationJoinsBeforeCacheCloseCaseInputs struct {
	workers  *int
	headOnly *bool
	tc       *struct {
		backing string

		injected bool
	}
}

func testRenderTreesCancellationJoinsBeforeCacheCloseCase(
	t *testing.T,
	testCaseInput testRenderTreesCancellationJoinsBeforeCacheCloseCaseInputs,
) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) { testRenderTreesCancellationCaseBubble(t, testCaseInput) })
}

func testRenderTreesCancellationJoinsBeforeCacheCloseHTTPBootstrap(t *testing.T) {
	t.Helper()
	for _, tc := range []struct{ headOnly, injected bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
		t.Run(fmt.Sprintf("head_only_%t/injected_%t", tc.headOnly, tc.injected), func(t *testing.T) {
			testRenderTreesCancellationJoinsBeforeCacheCloseHTTPBootstrapCase(
				t,
				testRenderTreesCancellationJoinsBeforeCacheCloseHTTPBootstrapCaseInputs{tc: &tc},
			)
		},
		)
	}
}

type testRenderDeclarationOnlyExcludesUnrelatedWorkCaseInputs struct {
	chart       *string
	inputs      *[2]*store.SourceArtifact
	changes     **change.Set
	side        *int
	root        *string
	changedOnly *bool
}

func testRenderDeclarationOnlyExcludesUnrelatedWorkCase(
	t *testing.T,
	testCaseInput testRenderDeclarationOnlyExcludesUnrelatedWorkCaseInputs,
) {
	t.Helper()

	tree := Tree{
		Path:     (*testCaseInput.root),
		RepoRoot: (*testCaseInput.root),
		Input:    testCaseInput.inputs[(*testCaseInput.side)],
		ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide((*testCaseInput.side))},
	}
	if *testCaseInput.changedOnly {
		tree.BaseInput, tree.ExternalChanges = testCaseInput.inputs[1-(*testCaseInput.side)], (*testCaseInput.changes)
	}
	o, err := newTestOrchestrator(t, tree, Config{Concurrency: 2})
	assert.NoError(t, err)
	fetcher := &countedChartFetcher{chart: (*testCaseInput.chart)}
	o.src.Fetchers[manifest.KindOCIRepository] = fetcher
	assert.NoError(t, o.bootstrap(t.Context()))
	ids := []manifest.NamedResource{
		o.entryBinding.Producer,
		testutil.KustomizationID("flux-system", "app-a"),
		testutil.KustomizationID("flux-system", "app-b"),
		{
			Group:     manifest.HelmReleaseDomain,
			Kind:      manifest.KindHelmRelease,
			Namespace: "flux-system",
			Name:      "unrelated-release",
		},
	}
	calls, admitted := map[manifest.NamedResource]*atomic.Int64{}, map[manifest.NamedResource]*atomic.Int64{}
	for _, id := range ids {
		calls[id], admitted[id] = new(atomic.Int64), new(atomic.Int64)
	}
	o.controllers.Kustomization = countedNodeController{o.ksc, calls, admitted}
	o.controllers.HelmRelease = countedNodeController{o.hrc, calls, admitted}
	runErr := o.run(t.Context())
	assert.NoError(t, runErr)
	for _, id := range ids {
		t.Logf("work %s calls=%d admitted=%d", id, calls[id].Load(), admitted[id].Load())
	}
	t.Logf("chart acquisitions=%d", fetcher.calls.Load())
	if *testCaseInput.changedOnly {
		assert.Equal(t, admitted[ids[2]].Load(), int64(0))
		assert.Equal(t, admitted[ids[3]].Load(), int64(0))
		assert.Equal(t, fetcher.calls.Load(), int64(0))
		assert.Equal(t, o.store.GetArtifact(ids[2], store.ArtifactContent), store.Artifact(nil))
		assert.Equal(t, o.store.GetArtifact(ids[3], store.ArtifactContent), store.Artifact(nil))
	} else {
		assert.Equal(t, admitted[ids[2]].Load(), int64(1))
		assert.FatalIf(
			t,
			admitted[ids[3]].Load() < 1,
			"%v",
			"full-render control did not reconcile the release",
		)
		assert.Equal(t, fetcher.calls.Load(), int64(1))
	}
	assert.Equal(t, admitted[ids[0]].Load(), int64(1))
	assert.Equal(t, admitted[ids[1]].Load(), int64(1))
	result, err := o.collectResult(runErr)
	t.Cleanup(result.Close)
	assert.NoError(t, err)
	var interval any
	for _, batch := range result.Manifests {
		for _, doc := range batch.Documents {
			metadata, _ := doc["metadata"].(map[string]any)
			if doc["kind"] == manifest.KindKustomization && metadata["name"] == "app-a" {
				interval = doc["spec"].(map[string]any)["interval"]
			}
		}
	}
	assert.Equal(t, interval, any([]string{"1h", "2h"}[(*testCaseInput.side)]))
}

func testRenderTreesEntryQualificationTransitionsCase(
	t *testing.T,
	testCaseInput *[2]string,
) {
	t.Helper()

	var trees []Tree
	if len(*testCaseInput) > 0 {
		trees = make([]Tree, 0, len(*testCaseInput))
	}
	for side, qualification := range *testCaseInput {
		root := t.TempDir()
		testutil.WriteFile(t, root, "Taskfile.yaml", "version: '3'\ntasks: {build: {cmds: [true]}}\n")
		for _, name := range []string{"a", "b"} {
			testutil.WriteFile(
				t,
				root,
				"clusters/"+name+"/ks.yaml",
				testutil.KustomizationYAML(name, "apps", "  path: ./workload/"+name+"\n"),
			)
			testutil.WriteFile(t, root, "workload/"+name+"/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"workload/"+name+"/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+"}\ndata: {side: '"+strconv.Itoa(
					side,
				)+"'}\n",
			)
		}
		if qualification == "package" {
			testutil.WriteFile(
				t,
				root,
				"kustomization.yaml",
				"resources: [clusters/a/ks.yaml, clusters/b/ks.yaml]\n",
			)
		}
		if qualification == "authored" {
			assert.NoError(t, os.Remove(filepath.Join(root, "Taskfile.yaml")))
			testutil.WriteFile(
				t,
				root,
				"owner.yaml",
				testutil.KustomizationYAML("root-owner", "apps", "  path: ./\n"),
			)
		}
		trees = append(
			trees,
			Tree{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)}, Path: root, RepoRoot: root},
		)
	}
	var previous []byte
	for range 2 {
		runs, err := renderTreesForTest(t.Context(), t, trees, Config{Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(runs), 2)
		for side, run := range runs {
			qualified := testCaseInput[side] != "discovery"
			assert.Equal(t, run.entryBinding != (discovery.Binding{}), qualified)
			assert.Diff(t, configMapNames(run.Result), map[string]bool{"a": true, "b": true})
		}
		projectedResults0, projectionErrors0 := Select(
			runs,
			nil,
			[]manifest.NamedResource{testutil.KustomizationID("", "")},
			nil,
		)
		selected, err, _ := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
		assert.NoError(t, err)
		assert.Equal(t, len(selected), 2)
		for side, selection := range selected {
			wantKeys := 0
			if testCaseInput[side] != "discovery" {
				wantKeys = 1
			}
			assert.Equal(t, len(selection.ProducerKeys), wantKeys)
			assert.Equal(t, len(selection.Unavailable), 0)
		}
		body, _, notes, err := diff.Compare(
			diff.Snapshot{
				Subjects:     selected[0].Subjects,
				Manifests:    selected[0].Manifests,
				ProducerKeys: selected[0].ProducerKeys,
				Unavailable:  selected[0].Unavailable,
			},
			diff.Snapshot{
				Subjects:     selected[1].Subjects,
				Manifests:    selected[1].Manifests,
				ProducerKeys: selected[1].ProducerKeys,
				Unavailable:  selected[1].Unavailable,
			},
			nil,
			diff.Options{Format: diff.FormatJSON},
		)
		assert.NoError(t, err)
		assert.Equal(t, len(notes), 0)
		if previous != nil {
			assert.Equal(t, string(body), string(previous))
		}
		previous = body
	}
}

type testRenderTreesCancellationJoinsBeforeCacheCloseHTTPBootstrapCaseInputs struct {
	tc *struct {
		headOnly bool
		injected bool
	}
}

func testRenderTreesCancellationCaseBubble(
	t *testing.T,
	testCaseInput testRenderTreesCancellationJoinsBeforeCacheCloseCaseInputs,
) {
	t.Helper()

	root := ""
	if testCaseInput.tc.backing == "disk" {
		root = t.TempDir()
	}
	owner, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)
	externalShare := true
	t.Cleanup(func() {
		if externalShare {
			assert.NoError(t, owner.Close())
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	release := make(chan struct{})
	defer close(release)
	active := 2
	if *testCaseInput.headOnly {
		active = 1
	}
	wantOrigErrors := 1
	if *testCaseInput.headOnly {
		wantOrigErrors = 0
	}
	entered, canceled, completed := make(
		chan struct{},
		2,
	), make(
		chan struct{},
		2,
	), make(
		chan struct{},
		2,
	)
	inputs := [2]*store.SourceArtifact{}
	for i := range inputs {
		input := &joinedTreeFS{payloadPath: "app/payload.txt", FS: fstest.MapFS{
			"kustomization.yaml": {Data: []byte("resources: [flux.yaml]\n")},
			"flux.yaml": {
				Data: []byte(
					testutil.KustomizationYAML(
						"owner",
						"apps",
						"  path: ./app\n  sourceRef: {kind: GitRepository, name: cluster}\n",
					),
				),
			},
			"app/kustomization.yaml": {
				Data: []byte("configMapGenerator: [{name: held, files: [payload.txt]}]\n"),
			},
			"app/payload.txt": {Data: fmt.Appendf(nil, "fixture-%d", i)},
		}}
		input.acquire = func() error {
			_, _, _, localErr := owner.Blob(
				ctx,
				sourcecache.BlobKey{Content: fmt.Sprintf("held-input-%d", i)},
				&sourcecache.Policy{},
				func(_ context.Context, _ sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
					entered <- struct{}{}
					<-ctx.Done()
					canceled <- struct{}{}
					<-release
					completed <- struct{}{}
					return sourcecache.ProducerResult{}, ctx.Err()
				},
			)
			return localErr
		}
		inputs[i] = &store.SourceArtifact{LocalPath: t.TempDir(), FS: input}
	}
	cfg := treeConfig{
		CacheDir:    root,
		Concurrency: (*testCaseInput.workers),
		HeadOnly:    (*testCaseInput.headOnly),
		StatusListener: func(id manifest.TreeID, resource manifest.DiagnosticIdentity, status store.StatusInfo) {
			if resource.ID == testutil.KustomizationID("apps", "owner") &&
				status.String() == "resolving dependencies" {
				input := inputs[id.Side].FS.(*joinedTreeFS)
				input.once.Do(func() { input.err = input.acquire() })
			}
		},
		BaseInput:       inputs[0],
		Input:           inputs[1],
		ExternalChanges: change.NewSet([]string{"app/payload.txt"}),
	}
	if testCaseInput.tc.injected {
		cfg.Cache = owner
	}
	done := make(chan error, 1)
	go func() {
		_, innerLocalErr := renderTreesForTest(ctx, t,
			[]Tree{
				{
					RepoRoot:        inputs[0].LocalPath,
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
					Input:           cfg.BaseInput,
					BaseInput:       cfg.Input,
					ExternalChanges: cfg.ExternalChanges,
				},
				{
					RepoRoot:        inputs[1].LocalPath,
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					Input:           cfg.Input,
					BaseInput:       cfg.BaseInput,
					ExternalChanges: cfg.ExternalChanges,
				},
			},
			cfg.Config,
		)
		done <- innerLocalErr
	}()
	for range active {
		<-entered
	}
	if !testCaseInput.tc.injected {
		assert.NoError(t, owner.Close())
		externalShare = false
	}
	cancel()
	for range active {
		<-canceled
	}
	synctest.Wait()
	select {
	case innerInnerLocalErr := <-done:
		t.Fatalf("render returned before acquisition cleanup: %v", innerInnerLocalErr)
	default:
	}
	if root != "" {
		res, innerInnerInnerLocalErr := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
		assert.FatalIf(
			t,
			innerInnerInnerLocalErr == nil && len(res.Errors) == 0,
			"%v",
			"backing released before held acquisitions joined",
		)
	}
	key := sourcecache.BlobKey{Content: "backing-survives"}
	_, _, _, err = owner.Blob(
		t.Context(),
		key,
		&sourcecache.Policy{MaxAge: time.Hour},
		func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			_, innerInnerInnerInnerLocalErr := io.WriteString(w, "live")
			return sourcecache.ProducerResult{}, innerInnerInnerInnerLocalErr
		},
	)
	assert.NoError(t, err)

	for range active {
		release <- struct{}{}
	}
	testRenderTreesCancellationJoinedError(t, done, &wantOrigErrors)
	for range active {
		<-completed
	}
	if testCaseInput.tc.injected {
		_, _, _, err = owner.Blob(t.Context(), key, &sourcecache.Policy{MaxAge: time.Hour}, nil)
		assert.NoError(t, err)

		assert.NoError(t, owner.Close())
		externalShare = false
	}
	var reacquired atomic.Int64
	_, _, _, err = owner.Blob(
		ctx,
		sourcecache.BlobKey{Content: "post-close-canceled"},
		&sourcecache.Policy{},
		func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			reacquired.Add(1)
			return sourcecache.ProducerResult{}, nil
		},
	)
	testRenderTreesCancellationClosedOwner(t, err, &reacquired, root)
}

func testRenderTreesCancellationJoinedError(t *testing.T, done <-chan error, wantOrigErrors *int) {
	t.Helper()
	innerInnerInnerInnerInnerLocalErr := <-done
	assert.FatalIf(t, !errors.Is(innerInnerInnerInnerInnerLocalErr, context.Canceled) ||
		strings.Count(innerInnerInnerInnerInnerLocalErr.Error(), "orig snapshot:") != (*wantOrigErrors) ||
		strings.Count(
			innerInnerInnerInnerInnerLocalErr.Error(),
			"current snapshot:",
		) != 1, "joined cancellation lost a side: %v", innerInnerInnerInnerInnerLocalErr)
}

func testRenderTreesCancellationClosedOwner(t *testing.T, err error, reacquired *atomic.Int64, root string) {
	t.Helper()
	if !errors.Is(err, context.Canceled) || reacquired.Load() != 0 {
		t.Fatalf(
			"canceled run acquired after owner close: calls=%d error=%v",
			reacquired.Load(),
			err,
		)
	}
	if root != "" {
		res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
		assert.FatalIf(
			t,
			err != nil || len(res.Errors) != 0,
			"joined render retained owner: %+v %v",
			res,
			err,
		)
	}
}
