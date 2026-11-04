package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type sharedRepositoryFetcher struct {
	mu      sync.Mutex
	calls   map[manifest.NamedResource]int
	observe func(context.Context, manifest.BaseManifest, *store.ReadSet)
}

func (f *sharedRepositoryFetcher) Fetch(
	ctx context.Context,
	obj manifest.BaseManifest,
	reads *store.ReadSet,
	_ sourcecache.Policy,
) (*store.SourceArtifact, error) {
	f.mu.Lock()
	f.calls[obj.Named()]++
	f.mu.Unlock()
	if f.observe != nil {
		f.observe(ctx, obj, reads)
	}
	return &store.SourceArtifact{Kind: manifest.KindHelmChart, Key: "chart", FS: fstest.MapFS{
		"chart/Chart.yaml": {Data: []byte("apiVersion: v2\nname: demo\nversion: 1.0.0\n")},
		"chart/templates/configmap.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-output\n"),
		},
	}}, nil
}

//nolint:paralleltest // Subtests share a synctest bubble and cannot run in parallel.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAGDispatch_SharedHelmRepositoryAcquisition(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, scenario := range []string{"unchanged", "effective_input_change", "withdrawal_reanchor"} {
			t.Run(fmt.Sprintf("workers=%d/%s", workers, scenario), func(t *testing.T) {
				testDAGDispatchSharedHelmRepositoryAcquisitionCase(
					t,
					testDAGDispatchSharedHelmRepositoryAcquisitionCaseInputs{workers: &workers, scenario: &scenario},
				)
			},
			)
		}
	}
}

type testDAGDispatchSharedHelmRepositoryAcquisitionCaseInputs struct {
	workers  *int
	scenario *string
}

func testDAGDispatchSharedHelmRepositoryAcquisitionCase(
	t *testing.T,
	testCaseInput testDAGDispatchSharedHelmRepositoryAcquisitionCaseInputs,
) {
	t.Helper()
	var outputs [2][][]map[string]any
	for run, arrival := range []string{"before_return", "after_acceptance"} {
		synctest.Test(t, func(t *testing.T) {
			testSharedRepositoryAcquisitionArrival(t, testCaseInput, arrival, &outputs[run])
		})
	}
	{
		{
			diff := cmp.Diff(outputs[0], outputs[1])
			assert.ErrorIf(t, diff != "", "controlled output differs (-want +got): %s", diff)
		}
	}
}

func testSharedRepositoryAcquisitionArrival(
	t *testing.T,
	testCaseInput testDAGDispatchSharedHelmRepositoryAcquisitionCaseInputs,
	arrival string,
	documents *[][]map[string]any,
) {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(
		slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	)
	defer slog.SetDefault(previous)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dir := t.TempDir()
	o, err := newTestOrchestrator(
		t,
		Tree{Path: dir, RepoRoot: dir},
		Config{Concurrency: (*testCaseInput.workers)},
	)
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	defer func() {
		o.stop()
	}()
	f := &sharedRepositoryFetcher{calls: map[manifest.NamedResource]int{}}
	o.src.Fetchers[manifest.KindHelmRepository] = f
	o.src.Fetchers[manifest.KindHelmChart] = f
	s := o.store
	p := testutil.KustomizationID("flux-system", "publisher")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	doc, children, original, repo := sharedRepositorySource(t, o, p)
	releases := make(sharedRepositoryConsumers, 0, 3)
	permits := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	releaseConsumers := make(sharedRepositoryReleases, 0, 3)
	for i := range 3 {
		hr := sharedRepositoryConsumer(i, repo)
		s.AddObject(hr)
		releases = append(releases, hr.Named())
		releaseConsumers = append(releaseConsumers, sync.OnceFunc(func() { close(permits[i]) }))
	}
	releaseConsumers[0]()
	o.configureControllers()
	o.src.StartLifecycle()
	o.ksc.StartLifecycle()
	o.hrc.StartLifecycle()
	publishPermit, published := make(chan struct{}), make(chan struct{})
	fetchPermit, accepted := make(chan struct{}), make(chan struct{})
	assessmentPermit := make(chan struct{})
	withdrawalObserved := make(chan struct{})
	releaseWithdrawal := sync.OnceFunc(func() { close(withdrawalObserved) })
	releasePublisher := sync.OnceFunc(func() { close(publishPermit) })
	releaseFetch := sync.OnceFunc(func() { close(fetchPermit) })
	releaseAssessment := sync.OnceFunc(func() { close(assessmentPermit) })
	signalAccepted := sync.OnceFunc(func() { close(accepted) })
	publish := sync.OnceFunc(func() {
		nextDoc := sharedRepositoryPublication(doc, p, testCaseInput)
		_, _, next := s.GetEmission(p)
		defer next.Finish()
		localChildren, retained := parseChildrenForTest(
			p,
			[]map[string]any{nextDoc},
			manifest.ParseDocOptions{},
		)
		next.Children, next.Manifests = localChildren, testutil.DocumentOccurrences(retained)
		next.Complete = true
		controllertest.Emit(t.Context(), o.ksc.Controller, p, &next)
		s.UpdateStatus(p, store.StatusInfo{Status: store.StatusReady})
		close(published)
	})
	if (*testCaseInput.scenario) != "effective_input_change" {
		_, _, initial := s.GetEmission(p)
		initial.Children = children
		controllertest.Emit(t.Context(), o.ksc.Controller, p, &initial)
		initial.Finish()
	}

	entered := make(chan sharedRepositoryAcquisition, 1)
	var firstFetch atomic.Bool
	f.observe = func(ctx context.Context, obj manifest.BaseManifest, reads *store.ReadSet) {
		chart := obj.(*manifest.HelmChartSource)
		r, ok := sharedRepositoryBacking(t, chart, reads)
		if ok && firstFetch.CompareAndSwap(false, true) {
			entered <- sharedRepositoryAcquisition{reads: reads, url: r.URL, id: chart.Named()}
			waitSharedRepositoryBarrier(ctx, fetchPermit)
		}
	}
	captured := make(chan *base.Execution, 1)
	var firstRepository sync.Once
	d := dagDispatcher{o}
	dispatch := fixtureDispatcher{
		store: s,
		dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
			if id == p {
				o.tasks.YieldSlot(func() {
					waitSharedRepositoryBarrier(ctx, publishPermit)
				})
				switch {
				case ctx.Err() != nil:
				case (*testCaseInput.scenario) == "withdrawal_reanchor":
					_, _, empty := s.GetEmission(p)
					defer empty.Finish()
					empty.Children, empty.Manifests, empty.Complete = nil, nil, true
					controllertest.Emit(t.Context(), o.ksc.Controller, p, &empty)
					{
						assert.ErrorIf(
							t,
							s.GetDeclaration(repo) != nil,
							"%v",
							"withdrawal retained repository authority",
						)
					}
					releaseWithdrawal()
				default:
					publish()
				}
				return schedule.Result{}
			}
			releases.waitFor(ctx, o.tasks, id, permits)
			hidden := hiddenSharedRepositoryDeclaration(s, id, repo)

			result := d.Dispatch(ctx, id, level)
			if testCaseInput.changesRepository(id, repo) {
				firstRepository.Do(func() {
					execution := result.Assessment.(*base.Execution)
					r, ok := execution.Reads.Get[*manifest.HelmRepository](repo)
					{
						assert.ErrorIf(
							t,
							!ok || r.URL != original.URL,
							"%v",
							"initial assessment lost the original repository URL",
						)
					}
					captured <- execution
					o.tasks.YieldSlot(func() {
						waitSharedRepositoryBarrier(ctx, assessmentPermit)
					})
				})
			}
			if testCaseInput.reanchorsRepository(hidden) {
				o.tasks.YieldSlot(func() {
					waitSharedRepositoryBarrier(ctx, withdrawalObserved)
				})
				if ctx.Err() != nil {
					return result
				}
				publish()
			}
			signalSharedRepositoryAcceptance(id, result, signalAccepted)
			return result
		},
	}
	sched := schedule.New(o.tasks, dispatch)
	unwatch := o.watchDAG(sched)
	defer unwatch()
	sched.Seed(append([]schedule.NodeID{repo, p}, releases...))
	done := make(chan error, 1)
	go func() { done <- sched.Run(ctx) }()
	joined := false
	defer func() {
		cancel()
		releaseWithdrawal()
		releasePublisher()
		releaseAssessment()
		releaseFetch()
		releaseConsumers.releaseAll()
		joinSharedRepositoryScheduler(joined, done)
		o.tasks.BlockTillDone()
	}()
	await := func(signal <-chan struct{}) {
		t.Helper()
		awaitSharedRepositorySignal(t, signal, done, &joined)
	}
	if (*testCaseInput.scenario) == "effective_input_change" {
		initial := initialSharedRepositoryAssessment(t, captured, done, &joined)
		synctest.Wait()
		{
			assert.FatalIf(
				t,
				!initial.Reads.Valid(),
				"%v",
				"initial assessment was already superseded",
			)
		}
		releasePublisher()
		await(published)
		{
			assert.ErrorIf(
				t,
				initial.Reads.Valid(),
				"%v",
				"changed URL retained the original captured assessment",
			)
		}
		releaseAssessment()
	}
	first := firstSharedRepositoryAcquisition(t, entered, done, &joined)
	synctest.Wait()
	{
		assert.FatalIf(t, !first.reads.Valid(), "%v", "initial acquisition was already superseded")
	}
	releaseConsumers.beforeReturn(t, arrival, first.reads)
	if (*testCaseInput.scenario) != "effective_input_change" {
		assertSharedRepositoryInitialURL(t, first.url, original)
		releasePublisher()
		await(published)
		synctest.Wait()
	}
	releaseFetch()
	await(accepted)
	synctest.Wait()
	releaseConsumers.afterAcceptance(arrival)
	err = <-done
	joined = true
	o.tasks.BlockTillDone()
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	finalChart := finalSharedRepositoryChart(t, s, repo, &first)
	completions := sharedRepositoryCompletions(t, &logs, f, s)
	completions.assertFetches(t, f, repo, finalChart)
	releases.appendOutputs(t, s, documents)
}

func sharedRepositoryCompletions(
	t *testing.T,
	logs *bytes.Buffer,
	f *sharedRepositoryFetcher,
	s *store.Store,
) sharedRepositoryCompletionCounts {
	t.Helper()
	completions := sharedRepositoryCompletionCounts{}
	for line := range bytes.SplitSeq(logs.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var event struct {
			Message string `json:"msg"`
			ID      string `json:"id"`
		}
		{
			{
				err := json.Unmarshal(line, &event)
				assert.FatalIf(t, err != nil, "%v", err)
			}
		}
		if event.Message == "source: fetch complete" {
			for id := range f.calls {
				if manifest.DiagnosticFor(id, s.GetObject(id)).String() == event.ID {
					completions[id]++
				}
			}
		}
	}
	return completions
}

func (cc sharedRepositoryCompletionCounts) assertFetches(
	t *testing.T,
	f *sharedRepositoryFetcher,
	repo, finalChart manifest.NamedResource,
) {
	t.Helper()
	t.Logf("acquisitions=%v completions=%v", f.calls, cc)
	{
		assert.ErrorIf(
			t,
			f.calls[repo] != 0 || cc[repo] != 0,
			"%v",
			"existence-only repository fetched",
		)
	}
	for id, calls := range f.calls {
		if cc[id] > 1 || cc[id] > calls || calls > 2 {
			t.Errorf("%s calls=%d completions=%d", id, calls, cc[id])
		}
	}
	if cc[finalChart] != 1 {
		t.Errorf("final chart completions=%d, want 1", cc[finalChart])
	}
}

func (cs sharedRepositoryConsumers) appendOutputs(
	t *testing.T,
	s *store.Store,

	documents *[][]map[string]any,
) {
	t.Helper()
	for _, id := range cs {
		{
			{
				info, ok := s.GetStatus(
					id,
				)
				assert.ErrorIf(t, !ok || info.Status != store.StatusReady ||
					info.Message != "", "consumer %s: %+v", id, info)
			}
		}
		_, _, output := s.GetEmission(id)
		{
			assert.ErrorIf(
				t,
				len(output.Manifests) != 1,
				"consumer %s emitted %d documents",
				id,
				len(output.Manifests),
			)
		}
		*documents = append(*documents, testutil.DocumentMaps(output.Manifests))
		output.Finish()
	}
}

type sharedRepositoryCompletionCounts map[manifest.NamedResource]int

func waitSharedRepositoryBarrier(ctx context.Context, signal <-chan struct{}) {
	select {
	case <-signal:
	case <-ctx.Done():
	}
}

func awaitSharedRepositorySignal(t *testing.T, signal <-chan struct{}, done <-chan error, joined *bool) {
	t.Helper()
	select {
	case <-signal:
	case localErr := <-done:
		*joined = true
		t.Fatalf("scheduler stopped before barrier: %v", localErr)
	}
}

func initialSharedRepositoryAssessment(
	t *testing.T,
	captured <-chan *base.Execution,
	done <-chan error,
	joined *bool,
) *base.Execution {
	t.Helper()
	var initial *base.Execution
	select {
	case initial = <-captured:
	case innerLocalErr := <-done:
		*joined = true
		t.Fatalf("scheduler stopped before capturing the original URL: %v", innerLocalErr)
	}
	return initial
}

func sharedRepositorySource(
	t *testing.T,
	o *Orchestrator,
	p manifest.NamedResource,
) (map[string]any, []store.Child, *manifest.HelmRepository, manifest.NamedResource) {
	t.Helper()
	s := o.store
	doc := map[string]any{
		"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindHelmRepository,
		"metadata": map[string]any{"name": "shared", "namespace": "apps"},
		"spec":     map[string]any{"url": "https://charts.example", "interval": "1h"},
	}
	children, _ := parseChildrenForTest(p, []map[string]any{doc}, manifest.ParseDocOptions{})
	{
		assert.FatalIf(t, len(children) != 1, "%v", "repository fixture was not parsed")
	}
	original := children[0].Obj.(*manifest.HelmRepository)
	repo := original.Named()
	s.AddObject(original)
	o.index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		repo: {Productions: []manifest.Production{{Owner: p, Source: manifest.BootstrapSourceID}}},
		p:    {Productions: []manifest.Production{{Source: manifest.BootstrapSourceID}}},
	}}
	return doc, children, original, repo
}

func sharedRepositoryConsumer(i int, repo manifest.NamedResource) *manifest.HelmRelease {
	hr := &manifest.HelmRelease{
		Name:      fmt.Sprintf("consumer-%d", i),
		Namespace: "apps",
		Chart:     manifest.HelmChart{Name: "demo", Version: "1.0.0", Source: repo},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:   "demo",
					Version: "1.0.0",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind:       repo.Kind,
						Name:       repo.Name,
						Namespace:  repo.Namespace,
						APIVersion: "",
					},
				},
			},
		},
	}
	return hr
}

func (rs sharedRepositoryReleases) releaseAll() {
	for _, release := range rs {
		release()
	}
}

func (rs sharedRepositoryReleases) beforeReturn(t *testing.T, arrival string, reads *store.ReadSet) {
	t.Helper()
	if arrival == "before_return" {
		rs[1]()
		synctest.Wait()
		rs[2]()
		synctest.Wait()
		{
			assert.ErrorIf(
				t,
				!reads.Valid(),
				"%v",
				"another release invalidated an independent generated chart acquisition",
			)
		}
	}
}

func (rs sharedRepositoryReleases) afterAcceptance(arrival string) {
	if arrival == "after_acceptance" {
		rs[1]()
		synctest.Wait()
		rs[2]()
	}
}

func joinSharedRepositoryScheduler(joined bool, done <-chan error) {
	if !joined {
		<-done
	}
}

type sharedRepositoryReleases []func()

func sharedRepositoryPublication(
	doc map[string]any,
	p manifest.NamedResource,
	testCaseInput testDAGDispatchSharedHelmRepositoryAcquisitionCaseInputs,
) map[string]any {
	nextDoc := manifest.DeepCopyMap(doc)
	nextDoc["metadata"].(map[string]any)["labels"] = map[string]any{
		"kustomize.toolkit.fluxcd.io/name": p.Name,
	}
	if (*testCaseInput.scenario) == "effective_input_change" {
		nextDoc["spec"].(map[string]any)["url"] = "https://changed.example"
	}
	return nextDoc
}

func signalSharedRepositoryAcceptance(id schedule.NodeID, result schedule.Result, signalAccepted func()) {
	if id.Kind == manifest.KindHelmChart && result.Assessment.Valid() &&
		result.Assessment.Outcome() == schedule.Terminal {
		signalAccepted()
	}
}

func (cs sharedRepositoryConsumers) waitFor(
	ctx context.Context,
	tasks *task.Service,
	id schedule.NodeID,
	permits []chan struct{},
) {
	for i, hr := range cs {
		if id == hr {
			tasks.
				YieldSlot(func() {
					waitSharedRepositoryBarrier(ctx, permits[i])
				})
		}
	}
}

type sharedRepositoryConsumers []manifest.NamedResource

func hiddenSharedRepositoryDeclaration(s *store.Store, id, repo manifest.NamedResource) bool {
	return id == repo && s.GetDeclaration(id) == nil
}

func (in testDAGDispatchSharedHelmRepositoryAcquisitionCaseInputs) changesRepository(
	id, repo manifest.NamedResource,
) bool {
	return id == repo && (*in.scenario) == "effective_input_change"
}

func (in testDAGDispatchSharedHelmRepositoryAcquisitionCaseInputs) reanchorsRepository(hidden bool) bool {
	return hidden && (*in.scenario) == "withdrawal_reanchor"
}

func firstSharedRepositoryAcquisition(
	t *testing.T,
	entered <-chan sharedRepositoryAcquisition,
	done <-chan error,
	joined *bool,
) sharedRepositoryAcquisition {
	t.Helper()
	var first sharedRepositoryAcquisition
	select {
	case first = <-entered:
	case innerInnerLocalErr := <-done:
		*joined = true
		t.Fatalf("scheduler stopped before fetch entry: %v", innerInnerLocalErr)
	}
	return first
}

func finalSharedRepositoryChart(
	t *testing.T,
	s *store.Store,
	repo manifest.NamedResource,
	firstValue *sharedRepositoryAcquisition,
) manifest.NamedResource {
	t.Helper()
	first := *firstValue
	final, ok := s.Get[*manifest.HelmRepository](repo)
	{
		assert.FatalIf(t, !ok, "%v", "final repository is absent")
	}
	finalChart := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: final.Namespace,
		Name:      "apps-consumer-0",
	}
	if first.id != finalChart || first.url != final.URL {
		t.Errorf(
			"captured chart acquisition=%s URL=%s, want %s URL=%s",
			first.id,
			first.url,
			finalChart,
			final.URL,
		)
	}
	return finalChart
}

func assertSharedRepositoryInitialURL(t *testing.T, got string, original *manifest.HelmRepository) {
	t.Helper()
	if got != original.URL {
		t.Errorf("initial fetch URL=%q, want %q", got, original.URL)
	}
}

type sharedRepositoryAcquisition struct {
	reads *store.ReadSet
	url   string
	id    manifest.NamedResource
}

func sharedRepositoryBacking(
	t *testing.T,
	chart *manifest.HelmChartSource,
	reads *store.ReadSet,
) (*manifest.HelmRepository, bool) {
	t.Helper()
	r, ok := reads.Get[*manifest.HelmRepository](
		manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindHelmRepository,
			Namespace: chart.Namespace,
			Name:      chart.SourceRef.Name,
		},
	)
	if !ok {
		t.Error("fetch did not capture its backing repository")
		return nil, false
	}
	return r, true
}
