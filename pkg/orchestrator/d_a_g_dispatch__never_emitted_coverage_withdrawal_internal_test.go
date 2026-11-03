package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/resourceset"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestDAGDispatch_NeverEmittedCoverageWithdrawal(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			t.Parallel()

			s := store.New()
			tasks := task.NewBounded(workers)
			p, k := testutil.KustomizationID(
				"flux-system",
				"publisher",
			), testutil.KustomizationID(
				"flux-system",
				"never-emitted",
			)
			l := &manifest.ResourceSet{
				Name:      "static",
				Namespace: "apps",
				Inputs:    []fluxopv1.ResourceSetInput{{}},
				ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: withdrawn-output}
data: {value: rendered}
`,
			}
			provider := &manifest.ResourceSetInputProvider{
				Name:      "dynamic",
				Namespace: l.Namespace,
				Type:      fluxopv1.InputProviderGitHubBranch,
			}
			l.InputsFrom = []fluxopv1.InputProviderReference{{Name: provider.Name}}
			s.AddObjects(
				[]manifest.BaseManifest{&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace}, l, provider},
			)
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				l.Named(): {Productions: []manifest.Production{{Owner: k, Source: manifest.BootstrapSourceID}}},
			}})
			s.RecordEmitted(
				p,
				&store.Emission{
					Children: []store.Child{{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}},
					Complete: true,
				},
				nil,
			)
			s.SetArtifact(l.Named(), &store.SourceArtifact{Key: "static-l"})
			s.UpdateStatus(l.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			{
				assert.FatalIf(
					t,
					s.GetObject(l.Named()) != l || s.GetArtifact(l.Named(), store.ArtifactContent) == nil,
					"%v",
					"fixture L is not live with an artifact",
				)
			}
			{
				{
					info, ok := s.GetStatus(l.Named())
					assert.FatalIf(t, !ok || info.Status != store.StatusReady, "%v", "fixture L is not Ready")
				}
			}
			{
				assert.FatalIf(
					t,
					s.GetArtifact(k, store.ArtifactContent) != nil,
					"%v",
					"fixture K unexpectedly emitted",
				)
			}
			var publications atomic.Int64
			c := resourceset.New(s, tasks, nil, true)
			c.Configure(base.Options{})
			c.StartLifecycle()
			d := dagDispatcher{&Orchestrator{store: s, rsc: c, controllers: base.Controllers{ResourceSet: c}}}
			unsub := s.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
				if id == l.Named() {
					publications.Add(1)
				}
			}, false)
			t.Cleanup(unsub)
			withdrawn := make(chan struct{})
			tasks.Go(t.Context(), "withdrawal", func(context.Context) {
				s.RecordEmitted(p, &store.Emission{Complete: true}, nil)
				close(withdrawn)
			})
			tasks.Go(t.Context(), "dispatch-withdrawn", func(ctx context.Context) {
				tasks.YieldSlot(func() { <-withdrawn })
				result := d.Dispatch(ctx, l.Named(), manifest.DrainNone)
				defer result.Assessment.Close()
				{
					assert.ErrorIf(
						t,
						result.Assessment.Outcome() == schedule.Blocked,
						"withdrawn dispatch blocked: %+v",
						result.Assessment,
					)
				}
			})
			tasks.BlockTillDone()
			output := manifest.NamedResource{
				Kind:      manifest.KindConfigMap,
				Namespace: l.Namespace,
				Name:      "withdrawn-output",
			}
			if tasks.Failures() != 0 || publications.Load() != 0 || s.GetObject(l.Named()) != nil ||
				s.GetArtifact(l.Named(), store.ArtifactContent) != nil ||
				s.GetObject(output) != nil {
				t.Fatalf("withdrawn body/artifact leaked: publications=%d", publications.Load())
			}
			s.AddObject(l)
			tasks.Go(t.Context(), "dispatch-restored", func(ctx context.Context) {
				d.Dispatch(ctx, l.Named(), manifest.DrainNone)
			})
			tasks.BlockTillDone()
			{
				assert.FatalIf(
					t,
					tasks.Failures() != 0 || publications.Load() != 1 || s.GetObject(output) == nil,
					"%v",
					"restored control did not execute the real render body",
				)
			}
		})
	}
}

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
			releases := make([]manifest.NamedResource, 0, 3)
			permits := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
			releaseConsumers := make([]func(), 0, 3)
			for i := range 3 {
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
				nextDoc := manifest.DeepCopyMap(doc)
				nextDoc["metadata"].(map[string]any)["labels"] = map[string]any{
					"kustomize.toolkit.fluxcd.io/name": p.Name,
				}
				if (*testCaseInput.scenario) == "effective_input_change" {
					nextDoc["spec"].(map[string]any)["url"] = "https://changed.example"
				}
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
			type acquisition struct {
				reads *store.ReadSet
				url   string
				id    manifest.NamedResource
			}
			entered := make(chan acquisition, 1)
			var firstFetch atomic.Bool
			f.observe = func(ctx context.Context, obj manifest.BaseManifest, reads *store.ReadSet) {
				chart := obj.(*manifest.HelmChartSource)
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
					return
				}
				if firstFetch.CompareAndSwap(false, true) {
					entered <- acquisition{reads: reads, url: r.URL, id: chart.Named()}
					select {
					case <-fetchPermit:
					case <-ctx.Done():
					}
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
							select {
							case <-publishPermit:
							case <-ctx.Done():
							}
						})
						if ctx.Err() != nil {
							return schedule.Result{}
						}
						if (*testCaseInput.scenario) == "withdrawal_reanchor" {
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
						} else {
							publish()
						}
						return schedule.Result{}
					}
					for i, hr := range releases {
						if id == hr {
							o.tasks.YieldSlot(func() {
								select {
								case <-permits[i]:
								case <-ctx.Done():
								}
							})
						}
					}
					hidden := id == repo && s.GetDeclaration(id) == nil
					result := d.Dispatch(ctx, id, level)
					if id == repo && (*testCaseInput.scenario) == "effective_input_change" {
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
								select {
								case <-assessmentPermit:
								case <-ctx.Done():
								}
							})
						})
					}
					if hidden && (*testCaseInput.scenario) == "withdrawal_reanchor" {
						o.tasks.YieldSlot(func() {
							select {
							case <-withdrawalObserved:
							case <-ctx.Done():
							}
						})
						if ctx.Err() != nil {
							return result
						}
						publish()
					}
					if id.Kind == manifest.KindHelmChart && result.Assessment.Valid() &&
						result.Assessment.Outcome() == schedule.Terminal {
						signalAccepted()
					}
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
				for _, release := range releaseConsumers {
					release()
				}
				if !joined {
					<-done
				}
				o.tasks.BlockTillDone()
			}()
			await := func(signal <-chan struct{}) {
				t.Helper()
				select {
				case <-signal:
				case localErr := <-done:
					joined = true
					t.Fatalf("scheduler stopped before barrier: %v", localErr)
				}
			}
			if (*testCaseInput.scenario) == "effective_input_change" {
				var initial *base.Execution
				select {
				case initial = <-captured:
				case innerLocalErr := <-done:
					joined = true
					t.Fatalf("scheduler stopped before capturing the original URL: %v", innerLocalErr)
				}
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
			var first acquisition
			select {
			case first = <-entered:
			case innerInnerLocalErr := <-done:
				joined = true
				t.Fatalf("scheduler stopped before fetch entry: %v", innerInnerLocalErr)
			}
			synctest.Wait()
			{
				assert.FatalIf(t, !first.reads.Valid(), "%v", "initial acquisition was already superseded")
			}
			if arrival == "before_return" {
				releaseConsumers[1]()
				synctest.Wait()
				releaseConsumers[2]()
				synctest.Wait()
				{
					assert.ErrorIf(
						t,
						!first.reads.Valid(),
						"%v",
						"another release invalidated an independent generated chart acquisition",
					)
				}
			}
			if (*testCaseInput.scenario) != "effective_input_change" {
				if first.url != original.URL {
					t.Errorf("initial fetch URL=%q, want %q", first.url, original.URL)
				}
				releasePublisher()
				await(published)
				synctest.Wait()
			}
			releaseFetch()
			await(accepted)
			synctest.Wait()
			if arrival == "after_acceptance" {
				releaseConsumers[1]()
				synctest.Wait()
				releaseConsumers[2]()
			}
			err = <-done
			joined = true
			o.tasks.BlockTillDone()
			{
				assert.FatalIf(t, err != nil, "%v", err)
			}
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
			completions := map[manifest.NamedResource]int{}
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
			t.Logf("acquisitions=%v completions=%v", f.calls, completions)
			{
				assert.ErrorIf(
					t,
					f.calls[repo] != 0 || completions[repo] != 0,
					"%v",
					"existence-only repository fetched",
				)
			}
			for id, calls := range f.calls {
				if completions[id] > 1 || completions[id] > calls || calls > 2 {
					t.Errorf("%s calls=%d completions=%d", id, calls, completions[id])
				}
			}
			if completions[finalChart] != 1 {
				t.Errorf("final chart completions=%d, want 1", completions[finalChart])
			}
			for _, id := range releases {
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
				outputs[run] = append(outputs[run], testutil.DocumentMaps(output.Manifests))
				output.Finish()
			}
		})
	}
	{
		{
			diff := cmp.Diff(outputs[0], outputs[1])
			assert.ErrorIf(t, diff != "", "controlled output differs (-want +got): %s", diff)
		}
	}
}
