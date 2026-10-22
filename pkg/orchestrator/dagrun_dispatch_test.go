package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/resourceset"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDAGDispatch_ResourceSetDedup(t *testing.T) {
	t.Parallel()
	s := store.New()
	tasks := task.NewBounded(2)
	c := resourceset.New(s, tasks, true)
	c.Configure(base.Options{})
	c.StartLifecycle()
	t.Cleanup(c.Close)
	rs := &manifest.ResourceSet{Name: "dedup", Namespace: "ns", ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {key: value}
`}
	s.AddObject(rs)
	var arrivals atomic.Int64
	unsub := s.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) { arrivals.Add(1) }, false)
	t.Cleanup(unsub)
	d := dagDispatcher{&Orchestrator{store: s, rsc: c}}
	first := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	defer first.Assessment.Close()
	if first.Assessment.Outcome() == schedule.Blocked || !first.Assessment.Valid() || arrivals.Load() != 1 {
		t.Fatalf("fresh assessment=%+v arrivals=%d", first.Assessment, arrivals.Load())
	}
	before := s.GetArtifact(rs.Named()).(*store.Emission)
	var reader store.ReadSet
	reader.Init(s, testutil.KustomizationID("ns", "reader"), nil)
	defer reader.Retire()
	reader.GetArtifact(rs.Named())
	var statusEvents, artifactEvents atomic.Int64
	unsubStatus := s.AddListener(store.EventStatusUpdated, func(manifest.NamedResource, any) { statusEvents.Add(1) }, false)
	defer unsubStatus()
	unsubArtifact := s.AddListener(store.EventArtifactUpdated, func(manifest.NamedResource, any) { artifactEvents.Add(1) }, false)
	defer unsubArtifact()
	second := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	defer second.Assessment.Close()
	after := s.GetArtifact(rs.Named()).(*store.Emission)
	if second.Assessment.Outcome() == schedule.Blocked || !second.Assessment.Valid() || !reader.Valid() || arrivals.Load() != 1 || statusEvents.Load() != 0 || artifactEvents.Load() != 0 || before.Fingerprint != after.Fingerprint || !cmp.Equal(before.Manifests, after.Manifests) {
		t.Fatalf("dedup assessment=%+v reader valid=%t object/status/artifact events=%d/%d/%d", second.Assessment, reader.Valid(), arrivals.Load()-1, statusEvents.Load(), artifactEvents.Load())
	}
	s.UpdateStatus(rs.Named(), store.StatusReady, store.ReasonSkipped, "")
	skipped := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	defer skipped.Assessment.Close()
	info, _ := s.GetStatus(rs.Named())
	after = s.GetArtifact(rs.Named()).(*store.Emission)
	if skipped.Assessment.Outcome() == schedule.Blocked || info.Outcome() != store.OutcomeSkipped || info.Message != "" || !reader.Valid() || arrivals.Load() != 1 || before.Fingerprint != after.Fingerprint || !cmp.Equal(before.Manifests, after.Manifests) {
		t.Fatalf("typed skip lost its outcome: status=%+v arrivals=%d", info, arrivals.Load())
	}
}

func TestDAGDispatch_ResourceSetClassification(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"dedup", "fresh fingerprint identical docs", "missing artifact", "empty fingerprint", "blocked gate", "failed gate", "preflight failure", "filter skip", "vanished", "render failure", "suspended", "refresh suspended"} {
		for _, selector := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/selector_%v", name, selector), func(t *testing.T) {
				t.Parallel()
				s := store.New()
				tasks := task.NewBounded(2)
				c := resourceset.New(s, tasks, true)
				c.Configure(base.Options{})
				c.StartLifecycle()
				rs := &manifest.ResourceSet{Name: "matrix", Namespace: "ns", Inputs: []fluxopv1.ResourceSetInput{{}}, ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {key: fixed}
`}
				if selector {
					rs.InputsFrom = []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSetInputProvider, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}}}}
				}
				s.AddObject(rs)
				d := dagDispatcher{&Orchestrator{store: s, rsc: c}}
				initial := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
				defer initial.Assessment.Close()
				if initial.Assessment.Outcome() == schedule.Blocked || !initial.Assessment.Valid() {
					t.Fatalf("fresh setup failed: %+v", initial.Assessment)
				}
				before := s.GetArtifact(rs.Named()).(*store.Emission)
				c.Close()
				opts := base.Options{}
				wantUnresolved := false
				fresh := *rs
				switch name {
				case "fresh fingerprint identical docs":
					fresh.Inputs = []fluxopv1.ResourceSetInput{{"unused": &apiextensionsv1.JSON{Raw: []byte(`"new"`)}}}
					s.AddObject(&fresh)
				case "missing artifact":
					s.DeleteObject(rs.Named())
					s.AddObject(rs)
				case "empty fingerprint":
					s.SetArtifact(rs.Named(), &store.Emission{Manifests: before.Manifests})
				case "blocked gate", "failed gate":
					fresh.DependsOn = []fluxopv1.Dependency{{APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: manifest.KindResourceSet, Name: "gate", Namespace: "ns"}}
					for _, dep := range fresh.DependsOn {
						manifest.KindOf(&manifest.RawObject{APIVersion: dep.APIVersion, Kind: dep.Kind}).Namespaced()
					}
					s.AddObject(&fresh)
					wantUnresolved = true
					if name == "failed gate" {
						gate := &manifest.ResourceSet{Name: "gate", Namespace: "ns"}
						s.AddObject(gate)
						s.UpdateStatus(gate.Named(), store.StatusFailed, "", "gate failed")
						wantUnresolved = false
					}
				case "preflight failure":
					opts.PreflightFailure = func(manifest.NamedResource) (string, bool) { return "cycle", true }
				case "filter skip":
					opts.Filter = change.NewFilter(change.NewSet(nil), indexForTest(nil), s)
				case "vanished":
					s.DeleteObject(rs.Named())
					s.SetArtifact(rs.Named(), before)
				case "suspended":
					fresh.Suspend = true
					s.AddObject(&fresh)
				case "refresh suspended":
					gate := &manifest.ResourceSet{Name: "gate", Namespace: "ns", Suspend: true}
					s.AddObject(gate)
					fresh.DependsOn = []fluxopv1.Dependency{{APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: manifest.KindResourceSet, Name: "gate", Namespace: "ns"}}
					for _, dep := range fresh.DependsOn {
						manifest.KindOf(&manifest.RawObject{APIVersion: dep.APIVersion, Kind: dep.Kind}).Namespaced()
					}
					s.AddObject(&fresh)
					s.UpdateStatus(gate.Named(), store.StatusReady, store.ReasonSuspended, "suspended")
					s.UpdateStatus(rs.Named(), store.StatusPending, "", "queued")
					var published atomic.Bool
					unsub := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
						info, ok := payload.(store.StatusInfo)
						if id == rs.Named() && ok && info.Message == "resolving inputs" && !published.Swap(true) {
							suspended := fresh
							suspended.Suspend = true
							s.AddObject(&suspended)
						}
					}, false)
					t.Cleanup(unsub)
				case "render failure":
					fresh.ResourcesTemplate = "<< .missing.field >>"
					s.AddObject(&fresh)
				}
				c = resourceset.New(s, tasks, true)
				c.Configure(opts)
				c.StartLifecycle()
				t.Cleanup(c.Close)
				d.o.rsc = c
				var arrivals, outputArrivals, outputWithdrawals atomic.Int64
				unsub := s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
					arrivals.Add(1)
					if id == (manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: rs.Namespace, Name: "output"}) {
						if payload == nil {
							outputWithdrawals.Add(1)
						} else {
							outputArrivals.Add(1)
						}
					}
				}, false)
				t.Cleanup(unsub)
				var result schedule.Result
				if name == "refresh suspended" {
					scheduler := schedule.New(tasks, fixtureDispatcher{store: s, dispatch: d.Dispatch})
					scheduler.Seed([]schedule.NodeID{rs.Named()})
					if err := scheduler.Run(t.Context()); err != nil {
						t.Fatal(err)
					}
					result = scheduler.Results()[rs.Named()]
				} else {
					result = d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
					defer result.Assessment.Close()
				}
				if result.Assessment == nil || result.Assessment.Outcome() == schedule.Blocked != wantUnresolved {
					t.Fatalf("assessment=%+v, want unresolved=%t", result.Assessment, wantUnresolved)
				}
				if name == "dedup" && arrivals.Load() != 0 {
					t.Fatalf("no-op emitted %d arrivals", arrivals.Load())
				}
				if name == "fresh fingerprint identical docs" {
					after := s.GetArtifact(rs.Named()).(*store.Emission)
					if before.Fingerprint == after.Fingerprint {
						t.Fatal("fresh fingerprint must replace artifact identity")
					}
					if diff := cmp.Diff(before.Manifests, after.Manifests); diff != "" {
						t.Fatalf("fixture documents must stay identical (-want +got): %s", diff)
					}
				}
				if name == "filter skip" {
					info, ok := s.GetStatus(rs.Named())
					_, retained := s.GetEmission(rs.Named())
					if !ok || info.Outcome() != store.OutcomeUnchanged || info.Message != "unchanged" || len(retained.Children) != 0 || len(retained.Manifests) != 0 {
						t.Fatalf("filter retained invalid output: %+v", info)
					}
				}
				if name == "suspended" || name == "refresh suspended" {
					info, _ := s.GetStatus(rs.Named())
					_, retained := s.GetEmission(rs.Named())
					output := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: rs.Namespace, Name: "output"}
					if info.Outcome() != store.OutcomeSuspended || len(retained.Manifests) != 0 || len(retained.Children) != 0 || s.GetObject(output) != nil || outputArrivals.Load() != 0 || outputWithdrawals.Load() != 1 {
						t.Fatalf("suspension retained or replayed stale output: status=%+v arrivals=%d withdrawals=%d", info, outputArrivals.Load(), outputWithdrawals.Load())
					}
				}
			})
		}
	}
}

func TestDAGDispatch_ConcurrentResourceSetUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s := store.New()
	tasks := task.NewBounded(2)
	c := resourceset.New(s, tasks, true)
	c.Configure(base.Options{})
	c.StartLifecycle()
	t.Cleanup(c.Close)
	rs := &manifest.ResourceSet{Name: "concurrent", Namespace: "ns", Inputs: []fluxopv1.ResourceSetInput{{}}, ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {key: initial}
`}
	s.AddObject(rs)
	d := dagDispatcher{&Orchestrator{store: s, rsc: c}}
	d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	s.UpdateStatus(rs.Named(), store.StatusPending, "", "queued")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unsub := s.AddListener(store.EventStatusUpdated, func(nid manifest.NamedResource, _ any) {
		if nid == rs.Named() {
			once.Do(func() {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
				}
			})
		}
	}, false)
	defer unsub()
	fresh := *rs
	fresh.ResourcesTemplate = strings.ReplaceAll(rs.ResourcesTemplate, "initial", "changed")
	s.AddObject(&fresh)
	var dispatches atomic.Int64
	dispatch := fixtureDispatcher{store: s, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
		result := d.Dispatch(ctx, id, level)
		if dispatches.Add(1) == 1 && result.Assessment.Valid() {
			t.Error("superseded declaration retained a valid assessment")
		}
		return result
	}}
	scheduler := schedule.New(tasks, dispatch)
	scheduler.Seed([]schedule.NodeID{rs.Named()})
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	newest := fresh
	newest.ResourcesTemplate = strings.ReplaceAll(fresh.ResourcesTemplate, "changed", "newest")
	newest.InputsFrom = []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSetInputProvider, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}}}}
	tasks.Go(ctx, "update", func(context.Context) {
		s.AddObject(&newest)
		close(release)
	})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if dispatches.Load() != 2 {
		t.Fatalf("captured attempts=%d, want 2", dispatches.Load())
	}
	before := s.GetArtifact(rs.Named()).(*store.Emission)
	var reader store.ReadSet
	reader.Init(s, testutil.KustomizationID("ns", "reader"), nil)
	defer reader.Retire()
	reader.GetArtifact(rs.Named())
	settled := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	defer settled.Assessment.Close()
	after := s.GetArtifact(rs.Named()).(*store.Emission)
	if settled.Assessment.Outcome() == schedule.Blocked || !settled.Assessment.Valid() || !reader.Valid() || before.Fingerprint != after.Fingerprint || !cmp.Equal(before.Manifests, after.Manifests) {
		t.Fatalf("settled newest content did not deduplicate: %+v", settled.Assessment)
	}
	cm, ok := s.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "output"})
	if !ok || cm.Data["key"] != "newest" {
		t.Fatalf("latest concurrent content was lost: %+v", cm)
	}
}

func TestDAGDispatch_WithdrawalNotificationDrain(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, lateFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers=%d/lateFirst=%t", workers, lateFirst), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					s := store.New()
					tasks := task.NewBounded(workers)
					p, k, consumer := testutil.KustomizationID("flux-system", "publisher"), testutil.KustomizationID("flux-system", "child"), testutil.KustomizationID("flux-system", "consumer")
					s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
					child := &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}
					s.RecordEmitted(p, &store.Emission{Children: []store.Child{{Obj: child}}, Complete: true})
					_, late := s.GetEmission(k)
					doc := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "late", "namespace": "apps"}, "data": map[string]any{"value": "private"}}
					late.Children, late.Manifests = base.ParseChildren(k, []map[string]any{doc}, manifest.ParseDocOptions{})
					late.Complete = true
					publishRelease, entered, deliveryRelease := make(chan struct{}), make(chan struct{}), make(chan struct{})
					consumerStarted, woken := make(chan struct{}), make(chan struct{})
					var calls atomic.Int64
					publishLate := func() {
						s.RecordEmitted(k, &late)
					}
					s.AddObject(&manifest.Kustomization{Name: consumer.Name, Namespace: consumer.Namespace})
					releasePublication := sync.OnceFunc(func() { close(publishRelease) })
					releaseDelivery := sync.OnceFunc(func() { close(deliveryRelease) })
					defer releasePublication()
					defer releaseDelivery()
					dispatch := fixtureDispatcher{store: s, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
						if id == consumer {
							execution := controllertest.NewExecution(base.New(s, tasks, "fixture"), id)
							execution.Dispatch(ctx, level, func(ctx context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
								if calls.Add(1) == 1 {
									close(consumerStarted)
									return execution.Require(ctx, []manifest.DependencyRef{{NamedResource: k}}, level)
								}
								if execution.Reads.GetObject(k) != nil {
									t.Error("withdrawal wake observed child still live")
								}
								close(woken)
								return nil
							})
							return schedule.Result{Assessment: execution}
						}
						tasks.YieldSlot(func() { <-publishRelease })
						if lateFirst {
							publishLate()
						}
						s.RecordEmitted(p, &store.Emission{Complete: true})
						if !lateFirst {
							publishLate()
						}
						return schedule.Result{}
					}}
					scheduler := schedule.New(tasks, dispatch)
					o := &Orchestrator{store: s}
					unsubscribe := o.watchDAG(scheduler)
					defer unsubscribe()
					unsubBarrier := s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
						if id == k && payload == nil {
							close(entered)
							tasks.YieldSlot(func() { <-deliveryRelease })
						}
					}, false)
					defer unsubBarrier()
					scheduler.Seed([]schedule.NodeID{p, consumer})
					done := make(chan error, 1)
					go func() { done <- scheduler.Run(t.Context()) }()
					<-consumerStarted
					synctest.Wait()
					releasePublication()
					<-entered
					synctest.Wait()
					select {
					case err := <-done:
						t.Fatalf("drain sealed while notification was outstanding: %v", err)
					default:
					}
					if calls.Load() > 2 {
						t.Fatal("outstanding publication queued duplicate successors")
					}
					releaseDelivery()
					if err := <-done; err != nil {
						t.Fatal(err)
					}
					select {
					case <-woken:
					default:
						t.Fatal("withdrawal did not wake the captured consumer")
					}
					if calls.Load() != 2 || tasks.Failures() != 0 || s.GetObject(k) != nil || s.GetObject(late.Children[0].Obj.Named()) != nil {
						t.Fatal("withdrawal lost wake or late publication manufactured support")
					}
				})
			})
		}
	}
}

func TestDAGDispatch_NeverEmittedCoverageWithdrawal(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			s := store.New()
			tasks := task.NewBounded(workers)
			p, k := testutil.KustomizationID("flux-system", "publisher"), testutil.KustomizationID("flux-system", "never-emitted")
			l := &manifest.ResourceSet{Name: "static", Namespace: "apps", Inputs: []fluxopv1.ResourceSetInput{{}}, ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: withdrawn-output}
data: {value: rendered}
`}
			provider := &manifest.ResourceSetInputProvider{Name: "dynamic", Namespace: l.Namespace, Type: fluxopv1.InputProviderGitHubBranch}
			l.InputsFrom = []fluxopv1.InputProviderReference{{Name: provider.Name}}
			s.AddObjects([]manifest.BaseManifest{&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace}, l, provider})
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				l.Named(): {Productions: []manifest.Production{{Owner: k, Source: manifest.BootstrapSourceID}}},
			}})
			s.RecordEmitted(p, &store.Emission{Children: []store.Child{{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}}, Complete: true})
			s.SetArtifact(l.Named(), &store.SourceArtifact{Key: "static-l"})
			s.UpdateStatus(l.Named(), store.StatusReady, "", "")
			if s.GetObject(l.Named()) != l || s.GetArtifact(l.Named()) == nil {
				t.Fatal("fixture L is not live with an artifact")
			}
			if info, ok := s.GetStatus(l.Named()); !ok || info.Status != store.StatusReady {
				t.Fatal("fixture L is not Ready")
			}
			if s.GetArtifact(k) != nil {
				t.Fatal("fixture K unexpectedly emitted")
			}
			var publications atomic.Int64
			c := resourceset.New(s, tasks, true)
			c.Configure(base.Options{})
			c.StartLifecycle()
			t.Cleanup(c.Close)
			d := dagDispatcher{&Orchestrator{store: s, rsc: c}}
			unsub := s.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
				if id == l.Named() {
					publications.Add(1)
				}
			}, false)
			t.Cleanup(unsub)
			withdrawn := make(chan struct{})
			tasks.Go(t.Context(), "withdrawal", func(context.Context) {
				s.RecordEmitted(p, &store.Emission{Complete: true})
				close(withdrawn)
			})
			tasks.Go(t.Context(), "dispatch-withdrawn", func(ctx context.Context) {
				tasks.YieldSlot(func() { <-withdrawn })
				result := d.Dispatch(ctx, l.Named(), manifest.DrainNone)
				defer result.Assessment.Close()
				if result.Assessment.Outcome() == schedule.Blocked {
					t.Errorf("withdrawn dispatch blocked: %+v", result.Assessment)
				}
			})
			tasks.BlockTillDone()
			output := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: l.Namespace, Name: "withdrawn-output"}
			if tasks.Failures() != 0 || publications.Load() != 0 || s.GetObject(l.Named()) != nil || s.GetArtifact(l.Named()) != nil || s.GetObject(output) != nil {
				t.Fatalf("withdrawn body/artifact leaked: publications=%d", publications.Load())
			}
			s.AddObject(l)
			tasks.Go(t.Context(), "dispatch-restored", func(ctx context.Context) {
				d.Dispatch(ctx, l.Named(), manifest.DrainNone)
			})
			tasks.BlockTillDone()
			if tasks.Failures() != 0 || publications.Load() != 1 || s.GetObject(output) == nil {
				t.Fatal("restored control did not execute the real render body")
			}
		})
	}
}

type sharedRepositoryFetcher struct {
	mu    sync.Mutex
	calls map[manifest.NamedResource]int
}

func (f *sharedRepositoryFetcher) Fetch(_ context.Context, obj manifest.BaseManifest, _ *store.ReadSet, _ sourcecache.Policy) (*store.SourceArtifact, error) {
	f.mu.Lock()
	f.calls[obj.Named()]++
	f.mu.Unlock()
	return &store.SourceArtifact{Kind: manifest.KindHelmChart, Key: "chart", FS: fstest.MapFS{
		"Chart.yaml":               {Data: []byte("apiVersion: v2\nname: demo\nversion: 1.0.0\n")},
		"templates/configmap.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-output\n")},
	}}, nil
}

func TestDAGDispatch_SharedHelmRepositoryAcquisition(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, scenario := range []string{"unchanged", "effective_input_change", "withdrawal_reanchor"} {
			t.Run(fmt.Sprintf("workers=%d/%s", workers, scenario), func(t *testing.T) {
				var logs bytes.Buffer
				previous := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
				defer slog.SetDefault(previous)
				dir := t.TempDir()
				o, err := New(t.Context(), Config{Path: dir, RepoRoot: dir, Concurrency: workers})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := o.Stop(); err != nil {
						t.Error(err)
					}
				})
				f := &sharedRepositoryFetcher{calls: map[manifest.NamedResource]int{}}
				o.WithFetcher(manifest.KindHelmRepository, f).WithFetcher(manifest.KindHelmChart, f)
				s := o.store
				p := testutil.KustomizationID("flux-system", "publisher")
				s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
				doc := map[string]any{
					"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindHelmRepository,
					"metadata": map[string]any{"name": "shared", "namespace": "apps"},
					"spec":     map[string]any{"url": "https://charts.example", "interval": "1h"},
				}
				children, _ := base.ParseChildren(p, []map[string]any{doc}, manifest.ParseDocOptions{})
				if len(children) != 1 {
					t.Fatal("repository fixture was not parsed")
				}
				repo := children[0].Obj.Named()
				s.AddObject(children[0].Obj)
				o.index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					repo: {Productions: []manifest.Production{{Owner: p, Source: manifest.BootstrapSourceID}}},
					p:    {Productions: []manifest.Production{{Source: manifest.BootstrapSourceID}}},
				}}
				var releases []manifest.NamedResource
				for i := range 3 {
					hr := &manifest.HelmRelease{Name: fmt.Sprintf("consumer-%d", i), Namespace: "apps", Chart: manifest.HelmChart{Name: "demo", Version: "1.0.0", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "apps", Name: repo.Name}}}
					s.AddObject(hr)
					releases = append(releases, hr.Named())
				}
				o.configureControllers()
				o.src.StartLifecycle()
				o.ksc.StartLifecycle()
				o.hrc.Start(t.Context())
				acquired, published := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unsub := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
					if info, ok := payload.(store.StatusInfo); id == repo && ok && info.Status == store.StatusReady {
						once.Do(func() { close(acquired) })
					}
				}, false)
				defer unsub()
				publish := sync.OnceFunc(func() {
					nextDoc := manifest.DeepCopyMap(doc)
					nextDoc["metadata"].(map[string]any)["labels"] = map[string]any{"kustomize.toolkit.fluxcd.io/name": p.Name}
					if scenario == "effective_input_change" {
						nextDoc["spec"].(map[string]any)["url"] = "https://changed.example"
					}
					_, next := s.GetEmission(p)
					next.Children, next.Manifests = base.ParseChildren(p, []map[string]any{nextDoc}, manifest.ParseDocOptions{})
					next.Complete = true
					t.Logf("parent %s emission: effective input equal=%t, seed document absent, emitted document present; source=%s support=%v", p, cmp.Equal(children[0].Obj, next.Children[0].Obj), manifest.BootstrapSourceID, s.Productions(repo, nil))
					o.ksc.Emit(p, &next)
					next.Finish()
					s.UpdateStatus(p, store.StatusReady, "", "")
					close(published)
				})
				d := dagDispatcher{o}
				dispatch := fixtureDispatcher{store: s, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
					if id != p {
						if id.Kind == manifest.KindHelmRelease && scenario == "effective_input_change" {
							o.tasks.YieldSlot(func() {
								select {
								case <-published:
								case <-ctx.Done():
								}
							})
						}
						hidden := id == repo && s.GetObject(id) == nil
						result := d.Dispatch(ctx, id, level)
						if hidden && scenario == "withdrawal_reanchor" {
							publish()
						}
						return result
					}
					o.tasks.YieldSlot(func() {
						select {
						case <-acquired:
						case <-ctx.Done():
						}
					})
					if scenario == "withdrawal_reanchor" {
						_, empty := s.GetEmission(p)
						empty.Complete = true
						o.ksc.Emit(p, &empty)
						if s.GetObject(repo) != nil {
							t.Error("withdrawal retained repository")
						}
						if _, ok := s.GetStatus(repo); ok {
							t.Error("withdrawal retained Ready")
						}
						return schedule.Result{}
					}
					publish()
					return schedule.Result{}
				}}
				sched := schedule.New(o.tasks, dispatch)
				unwatch := o.watchDAG(sched)
				defer unwatch()
				sched.Seed(append([]schedule.NodeID{repo, p}, releases...))
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				if err := sched.Run(ctx); err != nil {
					t.Fatal(err)
				}
				o.tasks.BlockTillDone()
				completions := map[manifest.NamedResource]int{}
				for line := range bytes.SplitSeq(logs.Bytes(), []byte("\n")) {
					if len(line) == 0 {
						continue
					}
					var event struct {
						Message string `json:"msg"`
						ID      string `json:"id"`
					}
					if err := json.Unmarshal(line, &event); err != nil {
						t.Fatal(err)
					}
					if event.Message == "fetch complete" {
						for id := range f.calls {
							if id.String() == event.ID {
								completions[id]++
							}
						}
					}
				}
				t.Logf("repository %s acquisitions=%d completions=%d; all acquisitions=%v completions=%v", repo, f.calls[repo], completions[repo], f.calls, completions)
				if f.calls[repo] != 0 || completions[repo] != 0 {
					t.Errorf("existence-only repository fetched: acquisitions=%d completions=%d", f.calls[repo], completions[repo])
				}
				for id, calls := range f.calls {
					if completions[id] > 1 || completions[id] > calls || calls > 2 {
						t.Errorf("%s calls=%d completions=%d", id, calls, completions[id])
					}
				}
				for _, id := range releases {
					if info, ok := s.GetStatus(id); !ok || info.Status != store.StatusReady || info.Message != "" {
						t.Errorf("consumer %s: %+v", id, info)
					}
					_, output := s.GetEmission(id)
					if len(output.Manifests) != 1 {
						t.Errorf("consumer %s emitted %d documents", id, len(output.Manifests))
					}
				}
			})
		}
	}
}

func TestDAGDispatch_DirtyTransitiveSubstitutionAfterDrain(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "consumer.yaml", ksYAML("consumer", "consumer", "")+`  postBuild:
    substituteFrom: [{kind: ConfigMap, name: bridge-settings, optional: true}]
`)
	testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources: [../shared/consumer.yaml]\n")
	testutil.WriteFile(t, root, "shared/consumer.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer, namespace: flux-system}\ndata: {value: '${value}', mode: dirty}\n")
	testutil.WriteFile(t, root, "bridge.yaml", ksYAML("bridge", "bridge", "")+`  postBuild:
    substituteFrom: [{kind: ConfigMap, name: hello, optional: true}]
`)
	testutil.WriteFile(t, root, "bridge/kustomization.yaml", "resources: [settings.yaml]\n")
	testutil.WriteFile(t, root, "bridge/settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: bridge-settings, namespace: flux-system}\ndata: {value: '${value}'}\n")
	testutil.WriteFile(t, root, "pin.yaml", ksYAML("pin", "pin", ""))
	testutil.WriteFile(t, root, "pin/kustomization.yaml", "resources: [hello.yaml]\n")
	testutil.WriteFile(t, root, "pin/hello.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: flux-system}\ndata: {value: provisional}\n")
	o, err := New(t.Context(), Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2, ExternalChanges: change.NewSet([]string{"shared/consumer.yaml"})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := o.Stop(); err != nil {
			t.Error(err)
		}
	})
	if err := o.Bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	o.configureControllers()
	o.src.StartLifecycle()
	o.ksc.StartLifecycle()
	o.hrc.Start(t.Context())
	o.rsc.StartLifecycle()
	consumer, pin := testutil.KustomizationID("flux-system", "consumer"), testutil.KustomizationID("flux-system", "pin")
	var blocked, settled atomic.Bool
	dispatch := fixtureDispatcher{store: o.store, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
		if id != pin {
			result := (dagDispatcher{o}).Dispatch(ctx, id, level)
			if id == consumer && result.Assessment.Outcome() == schedule.Blocked {
				blocked.Store(true)
			}
			return result
		}
		run := controllertest.NewExecution(o.ksc.Controller, id)
		run.Dispatch(ctx, level, func(_ context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
			if level == manifest.DrainNone {
				return &depwait.ErrBlocked{Deps: []manifest.NamedResource{consumer}}
			}
			if !blocked.Load() {
				t.Error("producer settled before the transitive consumer parked")
			}
			settled.Store(true)
			obj := &manifest.ConfigMap{Name: "hello", Namespace: "flux-system", Data: map[string]any{"value": "pinned"}}
			run.Emit(id, &store.Emission{Children: []store.Child{{Obj: obj}}, Manifests: []map[string]any{{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "hello", "namespace": "flux-system"}, "data": obj.Data}}, Complete: true})
			return nil
		})
		return schedule.Result{Assessment: run}
	}}
	scheduler := schedule.New(o.tasks, dispatch)
	unsubscribe := o.watchDAG(scheduler)
	defer unsubscribe()
	scheduler.Seed(o.seedNodes())
	if err := scheduler.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	output, ok := o.store.GetArtifact(consumer).(*store.Emission)
	if !blocked.Load() || !settled.Load() || !ok || len(output.Manifests) != 1 || output.Manifests[0]["data"].(map[string]any)["value"] != "pinned" {
		t.Fatalf("after-drain transitive output: blocked=%t settled=%t output=%+v", blocked.Load(), settled.Load(), output)
	}
}
