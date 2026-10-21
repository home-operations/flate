package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
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
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/resourceset"
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
	out, blocked, _ := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	if out != schedule.OutcomeTerminal || len(blocked) != 0 || arrivals.Load() != 1 {
		t.Fatalf("fresh result=%v blocked=%v arrivals=%d", out, blocked, arrivals.Load())
	}
	before := s.GetArtifact(rs.Named())
	out, blocked, _ = d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	if out != schedule.OutcomeTerminalNoop || len(blocked) != 0 || arrivals.Load() != 1 || s.GetArtifact(rs.Named()) != before {
		t.Fatalf("dedup result=%v blocked=%v arrivals=%d artifact changed=%v", out, blocked, arrivals.Load(), s.GetArtifact(rs.Named()) != before)
	}
	s.UpdateStatus(rs.Named(), store.StatusReady, store.ReasonSkipped, "")
	out, blocked, _ = d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	if out != schedule.OutcomeTerminal || len(blocked) != 0 || arrivals.Load() != 1 || s.GetArtifact(rs.Named()) != before {
		t.Fatalf("typed skip became a dedup no-op: outcome=%v blocked=%v arrivals=%d", out, blocked, arrivals.Load())
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
				if out, blocked, rerun := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone); rerun != selector || out != schedule.OutcomeTerminal || len(blocked) != 0 {
					t.Fatalf("fresh setup failed: %v %v", out, blocked)
				}
				before := s.GetArtifact(rs.Named()).(*store.Emission)
				c.Close()
				opts := base.Options{}
				want := schedule.OutcomeTerminal
				fresh := *rs
				switch name {
				case "dedup":
					want = schedule.OutcomeTerminalNoop
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
					want = schedule.OutcomeBlocked
					if name == "failed gate" {
						gate := &manifest.ResourceSet{Name: "gate", Namespace: "ns"}
						s.AddObject(gate)
						s.UpdateStatus(gate.Named(), store.StatusFailed, "", "gate failed")
						want = schedule.OutcomeTerminal
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
				var arrivals, outputArrivals atomic.Int64
				unsub := s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
					arrivals.Add(1)
					if id == (manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: rs.Namespace, Name: "output"}) {
						outputArrivals.Add(1)
					}
				}, false)
				t.Cleanup(unsub)
				out, blocked, rerun := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
				if rerun != (selector && name != "vanished" && name != "suspended" && name != "refresh suspended") || out != want || (len(blocked) > 0) != (want == schedule.OutcomeBlocked) {
					t.Fatalf("outcome=%v blocked=%v; want %v", out, blocked, want)
				}
				if want == schedule.OutcomeTerminalNoop && arrivals.Load() != 0 {
					t.Fatalf("no-op emitted %d arrivals", arrivals.Load())
				}
				if name == "fresh fingerprint identical docs" {
					after := s.GetArtifact(rs.Named()).(*store.Emission)
					if before == after || before.Fingerprint == after.Fingerprint {
						t.Fatal("fresh fingerprint must replace artifact identity")
					}
					if diff := cmp.Diff(before.Manifests, after.Manifests); diff != "" {
						t.Fatalf("fixture documents must stay identical (-want +got): %s", diff)
					}
				}
				if name == "filter skip" {
					info, ok := s.GetStatus(rs.Named())
					if !ok || info.Outcome() != store.OutcomeUnchanged || info.Message != "unchanged" || s.GetArtifact(rs.Named()) != before {
						t.Fatalf("filter must preserve artifact with Ready/unchanged: %+v", info)
					}
				}
				if name == "suspended" || name == "refresh suspended" {
					info, _ := s.GetStatus(rs.Named())
					_, retained := s.GetEmission(rs.Named())
					if info.Outcome() != store.OutcomeSuspended || s.GetArtifact(rs.Named()) != nil || !slices.EqualFunc(retained.Manifests, before.Manifests, func(a, b map[string]any) bool { return cmp.Equal(a, b) }) || outputArrivals.Load() != 0 {
						t.Fatalf("suspension replayed or pruned output: status=%+v arrivals=%d", info, outputArrivals.Load())
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
	done := make(chan schedule.Outcome, 1)
	tasks.Go(ctx, "dispatch", func(ctx context.Context) {
		out, _, rerun := d.Dispatch(ctx, rs.Named(), manifest.DrainNone)
		if !rerun {
			t.Error("post-reconcile intent ignored the updated selector")
		}
		done <- out
	})
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
	if out := <-done; out != schedule.OutcomeTerminal {
		t.Fatalf("concurrent fresh render classified as %v", out)
	}
	tasks.BlockTillDone()
	if out, _, _ := d.Dispatch(ctx, rs.Named(), manifest.DrainNone); out != schedule.OutcomeTerminal {
		t.Fatalf("newest content must render conservatively: %v", out)
	}
	if out, _, rerun := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone); !rerun || out != schedule.OutcomeTerminalNoop {
		t.Fatalf("settled newest content must dedup: %v", out)
	}
	cm, ok := s.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "output"})
	if !ok || cm.Data["key"] != "newest" {
		t.Fatalf("latest concurrent content was lost: %+v", cm)
	}
}

type withdrawalDispatcher func(context.Context, schedule.NodeID, manifest.DrainLevel) (schedule.Outcome, []schedule.NodeID, bool)

func (d withdrawalDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) (schedule.Outcome, []schedule.NodeID, bool) {
	return d(ctx, id, level)
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
					dispatch := withdrawalDispatcher(func(_ context.Context, id schedule.NodeID, _ manifest.DrainLevel) (schedule.Outcome, []schedule.NodeID, bool) {
						if id == consumer {
							if calls.Add(1) == 1 {
								close(consumerStarted)
								return schedule.OutcomeBlocked, []schedule.NodeID{k}, false
							}
							if s.GetObject(k) != nil {
								t.Error("withdrawal wake observed child still live")
							}
							close(woken)
							return schedule.OutcomeTerminal, nil, false
						}
						tasks.YieldSlot(func() { <-publishRelease })
						if lateFirst {
							publishLate()
						}
						s.RecordEmitted(p, &store.Emission{Complete: true})
						if !lateFirst {
							publishLate()
						}
						return schedule.OutcomeTerminal, nil, false
					})
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
					close(publishRelease)
					<-entered
					<-woken
					synctest.Wait()
					select {
					case err := <-done:
						t.Fatalf("drain sealed while notification was outstanding: %v", err)
					default:
					}
					close(deliveryRelease)
					if err := <-done; err != nil {
						t.Fatal(err)
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
			s.AddObjects([]manifest.BaseManifest{&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace}, l})
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
			var bodies, publications atomic.Int64
			c := resourceset.New(s, tasks, true)
			c.Configure(base.Options{PreflightFailure: func(id manifest.NamedResource) (string, bool) {
				if id == l.Named() {
					bodies.Add(1)
				}
				return "", false
			}})
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
				out, blocked, _ := d.Dispatch(ctx, l.Named(), manifest.DrainNone)
				if out != schedule.OutcomeTerminal || len(blocked) != 0 {
					t.Errorf("withdrawn dispatch = %v, blocked = %v", out, blocked)
				}
			})
			tasks.BlockTillDone()
			output := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: l.Namespace, Name: "withdrawn-output"}
			if tasks.Failures() != 0 || bodies.Load() != 0 || publications.Load() != 0 || s.GetObject(l.Named()) != nil || s.GetArtifact(l.Named()) != nil || s.GetObject(output) != nil {
				t.Fatalf("withdrawn body/artifact leaked: bodies=%d publications=%d", bodies.Load(), publications.Load())
			}
			s.AddObject(l)
			tasks.Go(t.Context(), "dispatch-restored", func(ctx context.Context) {
				d.Dispatch(ctx, l.Named(), manifest.DrainNone)
			})
			tasks.BlockTillDone()
			if tasks.Failures() != 0 || bodies.Load() == 0 || publications.Load() == 0 || s.GetObject(output) == nil {
				t.Fatal("restored control did not execute the real render body")
			}
		})
	}
}

type sharedRepositoryFetcher struct {
	mu    sync.Mutex
	calls map[manifest.NamedResource]int
}

func (f *sharedRepositoryFetcher) Fetch(_ context.Context, obj manifest.BaseManifest, _ sourcecache.Policy) (*store.SourceArtifact, error) {
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
					hr := &manifest.HelmRelease{Name: fmt.Sprintf("consumer-%d", i), Namespace: "apps", Chart: manifest.HelmChart{
						Name: "demo", Version: "1.0.0", RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindHelmRepository, RepoNamespace: "apps", RepoName: repo.Name,
					}}
					s.AddObject(hr)
					releases = append(releases, hr.Named())
				}
				o.configureControllers()
				o.src.StartLifecycle()
				o.ksc.StartLifecycle()
				o.hrc.Start(t.Context())
				acquired, withdrawn, published := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var withdrew sync.Once
				var once sync.Once
				unsub := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
					if info, ok := payload.(store.StatusInfo); id == repo && ok && info.Status == store.StatusReady {
						once.Do(func() { close(acquired) })
					}
				}, false)
				defer unsub()
				d := dagDispatcher{o}
				dispatch := withdrawalDispatcher(func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) (schedule.Outcome, []schedule.NodeID, bool) {
					if id != p {
						if id.Kind == manifest.KindHelmRelease && scenario != "unchanged" {
							o.tasks.YieldSlot(func() {
								select {
								case <-published:
								case <-ctx.Done():
								}
							})
						}
						hidden := id == repo && s.GetObject(id) == nil
						out, blocked, rerun := d.Dispatch(ctx, id, level)
						if hidden {
							withdrew.Do(func() { close(withdrawn) })
						}
						return out, blocked, rerun
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
						o.tasks.YieldSlot(func() {
							select {
							case <-withdrawn:
							case <-ctx.Done():
							}
						})
					}
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
					return schedule.OutcomeTerminal, nil, false
				})
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
					if completions[id] != calls {
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
