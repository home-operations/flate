package resourceset

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/schedule"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func BenchmarkReadRegistration_Collection(b *testing.B) {
	for _, count := range []int{1, 10_000} {
		for _, mode := range []string{"empty", "matching", "nonmatching", "membership"} {
			if count == 1 && mode == "nonmatching" {
				continue
			}
			for _, workers := range []int{2, 16, runtime.NumCPU() * 4} {
				b.Run(fmt.Sprintf("providers=%d/%s/workers=%d", count, mode, workers), func(b *testing.B) {
					s := store.New()
					tasks := task.NewBounded(workers)
					c := New(s, tasks, true)
					c.Configure(base.Options{})
					c.StartLifecycle()

					rs := selectorFixture("consumer", "blue")
					s.AddObject(rs)
					var pivot *manifest.ResourceSetInputProvider
					for i := range count {
						color := "blue"
						if mode == "empty" || mode == "nonmatching" && i == 0 {
							color = "red"
						}
						p := &manifest.ResourceSetInputProvider{Name: fmt.Sprintf("provider-%05d", i), Namespace: "ns", Labels: map[string]string{"color": color}}
						s.AddObject(p)
						if i == 0 {
							pivot = p
						}
					}
					versions := [2]*manifest.ResourceSetInputProvider{}
					for i := range versions {
						fresh := *pivot
						fresh.Labels = map[string]string{"color": pivot.Labels["color"], "revision": fmt.Sprint(i)}
						if mode == "membership" && i == 0 {
							fresh.Labels["color"] = "red"
						}
						versions[i] = &fresh
					}
					assessment := c.ReconcileNode(b.Context(), rs.Named(), manifest.DrainNone)
					defer func() { assessment.Close() }()
					var renders atomic.Int64
					unsub := s.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
						if id == rs.Named() {
							renders.Add(1)
						}
					}, false)
					defer unsub()
					generation := 0
					b.ReportAllocs()
					for b.Loop() {
						started, written := make(chan struct{}), make(chan struct{})
						start := sync.OnceFunc(func() { close(started) })
						for worker := range workers - 1 {
							tasks.Go(b.Context(), "collection", func(ctx context.Context) {
								if worker == 0 {
									defer start()
									start()
									tasks.YieldSlot(func() { <-written })
									next := c.ReconcileNode(ctx, rs.Named(), manifest.DrainNone)
									assessment.Retire()
									assessment = next
								} else {
									s.GetObject(pivot.Named())
								}
							})
						}
						tasks.Go(b.Context(), "metadata", func(context.Context) {
							defer close(written)
							tasks.YieldSlot(func() { <-started })
							s.AddObject(versions[generation%2])
						})
						tasks.BlockTillDone()
						for range s.Sweep() {
						}
						generation++
					}
					if tasks.Failures() != 0 {
						b.Fatal("worker panicked")
					}
					info, _ := s.GetStatus(rs.Named())
					if info.Status != store.StatusReady {
						b.Fatal("collection did not finish Ready")
					}
					b.ReportMetric(float64(renders.Load())/float64(b.N), "renders/op")
				})
			}
		}
	}
}

// Both snapshots and renders finish before either writer publishes. Equal
// rounds retain publication work but must not add another actual expansion.
func BenchmarkDirtyRerun_IndependentWriters(b *testing.B) {
	for _, order := range []string{"UV", "VU"} {
		for _, workers := range []int{2, 16, runtime.NumCPU() * 4} {
			b.Run(fmt.Sprintf("%s/workers=%d", order, workers), func(b *testing.B) {
				s := store.New()
				tasks := task.NewBounded(workers)
				var renders, dispatches atomic.Int64
				var assessments [2]*base.Execution
				defer func() {
					for _, execution := range assessments {
						if execution != nil {
							execution.Close()
						}
					}
				}()
				c := New(s, tasks, true)
				c.Configure(base.Options{})
				c.StartLifecycle()

				sets := [2]*manifest.ResourceSet{independentWriterFixture("u", "blue"), independentWriterFixture("v", "green")}
				for _, rs := range sets {
					s.AddObject(rs)
				}
				for i, color := range []string{"blue", "green"} {
					s.AddObject(&manifest.ResourceSetInputProvider{Name: fmt.Sprintf("selected-%d", i), Namespace: "ns", Labels: map[string]string{"color": color}})
				}
				commitOrder := []int{0, 1}
				if order == "VU" {
					commitOrder = []int{1, 0}
				}
				generation := 0
				var expected [2]struct {
					fingerprint string
					manifests   []map[string]any
				}
				b.ReportAllocs()
				for b.Loop() {
					for i := range sets {
						p, _ := s.Get[*manifest.ResourceSetInputProvider](manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "ns", Name: fmt.Sprintf("selected-%d", i)})
						fresh := *p
						fresh.Status.ExportedInputs = []fluxopv1.ResourceSetInput{{"revision": {Raw: []byte(fmt.Sprint(generation))}}}
						s.AddObject(&fresh)
					}
					var artifacts [2]*store.Emission
					var errs [2]error
					for i, rs := range sets {
						tasks.Go(b.Context(), "render", func(context.Context) {
							execution := c.newExecution(rs.Named())
							defer execution.run.Close()
							resolve := resourceset.StoreResolver(s)
							var fp string
							docs, _, err := resourceset.Render(rs, resolve, func(content any) (bool, error) {
								var err error
								fp, _, err = execution.run.FingerprintDedup(rs.Named(), content)
								return false, err
							})
							renders.Add(1)
							errs[i] = err
							artifacts[i] = &store.Emission{Manifests: docs, Fingerprint: fp}
						})
					}
					tasks.BlockTillDone()
					for _, i := range commitOrder {
						if errs[i] != nil {
							b.Fatal(errs[i])
						}
						id := sets[i].Named()
						base.Stamp(sets[i].EmissionPolicy(), artifacts[i].Manifests)
						_, next := s.GetEmission(id)
						next.Children, next.Manifests = c.ParseChildren(c.Store.GetObject(id), artifacts[i].Manifests, manifest.ParseDocOptions{WipeSecrets: true, DefaultNamespace: id.Namespace})
						next.Fingerprint, next.Complete = artifacts[i].Fingerprint, true
						c.Emit(id, &next)
						expected[i].fingerprint, expected[i].manifests = next.Fingerprint, next.Manifests
						next.Finish()
						s.UpdateStatus(sets[i].Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
					}
					for range 32 {
						for _, i := range commitOrder {
							docs := make([]map[string]any, len(artifacts[i].Manifests))
							for j, doc := range artifacts[i].Manifests {
								docs[j] = manifest.DeepCopyMap(doc)
							}
							publishResourceSetDocuments(b, c, sets[i].Named(), docs)
							tasks.Go(b.Context(), "equal", func(ctx context.Context) {
								dispatches.Add(1)
								next := c.ReconcileNode(ctx, sets[i].Named(), manifest.DrainNone)
								if previous := assessments[i]; previous != nil {
									previous.Retire()
								}
								assessments[i] = next
							})
						}
						tasks.BlockTillDone()
					}
					for range s.Sweep() {
					}
					generation++
				}
				if tasks.Failures() != 0 || renders.Load() != int64(2*b.N) || dispatches.Load() != int64(64*b.N) {
					b.Fatalf("panics=%d renders=%d dispatches=%d, want renders=%d dispatches=%d", tasks.Failures(), renders.Load(), dispatches.Load(), 2*b.N, 64*b.N)
				}
				for i, rs := range sets {
					info, _ := s.GetStatus(rs.Named())
					if info.Status != store.StatusReady {
						b.Fatal("writer did not finish Ready")
					}
					current, ok := s.GetArtifact(rs.Named()).(*store.Emission)
					if !ok || !current.Complete || len(current.Children) != 2 || current.Fingerprint != expected[i].fingerprint || !cmp.Equal(current.Manifests, expected[i].manifests) {
						b.Fatal("equal publications changed the complete two-document inventory")
					}
					for _, child := range current.Children {
						if s.GetObject(child.Obj.Named()) == nil {
							b.Fatalf("rendered identity %s is missing", child.Obj.Named())
						}
					}
				}
				b.ReportMetric(float64(renders.Load())/float64(b.N), "renders/op")
				b.ReportMetric(float64(dispatches.Load())/float64(b.N), "dispatches/op")
			})
		}
	}
}

func TestDirtyRerun_EqualOutputReuse(t *testing.T) {
	t.Parallel()
	s := store.New()
	tasks := task.NewBounded(2)
	c := New(s, tasks, true)
	c.Configure(base.Options{})
	c.StartLifecycle()

	rs := independentWriterFixture("u", "blue")
	s.AddObject(rs)
	provider := staticRSIP("selected", "ns", map[string]string{"color": "blue"}, nil)
	s.AddObject(provider)
	s.AddObject(&manifest.ResourceSetInputProvider{Name: "dynamic", Namespace: "ns", Type: fluxopv1.InputProviderGitHubBranch, Labels: map[string]string{"color": "blue"}})
	var updates atomic.Int64
	unsub := s.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
		if id == rs.Named() {
			updates.Add(1)
		}
	}, false)
	defer unsub()
	first := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone)
	defer first.Close()
	if first.Outcome() == schedule.Blocked || first.Valid() {
		t.Fatalf("first provider publication: unresolved=%t valid=%t", first.Outcome() == schedule.Blocked, first.Valid())
	}
	settled := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone)
	defer settled.Close()
	first.Retire()
	info, _ := s.GetStatus(rs.Named())
	if settled.Outcome() == schedule.Blocked || !settled.Valid() || info.Outcome() != store.OutcomePassed {
		t.Fatalf("initial settlement: unresolved=%t valid=%t status=%+v", settled.Outcome() == schedule.Blocked, settled.Valid(), info)
	}
	initial := s.GetArtifact(rs.Named()).(*store.Emission)
	if len(initial.Manifests) != 2 || len(initial.Children) != 2 {
		t.Fatal("initial render did not retain both documents and identities")
	}
	var reader store.ReadSet
	reader.Init(s, manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "ns", Name: "reader"}, nil)
	defer reader.Retire()
	reader.GetArtifact(rs.Named())
	freshProvider := *provider
	freshProvider.Labels = map[string]string{"color": "blue", "revision": "fresh"}
	s.AddObject(&freshProvider)
	if settled.Valid() {
		t.Fatal("provider metadata change did not invalidate captured inputs")
	}
	replay := c.newExecution(rs.Named())
	defer replay.run.Close()
	if _, _, err := resourceset.Render(rs, resourceset.StoreResolver(&replay.run.Reads), func(content any) (bool, error) {
		if manifest.Fingerprint(content) != initial.Fingerprint {
			t.Fatal("metadata refresh changed the rendered input projection")
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	_, inventory := replay.run.Reads.GetEmission(rs.Named())
	inventory.Children, inventory.Manifests = c.ParseChildren(rs, initial.Manifests, manifest.ParseDocOptions{WipeSecrets: true})
	inventory.Complete = true
	replay.run.Emit(rs.Named(), &inventory)
	inventory.Finish()
	if accepted := s.GetArtifact(rs.Named()).(*store.Emission); accepted.Fingerprint != initial.Fingerprint || !cmp.Equal(accepted.Manifests, initial.Manifests) || !cmp.Equal(accepted.Children, initial.Children) {
		t.Error("same-inventory publication lost its rendered identity or bytes")
	}
	var objectEvents, statusEvents atomic.Int64
	var statusChange atomic.Value
	unsubObjects := s.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) { objectEvents.Add(1) }, false)
	defer unsubObjects()
	unsubStatus := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		statusEvents.Add(1)
		change := fmt.Sprintf("%s: %+v", id, payload)
		statusChange.Store(change)
	}, false)
	defer unsubStatus()
	second := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone)
	defer second.Close()
	settled.Retire()
	after := s.GetArtifact(rs.Named()).(*store.Emission)
	if second.Outcome() == schedule.Blocked || !second.Valid() || !reader.Valid() || updates.Load() != 1 || objectEvents.Load() != 0 || statusEvents.Load() != 0 || !second.Reused {
		t.Fatalf("equal output: unresolved=%t reader valid=%t artifact/object/status events=%d/%d/%d reused=%t status=%v", second.Outcome() == schedule.Blocked, reader.Valid(), updates.Load()-1, objectEvents.Load(), statusEvents.Load(), second.Reused, statusChange.Load())
	}
	if initial.Fingerprint != after.Fingerprint || !cmp.Equal(initial.Manifests, after.Manifests) {
		t.Fatal("equal effective inputs changed accepted content")
	}
	fresh := *rs
	fresh.ResourcesTemplate += "\n"
	s.AddObject(&fresh)
	third := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone)
	defer third.Close()
	changed := s.GetArtifact(rs.Named()).(*store.Emission)
	if third.Outcome() == schedule.Blocked || reader.Valid() || updates.Load() != 2 || third.Reused || changed.Fingerprint == initial.Fingerprint || !cmp.Equal(changed.Manifests, initial.Manifests) {
		t.Fatalf("changed fingerprint: reader valid=%t artifact events=%d reused=%t", reader.Valid(), updates.Load(), third.Reused)
	}
}

func independentWriterFixture(name, color string) *manifest.ResourceSet {
	rs := selectorFixture(name, color)
	rs.ResourcesTemplate = "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSetInputProvider\nmetadata:\n  name: emitted-" + name + "\n  namespace: ns\n  labels: {color: red}\nspec: {type: Static}\n---\napiVersion: example.com/v1\nkind: RenderMarker\nmetadata: {name: rendered-" + name + ", namespace: ns}\n"
	return rs
}

func selectorFixture(name, color string) *manifest.ResourceSet {
	return &manifest.ResourceSet{Name: name, Namespace: "ns",
		InputsFrom:        []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSetInputProvider, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"color": color}}}},
		ResourcesTemplate: "<< if inputs >>\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.provider.name >>', namespace: ns}\ndata: {value: selected}\n<< end >>\n"}
}

func publishResourceSetDocuments(tb testing.TB, c *Controller, id manifest.NamedResource, docs []map[string]any) {
	tb.Helper()
	parent := c.Store.GetObject(id).(*manifest.ResourceSet)
	base.Stamp(parent.EmissionPolicy(), docs)
	_, next := c.Store.GetEmission(id)
	defer next.Finish()
	next.Children, next.Manifests = c.ParseChildren(parent, docs, manifest.ParseDocOptions{})
	next.Complete = true
	c.Emit(id, &next)
}
