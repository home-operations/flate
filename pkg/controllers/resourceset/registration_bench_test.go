package resourceset

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
					c.Configure(Options{})
					c.StartLifecycle()
					defer c.Close()
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
					c.ReconcileNode(b.Context(), rs.Named(), manifest.DrainNone)
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
									c.ReconcileNode(ctx, rs.Named(), manifest.DrainNone)
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
				c := renderCountingController(s, tasks, &renders)
				c.StartLifecycle()
				defer c.Close()
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
				b.ReportAllocs()
				for b.Loop() {
					for i := range sets {
						p, _ := s.Get[*manifest.ResourceSetInputProvider](manifest.NamedResource{Kind: manifest.KindResourceSetInputProvider, Namespace: "ns", Name: fmt.Sprintf("selected-%d", i)})
						fresh := *p
						fresh.Status.ExportedInputs = []fluxopv1.ResourceSetInput{{"revision": {Raw: []byte(fmt.Sprint(generation))}}}
						s.AddObject(&fresh)
					}
					var artifacts [2]*store.ResourceSetArtifact
					var errs [2]error
					for i, rs := range sets {
						tasks.Go(b.Context(), "render", func(context.Context) {
							fp := resourceSetFingerprint(rs, s)
							docs, err := resourceset.Render(rs, resourceset.StoreResolver(s))
							errs[i] = err
							artifacts[i] = &store.ResourceSetArtifact{Manifests: docs, Fingerprint: fp}
						})
					}
					tasks.BlockTillDone()
					for _, i := range commitOrder {
						if errs[i] != nil {
							b.Fatal(errs[i])
						}
						c.emit(sets[i].Named(), artifacts[i].Manifests, true)
						s.SetArtifact(sets[i].Named(), artifacts[i])
						s.UpdateStatus(sets[i].Named(), store.StatusReady, "")
					}
					for range 32 {
						for _, i := range commitOrder {
							c.emit(sets[i].Named(), []map[string]any{manifest.DeepCopyMap(artifacts[i].Manifests[0])}, true)
							tasks.Go(b.Context(), "equal", func(ctx context.Context) {
								dispatches.Add(1)
								c.ReconcileNode(ctx, sets[i].Named(), manifest.DrainNone)
							})
						}
						tasks.BlockTillDone()
					}
					generation++
				}
				if tasks.Failures() != 0 || renders.Load() != int64(2*b.N) || dispatches.Load() != int64(64*b.N) {
					b.Fatalf("panics=%d renders=%d dispatches=%d, want renders=%d dispatches=%d", tasks.Failures(), renders.Load(), dispatches.Load(), 2*b.N, 64*b.N)
				}
				for _, rs := range sets {
					info, _ := s.GetStatus(rs.Named())
					if info.Status != store.StatusReady {
						b.Fatal("writer did not finish Ready")
					}
				}
				b.ReportMetric(float64(renders.Load())/float64(b.N), "renders/op")
				b.ReportMetric(float64(dispatches.Load())/float64(b.N), "dispatches/op")
			})
		}
	}
}

func TestDirtyRerun_EqualOutputRenderCount(t *testing.T) {
	s := store.New()
	tasks := task.NewBounded(2)
	var renders, updates atomic.Int64
	c := renderCountingController(s, tasks, &renders)
	c.StartLifecycle()
	defer c.Close()
	rs := independentWriterFixture("u", "blue")
	s.AddObject(rs)
	s.AddObject(staticRSIP("selected", "ns", map[string]string{"color": "blue"}, nil))
	unsub := s.AddListener(store.EventArtifactUpdated, func(manifest.NamedResource, any) { updates.Add(1) }, false)
	defer unsub()
	if blocked := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone); len(blocked) != 0 {
		t.Fatalf("initial dispatch blocked on %v", blocked)
	}
	if renders.Load() != 1 {
		t.Fatalf("initial renders=%d, want 1", renders.Load())
	}
	initial := s.GetArtifact(rs.Named())
	docs, err := resourceset.Render(rs, resourceset.StoreResolver(s))
	if err != nil {
		t.Fatal(err)
	}
	c.emit(rs.Named(), docs, true)
	s.SetArtifact(rs.Named(), &store.ResourceSetArtifact{Manifests: docs, Fingerprint: resourceSetFingerprint(rs, s)})
	if renders.Load() != 2 || updates.Load() != 1 || s.GetArtifact(rs.Named()) != initial {
		t.Fatalf("equal-output renders=%d artifact updates=%d, want renders=2 updates=1 and unchanged artifact", renders.Load(), updates.Load())
	}
	if blocked := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone); len(blocked) != 0 {
		t.Fatalf("equal dispatch blocked on %v", blocked)
	}
	c.emit(rs.Named(), docs, false)
	if renders.Load() != 2 || updates.Load() != 1 {
		t.Fatalf("dedup renders=%d artifact updates=%d, want renders=2 updates=1", renders.Load(), updates.Load())
	}
}

func renderCountingController(s *store.Store, tasks *task.Service, renders *atomic.Int64) *Controller {
	parent := &manifest.Kustomization{Name: "parent", Namespace: "ns"}
	s.AddObject(parent)
	s.UpdateStatus(parent.Named(), store.StatusReady, "")
	c := New(s, tasks, true)
	c.Configure(Options{
		ParentOf: func(manifest.NamedResource) (manifest.NamedResource, bool) { return parent.Named(), true },
		// One raw marker per successful Render; equal publication replays only
		// the provider document, while fresh equal-output renders reach RawSink.
		RawSink: func(manifest.NamedResource, manifest.NamedResource, map[string]any) { renders.Add(1) },
	})
	return c
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
