package resourceset

import (
	"context"
	"fmt"
	"github.com/home-operations/flate/pkg/controllers/base"
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
					c.Configure(base.Options{})
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
				c := New(s, tasks, true)
				c.Configure(base.Options{})
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
						p, _ := s.Get[*manifest.ResourceSetInputProvider](manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "ns", Name: fmt.Sprintf("selected-%d", i)})
						fresh := *p
						fresh.Status.ExportedInputs = []fluxopv1.ResourceSetInput{{"revision": {Raw: []byte(fmt.Sprint(generation))}}}
						s.AddObject(&fresh)
					}
					var artifacts [2]*store.Emission
					var errs [2]error
					for i, rs := range sets {
						tasks.Go(b.Context(), "render", func(context.Context) {
							resolve := resourceset.StoreResolver(s)
							var fp string
							docs, err := resourceset.Render(rs, resolve, func(content any) (bool, error) {
								var err error
								fp, _, err = c.FingerprintDedup(rs.Named(), content)
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
						publishResourceSetDocuments(b, c, sets[i].Named(), artifacts[i].Manifests)
						s.SetArtifact(sets[i].Named(), artifacts[i])
						s.UpdateStatus(sets[i].Named(), store.StatusReady, "", "")
					}
					for range 32 {
						for _, i := range commitOrder {
							publishResourceSetDocuments(b, c, sets[i].Named(), []map[string]any{manifest.DeepCopyMap(artifacts[i].Manifests[0])})
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
	t.Parallel()
	s := store.New()
	tasks := task.NewBounded(2)
	c := New(s, tasks, true)
	c.Configure(base.Options{})
	c.StartLifecycle()
	defer c.Close()
	rs := independentWriterFixture("u", "blue")
	s.AddObject(rs)
	s.AddObject(staticRSIP("selected", "ns", map[string]string{"color": "blue"}, nil))
	var updates atomic.Int64
	unsub := s.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
		if id == rs.Named() {
			updates.Add(1)
		}
	}, false)
	defer unsub()
	if blocked := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone); len(blocked) != 0 {
		t.Fatal(blocked)
	}
	initial := s.GetArtifact(rs.Named())
	if blocked := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone); len(blocked) != 0 {
		t.Fatal(blocked)
	}
	if updates.Load() != 1 || s.GetArtifact(rs.Named()) != initial {
		t.Fatal("equal effective inputs replaced the accepted artifact")
	}
	fresh := *rs
	fresh.ResourcesTemplate += "\n"
	s.AddObject(&fresh)
	if blocked := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone); len(blocked) != 0 {
		t.Fatal(blocked)
	}
	if updates.Load() != 2 || s.GetArtifact(rs.Named()) == initial {
		t.Fatal("new fingerprint with equal output did not replace the accepted artifact")
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
