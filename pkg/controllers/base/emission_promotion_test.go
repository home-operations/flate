package base

import (
	"context"
	"slices"
	"sync"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestKeepEmitted_LatePromotionConverges(t *testing.T) {
	t.Parallel()
	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "before_admission", false: "after_admission"}[before], func(t *testing.T) {
			t.Parallel()
			for range 2 {
				s := store.New()
				tasks := task.NewBounded(2)
				c := New(s, tasks)
				parent := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
				primary := &manifest.Kustomization{Name: "changed", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "settings"}}}}
				settings := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
				source := &manifest.GitRepository{Name: "late-source", Namespace: "apps"}
				child := &manifest.Kustomization{Name: "late-consumer", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: source.Name}}
				s.AddObjects([]manifest.BaseManifest{parent, primary})
				index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					primary.Named(): {File: "changed.yaml"},
					settings:        {Productions: []manifest.Production{{Owner: parent.Named(), Source: manifest.BootstrapSourceID}}},
				}}
				filter := change.NewFilter(change.NewSet([]string{"changed.yaml"}), index, s, "")
				c.Configure(Options{Filter: filter, Index: index})
				var rendered string
				arrived := false
				stop := s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
					if id != child.Named() {
						return
					}
					arrived = true
					run := func() bool {
						execution := new(Execution)
						execution.Init(c, id)
						defer execution.Close()
						execution.Dispatch(t.Context(), manifest.DrainNone, execution, func(execution *Execution, _ context.Context, obj *manifest.Kustomization, _ manifest.DrainLevel) error {
							rendered = "late consumer reconciled\n"
							_, _, next := execution.Reads.GetEmission(obj.Named())
							defer next.Finish()
							next.Complete = true
							next.Manifests = []map[string]any{{"value": rendered}}
							return execution.Emit(obj.Named(), &next, nil)
						})
						return execution.Valid()
					}
					if !run() {
						assert.Equal(t, run(), true)
					}
				}, false)
				admitted, promoted := make(chan struct{}), make(chan struct{})
				tasks.Go(t.Context(), "admission", func(context.Context) {
					if before {
						<-promoted
					}
					c.KeepEmitted(parent.Named(), store.Child{Obj: child}, nil)
					close(admitted)
					if !before {
						<-promoted
					}
					s.RecordEmitted(parent.Named(), captureEmission(s, parent.Named(), &store.Emission{Children: []store.Child{{Obj: child}}, Complete: true}), nil)
				})
				tasks.Go(t.Context(), "promotion", func(context.Context) {
					if !before {
						<-admitted
					}
					assert.Equal(t, s.GetObject(child.Named()) == nil, true)
					c.Emit(primary.Named(), captureEmission(s, primary.Named(), &store.Emission{Children: []store.Child{{Obj: source}}, Complete: true}), nil)
					close(promoted)
				})
				tasks.BlockTillDone()
				stop()
				assert.Equal(t, tasks.Failures(), 0)
				assert.Equal(t, arrived, true)
				assert.Equal(t, filter.ShouldReconcile(child.Named()), true)
				assert.Equal(t, rendered, "late consumer reconciled\n")
				output, ok := s.GetArtifact(child.Named()).(*store.Emission)
				if !ok {
					t.Fatal("missing selected output")
				}
				assert.Diff(t, output.Manifests, []map[string]any{{"value": "late consumer reconciled\n"}})
			}
		})
	}
}

func TestKeepEmitted_ConcurrentAdmissionRefiresOnce(t *testing.T) {
	t.Parallel()
	s := store.New()
	slots := task.NewBounded(2)
	c := New(s, slots)
	parent := testutil.KustomizationID("apps", "parent")
	source := &manifest.OCIRepository{Name: "chart", Namespace: "apps"}
	s.AddObject(source)
	s.SetIncluded(source.Named(), false)
	child := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", Chart: manifest.HelmChart{Source: source.Named()}}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent: {File: "parent.yaml"}}}
	filter := change.NewFilter(change.NewSet([]string{"parent.yaml"}), index, s, "/repo")
	c.Configure(Options{Filter: filter, Index: index})
	captured := new(Execution)
	captured.Init(c, source.Named())
	defer captured.Close()
	captured.Reads.GetObject(source.Named())
	assert.Equal(t, captured.Reads.Admitted(), false)
	captured.Reads.Accept()
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(2)
	for range 2 {
		slots.Go(t.Context(), "admit", func(context.Context) {
			ready.Done()
			<-start
			c.KeepEmitted(parent, store.Child{Obj: child}, nil)
		})
	}
	ready.Wait()
	close(start)
	slots.BlockTillDone()
	assert.Equal(t, slots.Failures(), 0)
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{source.Named()})
	assert.Equal(t, captured.Reads.Valid(), false)
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	assert.Equal(t, filter.ShouldReconcile(source.Named()), true)
}

func TestFingerprintDedup_IncomingSelectionEvidence(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"emit", "cached", "accepted"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			c := New(s, nil)
			parent := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
			s.AddObject(parent)
			index := &manifest.DiscoveryIndex{}
			child := store.Child{Obj: &manifest.ConfigMap{Name: "settings", Namespace: "apps", Data: map[string]any{"VALUE": "incoming"}}, Productions: []manifest.Production{{Owner: parent.Named(), Source: manifest.BootstrapSourceID, File: "incoming.yaml"}}}
			attempt := captureEmission(s, parent.Named(), &store.Emission{Fingerprint: manifest.Fingerprint("same"), Children: []store.Child{child}, Complete: true})
			filter := change.NewFilter(change.NewSet([]string{"incoming.yaml"}), index, s, "/repo")
			if mode == "cached" {
				s.SetArtifact(parent.Named(), attempt)
			}
			if mode == "accepted" {
				s.RecordEmitted(parent.Named(), attempt, nil)
				replacement := parent.Clone()
				replacement.Path = "replacement"
				s.AddObject(replacement)
			}
			assert.Equal(t, filter.ShouldReconcile(child.Obj.Named()), false)
			run := new(Execution)
			run.Init(c, parent.Named())
			defer run.Close()
			c.Configure(Options{Filter: filter, Index: index})
			arrivals := 0
			stop := s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
				if id == child.Obj.Named() && payload != nil {
					arrivals++
					if !filter.ShouldReconcile(id) {
						t.Error("incoming contribution was not selected before arrival")
					}
				}
			}, false)
			defer stop()
			if mode == "emit" {
				run.Emit(parent.Named(), attempt, nil)
			} else {
				_, handled, err := run.FingerprintDedup(parent.Named(), "same")
				assert.NoError(t, err)
				assert.Equal(t, handled, true)
			}
			assert.Equal(t, filter.ShouldReconcile(child.Obj.Named()), true)
			if mode == "emit" {
				assert.Equal(t, arrivals, 1)
			}
			if mode == "cached" {
				return
			}
			obj, ok := s.Get[*manifest.ConfigMap](child.Obj.Named())
			if !ok {
				t.Fatal("selected incoming output is missing")
			}
			assert.Diff(t, obj.Data, map[string]any{"VALUE": "incoming"})
		})
	}
}
