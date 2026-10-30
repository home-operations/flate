package base

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type configGenerator struct {
	*manifest.Kustomization
	objects []manifest.BaseManifest
	calls   *atomic.Int64
	err     error
}

func (g *configGenerator) GeneratedObjects() ([]manifest.BaseManifest, error) {
	g.calls.Add(1)
	return g.objects, g.err
}

func TestGeneratedObjects_Lifecycle(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"complete_empty", "replay", "failure", "removed", "invalid_rule", "collision"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			c := New(s, task.NewBounded(2))
			var calls atomic.Int64
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "auxiliary"}
			first := &configGenerator{Kustomization: &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "first"},
				objects: []manifest.BaseManifest{manifest.NewConfigMap(id, map[string]any{"value": "first"})}, calls: &calls}
			s.AddObject(first)
			run := newExecution(c, first.Named())
			var rendered store.Child
			run.Dispatch(t.Context(), manifest.DrainNone, run, func(run *Execution, _ context.Context, _ *configGenerator, _ manifest.DrainLevel) error {
				_, _, next := run.Reads.GetEmission(first.Named())
				defer next.Finish()
				rendered = store.Child{Obj: manifest.NewConfigMap(manifest.NamedResource{Name: "rendered", Namespace: "apps"}, map[string]any{"value": "rendered"})}
				next.Children, next.Complete, next.Fingerprint = []store.Child{rendered}, true, manifest.Fingerprint("same")
				return run.Emit(first.Named(), &next, nil)
			})
			if run.Outcome() != schedule.Terminal || s.GetObject(id) == nil {
				t.Fatal("first execution did not publish its generated object")
			}
			next := *first
			next.Kustomization = first.Clone()
			next.Path = "second"
			next.objects = []manifest.BaseManifest{manifest.NewConfigMap(id, map[string]any{"value": "second"})}
			if mode == "removed" {
				next.objects = nil
			}
			if mode == "invalid_rule" {
				next.err = manifest.ErrInput
			}
			s.AddObject(&next)
			second := newExecution(c, next.Named())
			defer second.Close()
			second.Dispatch(t.Context(), manifest.DrainNone, second, func(run *Execution, _ context.Context, _ *configGenerator, _ manifest.DrainLevel) error {
				if mode == "failure" {
					return manifest.ErrInput
				}
				if mode == "replay" {
					_, handled, err := run.FingerprintDedup(next.Named(), "same", &store.Emission{})
					if !handled {
						t.Error("completed output was unavailable for replay")
					}
					return err
				}
				_, _, output := run.Reads.GetEmission(next.Named())
				defer output.Finish()
				output.Children, output.Manifests, output.Complete = nil, nil, true
				if mode == "collision" {
					doc := map[string]any{"apiVersion": "v1", "kind": manifest.KindConfigMap, "metadata": map[string]any{"name": id.Name, "namespace": id.Namespace}, "data": map[string]any{"value": "authored"}}
					output.Children, output.Manifests, output.Failures = c.ParseChildren(&next, []manifest.DocOccurrence{{Document: doc}}, manifest.ParseDocOptions{})
				}
				return run.Emit(next.Named(), &output, nil)
			})
			if calls.Load() != 2 {
				t.Fatalf("rule evaluated %d times, want once per declaration", calls.Load())
			}
			_, _, output := s.GetEmission(next.Named())
			defer output.Finish()
			if mode == "removed" || mode == "invalid_rule" {
				if s.GetObject(id) != nil || slices.ContainsFunc(output.Children, func(child store.Child) bool { return child.Generated }) {
					t.Fatal("empty or invalid rule retained an obsolete generated object")
				}
			} else {
				value := s.GetObject(id).(*manifest.ConfigMap).Data["value"]
				want := "second"
				if mode == "collision" {
					want = "authored"
				}
				if value != want || s.ReportHidden(id) != (mode != "collision") {
					t.Fatalf("representative=%v hidden=%t", value, s.ReportHidden(id))
				}
			}
			if mode == "failure" && s.GetObject(rendered.Obj.Named()) == nil {
				t.Fatal("partial failure discarded ordinary rendered inventory")
			}
		})
	}
}

func TestGeneratedObjects_WithdrawalBeforeBlockedSource(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, task.NewBounded(2))
	var calls atomic.Int64
	child := manifest.NewConfigMap(manifest.NamedResource{Name: "auxiliary", Namespace: "apps"}, nil)
	first := &configGenerator{Kustomization: &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "first"}, objects: []manifest.BaseManifest{child}, calls: &calls}
	s.AddObject(first)
	initial := newExecution(c, first.Named())
	initial.Dispatch(t.Context(), manifest.DrainNone, struct{}{}, func(struct{}, context.Context, *configGenerator, manifest.DrainLevel) error { return nil })
	next := *first
	next.Kustomization = first.Clone()
	next.Path, next.objects = "second", nil
	s.AddObject(&next)
	run := newExecution(c, next.Named())
	defer run.Close()
	blocked := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Name: "replacement", Namespace: "apps"}
	check := func(_ struct{}, _ context.Context, _ *configGenerator, _ manifest.DrainLevel) error {
		if s.GetObject(child.Named()) != nil {
			t.Error("obsolete generated object remained visible at the source wait")
		}
		return &depwait.ErrBlocked{Deps: []manifest.NamedResource{blocked}}
	}
	run.Dispatch(t.Context(), manifest.DrainNone, struct{}{}, check)
	if run.Outcome() != schedule.Blocked {
		t.Fatal("execution did not retain the source wait")
	}
	run.Settle(t.Context(), manifest.DrainNone)
	if calls.Load() != 2 || run.Outcome() != schedule.Blocked {
		t.Fatalf("resumed rule evaluated again: %d", calls.Load())
	}
}

func TestGeneratedObjects_NoRuleNoAllocations(t *testing.T) {
	s := store.New()
	object := &manifest.Kustomization{Name: "ordinary", Namespace: "apps"}
	s.AddObject(object)
	run := newExecution(New(s, task.NewBounded(2)), object.Named())
	defer run.Close()
	run.Reads.GetDeclaration(object.Named())
	reconcile := func(context.Context, *manifest.Kustomization, manifest.DrainLevel) error { return nil }
	if allocations := testing.AllocsPerRun(100, func() {
		run.prepared = false
		if err := run.reconcile(t.Context(), object, manifest.DrainNone, reconcile); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 || s.HasInventory(object.Named()) {
		t.Fatalf("no-rule preparation allocations=%g inventory=%t", allocations, s.HasInventory(object.Named()))
	}
}

func TestGeneratedObjects_AdmissionWithdrawsPartialRule(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"suspended", "excluded", "preflight"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			c := New(s, task.NewBounded(2))
			var calls atomic.Int64
			child := manifest.NewConfigMap(manifest.NamedResource{Name: "auxiliary", Namespace: "apps"}, nil)
			object := &configGenerator{Kustomization: &manifest.Kustomization{Name: "owner", Namespace: "apps"}, objects: []manifest.BaseManifest{child}, calls: &calls}
			s.AddObject(object)
			first := newExecution(c, object.Named())
			first.Dispatch(t.Context(), manifest.DrainNone, struct{}{}, func(struct{}, context.Context, *configGenerator, manifest.DrainLevel) error { return nil })
			if s.GetObject(child.Named()) == nil {
				t.Fatal("initial provisional child was not published")
			}
			switch mode {
			case "suspended":
				next := *object
				next.Kustomization = object.Clone()
				next.Suspend = true
				s.AddObject(&next)
			case "excluded":
				s.SetIncluded(object.Named(), false)
			case "preflight":
				s.SetPreflight(object.Named(), "cycle")
			}
			next := newExecution(c, object.Named())
			defer next.Close()
			next.Dispatch(t.Context(), manifest.DrainNone, struct{}{}, func(struct{}, context.Context, *configGenerator, manifest.DrainLevel) error {
				t.Error("unadmitted producer reached its body")
				return nil
			})
			if s.GetObject(child.Named()) != nil || calls.Load() != 1 {
				t.Fatal("unadmitted declaration retained or reevaluated its provisional rule")
			}
		})
	}
}

func TestGeneratedObjects_ReplayReadinessAndReceipt(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		readmitted bool
	}{{name: "unchanged"}, {name: "readmitted", readmitted: true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			c := New(s, task.NewBounded(2))
			var calls atomic.Int64
			child := manifest.NewConfigMap(manifest.NamedResource{Name: "auxiliary", Namespace: "apps"}, nil)
			object := &configGenerator{Kustomization: &manifest.Kustomization{Name: "owner", Namespace: "apps"}, objects: []manifest.BaseManifest{child}, calls: &calls}
			s.AddObject(object)
			receipt := &manifest.RenderReceipt{}
			first := newExecution(c, object.Named())
			first.Dispatch(t.Context(), manifest.DrainNone, first, func(run *Execution, _ context.Context, _ *configGenerator, _ manifest.DrainLevel) error {
				_, _, output := run.Reads.GetEmission(object.Named())
				defer output.Finish()
				doc := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "rendered", "namespace": "apps"}, "data": map[string]any{"VALUE": "output"}}
				output.Children = append(output.Children, store.Child{Obj: manifest.NewConfigMap(manifest.NamedResource{Name: "rendered", Namespace: "apps"}, map[string]any{"VALUE": "output"}), Doc: doc})
				output.Manifests = []manifest.DocOccurrence{{Document: doc}}
				output.Complete, output.Fingerprint, output.Render = true, manifest.Fingerprint("same"), receipt
				return run.Emit(object.Named(), &output, nil)
			})
			if tc.readmitted {
				s.SetIncluded(object.Named(), false)
				s.UpdateStatus(object.Named(), store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged})
				s.SetIncluded(object.Named(), true)
				if info, _ := s.GetStatus(object.Named()); info.Outcome() != store.OutcomePending {
					t.Fatalf("readmitted output remained ready: %+v", info)
				}
			}
			var authority store.ReadSet
			authority.Init(s, manifest.NamedResource{Name: "artifact-reader"}, nil)
			defer authority.Retire()
			authority.GetArtifact(object.Named(), store.ArtifactContent)
			var events atomic.Int64
			stop := s.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
				if id == object.Named() {
					events.Add(1)
				}
			}, false)
			defer stop()
			second := newExecution(c, object.Named())
			defer second.Close()
			second.Dispatch(t.Context(), manifest.DrainNone, second, func(run *Execution, _ context.Context, _ *configGenerator, _ manifest.DrainLevel) error {
				_, handled, err := run.FingerprintDedup(object.Named(), "same", &store.Emission{})
				if !handled {
					t.Error("completed render was unavailable after provisional publication")
				}
				return err
			})
			if !tc.readmitted && (!authority.Valid() || events.Load() != 0) {
				t.Fatalf("unchanged generated preparation invalidated output: valid=%t events=%d", authority.Valid(), events.Load())
			}
			wantReceipt := receipt
			if tc.readmitted {
				wantReceipt = nil
			}
			if output, _ := s.GetArtifact(object.Named(), store.ArtifactContent).(*store.Emission); output == nil || output.Render != wantReceipt || !second.Reused {
				t.Fatalf("replay receipt = %v, reused = %t", output, second.Reused)
			}
			if info, _ := s.GetStatus(object.Named()); info.Outcome() != store.OutcomePassed {
				t.Fatalf("accepted replay did not restore readiness: %+v", info)
			}
		})
	}
}
