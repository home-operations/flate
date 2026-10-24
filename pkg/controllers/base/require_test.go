package base_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/schedule"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRequire_Classifications(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"ready", "blocked", "failed", "mixed", "force"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			id := testutil.HelmReleaseID("apps", "consumer")
			ready := manifest.DependencyRef{NamedResource: testutil.KustomizationID("apps", "ready")}
			failed := manifest.DependencyRef{NamedResource: testutil.KustomizationID("apps", "failed")}
			pending := manifest.DependencyRef{NamedResource: testutil.KustomizationID("apps", "pending")}
			st.UpdateStatus(ready.NamedResource, store.StatusReady, "", "")
			st.UpdateStatus(failed.NamedResource, store.StatusFailed, "", "render failed")
			st.UpdateStatus(pending.NamedResource, store.StatusPending, "", "working")
			st.UpdateStatus(id, store.StatusReady, "", "informative")
			st.AddObject(&manifest.HelmRelease{Name: id.Name, Namespace: id.Namespace, Suspend: mode != "ready"})
			deps := []manifest.DependencyRef{ready}
			level := manifest.DrainNone
			switch mode {
			case "blocked":
				deps = append(deps, pending)
			case "failed":
				deps = append(deps, failed)
			case "mixed", "force":
				deps = append(deps, pending, failed)
				if mode == "force" {
					level = manifest.DrainForce
				}
			}
			err := controllertest.NewExecution(base.New(st, nil, "test"), id).Require(t.Context(), deps, level)
			info, _ := st.GetStatus(id)
			if info.Status != store.StatusReady || info.Message != "informative" {
				t.Fatalf("gate changed caller status: %+v", info)
			}
			switch mode {
			case "ready":
				if err != nil {
					t.Fatal(err)
				}
			case "blocked", "mixed":
				blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
				if !ok || !slices.Equal(blocked.Deps, []manifest.NamedResource{pending.NamedResource}) || errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("blocked gate = %v, want only pending dependency", err)
				}
			case "failed", "force":
				failure, ok := errors.AsType[*manifest.DependencyFailedError](err)
				want := []manifest.NamedResource{failed.NamedResource}
				if mode == "force" {
					want = append(want, pending.NamedResource)
				}
				if !ok || failure.Parent != id || !slices.Equal(failure.Failed, want) || !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("failed gate = %#v, want sorted dependencies %v", err, want)
				}
				if failure.Reasons[failed.NamedResource] != "render failed" || mode == "force" && failure.Reasons[pending.NamedResource] != "not ready" {
					t.Fatalf("failure reasons = %v", failure.Reasons)
				}
			}
		})
	}
}

func TestExecution_BlockedSuccessorRetainsAssessment(t *testing.T) {
	t.Parallel()
	st := store.New()
	obj := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	input := &manifest.ConfigMap{Name: "values", Namespace: "apps", Data: map[string]any{"value": "old"}}
	gate := testutil.KustomizationID("apps", "gate")
	st.AddObjects([]manifest.BaseManifest{obj, input})
	st.UpdateStatus(gate, store.StatusPending, "", "working")
	c := base.New(st, nil, "test")
	first := controllertest.NewExecution(c, obj.Named())
	first.Reads.GetObject(input.Named())
	_, output := first.Reads.GetEmission(obj.Named())
	output.Complete = true
	first.Emit(obj.Named(), &output)
	output.Finish()
	defer first.Close()
	st.AddObject(&manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace, Data: map[string]any{"value": "new"}})
	if first.Valid() {
		t.Fatal("changed input left the predecessor current")
	}
	second := controllertest.NewExecution(c, obj.Named())
	defer second.Close()
	second.Dispatch(t.Context(), manifest.DrainNone, func(ctx context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
		return second.Require(ctx, []manifest.DependencyRef{{NamedResource: gate}}, level)
	})
	if second.Outcome() != schedule.Blocked || !second.Valid() {
		t.Fatal("current blocked assessment was invalidated by stale predecessor output")
	}
	st.UpdateStatus(gate, store.StatusReady, "", "")
	if second.Valid() {
		t.Fatal("changed readiness did not invalidate the blocked assessment")
	}
}

func TestRecordEmitted_PhantomSupersession(t *testing.T) {
	t.Parallel()
	for _, invalidated := range []bool{false, true} {
		t.Run(map[bool]string{false: "settled", true: "invalidated"}[invalidated], func(t *testing.T) {
			t.Parallel()
			st := store.New()
			obj := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			input := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"value": "old"}}
			child := &manifest.ConfigMap{Name: "output", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{obj, input})
			c := base.New(st, nil, "test")
			first := controllertest.NewExecution(c, obj.Named())
			defer first.Close()
			first.Dispatch(t.Context(), manifest.DrainNone, func(context.Context, *manifest.Kustomization, manifest.DrainLevel) error {
				first.Reads.GetObject(input.Named())
				_, output := first.Reads.GetEmission(obj.Named())
				output.Children, output.Complete = []store.Child{{Obj: child, Doc: child.Snapshot()}}, true
				first.Emit(obj.Named(), &output)
				output.Finish()
				return nil
			})
			if !first.Valid() {
				t.Fatal("initial inventory was never settled")
			}
			if invalidated {
				st.AddObject(&manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace, Data: map[string]any{"value": "new"}})
			}
			second := controllertest.NewExecution(c, obj.Named())
			defer second.Close()
			second.Dispatch(t.Context(), manifest.DrainNone, func(context.Context, *manifest.Kustomization, manifest.DrainLevel) error {
				return errors.New("successor failed")
			})
			if !second.Valid() || (st.GetObject(child.Named()) == nil) != invalidated {
				t.Fatalf("failed successor retained invalid output or discarded settled output: valid=%t output=%v", second.Valid(), st.GetObject(child.Named()))
			}
		})
	}
}

func TestExecution_CanonicalManifest(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"active", "missing", "suspended"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			found := mode != "missing"
			st := store.New()
			obj := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", Values: map[string]any{"value": "old"}}
			fresh := obj.Clone()
			fresh.Values["value"] = "fresh"
			fresh.Suspend = mode == "suspended"
			var want *manifest.HelmRelease
			if found {
				st.AddObject(fresh)
				want = fresh
			}
			execution := controllertest.NewExecution(base.New(st, nil, "test"), obj.Named())
			defer execution.Close()
			got, ok := execution.Reads.Get[*manifest.HelmRelease](obj.Named())
			if ok != found || got != want || obj.Values["value"] != "old" {
				t.Fatalf("captured = %v, %v; want canonical %v, original=%v", got, ok, want, obj.Values)
			}
			called := false
			execution.Dispatch(t.Context(), manifest.DrainNone, func(_ context.Context, got *manifest.HelmRelease, _ manifest.DrainLevel) error {
				called = true
				if got != want {
					t.Fatalf("dispatched declaration = %v, want captured %v", got, want)
				}
				return nil
			})
			if called != (mode == "active") {
				t.Fatalf("reconcile called=%v for %s", called, mode)
			}
			info, has := st.GetStatus(obj.Named())
			if mode == "suspended" {
				if !has || info.Outcome() != store.OutcomeSuspended {
					t.Fatalf("suspended refresh status: %+v", info)
				}
			} else if mode == "missing" && has {
				t.Fatalf("missing declaration changed status: %+v", info)
			}
		})
	}
}

func TestRequire_ReadyAllocation(t *testing.T) {
	st := store.New()
	dependency := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
	st.AddObject(dependency)
	st.UpdateStatus(dependency.Named(), store.StatusReady, "", "")
	execution := controllertest.NewExecution(base.New(st, nil, "test"), testutil.HelmReleaseID("apps", "consumer"))
	defer execution.Close()
	deps := []manifest.DependencyRef{{NamedResource: dependency.Named()}}
	ctx := t.Context()
	if allocations := testing.AllocsPerRun(100, func() {
		if err := execution.Require(ctx, deps, manifest.DrainNone); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatalf("ready classification allocations=%v, want 0", allocations)
	}
}

func TestRequire_MultiplePredicates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		expressions []string
		status      store.Status
		level       manifest.DrainLevel
		want        schedule.Kind
	}{
		{name: "authored then builtin", expressions: []string{"false", ""}, status: store.StatusReady, level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "builtin then authored", expressions: []string{"", "false"}, status: store.StatusReady, level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "true then false", expressions: []string{"true", "false"}, status: store.StatusReady, level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "false then true", expressions: []string{"false", "true"}, status: store.StatusReady, level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "multiple failed", expressions: []string{"false", "1 == 2", "not valid CEL"}, status: store.StatusReady, level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "multiple blocked", expressions: []string{"false", "1 == 2"}, status: store.StatusReady, want: schedule.Blocked},
		{name: "expression replaces own builtin", expressions: []string{"true"}, status: store.StatusPending, want: schedule.Terminal},
		{name: "separate builtin still required", expressions: []string{"true", ""}, status: store.StatusPending, want: schedule.Blocked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			obj := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
			consumer := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{obj, consumer})
			st.UpdateStatus(obj.Named(), tc.status, "", "")
			var deps []manifest.DependencyRef
			for _, expr := range tc.expressions {
				deps = append(deps, manifest.DependencyRef{NamedResource: obj.Named(), ReadyExpr: expr})
			}
			var reason string
			for range 2 {
				execution := controllertest.NewExecution(base.New(st, nil, "test"), consumer.Named())
				defer execution.Close()
				err := execution.Require(t.Context(), deps, tc.level)
				switch tc.want {
				case schedule.DependencyFailed:
					failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
					if !ok || !slices.Equal(failed.Failed, []manifest.NamedResource{obj.Named()}) || len(failed.Reasons) != 1 {
						t.Fatalf("failed identities were lost or duplicated: %v", err)
					}
					if reason != "" && reason != failed.Reasons[obj.Named()] {
						t.Fatalf("registration order changed diagnostic: %q vs %q", reason, failed.Reasons[obj.Named()])
					}
					reason = failed.Reasons[obj.Named()]
				case schedule.Blocked:
					blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
					if !ok || !slices.Equal(blocked.Deps, []manifest.NamedResource{obj.Named()}) {
						t.Fatalf("blocked identities were lost or duplicated: %v", err)
					}
				default:
					if err != nil {
						t.Fatal(err)
					}
				}
				_ = execution.Require(t.Context(), deps, tc.level)
				if got := slices.Collect(execution.Reads.Gates()); len(got) != len(tc.expressions) {
					t.Fatalf("repeated registration changed predicates: %v", got)
				}
				slices.Reverse(deps)
			}
		})
	}
}

func TestExecution_ResumedPredicates(t *testing.T) {
	t.Parallel()
	st := store.New()
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	dependency := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{consumer, dependency})
	st.UpdateStatus(dependency.Named(), store.StatusReady, "", "")
	execution := controllertest.NewExecution(base.New(st, nil, "test"), consumer.Named())
	defer execution.Close()
	deps := []manifest.DependencyRef{{NamedResource: dependency.Named(), ReadyExpr: "false"}, {NamedResource: dependency.Named()}}
	execution.Dispatch(t.Context(), manifest.DrainNone, func(ctx context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
		return execution.Require(ctx, deps, level)
	})
	if execution.Outcome() != schedule.Blocked || !slices.Equal(execution.Blocked, []manifest.NamedResource{dependency.Named()}) {
		t.Fatal("false predicate did not park the execution")
	}
	execution.Settle(t.Context(), manifest.DrainForce)
	info, _ := st.GetStatus(consumer.Named())
	if execution.Outcome() != schedule.DependencyFailed || !info.IsDependencyFailure() || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{dependency.Named()}) {
		t.Fatalf("resumed execution lost its false predicate: %+v", info)
	}
	if got := slices.Collect(execution.Reads.Gates()); !slices.Equal(got, deps) {
		t.Fatalf("resumption duplicated or lost predicates: %v", got)
	}
}
