package base_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"weak"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
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
			st.UpdateStatus(ready.NamedResource, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			st.UpdateStatus(failed.NamedResource, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "render failed"})
			st.UpdateStatus(pending.NamedResource, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "working"})
			st.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "informative"})
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
			err := controllertest.NewExecution(base.New(st, nil), id).Require(t.Context(), deps, level)
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
	st.UpdateStatus(gate, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "working"})
	c := base.New(st, nil)
	first := controllertest.NewExecution(c, obj.Named())
	first.Reads.GetObject(input.Named())
	_, _, output := first.Reads.GetEmission(obj.Named())
	output.Complete = true
	first.Emit(obj.Named(), &output, nil)
	output.Finish()
	defer first.Close()
	st.AddObject(&manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace, Data: map[string]any{"value": "new"}})
	if first.Valid() {
		t.Fatal("changed input left the predecessor current")
	}
	second := controllertest.NewExecution(c, obj.Named())
	defer second.Close()
	type state struct {
		run *base.Execution
		obj *manifest.Kustomization
	}
	calls := 0
	retained := func() weak.Pointer[state] {
		s := &state{run: second, obj: obj}
		ref := weak.Make(s)
		second.Dispatch(t.Context(), manifest.DrainNone, s, func(s *state, ctx context.Context, got *manifest.Kustomization, level manifest.DrainLevel) error {
			if got != s.obj {
				t.Fatal("resumption lost the captured declaration")
			}
			calls++
			return s.run.Require(ctx, []manifest.DependencyRef{{NamedResource: gate}}, level)
		})
		return ref
	}()
	runtime.GC()
	if retained.Value() == nil {
		t.Fatal("blocked successor released its state")
	}
	if second.Outcome() != schedule.Blocked || !second.Valid() {
		t.Fatal("current blocked assessment was invalidated by stale predecessor output")
	}
	second.Settle(t.Context(), manifest.DrainForce)
	if calls != 2 || second.Outcome() != schedule.DependencyFailed || !second.Valid() {
		t.Fatal("successor did not settle with its captured state")
	}
	runtime.GC()
	if retained.Value() != nil {
		t.Fatal("terminal failure retained successor state")
	}
	runtime.KeepAlive(second)
	st.UpdateStatus(gate, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
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
			c := base.New(st, nil)
			first := controllertest.NewExecution(c, obj.Named())
			defer first.Close()
			first.Dispatch(t.Context(), manifest.DrainNone, first, func(first *base.Execution, _ context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
				first.Reads.GetObject(input.Named())
				_, _, output := first.Reads.GetEmission(obj.Named())
				output.Children, output.Complete = []store.Child{{Obj: child, Doc: child.Snapshot()}}, true
				first.Emit(obj.Named(), &output, nil)
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
			second.Dispatch(t.Context(), manifest.DrainNone, second, func(_ *base.Execution, _ context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
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
			execution := controllertest.NewExecution(base.New(st, nil), obj.Named())
			defer execution.Close()
			got, ok := execution.Reads.Get[*manifest.HelmRelease](obj.Named())
			if ok != found || got != want || obj.Values["value"] != "old" {
				t.Fatalf("captured = %v, %v; want canonical %v, original=%v", got, ok, want, obj.Values)
			}
			called := false
			execution.Dispatch(t.Context(), manifest.DrainNone, execution, func(_ *base.Execution, _ context.Context, got *manifest.HelmRelease, _ manifest.DrainLevel) error {
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
	st.UpdateStatus(dependency.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	execution := controllertest.NewExecution(base.New(st, nil), testutil.HelmReleaseID("apps", "consumer"))
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
			st.UpdateStatus(obj.Named(), store.StatusInfo{Status: tc.status, Reason: "", Message: ""})
			var deps []manifest.DependencyRef
			for _, expr := range tc.expressions {
				deps = append(deps, manifest.DependencyRef{NamedResource: obj.Named(), ReadyExpr: new(expr)})
			}
			var reason string
			for range 2 {
				execution := controllertest.NewExecution(base.New(st, nil), consumer.Named())
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

type countingSettler struct {
	error
	calls int
}

func (s *countingSettler) Unwrap() error { return s.error }

func (s *countingSettler) Settle(context.Context, manifest.DrainLevel) error {
	s.calls++
	return nil
}

func TestExecution_SettleInvalidReads(t *testing.T) {
	t.Parallel()
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("stale=%t", stale), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			input := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"value": "old"}}
			st.AddObjects([]manifest.BaseManifest{consumer, input})
			st.UpdateStatus(consumer.Named(), store.StatusInfo{Status: store.StatusPending, Message: "waiting for input"})
			execution := controllertest.NewExecution(base.New(st, nil), consumer.Named())
			t.Cleanup(execution.Close)
			pending := &countingSettler{error: &depwait.ErrBlocked{Deps: []manifest.NamedResource{input.Named()}}}
			execution.Dispatch(t.Context(), manifest.DrainNone, struct{}{}, func(_ struct{}, _ context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
				execution.Reads.GetObject(input.Named())
				return pending
			})
			if execution.Outcome() != schedule.Blocked || !slices.Equal(execution.Blocked, []manifest.NamedResource{input.Named()}) || pending.calls != 0 {
				t.Fatal("dispatch did not retain the blocked settler")
			}
			before, _ := st.GetStatus(consumer.Named())
			if stale {
				replacement := *input
				replacement.Data = map[string]any{"value": "new"}
				st.AddObject(&replacement)
			}
			if execution.Reads.Valid() == stale {
				t.Fatal("captured input validity does not match replacement")
			}
			execution.Settle(t.Context(), manifest.DrainForce)
			after, _ := st.GetStatus(consumer.Named())
			if stale {
				if pending.calls != 0 {
					t.Fatalf("stale execution called retained settler %d times", pending.calls)
				}
				if execution.Outcome() != schedule.Blocked || !slices.Equal(execution.Blocked, []manifest.NamedResource{input.Named()}) || after != before {
					t.Fatalf("stale settlement changed retained assessment or status: %+v", after)
				}
			} else if pending.calls != 1 || execution.Outcome() != schedule.Terminal || after.Status != store.StatusReady {
				t.Fatalf("valid settlement did not run retained work: calls=%d outcome=%v status=%+v", pending.calls, execution.Outcome(), after)
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
	st.UpdateStatus(dependency.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	execution := controllertest.NewExecution(base.New(st, nil), consumer.Named())
	defer execution.Close()
	deps := []manifest.DependencyRef{{NamedResource: dependency.Named(), ReadyExpr: new("false")}, {NamedResource: dependency.Named()}}
	execution.Dispatch(t.Context(), manifest.DrainNone, execution, func(execution *base.Execution, ctx context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
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
	want := make([]store.Gate, len(deps))
	for i, dep := range deps {
		want[i].DependencyRef = dep
	}
	if got := slices.Collect(execution.Reads.Gates()); !slices.Equal(got, want) {
		t.Fatalf("resumption duplicated or lost predicates: %v", got)
	}
}

func TestRequire_RetainsArtifactFactsThroughSettlement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ reverse, dataWait bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		t.Run(fmt.Sprintf("reverse=%t/data=%t", tc.reverse, tc.dataWait), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			consumer := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
			source := &manifest.OCIRepository{Name: "source", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{consumer, source})
			st.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady})
			st.SetArtifact(source.Named(), &store.SourceArtifact{})
			execution := controllertest.NewExecution(base.New(st, nil), consumer.Named())
			defer execution.Close()
			deps := []manifest.DependencyRef{
				{NamedResource: source.Named(), ReadyExpr: new("true")},
				{NamedResource: source.Named(), ReadyExpr: new("true"), Artifact: true, UnknownArtifactRoot: true, ChartSource: true},
			}
			if tc.reverse {
				slices.Reverse(deps)
			}
			rendered := false
			execution.Dispatch(t.Context(), manifest.DrainNone, execution, func(execution *base.Execution, ctx context.Context, _ *manifest.HelmRelease, level manifest.DrainLevel) error {
				err := execution.Require(ctx, deps, level)
				if !tc.dataWait {
					return err
				}
				return execution.AwaitData(ctx, base.DataWait{Resume: func(context.Context, manifest.DrainLevel, []manifest.NamedResource) error {
					rendered = true
					return nil
				}}, level)
			})
			if execution.Outcome() != schedule.Blocked || !slices.Equal(execution.Blocked, []manifest.NamedResource{source.Named()}) {
				t.Fatalf("missing content did not block: %v %v", execution.Outcome(), execution.Blocked)
			}
			execution.Settle(t.Context(), manifest.DrainCascade)
			info, _ := st.GetStatus(consumer.Named())
			if rendered || execution.Outcome() != schedule.Terminal || info.Status != store.StatusFailed || !strings.Contains(info.String(), "chart source "+source.Named().String()+" artifact content unavailable") {
				t.Fatalf("settlement lost artifact facts: outcome=%v status=%+v", execution.Outcome(), info)
			}
		})
	}
}

func TestDispatch_ExactMaterializers(t *testing.T) {
	t.Parallel()
	for _, state := range []store.Status{store.StatusPending, store.StatusFailed, store.StatusReady} {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			id := testutil.HelmReleaseID("apps", "missing")
			owner := &manifest.ResourceSet{Name: "a-owner", Namespace: "apps"}
			peer := &manifest.ResourceSet{Name: "z-failed", Namespace: "apps"}
			unrelated := &manifest.ResourceSet{Name: "unrelated", Namespace: "other"}
			st.AddObjects([]manifest.BaseManifest{owner, peer, unrelated})
			st.UpdateStatus(owner.Named(), store.StatusInfo{Status: state, Reason: "", Message: "owner state"})
			st.UpdateStatus(peer.Named(), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "peer failed"})
			st.UpdateStatus(unrelated.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "unrelated"})
			a := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "a"}
			z := a
			z.Name = "z"
			other := id
			other.Namespace = "other"
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id:    {Productions: []manifest.Production{{Owner: owner.Named(), Source: z}, {Owner: peer.Named(), Source: a}, {Owner: owner.Named(), Source: a}, {}, {Owner: id}}},
				other: {Productions: []manifest.Production{{Owner: unrelated.Named(), Source: a}}},
			}})
			run := controllertest.NewExecution(base.New(st, nil), id)
			run.Dispatch(t.Context(), manifest.DrainNone, struct{}{}, func(_ struct{}, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
				t.Error("missing declaration reached body")
				return nil
			})
			info, _ := st.GetStatus(id)
			if state == store.StatusPending {
				blocked, isBlocked := run.Blocked, run.Outcome() == schedule.Blocked
				if !isBlocked || !slices.Equal(blocked, []manifest.NamedResource{owner.Named()}) {
					t.Fatalf("blocked=%v error=%v, want exact pending owner once", blocked, info)
				}
			} else {
				failed, isFailed := info.BlockedBy(), info.IsDependencyFailure()
				if !isFailed || !slices.Equal(failed, []manifest.NamedResource{owner.Named(), peer.Named()}) {
					t.Fatalf("failure=%v, want sorted exact owners once", info)
				}
				if state == store.StatusReady && !strings.Contains(info.Message, "declaration not emitted") {
					t.Fatalf("ready owner reason = %q", info.Message)
				}
			}
		})
	}
}

func TestDispatch_ArrivalDuringClassification(t *testing.T) {
	t.Parallel()
	for _, suspended := range []bool{false, true} {
		t.Run(fmt.Sprint(suspended), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(2)
			parent := &manifest.Kustomization{Name: "publisher", Namespace: "apps"}
			child := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
			st.AddObject(child)
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{child.Named(): {Productions: []manifest.Production{{Owner: parent.Named()}}}}})
			entered, published := make(chan struct{}), make(chan struct{})
			c := base.New(st, tasks)
			c.Configure(base.Options{Existence: declarationPromotion(func(id manifest.NamedResource) bool {
				if id != parent.Named() {
					t.Errorf("unrelated promotion: %s", id)
					return false
				}
				close(entered)
				tasks.YieldSlot(func() { <-published })
				return true
			})})
			var run *base.Execution
			var bodies atomic.Int64
			tasks.Go(t.Context(), "dispatch", func(ctx context.Context) {
				run = controllertest.NewExecution(c, child.Named())
				run.Dispatch(ctx, manifest.DrainNone, struct{}{}, func(_ struct{}, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
					bodies.Add(1)
					return nil
				})
			})
			tasks.Go(t.Context(), "publish", func(context.Context) {
				tasks.YieldSlot(func() { <-entered })
				st.AddObject(parent)
				accepted := child.Clone()
				accepted.Suspend = suspended
				st.RecordEmitted(parent.Named(), &store.Emission{Children: []store.Child{{Obj: accepted}}}, nil)
				st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing peers"})
				close(published)
			})
			tasks.BlockTillDone()
			if tasks.Failures() != 0 || run.Valid() || bodies.Load() != 0 {
				t.Fatalf("stale dispatch continued: bodies=%d valid=%t", bodies.Load(), run.Valid())
			}
			run.Close()
			fresh := controllertest.NewExecution(c, child.Named())
			fresh.Dispatch(t.Context(), manifest.DrainNone, struct{}{}, func(_ struct{}, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
				bodies.Add(1)
				return nil
			})
			info, _ := st.GetStatus(child.Named())
			if suspended && (bodies.Load() != 0 || info.Outcome() != store.OutcomeSuspended) || !suspended && bodies.Load() != 1 {
				t.Fatalf("fresh declaration gate: bodies=%d status=%+v", bodies.Load(), info)
			}
		})
	}
}

type declarationPromotion func(manifest.NamedResource) bool

func (declarationPromotion) IsFileIndexed(manifest.NamedResource) bool { return false }
func (p declarationPromotion) Promote(_ context.Context, _ *store.ReadSet, id manifest.NamedResource) bool {
	return p(id)
}

func TestRequire_MaterializerPurpose(t *testing.T) {
	t.Parallel()
	for _, materializerFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(materializerFirst), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			owner := &manifest.ResourceSet{Name: "owner", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{consumer, owner})
			st.UpdateStatus(owner.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing"})
			run := controllertest.NewExecution(base.New(st, nil), consumer.Named())
			deps := []manifest.DependencyRef{{NamedResource: owner.Named()}, {NamedResource: owner.Named(), ReadyExpr: new("false")}}
			if materializerFirst {
				run.Reads.RequireMaterializer(owner.Named())
			}
			for _, dep := range deps {
				run.Reads.Require(dep)
			}
			run.Reads.RequireMaterializer(owner.Named())
			run.Dispatch(t.Context(), manifest.DrainNone, struct{}{}, func(_ struct{}, ctx context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
				return run.Require(ctx, deps, level)
			})
			if run.Outcome() != schedule.Blocked || !slices.Equal(run.Blocked, []manifest.NamedResource{owner.Named()}) || len(slices.Collect(run.Reads.Gates())) != 3 {
				t.Fatal("authored existence gate erased materializer readiness")
			}
			run.Settle(t.Context(), manifest.DrainForce)
			info, _ := st.GetStatus(consumer.Named())
			if !info.IsDependencyFailure() || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{owner.Named()}) {
				t.Fatalf("settlement lost gate purpose: %+v", info)
			}
		})
	}
}
