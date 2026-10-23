package depwait

import (
	"context"
	"errors"
	"github.com/home-operations/flate/pkg/task"
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestResolve_SelectedData(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                         string
		supplied, accepted, independent, self, other bool
		coveredFile                                  bool
		want                                         string
	}{
		{name: "supplied", supplied: true, other: true, want: "supplied"},
		{name: "accepted", accepted: true, other: true, want: "stored"},
		{name: "independent", independent: true, other: true, want: "stored"},
		{name: "self", self: true, want: "stored"},
		{name: "covered prediction", other: true},
		{name: "owned indexed prediction", other: true, coveredFile: true},
		{name: "self and other", self: true, other: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			ks := &manifest.Kustomization{Name: "reader", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "input", Optional: true}}}}
			data := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"VALUE": "stored"}}
			supplier := testutil.KustomizationID("apps", "supplier")
			st.AddObject(data)
			var productions []manifest.Production
			if tc.other {
				production := manifest.Production{Owner: supplier}
				if tc.coveredFile {
					production.Path = "covered.yaml"
				}
				productions = append(productions, production)
			}
			if tc.self {
				productions = append(productions, manifest.Production{Owner: ks.Named()})
			}
			if tc.independent {
				productions = append(productions, manifest.Production{Path: "static.yaml"})
			}
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{data.Named(): {Productions: productions}}})
			if tc.accepted {
				st.AddObject(&manifest.Kustomization{Name: supplier.Name, Namespace: supplier.Namespace})
				st.RecordEmitted(supplier, &store.Emission{Children: []store.Child{{Obj: data}}, Complete: true})
				st.UpdateStatus(supplier, store.StatusFailed, "", "successor failed")
			}
			supplied := map[manifest.NamedResource]manifest.BaseManifest{data.Named(): &manifest.Kustomization{Name: "not-data"}}
			if tc.supplied {
				supplied[data.Named()] = &manifest.ConfigMap{Name: data.Name, Namespace: data.Namespace, Data: map[string]any{"VALUE": "supplied"}}
			}
			w := Waiter{Store: st, Parent: ks.Named(), Declaration: ks, Supplied: supplied}
			var resolved Resolution
			w.Classify(manifest.ReconcileAttempt{}, &resolved)
			if tc.want == "" {
				if _, ok := errors.AsType[*ErrBlocked](resolved.Err()); !ok {
					t.Fatalf("covered prediction admitted: %v", resolved.Err())
				}
				assert.Equal(t, resolved.Inputs.Data(data.Named()), manifest.Data(nil))
			} else {
				assert.NoError(t, resolved.Err())
				got, ok, err := resolved.Inputs.Data(data.Named()).Value("VALUE")
				assert.NoError(t, err)
				assert.Equal(t, ok, true)
				assert.Equal(t, got, tc.want)
			}
		})
	}
}

func TestClassify_RepeatedInputAllocations(t *testing.T) {
	st := store.New()
	declaration := &manifest.RawObject{Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Name: "declaration", Namespace: "apps", Spec: map[string]any{"target": map[string]any{"name": "input"}, "data": []any{map[string]any{"secretKey": "VALUE", "remoteRef": map[string]any{"key": "fixture"}}}}}
	st.AddObject(declaration)
	id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Productions: []manifest.Production{{Declaration: declaration.Named()}}}}})
	ks := &manifest.Kustomization{Name: "reader", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: id.Kind, Name: id.Name}}}}
	w := Waiter{Store: st, Parent: ks.Named(), Declaration: ks, WipeSecrets: true}
	check := func() {
		t.Helper()
		var r Resolution
		w.Classify(manifest.ReconcileAttempt{}, &r)
		assert.NoError(t, r.Err())
		assert.Equal(t, len(r.Inputs), 1)
		got, found, err := r.Inputs[0].Value.Value("VALUE")
		assert.NoError(t, err)
		assert.Equal(t, found, true)
		assert.Equal(t, got, "..PLACEHOLDER_VALUE..")
		assert.Diff(t, slices.Collect(r.Inputs[0].Value.Keys()), []string{"VALUE"})
	}
	check()
	one := testing.AllocsPerRun(100, func() {
		var r Resolution
		w.Classify(manifest.ReconcileAttempt{}, &r)
	})
	ks.PostBuild.SubstituteFrom = append(ks.PostBuild.SubstituteFrom, ks.PostBuild.SubstituteFrom[0])
	check()
	two := testing.AllocsPerRun(100, func() {
		var r Resolution
		w.Classify(manifest.ReconcileAttempt{}, &r)
	})
	t.Logf("classification allocations: one=%g repeated=%g", one, two)
	assert.Equal(t, two, one)
}

func TestResolvedProvider_AbsenceIsFrozen(t *testing.T) {
	t.Parallel()
	st := store.New()
	ks := &manifest.Kustomization{Name: "reader", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "input", Optional: true}}}}
	w := Waiter{Store: st, Parent: ks.Named(), Declaration: ks}
	var r Resolution
	w.Classify(manifest.ReconcileAttempt{}, &r)
	assert.NoError(t, r.Err())
	assert.Equal(t, len(r.Inputs), 1)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	st.AddObject(&manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "late"}})
	if r.Inputs.Data(id) != nil {
		t.Fatal("frozen absence fell through to Store")
	}
	if values.NewStoreProvider(st, false).Data(id) == nil {
		t.Fatal("data-only provider did not observe arrival")
	}
}

type inputPromotion struct {
	store       *store.Store
	data        manifest.BaseManifest
	calls       int
	materialize bool
	before      func()
}

func (p *inputPromotion) IsFileIndexed(manifest.NamedResource) bool { return true }
func (p *inputPromotion) Promote(manifest.NamedResource) bool {
	p.calls++
	if p.before != nil {
		p.before()
	}
	if p.materialize {
		p.store.AddObject(p.data)
	}
	return p.materialize
}

func TestClassify_RepeatedInputFrozenPolicy(t *testing.T) {
	t.Parallel()
	st := store.New()
	data := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"VALUE": "first"}}
	ks := &manifest.Kustomization{Name: "reader", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
		{Kind: manifest.KindConfigMap, Name: data.Name, Optional: true},
		{Kind: manifest.KindConfigMap, Name: "trigger", Optional: true},
		{Kind: manifest.KindConfigMap, Name: data.Name},
	}}}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{data.Named(): {Productions: []manifest.Production{{Path: "input.yaml"}}}}})
	supplied := map[manifest.NamedResource]manifest.BaseManifest{data.Named(): data}
	promotion := &inputPromotion{store: st, before: func() { delete(supplied, data.Named()) }}
	w := Waiter{Store: st, Parent: ks.Named(), Declaration: ks, Supplied: supplied, Existence: promotion}
	var r Resolution
	w.Classify(manifest.ReconcileAttempt{}, &r)
	assert.NoError(t, r.Err())
	assert.Equal(t, promotion.calls, 1)
	assert.Equal(t, r.Inputs.Data(data.Named()), manifest.Data(data))
	assert.Equal(t, len(r.Inputs), 2)
	assert.Equal(t, len(r.Omitted), 0)
}

func TestResolve_FileIndexedData(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                  string
		materialize, supplied bool
	}{
		{name: "missing"},
		{name: "promoted", materialize: true},
		{name: "supplied", materialize: true, supplied: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			data := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"VALUE": "file"}}
			ks := &manifest.Kustomization{Name: "reader", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: data.Named().Kind, Name: data.Name}}}}
			promotion := &inputPromotion{store: st, data: data, materialize: tc.materialize}
			w := Waiter{Store: st, Parent: ks.Named(), Declaration: ks, Existence: promotion}
			if tc.supplied {
				w.Supplied = map[manifest.NamedResource]manifest.BaseManifest{data.Named(): data}
			}
			var r Resolution
			w.Classify(manifest.ReconcileAttempt{Drain: manifest.DrainCascade}, &r)
			wantPromotions := 1
			if tc.supplied {
				wantPromotions = 0
			}
			assert.Equal(t, promotion.calls, wantPromotions)
			if tc.materialize {
				assert.NoError(t, r.Err())
				assert.Equal(t, r.Inputs.Data(data.Named()), manifest.Data(data))
			} else {
				failure, ok := errors.AsType[*manifest.DependencyFailedError](r.Err())
				if !ok {
					t.Fatalf("missing input error=%v", r.Err())
				}
				assert.Diff(t, failure.Failed, []manifest.NamedResource{data.Named()})
				assert.Equal(t, failure.Reasons[data.Named()], "dependency not found")
			}
		})
	}
}

type resolutionDispatcher func(context.Context, schedule.NodeID, manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait)

func (d resolutionDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait) {
	return d(ctx, id, attempt)
}

func TestResolve_OptionalFailureCauses(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			supplier := testutil.KustomizationID("flux-system", "supplier")
			ks := &manifest.Kustomization{Name: "reader", Namespace: "flux-system", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "optional", Optional: true}, {Kind: manifest.KindConfigMap, Name: "cluster-vars"}}}}
			var reader manifest.BaseManifest = ks
			if kind == manifest.KindHelmRelease {
				reader = &manifest.HelmRelease{Name: ks.Name, Namespace: ks.Namespace, ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "optional", Optional: true}, {Kind: manifest.KindConfigMap, Name: "cluster-vars"}}}
			}

			target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: ks.Namespace, Name: "optional"}
			required := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: ks.Namespace, Name: "cluster-vars"}
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{target: {Productions: []manifest.Production{{Owner: supplier}}}}})
			var failed *manifest.DependencyFailedError
			d := resolutionDispatcher(func(_ context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait) {
				if id == supplier {
					st.UpdateStatus(id, store.StatusFailed, "", "supplier failed")
					return schedule.Outcome{}, manifest.ReconcileWait{}
				}
				w := Waiter{Store: st, Parent: reader.Named(), Declaration: reader}
				var r Resolution
				w.Classify(attempt, &r)
				if blocked, ok := errors.AsType[*ErrBlocked](r.Err()); ok {
					return schedule.Outcome{Kind: schedule.Blocked}, blocked.ReconcileWait
				}
				var ok bool
				failed, ok = errors.AsType[*manifest.DependencyFailedError](r.Err())
				if !ok {
					t.Errorf("expected required failure, got %v", r.Err())
				}
				return schedule.Outcome{}, manifest.ReconcileWait{}
			})
			sched := schedule.New(task.NewBounded(2), d)
			unsub := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
				info := payload.(store.StatusInfo)
				sched.OnStatusWake(id, info.Status == store.StatusReady, info.Status == store.StatusFailed)
			}, false)
			defer unsub()
			sched.Seed([]schedule.NodeID{reader.Named(), supplier})
			assert.NoError(t, sched.Run(t.Context()))
			if failed == nil {
				t.Fatal("required failure missing")
			}
			assert.Diff(t, failed.Failed, []manifest.NamedResource{required})
			assert.Diff(t, failed.Reasons, map[manifest.NamedResource]string{required: "dependency not found"})
			assert.Equal(t, failed.Error(), "dependencies failed: ConfigMap/flux-system/cluster-vars: dependency not found")
		})
	}
}

func TestResolve_CoveredPrediction(t *testing.T) {
	t.Parallel()
	st := store.New()
	supplier := testutil.KustomizationID("apps", "supplier")
	id := manifest.NamedResource{Kind: manifest.KindSecret, Name: "input", Namespace: "apps"}
	declaration := &manifest.RawObject{Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Name: "declaration", Namespace: "apps", Spec: map[string]any{"target": map[string]any{"name": id.Name}, "data": []any{map[string]any{"secretKey": "VALUE", "remoteRef": map[string]any{"key": "fixture"}}}}}
	st.AddObject(declaration)
	st.AddObject(&manifest.Secret{Name: id.Name, Namespace: id.Namespace})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Productions: []manifest.Production{{Owner: supplier, Declaration: declaration.Named()}}}}})
	ks := &manifest.Kustomization{Name: "reader", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: id.Kind, Name: id.Name}}}}
	var selected manifest.Data
	release := make(chan struct{})
	observed := make(chan error, 1)
	d := resolutionDispatcher(func(_ context.Context, n schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait) {
		if n == supplier {
			<-release
			st.UpdateStatus(n, store.StatusReady, "", "")
			return schedule.Outcome{}, manifest.ReconcileWait{}
		}
		w := Waiter{Store: st, Parent: ks.Named(), Declaration: ks, WipeSecrets: true}
		var result Resolution
		w.Classify(attempt, &result)
		if (attempt.Content != nil && len(attempt.Content.Suppliers) > 0) || result.Err() == nil {
			observed <- result.Err()
		}
		if blocked, ok := errors.AsType[*ErrBlocked](result.Err()); ok {
			return schedule.Outcome{Kind: schedule.Blocked}, blocked.ReconcileWait
		}
		if err := result.Err(); err != nil {
			t.Errorf("admission error=%v", err)
		}
		selected = result.Inputs.Data(id)
		return schedule.Outcome{}, manifest.ReconcileWait{}
	})
	sched := schedule.New(task.NewBounded(2), d)
	sched.Seed([]schedule.NodeID{supplier, ks.Named()})
	done := make(chan error, 1)
	go func() { done <- sched.Run(t.Context()) }()
	if _, blocked := errors.AsType[*ErrBlocked](<-observed); !blocked {
		t.Error("pending offline supplier allowed declared-key selection")
	}
	close(release)
	assert.NoError(t, <-done)
	if selected == nil {
		t.Fatal("settled covered prediction suppressed declared-key placeholders")
	}
	value, found, err := selected.Value("VALUE")
	assert.NoError(t, err)
	assert.Equal(t, found, true)
	assert.Equal(t, value, "..PLACEHOLDER_VALUE..")
}
