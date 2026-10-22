package depwait

import (
	"context"
	"testing"

	"github.com/home-operations/flate/pkg/schedule"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestResolve_SelectedData(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                                  string
		supplied, accepted, independent, self, other, pending bool
		coveredFile                                           bool
		want                                                  string
	}{
		{name: "supplied", supplied: true, other: true, want: "supplied"},
		{name: "accepted", accepted: true, other: true, want: "stored"},
		{name: "accepted while pending", accepted: true, other: true, pending: true, want: "stored"},
		{name: "independent", independent: true, other: true, want: "stored"},
		{name: "self", self: true, want: "stored"},
		{name: "covered prediction", other: true},
		{name: "owned indexed prediction", other: true, coveredFile: true},
		{name: "self and other", self: true, other: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			ks := &manifest.Kustomization{Name: "reader", Namespace: "apps"}
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
				status := store.StatusFailed
				if tc.pending {
					status = store.StatusPending
				}
				st.UpdateStatus(supplier, status, "", "successor")
			}
			supplied := map[manifest.NamedResource]manifest.BaseManifest{data.Named(): &manifest.Kustomization{Name: "not-data"}}
			if tc.supplied {
				supplied[data.Named()] = &manifest.ConfigMap{Name: data.Name, Namespace: data.Namespace, Data: map[string]any{"VALUE": "supplied"}}
			}
			var reads store.ReadSet
			reads.Init(st, ks.Named(), nil)
			defer reads.Retire()
			provider := values.NewStoreProvider(&reads, false, supplied)
			input := provider.Lookup(data.Named())
			w := Waiter{Store: &reads, Parent: ks.Named()}
			result := w.Available(t.Context(), DataDependency{NamedResource: data.Named(), Input: input, Optional: true}, manifest.DrainNone)
			if tc.want == "" {
				assert.Equal(t, result.Kind, schedule.Blocked)
				assert.Equal(t, input.Data, manifest.Data(nil))
			} else {
				assert.Equal(t, result.Kind, schedule.Terminal)
				selected := provider.Data(data.Named())
				if selected == nil {
					t.Fatal("usable accepted input was discarded")
				}
				got, ok, err := selected.Value("VALUE")
				assert.NoError(t, err)
				assert.Equal(t, ok, true)
				assert.Equal(t, got, tc.want)
			}
		})
	}
}

func TestResolvedProvider_AbsenceIsFrozen(t *testing.T) {
	t.Parallel()
	st := store.New()
	var reads store.ReadSet
	reads.Init(st, testutil.KustomizationID("apps", "reader"), nil)
	defer reads.Retire()
	provider := values.NewStoreProvider(&reads, false, nil)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	assert.Equal(t, provider.Data(id), manifest.Data(nil))
	st.AddObject(&manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "late"}})
	assert.Equal(t, provider.Data(id), manifest.Data(nil))
	assert.Equal(t, reads.Valid(), false)
	if values.NewStoreProvider(st, false, nil).Data(id) == nil {
		t.Fatal("new invocation missed arrival")
	}
}

func TestClassify_RepeatedInputFrozenPolicy(t *testing.T) {
	t.Parallel()
	st := store.New()
	data := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"VALUE": "first"}}
	supplied := map[manifest.NamedResource]manifest.BaseManifest{data.Named(): data}
	provider := values.NewStoreProvider(st, false, supplied)
	assert.Equal(t, provider.Data(data.Named()), manifest.Data(data))
	delete(supplied, data.Named())
	st.AddObject(&manifest.ConfigMap{Name: data.Name, Namespace: data.Namespace, Data: map[string]any{"VALUE": "second"}})
	assert.Equal(t, provider.Data(data.Named()), manifest.Data(data))
}

func TestClassify_RepeatedInputAllocations(t *testing.T) {
	st := store.New()
	data := &manifest.ConfigMap{Name: "input", Data: map[string]any{"VALUE": "first"}}
	st.AddObject(data)
	provider := values.NewStoreProvider(st, false, nil)
	provider.Lookup(data.Named())
	one := testing.AllocsPerRun(100, func() { provider.Lookup(data.Named()) })
	two := testing.AllocsPerRun(100, func() { provider.Lookup(data.Named()); provider.Lookup(data.Named()) })
	assert.Equal(t, two, one)
	assert.Equal(t, one, float64(0))
}

func TestResolve_CoveredPrediction(t *testing.T) {
	t.Parallel()
	st := store.New()
	supplier := testutil.KustomizationID("apps", "supplier")
	id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	declaration := &manifest.RawObject{Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Name: "declaration", Namespace: "apps", Spec: map[string]any{"target": map[string]any{"name": id.Name}, "data": []any{map[string]any{"secretKey": "VALUE", "remoteRef": map[string]any{"key": "fixture"}}}}}
	st.AddObject(&manifest.Kustomization{Name: supplier.Name, Namespace: supplier.Namespace})
	st.AddObject(declaration)
	st.AddObject(&manifest.Secret{Name: id.Name, Namespace: id.Namespace})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Productions: []manifest.Production{{Owner: supplier, Declaration: declaration.Named()}}}}})
	var before store.ReadSet
	parent := testutil.KustomizationID("apps", "reader")
	before.Init(st, parent, nil)
	input := values.NewStoreProvider(&before, true, nil).Lookup(id)
	w := Waiter{Store: &before, Parent: parent}
	result := w.Available(t.Context(), DataDependency{NamedResource: id, Input: input, Optional: true}, manifest.DrainNone)
	assert.Equal(t, result.Kind, schedule.Blocked)
	assert.Equal(t, input.Data, manifest.Data(nil))
	st.RecordEmitted(supplier, &store.Emission{Children: []store.Child{{Obj: declaration}}, Complete: true})
	before.Retire()
	var after store.ReadSet
	after.Init(st, parent, nil)
	defer after.Retire()
	data := values.NewStoreProvider(&after, true, nil).Data(id)
	if data == nil {
		t.Fatal("settled declaration lost declared keys")
	}
	value, found, err := data.Value("VALUE")
	assert.NoError(t, err)
	assert.Equal(t, found, true)
	assert.Equal(t, value, "..PLACEHOLDER_VALUE..")
}

type inputPromotion struct {
	store       *store.Store
	data        manifest.BaseManifest
	calls       int
	materialize bool
	before      func()
}

func (p *inputPromotion) IsFileIndexed(manifest.NamedResource) bool { return true }
func (p *inputPromotion) Promote(_ context.Context, _ *store.ReadSet, _ manifest.NamedResource) bool {
	p.calls++
	if p.before != nil {
		p.before()
	}
	if p.materialize {
		p.store.AddObject(p.data)
	}
	return p.materialize
}
