package depwait

import (
	"context"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

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
			testResolveSelectedDataCase(t, &tc)
		},
		)
	}
}

func TestResolvedProvider_AbsenceIsFrozen(t *testing.T) {
	t.Parallel()
	st := store.New()
	var reads store.ReadSet
	reads.Init(st, testutil.KustomizationID("apps", "reader"), nil)
	defer reads.Retire()
	provider := values.NewStoreProvider(&reads)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	assert.Equal(t, provider.Data(id), manifest.Data(nil))
	st.AddObject(&manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "late"}})
	assert.Equal(t, provider.Data(id), manifest.Data(nil))
	assert.Equal(t, reads.Valid(), false)
	assert.FatalIf(t, values.NewStoreProvider(st).Data(id) == nil, "%v", "new invocation missed arrival")
}

func TestClassify_RepeatedInputFrozenPolicy(t *testing.T) {
	t.Parallel()
	st := store.New()
	data := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"VALUE": "first"}}
	st.AddDeclaration(
		store.Declaration{Obj: data, Doc: testutil.ObjectDocument(data), Origin: manifest.SuppliedDeclaration},
	)
	provider := values.NewStoreProvider(st)
	captured := provider.Data(data.Named())
	result1, err := captured.Value("VALUE")
	value := result1.Value
	assert.NoError(t, err)
	assert.Equal(t, value, "first")
	data.Data["VALUE"] = "caller mutation"
	st.AddObject(
		&manifest.ConfigMap{Name: data.Name, Namespace: data.Namespace, Data: map[string]any{"VALUE": "second"}},
	)
	assert.Equal(t, provider.Data(data.Named()), captured)
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestClassify_RepeatedInputAllocations(t *testing.T) {
	st := store.New()
	data := &manifest.ConfigMap{Name: "input", Data: map[string]any{"VALUE": "first"}}
	st.AddObject(data)
	provider := values.NewStoreProvider(st)
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
	declaration := &manifest.RawObject{
		Kind:       "ExternalSecret",
		APIVersion: "external-secrets.io/v1",
		Name:       "declaration",
		Namespace:  "apps",
		Spec: map[string]any{
			"target": map[string]any{"name": id.Name},
			"data":   []any{map[string]any{"secretKey": "VALUE", "remoteRef": map[string]any{"key": "fixture"}}},
		},
	}
	st.AddObject(&manifest.Kustomization{Name: supplier.Name, Namespace: supplier.Namespace})
	st.AddObject(declaration)
	st.AddObject(&manifest.Secret{Name: id.Name, Namespace: id.Namespace})
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Productions: []manifest.Production{{Owner: supplier, Declaration: declaration.Named()}}},
			},
		},
	)
	var before store.ReadSet
	parent := testutil.KustomizationID("apps", "reader")
	before.Init(st, parent, nil)
	input := values.NewStoreProvider(&before).Lookup(id)
	w := Waiter{
		Store: &before, Parent: parent, Missing: NewMissingPolicy(
			nil),
	}
	result := w.Available(
		t.Context(),
		DataDependency{NamedResource: id, Optional: true, Input: input},
		manifest.DrainNone,
	)
	assert.Equal(t, result.Kind, schedule.Blocked)
	assert.Equal(t, input.Data, manifest.Data(nil))
	st.RecordEmitted(supplier, &store.Emission{Children: []store.Child{{Obj: declaration}}, Complete: true}, nil)
	before.Retire()
	var after store.ReadSet
	after.Init(st, parent, nil)
	defer after.Retire()
	data := values.NewStoreProvider(&after).Data(id)
	assert.FatalIf(t, data != nil, "%s", "settled declaration fabricated data")
}

type inputPromotion struct {
	store       *store.Store
	data        manifest.BaseManifest
	calls       int
	materialize bool
	before      func()
	request     selection.PrepareRequest
}

func (p *inputPromotion) IsFileIndexed(manifest.NamedResource) bool { return true }

//nolint:gocritic // The admission contract requires an independent request snapshot.
func (p *inputPromotion) Promote(
	_ context.Context,
	request selection.PrepareRequest,
	_ manifest.NamedResource,
) (bool, error) {
	p.calls++
	p.request = request
	if p.before != nil {
		p.before()
	}
	if p.materialize {
		p.store.AddObject(p.data)
		id := request.Reads.ID()
		request.Reads.Retire()
		request.Reads.Init(p.store, id, nil)
	}
	return p.materialize, nil
}

func testResolveSelectedDataCase(t *testing.T, testCaseInput *struct {
	name string

	supplied    bool
	accepted    bool
	independent bool
	self        bool
	other       bool
	pending     bool
	coveredFile bool
	want        string
},
) {
	t.Helper()

	st := store.New()
	ks := &manifest.Kustomization{Name: "reader", Namespace: "apps"}
	data := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"VALUE": "stored"}}
	supplier := testutil.KustomizationID("apps", "supplier")
	st.AddObject(data)
	var productions []manifest.Production
	if testCaseInput.other {
		production := manifest.Production{Owner: supplier}
		if testCaseInput.coveredFile {
			production.Path = "covered.yaml"
		}
		productions = append(productions, production)
	}
	if testCaseInput.self {
		productions = append(productions, manifest.Production{Owner: ks.Named()})
	}
	if testCaseInput.independent {
		productions = append(productions, manifest.Production{Path: "static.yaml"})
	}
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				data.Named(): {Productions: productions},
			},
		},
	)
	if testCaseInput.accepted {
		st.AddObject(&manifest.Kustomization{Name: supplier.Name, Namespace: supplier.Namespace})
		st.RecordEmitted(supplier, &store.Emission{Children: []store.Child{{Obj: data}}, Complete: true}, nil)
		status := store.StatusFailed
		if testCaseInput.pending {
			status = store.StatusPending
		}
		st.UpdateStatus(supplier, store.StatusInfo{Status: status, Reason: "", Message: "successor"})
	}
	if testCaseInput.supplied {
		supplied := &manifest.ConfigMap{
			Name:      data.Name,
			Namespace: data.Namespace,
			Data:      map[string]any{"VALUE": "supplied"},
		}
		st.AddDeclaration(
			store.Declaration{
				Obj:    supplied,
				Doc:    testutil.ObjectDocument(supplied),
				Origin: manifest.SuppliedDeclaration,
			},
		)
	}
	var reads store.ReadSet
	reads.Init(st, ks.Named(), nil)
	defer reads.Retire()
	provider := values.NewStoreProvider(&reads)
	input := provider.Lookup(data.Named())
	w := Waiter{
		Store: &reads, Parent: ks.Named(), Missing: NewMissingPolicy(
			nil),
	}
	result := w.Available(
		t.Context(),
		DataDependency{NamedResource: data.Named(), Optional: true, Input: input},
		manifest.DrainNone,
	)
	if testCaseInput.want == "" {
		assert.Equal(t, result.Kind, schedule.Blocked)
		assert.Equal(t, input.Data, manifest.Data(nil))
	} else {
		assert.Equal(t, result.Kind, schedule.Terminal)
		selected := provider.Data(data.Named())
		assert.FatalIf(t, selected == nil, "%v", "usable accepted input was discarded")
		result2, err := selected.Value("VALUE")
		got, ok := result2.Value, result2.Found
		assert.NoError(t, err)
		assert.Equal(t, ok, true)
		assert.Equal(t, got, testCaseInput.want)
	}
}
