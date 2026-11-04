package resourceset_test

import (
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
)

type testStoreResolverSelectorInputsCase2Inputs struct {
	db      **manifest.ResourceSetInputProvider
	invalid **metav1.LabelSelector
	scope   *manifest.NamedResource
	tc      *struct {
		name  string
		prior *metav1.LabelSelector
	}
}

func testStoreResolverSelectorInputsCase2(t *testing.T, testCaseInput *testStoreResolverSelectorInputsCase2Inputs) {
	t.Helper()

	st := store.New()
	st.AddObject((*testCaseInput.db))
	resolve := resourceset.StoreResolver(st)
	if testCaseInput.tc.prior != nil {
		got, err := resolve(
			fluxopv1.InputProviderReference{Selector: testCaseInput.tc.prior},
			(*testCaseInput.scope),
		)
		assert.NoError(t, err)
		assert.Equal(t, len(got), 1)
	}
	ref := fluxopv1.InputProviderReference{Selector: (*testCaseInput.invalid)}
	empty := (*testCaseInput.scope)
	empty.Namespace = "empty"
	for range 2 {
		{
			_, err := resolve(ref, (*testCaseInput.scope))
			assert.FatalIf(t, err == nil, "%s", "invalid selector accepted with an eligible provider")
		}
		got, err := resolve(ref, empty)
		{
			assert.FatalIf(
				t,
				err == nil || len(got) != 0,
				"empty scope query = %v, %v; want selector error",
				got,
				err,
			)
		}
	}
}

type testStoreResolverSelectorInputsDistinctSelectorsAndLiveProvidersInputs struct {
	db       **manifest.ResourceSetInputProvider
	web      *manifest.ResourceSetInputProvider
	selected **metav1.LabelSelector
	scope    *manifest.NamedResource
}

func testStoreResolverSelectorInputsDistinctSelectorsAndLiveProviders(
	t *testing.T,
	testCaseInput *testStoreResolverSelectorInputsDistinctSelectorsAndLiveProvidersInputs,
) {
	t.Helper()

	st := store.New()
	st.AddObject((*testCaseInput.db))
	st.AddObject(testCaseInput.web)
	resolve := resourceset.StoreResolver(st)
	webSelector := &metav1.LabelSelector{MatchLabels: testCaseInput.web.Labels}
	for _, tc := range []struct {
		selector *metav1.LabelSelector
		want     manifest.NamedResource
	}{{(*testCaseInput.selected), (*testCaseInput.db).Named()}, {webSelector, testCaseInput.web.Named()}, {(*testCaseInput.selected), (*testCaseInput.db).Named()}, {webSelector, testCaseInput.web.Named()}} {
		got, err := resolve(fluxopv1.InputProviderReference{Selector: tc.selector}, (*testCaseInput.scope))
		assert.NoError(t, err)
		{
			assert.FatalIf(t, len(got) != 1 || got[0].Named() != tc.want, "providers=%v, want %s", got, tc.want)
		}
	}
	fresh := *(*testCaseInput.db)
	fresh.Labels = map[string]string{"role": "web"}
	fresh.DefaultValues = fluxopv1.ResourceSetInput{"value": jsonTmpl(t, `"updated"`)}
	st.AddObject(&fresh)
	got, err := resolve(fluxopv1.InputProviderReference{Selector: (*testCaseInput.selected)}, (*testCaseInput.scope))
	assert.NoError(t, err)
	assert.Equal(t, len(got), 0)
	got, err = resolve(fluxopv1.InputProviderReference{Selector: webSelector}, (*testCaseInput.scope))
	assert.NoError(t, err)
	{
		assert.FatalIf(
			t,
			len(got) != 2 || got[0].Named() != (*testCaseInput.db).Named() ||
				got[1].Named() != testCaseInput.web.Named(),
			"updated providers=%v, want db and web",
			got,
		)
	}
	plan, err := got[0].InputPlan()
	inputs := plan.Inputs
	assert.NoError(t, err)
	assert.Equal(t, inputs[0]["value"], "updated")
}

func TestStoreResolver_CollectionAssessmentTotal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		provider  bool
		selector  *metav1.LabelSelector
		wantError bool
		wantDocs  int
	}{
		{name: "empty", selector: &metav1.LabelSelector{}},
		{name: "null values", provider: true, selector: &metav1.LabelSelector{}, wantDocs: 1},
		{name: "selector error", selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}}}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			if tc.provider {
				st.AddObject(
					&manifest.ResourceSetInputProvider{
						Name:          "provider",
						Namespace:     "apps",
						DefaultValues: fluxopv1.ResourceSetInput{"value": nil},
					},
				)
			}
			rs := &manifest.ResourceSet{
				Name:              "reader",
				Namespace:         "apps",
				InputsFrom:        []fluxopv1.InputProviderReference{{Selector: tc.selector}},
				ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n",
			}
			var reads store.ReadSet
			reads.Init(st, rs.Named(), nil)
			defer reads.Retire()
			docs, _, err := renderProviders(rs, resourceset.StoreResolver(&reads), nil)
			{
				assert.FatalIf(t, (err != nil) != tc.wantError, "prepare error=%v, want error=%t", err, tc.wantError)
			}
			if tc.wantError {
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, len(docs), tc.wantDocs)
			assert.Equal(t, reads.Valid(), true)
		})
	}
}

func TestStoreResolver_SelectorInputs(t *testing.T) {
	t.Parallel()
	db := &manifest.ResourceSetInputProvider{
		Name:          "db",
		Namespace:     "apps",
		Labels:        map[string]string{"role": "db"},
		DefaultValues: fluxopv1.ResourceSetInput{"value": jsonTmpl(t, `"first"`)},
	}
	web := *db
	web.Name, web.Labels = "web", map[string]string{"role": "web"}
	other := *db
	other.Namespace = "other"
	invalid := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}},
	}
	selected := &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}}
	scope := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSetInputProvider,
		Namespace: "apps",
	}
	for _, tc := range []struct {
		name      string
		providers []manifest.BaseManifest
		ref       fluxopv1.InputProviderReference
		id        manifest.NamedResource
		want      []string
		wantError bool
	}{
		{"success", []manifest.BaseManifest{db, &web}, fluxopv1.InputProviderReference{Selector: selected}, scope, []string{"db"}, false},
		{"empty selector", []manifest.BaseManifest{db, &web}, fluxopv1.InputProviderReference{Selector: &metav1.LabelSelector{}}, scope, []string{"db", "web"}, false},
		{"invalid eligible", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{Selector: invalid}, scope, nil, true},
		{"invalid empty", nil, fluxopv1.InputProviderReference{Selector: invalid}, scope, nil, true},
		{"invalid other namespace", []manifest.BaseManifest{&other}, fluxopv1.InputProviderReference{Selector: invalid}, scope, nil, true},
		{"invalid foreign group", []manifest.BaseManifest{db, &manifest.RawObject{APIVersion: "other.example/v1", Kind: scope.Kind, Namespace: scope.Namespace, Name: db.Name}}, fluxopv1.InputProviderReference{Selector: invalid}, manifest.NamedResource{Group: "other.example", Kind: scope.Kind, Namespace: scope.Namespace}, nil, true},
		{"invalid foreign kind", []manifest.BaseManifest{db, &manifest.ResourceSet{Name: db.Name, Namespace: scope.Namespace}}, fluxopv1.InputProviderReference{Selector: invalid}, manifest.NamedResource{Group: scope.Group, Kind: manifest.KindResourceSet, Namespace: scope.Namespace}, nil, true},
		{"named invalid selector", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{Selector: invalid}, db.Named(), []string{"db"}, false},
		{"unnamed nil selector", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{}, scope, nil, false},
		{"omitted group and kind", []manifest.BaseManifest{db, &web}, fluxopv1.InputProviderReference{Selector: selected}, manifest.NamedResource{Namespace: scope.Namespace}, []string{"db"}, false},
		{"omitted group", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{Selector: invalid}, manifest.NamedResource{Kind: scope.Kind, Namespace: scope.Namespace}, nil, true},
		{"omitted kind", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{Selector: invalid}, manifest.NamedResource{Group: scope.Group, Namespace: scope.Namespace}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testStoreResolverSelectorInputsCase(t, &tc)
		},
		)
	}
	for _, tc := range []struct {
		name  string
		prior *metav1.LabelSelector
	}{
		{"first cached error in empty scope", nil},
		{"additional cached error in empty scope", selected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testStoreResolverSelectorInputsCase2(
				t,
				&testStoreResolverSelectorInputsCase2Inputs{db: &db, invalid: &invalid, scope: &scope, tc: &tc},
			)
		},
		)
	}
	t.Run("distinct selectors and live providers", func(t *testing.T) {
		t.Parallel()
		testStoreResolverSelectorInputsDistinctSelectorsAndLiveProviders(
			t,
			&testStoreResolverSelectorInputsDistinctSelectorsAndLiveProvidersInputs{
				db:       &db,
				web:      &web,
				selected: &selected,
				scope:    &scope,
			},
		)
	},
	)
}

func testStoreResolverSelectorInputsCase(t *testing.T, testCaseInput *struct {
	name      string
	providers []manifest.BaseManifest
	ref       fluxopv1.InputProviderReference
	id        manifest.NamedResource
	want      []string
	wantError bool
},
) {
	t.Helper()

	st := store.New()
	for _, p := range testCaseInput.providers {
		st.AddObject(p)
	}
	resolve := resourceset.StoreResolver(st)
	for range 2 {
		got, err := resolve(testCaseInput.ref, testCaseInput.id)
		if (err != nil) != testCaseInput.wantError {
			t.Fatalf("resolve error=%v, want error=%t", err, testCaseInput.wantError)
		}
		names := make([]string, 0, len(got))
		if len(got) == 0 {
			names = nil
		}
		for _, p := range got {
			names = append(names, p.Name)
		}
		assert.Diff(t, names, testCaseInput.want)
	}
}
