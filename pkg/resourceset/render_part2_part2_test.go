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
	st.AddObject(&(*testCaseInput.web))
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
