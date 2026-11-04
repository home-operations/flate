package resourceset

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
)

func TestResourceSetFingerprint_SelectorInputs(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name:      "matrix",
		Namespace: "apps",
		InputsFrom: []fluxopv1.InputProviderReference{{
			Kind:     manifest.KindResourceSetInputProvider,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}},
		}},
		ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.provider.name >>'}\ndata: {value: '<< inputs.value >>'}\n",
	}
	a := staticRSIP("a", "apps", map[string]string{"role": "db"}, map[string]string{"id": "one", "value": "first"})
	b := staticRSIP("b", "apps", map[string]string{"role": "db"}, map[string]string{"id": "two", "value": "second"})
	s := store.New()
	s.AddObject(b)
	s.AddObject(a)
	resolve := resourceset.StoreResolver(s)
	renderWithAdmissionResult1, renderWithAdmissionResult1Err := renderWithAdmission(rs, resolve, "")
	docs, fp, err := renderWithAdmissionResult1.documents, renderWithAdmissionResult1.fingerprint, renderWithAdmissionResult1Err
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 2)
	encoded, err := json.Marshal(docs)
	assert.NoError(t, err)
	assert.FatalIf(t, fp == "", "%v", "empty fingerprint")
	other := store.New()
	other.AddObject(a)
	other.AddObject(b)
	otherResolve := resourceset.StoreResolver(other)
	renderWithAdmissionResult2, renderWithAdmissionResult2Err := renderWithAdmission(rs, otherResolve, "")
	otherDocs, otherFP, err := renderWithAdmissionResult2.documents, renderWithAdmissionResult2.fingerprint, renderWithAdmissionResult2Err
	assert.Equal(t, otherFP, fp)
	assert.NoError(t, err)
	otherEncoded, err := json.Marshal(otherDocs)
	assert.NoError(t, err)
	assert.Equal(t, string(otherEncoded), string(encoded))

	for _, field := range []string{"name", "namespace", "provider type", "provider identity"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			testResourceSetFingerprintSelectorInputsCase(
				t,
				testResourceSetFingerprintSelectorInputsCaseInputs{rs: &rs, a: &a, b: &b, fp: &fp, field: &field},
			)
		},
		)
	}

	t.Run("resolver error cannot replay", func(t *testing.T) {
		t.Parallel()
		testResourceSetFingerprintSelectorInputsResolverErrorCannotReplay(
			t,
			testResourceSetFingerprintSelectorInputsResolverErrorCannotReplayInputs{rs: &rs, fp: &fp},
		)
	},
	)
	t.Run("inline order", func(t *testing.T) { t.Parallel(); testResourceSetFingerprintSelectorInputsInlineOrder(t) })

	t.Run("resolver delivery order", func(t *testing.T) {
		t.Parallel()
		testResourceSetFingerprintSelectorInputsResolverDeliveryOrder(
			t,
			testResourceSetFingerprintSelectorInputsResolverDeliveryOrderInputs{
				rs:   &rs,
				a:    &a,
				b:    &b,
				docs: &docs,
				fp:   &fp,
			},
		)
	},
	)

	invalid := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}},
	}
	for _, tc := range []struct {
		name   string
		refs   []fluxopv1.InputProviderReference
		inputs []any
	}{
		{"Flux named", []fluxopv1.InputProviderReference{{APIVersion: fluxopv1.GroupVersion.String(), Kind: manifest.KindResourceSetInputProvider, Name: "a"}}, []any{a}},
		{"foreign named", []fluxopv1.InputProviderReference{{APIVersion: "other.example/v1", Kind: manifest.KindResourceSetInputProvider, Name: "a"}}, nil},
		{"Flux selector", []fluxopv1.InputProviderReference{{APIVersion: fluxopv1.GroupVersion.String(), Kind: manifest.KindResourceSetInputProvider, Selector: rs.InputsFrom[0].Selector}}, []any{a, b}},
		{"foreign selector", []fluxopv1.InputProviderReference{{APIVersion: "other.example/v1", Kind: manifest.KindResourceSetInputProvider, Selector: rs.InputsFrom[0].Selector}}, nil},
		{"expressions", []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: metav1.LabelSelectorOpIn, Values: []string{"db"}}}}}}, []any{a, b}},
		{"empty selector", []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{}}}, []any{a, b}},
		{"named invalid selector", []fluxopv1.InputProviderReference{{Name: "a", Selector: invalid}}, []any{a}},
		{"nil selector", []fluxopv1.InputProviderReference{{}}, nil},
		{"foreign kind", []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSet, Selector: &metav1.LabelSelector{}}}, nil},
		{"omitted reference fields", []fluxopv1.InputProviderReference{{Selector: rs.InputsFrom[0].Selector}}, []any{a, b}},
		{"overlap", []fluxopv1.InputProviderReference{{Name: "a"}, rs.InputsFrom[0], rs.InputsFrom[0]}, []any{a, b}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testResourceSetFingerprintSelectorInputsCase2(
				t,
				testResourceSetFingerprintSelectorInputsCase2Inputs{rs: &rs, s: &s, docs: &docs, tc: &tc},
			)
		},
		)
	}

	for _, tc := range []struct {
		name     string
		provider *manifest.ResourceSetInputProvider
		changed  bool
		want     map[string]string
	}{
		{"nonmatching label", staticRSIP("c", "apps", map[string]string{"role": "web"}, nil), false, map[string]string{"a": "first", "b": "second"}},
		{"other namespace", staticRSIP("c", "other", map[string]string{"role": "db"}, nil), false, map[string]string{"a": "first", "b": "second"}},
		{"new match", staticRSIP("c", "apps", map[string]string{"role": "db"}, map[string]string{"value": "third"}), true, map[string]string{"a": "first", "b": "second", "c": "third"}},
		{"changed inputs", staticRSIP("a", "apps", map[string]string{"role": "db"}, map[string]string{"value": "updated"}), true, map[string]string{"a": "updated", "b": "second"}},
		{"removed match", staticRSIP("a", "apps", map[string]string{"role": "web"}, map[string]string{"id": "one", "value": "first"}), true, map[string]string{"b": "second"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testResourceSetFingerprintSelectorInputsCase3(
				t,
				testResourceSetFingerprintSelectorInputsCase3Inputs{rs: &rs, a: &a, b: &b, fp: &fp, tc: &tc},
			)
		},
		)
	}
}

func TestResourceSetFingerprint_CapturedInputs(t *testing.T) {
	t.Parallel()
	s := store.New()
	rs := selectorFixture("reader", "blue")
	rs.ResourcesTemplate = "<< $_ := set inputs.nested \"value\" \"template\" >>apiVersion: v1\nkind: ConfigMap\nmetadata: {name: captured}\ndata: {value: '<< inputs.value >>'}"
	s.AddObject(rs)
	p := staticRSIP("provider", "ns", map[string]string{"color": "blue"}, map[string]string{"value": "spec"})
	p.Status.ExportedInputs = []fluxopv1.ResourceSetInput{
		{"value": {Raw: []byte(`"status-a"`)}, "nested": {Raw: []byte(`{"value":"stored"}`)}},
	}
	s.AddObject(p)
	resolve := resourceset.StoreResolver(s)
	renderWithAdmissionResult3, renderWithAdmissionResult3Err := renderWithAdmission(rs, resolve, "")
	docs, fingerprint, err := renderWithAdmissionResult3.documents, renderWithAdmissionResult3.fingerprint, renderWithAdmissionResult3Err
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 1)
	assert.Equal(t, docs[0]["data"].(map[string]any)["value"].(string), "status-a")
	assert.Equal(t, string(p.Status.ExportedInputs[0]["nested"].Raw), `{"value":"stored"}`)
	renderWithAdmissionResult4, renderWithAdmissionResult4Err := renderWithAdmission(rs, resolve, fingerprint)
	hit, key, err := renderWithAdmissionResult4.documents, renderWithAdmissionResult4.fingerprint, renderWithAdmissionResult4Err
	assert.NoError(t, err)
	assert.Equal(t, key, fingerprint)
	assert.Equal(t, len(hit), 0)
	fresh := *p
	fresh.Status.ExportedInputs = []fluxopv1.ResourceSetInput{
		{"value": {Raw: []byte(`"status-b"`)}, "nested": {Raw: []byte(`{"value":"stored"}`)}},
	}
	s.AddObject(&fresh)
	renderWithAdmissionResult5, renderWithAdmissionResult5Err := renderWithAdmission(rs, resolve, fingerprint)
	docs, current, err := renderWithAdmissionResult5.documents, renderWithAdmissionResult5.fingerprint, renderWithAdmissionResult5Err
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 1)
	assert.Equal(t, docs[0]["data"].(map[string]any)["value"].(string), "status-b")
	assert.FatalIf(t, current == fingerprint, "%v", "successor did not capture the changed exported inputs")
}

type admittedRender struct {
	documents   []map[string]any
	fingerprint string
}

func renderWithAdmission(
	rs *manifest.ResourceSet,
	resolve resourceset.ProviderResolver,
	previous string,
) (admittedRender, error) {
	s := store.New()
	s.AddObject(rs)
	s.RecordEmitted(rs.Named(), &store.Emission{Fingerprint: previous, Complete: true}, nil)
	c := base.New(s, nil)
	run := controllertest.NewExecution(c, rs.Named())
	defer run.Close()
	var fp string
	docs, _, err := renderProviders(rs, resolve, func(content any) (bool, error) {
		var hit bool
		var err error
		fingerprintDedupResult1, fingerprintDedupResult1Err := run.FingerprintDedup(
			context.TODO(),
			rs.Named(),
			content,
			&store.Emission{},
		)
		fp, hit, err = fingerprintDedupResult1.Fingerprint, fingerprintDedupResult1.Reused, fingerprintDedupResult1Err
		return hit, err
	})
	return admittedRender{documents: testutil.DocumentMaps(docs), fingerprint: fp}, err
}

func renderProviders(
	rs *manifest.ResourceSet,
	resolve resourceset.ProviderResolver,
	admit func(any) (bool, error),
) ([]manifest.DocOccurrence, []manifest.Warning, error) {
	inputs, err := resourceset.CollectProviders(rs, resolve)
	if err != nil {
		return nil, nil, err
	}
	for i := range inputs {
		input := &inputs[i]
		plan, err := input.Declaration.InputPlan()
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", manifest.ErrInput, err)
		}
		input.Inputs = plan.Inputs
		if plan.Kind == manifest.ProviderInputUnsupported {
			input.State = resourceset.ProviderInputUnsupported
		}
	}
	return resourceset.Render(rs, inputs, admit, nil)
}

type testResourceSetFingerprintSelectorInputsCaseInputs struct {
	rs    **manifest.ResourceSet
	a     **manifest.ResourceSetInputProvider
	b     **manifest.ResourceSetInputProvider
	fp    *string
	field *string
}

func testResourceSetFingerprintSelectorInputsCase(
	t *testing.T,
	testCaseInput testResourceSetFingerprintSelectorInputsCaseInputs,
) {
	t.Helper()

	candidate := *(*testCaseInput.rs)
	providers := []*manifest.ResourceSetInputProvider{(*testCaseInput.a), (*testCaseInput.b)}
	changed := *(*testCaseInput.a)
	providers[0] = &changed
	switch *testCaseInput.field {
	case "name":
		candidate.Name = "other"
	case "namespace":
		candidate.Namespace = "other"
	case "provider identity":
		providers[0].Name = "renamed"
	case "provider type":
		providers[0].Type = fluxopv1.InputProviderGitHubBranch
		providers[0].Status.ExportedInputs = []fluxopv1.ResourceSetInput{(*testCaseInput.a).DefaultValues}
	}
	calls := 0
	renderWithAdmissionResult6, renderWithAdmissionResult6Err := renderWithAdmission(
		&candidate,
		func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			calls++
			return providers, nil
		}, (*testCaseInput.fp),
	)
	got, key, err := renderWithAdmissionResult6.documents, renderWithAdmissionResult6.fingerprint, renderWithAdmissionResult6Err
	assert.NoError(t, err)
	assert.Equal(t, calls, len(candidate.InputsFrom))
	assert.Equal(t, len(got), 2)
	assert.FatalIf(t, key == (*testCaseInput.fp), "%v", "effective content change replayed")
}

type testResourceSetFingerprintSelectorInputsResolverErrorCannotReplayInputs struct {
	rs **manifest.ResourceSet
	fp *string
}

func testResourceSetFingerprintSelectorInputsResolverErrorCannotReplay(
	t *testing.T,
	testCaseInput testResourceSetFingerprintSelectorInputsResolverErrorCannotReplayInputs,
) {
	t.Helper()

	failure := errors.New("captured provider unavailable")
	renderWithAdmissionResult7, renderWithAdmissionResult7Err := renderWithAdmission(
		(*testCaseInput.rs),
		func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			return nil, failure
		},
		(*testCaseInput.fp),
	)
	localDocs, key, err := renderWithAdmissionResult7.documents, renderWithAdmissionResult7.fingerprint, renderWithAdmissionResult7Err
	assert.FatalIf(
		t,
		!errors.Is(err, failure) || !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) ||
			key != "" ||
			localDocs != nil,
		"resolver error became a hit: docs=%v key=%q error=%v",
		localDocs,
		key,
		err,
	)
}

func testResourceSetFingerprintSelectorInputsInlineOrder(t *testing.T) {
	t.Helper()

	candidate := &manifest.ResourceSet{Name: "inline", Namespace: "apps", Inputs: []fluxopv1.ResourceSetInput{
		{"name": {Raw: []byte(`"first"`)}}, {"name": {Raw: []byte(`"second"`)}},
	}, ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.name >>'}"}
	renderWithAdmissionResult8, renderWithAdmissionResult8Err := renderWithAdmission(candidate, nil, "")
	localDocs, key, err := renderWithAdmissionResult8.documents, renderWithAdmissionResult8.fingerprint, renderWithAdmissionResult8Err
	assert.NoError(t, err)
	metadata := manifest.DocMetadata(localDocs[0])
	first := metadata.Name
	assert.Equal(t, first, "first")
	candidate.Inputs[0], candidate.Inputs[1] = candidate.Inputs[1], candidate.Inputs[0]
	renderWithAdmissionResult9, renderWithAdmissionResult9Err := renderWithAdmission(candidate, nil, key)
	localDocs, current, err := renderWithAdmissionResult9.documents, renderWithAdmissionResult9.fingerprint, renderWithAdmissionResult9Err
	assert.NoError(t, err)
	assert.FatalIf(t, current == key, "%v", "inline order replayed")
	metadata2 := manifest.DocMetadata(localDocs[0])
	first = metadata2.Name
	assert.Equal(t, first, "second")
}

type testResourceSetFingerprintSelectorInputsResolverDeliveryOrderInputs struct {
	rs   **manifest.ResourceSet
	a    **manifest.ResourceSetInputProvider
	b    **manifest.ResourceSetInputProvider
	docs *[]map[string]any
	fp   *string
}

func testResourceSetFingerprintSelectorInputsResolverDeliveryOrder(
	t *testing.T,
	testCaseInput testResourceSetFingerprintSelectorInputsResolverDeliveryOrderInputs,
) {
	t.Helper()

	reversed := []*manifest.ResourceSetInputProvider{(*testCaseInput.b), nil, (*testCaseInput.a), (*testCaseInput.a)}
	calls := 0
	renderWithAdmissionResult10, renderWithAdmissionResult10Err := renderWithAdmission(
		(*testCaseInput.rs),
		func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			calls++
			return reversed, nil
		},
		"",
	)
	got, key, err := renderWithAdmissionResult10.documents, renderWithAdmissionResult10.fingerprint, renderWithAdmissionResult10Err
	assert.NoError(t, err)
	assert.Equal(t, calls, 1)
	assert.Equal(t, key, (*testCaseInput.fp))
	assert.Diff(t, got, (*testCaseInput.docs))
	assert.Equal(t, reversed[0].Named(), (*testCaseInput.b).Named())
	overlapped := *(*testCaseInput.rs)
	overlapped.InputsFrom = slices.Clone((*testCaseInput.rs).InputsFrom)
	overlapped.InputsFrom = append(overlapped.InputsFrom, fluxopv1.InputProviderReference{Name: "a"})
	calls = 0
	renderWithAdmissionResult11, renderWithAdmissionResult11Err := renderWithAdmission(
		&overlapped,
		func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			calls++
			return reversed, nil
		},
		"",
	)
	got, key, err = renderWithAdmissionResult11.documents, renderWithAdmissionResult11.fingerprint, renderWithAdmissionResult11Err
	assert.NoError(t, err)
	assert.Equal(t, calls, 2)
	assert.Diff(t, got, (*testCaseInput.docs))
	renderWithAdmissionResult12, renderWithAdmissionResult12Err := renderWithAdmission(
		&overlapped,
		func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			return []*manifest.ResourceSetInputProvider{(*testCaseInput.a), (*testCaseInput.b), nil}, nil
		},
		key,
	)
	hit, replayKey, err := renderWithAdmissionResult12.documents, renderWithAdmissionResult12.fingerprint, renderWithAdmissionResult12Err
	assert.NoError(t, err)
	assert.Equal(t, replayKey, key)
	assert.FatalIf(t, hit != nil, "equal canonical input returned documents: %v", hit)
}

type testResourceSetFingerprintSelectorInputsCase2Inputs struct {
	rs   **manifest.ResourceSet
	s    **store.Store
	docs *[]map[string]any
	tc   *struct {
		name string

		refs   []fluxopv1.InputProviderReference
		inputs []any
	}
}

func testResourceSetFingerprintSelectorInputsCase2(
	t *testing.T,
	testCaseInput testResourceSetFingerprintSelectorInputsCase2Inputs,
) {
	t.Helper()

	referenced := *(*testCaseInput.rs)
	referenced.InputsFrom = testCaseInput.tc.refs
	resolve := resourceset.StoreResolver((*testCaseInput.s))
	renderWithAdmissionResult13, renderWithAdmissionResult13Err := renderWithAdmission(&referenced, resolve, "")
	got, _, err := renderWithAdmissionResult13.documents, renderWithAdmissionResult13.fingerprint, renderWithAdmissionResult13Err

	assert.NoError(t, err)
	var want []map[string]any
	if len(testCaseInput.tc.inputs) != 0 {
		want = (*testCaseInput.docs)[:len(testCaseInput.tc.inputs)]
	}
	assert.Diff(t, got, want)
}

type testResourceSetFingerprintSelectorInputsCase3Inputs struct {
	rs **manifest.ResourceSet
	a  **manifest.ResourceSetInputProvider
	b  **manifest.ResourceSetInputProvider
	fp *string
	tc *struct {
		name string

		provider *manifest.
				ResourceSetInputProvider
		changed bool
		want    map[string]string
	}
}

func testResourceSetFingerprintSelectorInputsCase3(
	t *testing.T,
	testCaseInput testResourceSetFingerprintSelectorInputsCase3Inputs,
) {
	t.Helper()

	st := store.New()
	st.AddObject((*testCaseInput.a))
	st.AddObject((*testCaseInput.b))
	st.AddObject(testCaseInput.tc.provider)
	resolve := resourceset.StoreResolver(st)
	renderWithAdmissionResult14, renderWithAdmissionResult14Err := renderWithAdmission((*testCaseInput.rs), resolve, "")
	got, key, err := renderWithAdmissionResult14.documents, renderWithAdmissionResult14.fingerprint, renderWithAdmissionResult14Err
	assert.Equal(t, key != (*testCaseInput.fp), testCaseInput.tc.changed)
	assert.NoError(t, err)
	values := make(map[string]string)
	for _, doc := range got {
		values[doc["metadata"].(map[string]any)["name"].(string)] = doc["data"].(map[string]any)["value"].(string)
	}
	assert.Diff(t, values, testCaseInput.tc.want)
}
