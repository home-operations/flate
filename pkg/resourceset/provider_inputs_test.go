package resourceset_test

import (
	"fmt"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
)

func TestRender_InputsFrom_StaticProvider(t *testing.T) {
	t.Parallel()
	rsip := &manifest.ResourceSetInputProvider{
		Name: "apps", Namespace: "kube-system", Labels: map[string]string{"set": "apps"},
		Type: fluxopv1.InputProviderStatic,
		DefaultValues: fluxopv1.ResourceSetInput{
			"defaults": jsonTmpl(t, `{"capacity": "1Gi"}`),
			"apps":     jsonTmpl(t, `[{"app": "alpha"}, {"app": "bravo", "capacity": "5Gi"}]`),
		},
	}
	rs := &manifest.ResourceSet{
		Name: "volsync", Namespace: "kube-system",
		InputsFrom: []fluxopv1.InputProviderReference{
			{Name: "apps"},
		},
		ResourcesTemplate: `<<- range $app := inputs.apps >>
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: << $app.app >>
  namespace: default
data:
  capacity: << get $app "capacity" | default inputs.defaults.capacity >>
<<- end >>
`,
	}
	st := store.New()
	st.AddObject(rsip)
	other := *rsip
	other.Namespace = "other"
	st.AddObject(&other)
	st.AddObject(
		&manifest.RawObject{
			APIVersion: "example.test/v1",
			Kind:       manifest.KindResourceSetInputProvider,
			Name:       rsip.Name,
			Namespace:  rsip.Namespace,
		},
	)
	docs, _, err := renderProviders(rs, resourceset.StoreResolver(st), nil)
	{
		assert.FatalIf(t, err != nil, "Render: %v", err)
	}
	{
		assert.FatalIf(t, len(docs) != 2, "expected 2 ConfigMaps, got %d", len(docs))
	}
	caps := map[string]string{}
	for _, doc := range docs {
		md := doc.Document["metadata"].(map[string]any)
		data := doc.Document["data"].(map[string]any)
		caps[md["name"].(string)] = data["capacity"].(string)
	}
	assert.Diff(t, caps, map[string]string{"alpha": "1Gi", "bravo": "5Gi"})
	for _, selector := range []bool{false, true} {
		for _, tc := range []struct {
			name, apiVersion, kind string
			want                   int
		}{
			{"omitted", "", "", 2},
			{"explicit", "fluxcd.controlplane.io/v1", manifest.KindResourceSetInputProvider, 2},
			{"kind only", "", manifest.KindResourceSetInputProvider, 2},
			{"version only", "fluxcd.controlplane.io/v1", "", 2},
			{"foreign", "example.test/v1", "", 0},
			{"core", "v1", "", 0},
			{"other kind", "", manifest.KindResourceSet, 0},
		} {
			t.Run(fmt.Sprintf("%s/selector=%t", tc.name, selector), func(t *testing.T) {
				t.Parallel()
				testRenderInputsFromStaticProviderCase(
					t,
					&testRenderInputsFromStaticProviderCaseInputs{
						rsip:     &rsip,
						rs:       &rs,
						st:       &st,
						selector: &selector,
						tc:       &tc,
					},
				)
			},
			)
		}
	}
	for _, tc := range []struct {
		name string
		refs []fluxopv1.InputProviderReference
		want int
	}{
		{"expressions", []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "set", Operator: metav1.LabelSelectorOpIn, Values: []string{"apps"}}}}}}, 2},
		{"empty selector", []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{}}}, 2},
		{"named invalid selector", []fluxopv1.InputProviderReference{{Name: rsip.Name, Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "set", Operator: "invalid"}}}}}, 2},
		{"nil selector", []fluxopv1.InputProviderReference{{}}, 0},
		{"overlapping references", []fluxopv1.InputProviderReference{{Name: rsip.Name}, {Selector: &metav1.LabelSelector{}}, {Selector: &metav1.LabelSelector{MatchLabels: rsip.Labels}}}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderInputsFromStaticProviderCase2(
				t,
				&testRenderInputsFromStaticProviderCase2Inputs{rs: &rs, st: &st, docs: &docs, tc: &tc},
			)
		},
		)
	}
}

func TestRender_InputsFrom_DynamicProviderEmptySkip(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{fluxopv1.InputProviderACRArtifactTag, fluxopv1.InputProviderECRArtifactTag, fluxopv1.InputProviderGARArtifactTag, fluxopv1.InputProviderGitHubBranch} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			provider := &manifest.ResourceSetInputProvider{Name: "remote", Namespace: "apps", Type: kind}
			rs := &manifest.ResourceSet{
				Name: "matrix", Namespace: "apps",
				Inputs:            []fluxopv1.ResourceSetInput{{"tenant": jsonTmpl(t, `"inline-only"`)}},
				InputsFrom:        []fluxopv1.InputProviderReference{{Name: provider.Name}},
				ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.tenant >>'}\n",
			}
			docs, warnings, err := renderProviders(
				rs,
				func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
					return []*manifest.ResourceSetInputProvider{provider}, nil
				},
				nil,
			)
			assert.NoError(t, err)
			assert.Equal(t, len(docs), 1)
			assert.Equal(t, docs[0].Document["metadata"].(map[string]any)["name"], any("inline-only"))
			{
				assert.FatalIf(t, len(warnings) != 1 || warnings[0].Category != manifest.WarnOfflineInputProvider ||
					warnings[0].Resource != rs.Named(), "unsupported provider warning = %+v", warnings)
			}
			assert.Diff(t, warnings[0].Detail, []string{provider.Named().String(), kind})
		})
	}
}

type testRenderInputsFromStaticProviderCaseInputs struct {
	rsip     **manifest.ResourceSetInputProvider
	rs       **manifest.ResourceSet
	st       **store.Store
	selector *bool
	tc       *struct {
		name       string
		apiVersion string
		kind       string
		want       int
	}
}

func testRenderInputsFromStaticProviderCase(t *testing.T, testCaseInput *testRenderInputsFromStaticProviderCaseInputs) {
	t.Helper()

	candidate := *(*testCaseInput.rs)
	ref := fluxopv1.InputProviderReference{
		APIVersion: testCaseInput.tc.apiVersion,
		Kind:       testCaseInput.tc.kind,
		Name:       (*testCaseInput.rsip).Name,
	}
	if *testCaseInput.selector {
		ref.Name = ""
		ref.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"set": "apps"}}
	}
	candidate.InputsFrom = []fluxopv1.InputProviderReference{ref}
	renderProvidersDocs, _, err := renderProviders(&candidate, resourceset.StoreResolver((*testCaseInput.st)), nil)
	if err != nil || len(renderProvidersDocs) != testCaseInput.tc.want {
		t.Fatalf("docs=%d, error=%v, want %d", len(renderProvidersDocs), err, testCaseInput.tc.want)
	}
}

type testRenderInputsFromStaticProviderCase2Inputs struct {
	rs   **manifest.ResourceSet
	st   **store.Store
	docs *[]manifest.DocOccurrence
	tc   *struct {
		name string
		refs []fluxopv1.InputProviderReference
		want int
	}
}

func testRenderInputsFromStaticProviderCase2(
	t *testing.T,
	testCaseInput *testRenderInputsFromStaticProviderCase2Inputs,
) {
	t.Helper()

	candidate := *(*testCaseInput.rs)
	candidate.InputsFrom = testCaseInput.tc.refs
	got, _, err := renderProviders(&candidate, resourceset.StoreResolver((*testCaseInput.st)), nil)
	assert.NoError(t, err)
	if len(got) != testCaseInput.tc.want {
		t.Fatalf("docs=%d, want %d", len(got), testCaseInput.tc.want)
	}
	if testCaseInput.tc.want != 0 {
		assert.Diff(t, got, (*testCaseInput.docs))
	}
}
