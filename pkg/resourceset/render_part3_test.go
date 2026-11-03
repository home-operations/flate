package resourceset_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
)

func TestRender_ListItems(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{Name: "list", Namespace: "default", ResourcesTemplate: `apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: first}
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: second}
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: first}
- apiVersion: v1
  kind: ConfigMap
  metadata:
    name: excluded
    annotations: {fluxcd.controlplane.io/reconcile: disabled}`}
	docs, _, err := renderProviders(rs, nil, nil)
	{
		assert.NoError(t, err)
	}
	names := make([]string, 0, len(docs))
	for _, doc := range docs {
		name, ns := manifest.DocMetadata(doc.Document)
		names = append(names, name)
		{
			assert.ErrorIf(t, ns != "", "namespace = %q", ns)
		}
	}
	assert.Diff(t, names, []string{"first", "second"})
	for _, tc := range []struct {
		name, template string
		count          int
		wantErr        bool
	}{
		{"ordinary missing name", "apiVersion: v1\nkind: ConfigMap", 0, false},
		{"ordinary missing kind", "apiVersion: v1\nmetadata: {name: excluded}", 0, false},
		{"ordinary missing apiVersion", "kind: ConfigMap\nmetadata: {name: excluded}", 0, false},
		{"mixed case directive", "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: kUsToMiZaTiOn\nmetadata: {name: excluded}", 0, false},
		{"non directive group", "apiVersion: another.example/v1\nkind: Kustomization\nmetadata: {name: kept}", 1, false},
		{"items without suffix", "items: [{apiVersion: v1, kind: ConfigMap, metadata: {name: item}}]", 1, false},
		{"suffix without items", "apiVersion: example.test/v1\nkind: WidgetList\nmetadata: {name: ordinary}", 1, false},
		{"typed incomplete items", "apiVersion: v1\nkind: ConfigMapList\nitems: [{metadata: {name: kindless}}, {apiVersion: v1, kind: ConfigMap}]", 2, false},
		{"nested list stays wrapped", "items: [{kind: List, items: [{apiVersion: v1, kind: ConfigMap, metadata: {name: nested}}]}]", 1, false},
		{"directive item survives", "items: [{apiVersion: kustomize.config.k8s.io/v1beta1, kind: Kustomization, metadata: {name: directive}}]", 1, false},
		{"invalid YAML", "metadata: [unterminated", 0, true},
		{"null member", "items: [null]", 0, true},
		{"scalar member", "items: [plain]", 0, true},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) { t.Parallel(); testRenderListItemsCase(t, &tc) },
		)
	}
}

func TestRender_Admission(t *testing.T) {
	t.Parallel()
	failure := errors.New("admission failed")
	for _, tc := range []struct {
		name string
		hit  bool
		err  error
	}{
		{"replay", true, nil},
		{"error", false, failure},
		{"template", false, manifest.ErrInput},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rs := &manifest.ResourceSet{
				Name:              "reader",
				Namespace:         "apps",
				ResourcesTemplate: "<< invalid template",
				Inputs: []fluxopv1.ResourceSetInput{
					{"name": jsonTmpl(t, `"inline"`)},
				},
				InputsFrom: []fluxopv1.InputProviderReference{{Name: "dynamic"}},
			}
			provider := &manifest.ResourceSetInputProvider{
				Name:      "dynamic",
				Namespace: "apps",
				Type:      fluxopv1.InputProviderGitHubBranch,
			}
			resolved := 0
			resolve := func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
				resolved++
				return []*manifest.ResourceSetInputProvider{provider}, nil
			}
			calls := 0
			docs, warnings, err := renderProviders(rs, resolve, func(any) (bool, error) {
				calls++
				if tc.name == "template" {
					return false, nil
				}
				return tc.hit, tc.err
			})
			assert.Equal(t, calls, 1)
			assert.Equal(t, resolved, 1)
			{
				assert.FatalIf(t, len(warnings) != 1 || warnings[0].Category != manifest.WarnOfflineInputProvider ||
					warnings[0].Resource != rs.Named(), "admission lost provider warning: %+v", warnings)
			}
			{
				assert.FatalIf(t, docs != nil || (tc.name == "template" && err == nil) ||
					(tc.name != "template" && !errors.Is(err, tc.err)), "admission: documents=%v error=%v, want %v", docs, err, tc.err)
			}
		})
	}
	t.Run("canonical content stays immutable", func(t *testing.T) {
		t.Parallel()
		rs := &manifest.ResourceSet{
			Name:              "matrix",
			Namespace:         "apps",
			InputsFrom:        []fluxopv1.InputProviderReference{{Name: "images"}},
			ResourcesTemplate: "<< $_ := set inputs.nested \"value\" \"mutated\" >>apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.tag >>'}\n",
		}
		provider := &manifest.ResourceSetInputProvider{
			Name:      "images",
			Namespace: "apps",
			DefaultValues: fluxopv1.ResourceSetInput{
				"tag": jsonTmpl(t, `"selected"`), "nested": jsonTmpl(t, `{"value":"captured"}`),
			},
		}
		resolve := func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			return []*manifest.ResourceSetInputProvider{provider}, nil
		}
		var captured any
		var before []byte
		docs, _, err := renderProviders(rs, resolve, func(content any) (bool, error) {
			captured = content
			var err error
			before, err = json.Marshal(content)
			return false, err
		})
		assert.NoError(t, err)
		assert.Equal(t, len(docs), 1)
		after, err := json.Marshal(captured)
		assert.NoError(t, err)
		assert.Equal(t, string(after), string(before))
	})
	invalid := &manifest.ResourceSet{Name: "invalid", Steps: []fluxopv1.ResourceSetStep{{Name: "empty"}}}
	called := false
	_, _, err := renderProviders(invalid, nil, func(any) (bool, error) { called = true; return true, nil })
	{
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrInput) || called,
			"invalid steps reached admission: called=%v error=%v",
			called,
			err,
		)
	}
}

func TestRender_UsesOneProviderQuery(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "apps", Namespace: "apps",
		InputsFrom:        []fluxopv1.InputProviderReference{{Name: "provider"}},
		ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.value >>'}\n",
	}
	provider := &manifest.ResourceSetInputProvider{
		Name: "provider", Namespace: "apps",
		DefaultValues: fluxopv1.ResourceSetInput{"value": jsonTmpl(t, `"captured"`)},
	}
	calls := 0
	docs, _, err := renderProviders(
		rs,
		func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			calls++
			{
				assert.FatalIf(t, calls != 1, "%s", "provider query repeated")
			}
			return []*manifest.ResourceSetInputProvider{provider}, nil
		},
		nil,
	)
	assert.NoError(t, err)
	{
		assert.FatalIf(
			t,
			calls != 1 || len(docs) != 1 || docs[0].Document["metadata"].(map[string]any)["name"] != "captured",
			"query calls=%d output=%v",
			calls,
			docs,
		)
	}
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
	return resourceset.Render(rs, inputs, admit)
}

func TestRender_EvaluatedInputsRemainImmutable(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name:              "matrix",
		Namespace:         "apps",
		InputsFrom:        []fluxopv1.InputProviderReference{{Name: "images"}},
		ResourcesTemplate: "<< $_ := set inputs.nested \"value\" \"mutated\" >>apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.tag >>'}\n",
	}
	inputs := []resourceset.ProviderInputs{
		{
			ID: manifest.NamedResource{
				Group:     manifest.FluxOperatorDomain,
				Kind:      manifest.KindResourceSetInputProvider,
				Namespace: "apps",
				Name:      "images",
			},
			Inputs: []map[string]any{{"tag": "selected", "nested": map[string]any{"value": "captured"}}},
		},
	}
	for range 2 {
		docs, warnings, err := resourceset.Render(rs, inputs, nil)
		assert.NoError(t, err)
		assert.Equal(t, len(docs), 1)
		assert.Equal(t, len(warnings), 0)
		assert.Equal(t, inputs[0].Inputs[0]["nested"].(map[string]any)["value"], any("captured"))
		{
			_, exists := inputs[0].Inputs[0]["provider"]
			assert.FatalIf(t, exists, "%s", "render mutated captured provider inputs")
		}
	}
	inputs[0].Inputs = nil
	docs, warnings, err := resourceset.Render(rs, inputs, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 0)
	assert.Equal(t, len(warnings), 0)
}

func testRenderListItemsCase(t *testing.T, testCaseInput *struct {
	name     string
	template string
	count    int
	wantErr  bool
},
) {
	t.Helper()

	candidate := &manifest.ResourceSet{
		Name:              "boundary",
		Namespace:         "apps",
		ResourcesTemplate: testCaseInput.template,
	}
	got, _, err := renderProviders(candidate, nil, nil)
	if testCaseInput.wantErr {
		{
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
				"error = %v, want classified malformed list error",
				err,
			)
		}
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, len(got), testCaseInput.count)
	if testCaseInput.name == "typed incomplete items" && len(got) == 2 {
		assert.Equal(t, manifest.DocKind(got[0].Document), "")
		assert.Equal(t, manifest.DocAPIVersion(got[0].Document), "")
		name, _ := manifest.DocMetadata(got[1].Document)
		assert.Equal(t, name, "")
	}
	if testCaseInput.name == "nested list stays wrapped" && len(got) == 1 {
		assert.Equal(t, manifest.DocKind(got[0].Document), "List")
		assert.Equal(t, len(got[0].Document["items"].([]any)), 1)
	}
}
