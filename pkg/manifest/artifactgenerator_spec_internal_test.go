package manifest

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
)

type fingerprintSpecification struct {
	CommonMetadata struct {
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"commonMetadata"`
	Sources     []fingerprintSource `json:"sources"`
	PathPattern string              `json:"pathPattern"`
	Artifacts   []fingerprintOutput `json:"artifacts"`
}

type fingerprintSource struct {
	Alias     string `json:"alias"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
}

type fingerprintOutput struct {
	Name           string                 `json:"name"`
	Revision       string                 `json:"revision"`
	OriginRevision string                 `json:"originRevision"`
	Copy           []fingerprintOperation `json:"copy"`
}

type fingerprintOperation struct {
	From     string   `json:"from"`
	To       string   `json:"to"`
	Strategy string   `json:"strategy"`
	Exclude  []string `json:"exclude"`
}

func TestArtifactGeneratorSpec_JSON(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		spec map[string]any
	}{
		{"absent", nil},
		{"empty", map[string]any{"sources": []any{}, "artifacts": []any{}}},
		{"null", map[string]any{"sources": nil, "artifacts": nil, "pathPattern": nil, "commonMetadata": nil}},
		{"incomplete", map[string]any{"sources": []any{nil, map[string]any{}, map[string]any{"kind": KindGitRepository, "name": "repo"}}}},
		{"namespaces", map[string]any{"sources": []any{map[string]any{"namespace": ""}, map[string]any{"namespace": nil}, map[string]any{"namespace": "apps"}}}},
		{"metadata", map[string]any{"commonMetadata": map[string]any{"labels": map[string]string{"app": "test"}, "annotations": map[string]any{"null": nil}}}},
		{"copies", map[string]any{"pathPattern": "@repo/{app}", "artifacts": []any{map[string]any{"name": "app-{app}", "revision": "@repo", "originRevision": "@repo", "copy": []any{nil, map[string]any{"from": "@repo/file", "to": "@artifact/file", "strategy": "Overwrite", "exclude": []string{"ignored"}}, map[string]any{"exclude": []any{}}}}}}},
		{"mixed_case_fields", map[string]any{"PathPattern": "@repo/{app}", "sources": []any{map[string]any{"Alias": "repo", "Kind": KindGitRepository, "Name": "repo", "Namespace": "apps"}}, "Artifacts": []any{map[string]any{"Name": "app", "Revision": "@repo", "OriginRevision": "@repo", "Copy": []any{map[string]any{"From": "@repo/file", "To": "@artifact/file", "Strategy": "Overwrite", "Exclude": []string{"ignored"}}}}}}},
		{"mixed_case_exclude_type", map[string]any{"artifacts": []any{map[string]any{"copy": []any{map[string]any{"Exclude": 1}}}}}},
		{"namespace_type", map[string]any{"sources": []any{map[string]any{"namespace": 1}}}},
		{"source_type", map[string]any{"sources": []any{"invalid"}}},
		{"sources_type", map[string]any{"sources": "invalid"}},
		{"artifact_type", map[string]any{"artifacts": []any{1}}},
		{"artifacts_type", map[string]any{"artifacts": "invalid"}},
		{"pattern_type", map[string]any{"pathPattern": []any{}}},
		{"metadata_type", map[string]any{"commonMetadata": []any{}}},
		{"copy_type", map[string]any{"artifacts": []any{map[string]any{"copy": []any{false}}}}},
		{"exclude_type", map[string]any{"artifacts": []any{map[string]any{"copy": []any{map[string]any{"exclude": []any{1}}}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data, err := json.Marshal(tc.spec)
			assert.NoError(t, err)
			var prior fingerprintSpecification
			priorErr := json.Unmarshal(data, &prior)
			raw := &RawObject{Spec: tc.spec}
			before := Fingerprint(raw)
			got, err := raw.ArtifactGeneratorSpec()
			assert.Equal(t, err == nil, priorErr == nil)
			if err != nil {
				return
			}
			assert.Equal(t, Fingerprint(got), Fingerprint(prior))
			for i := range got.Sources {
				fields, _ := tc.spec["sources"].([]any)[i].(map[string]any)
				_, present := fields[namespaceKey]
				assert.Equal(t, got.Sources[i].NamespaceSet, present)
				got.Sources[i].NamespaceSet = !present
			}
			assert.Equal(t, Fingerprint(got), Fingerprint(prior))
			if got.CommonMetadata.Labels != nil {
				got.CommonMetadata.Labels["app"] = "changed"
			}
			if len(got.Artifacts) > 0 && len(got.Artifacts[0].Copy) > 1 {
				got.Artifacts[0].Copy[1].Exclude[0] = "changed"
			}
			assert.Equal(t, Fingerprint(raw), before)
		})
	}
}

func TestArtifactGeneratorSpec_SourceContainers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		spec map[string]any
	}{
		{"typed_list", map[string]any{"sources": []map[string]any{{"name": "repo"}}}},
		{"uppercase_list", map[string]any{"Sources": []any{map[string]any{"name": "repo"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := &RawObject{Spec: tc.spec}
			_, err := raw.ArtifactGeneratorSpec()
			assert.ErrorIf(t, err == nil, "noncanonical sources container decoded without an error")
		})
	}
}

func TestArtifactGeneratorSpec_DeclarationPositions(t *testing.T) {
	t.Parallel()
	raw := &RawObject{
		APIVersion: "source.extensions.fluxcd.io/v1beta1",
		Kind:       "ArtifactGenerator",
		Namespace:  "apps",
		Spec: map[string]any{
			"sources": []any{
				map[string]any{},
				map[string]any{"alias": "repo", "kind": KindGitRepository, "name": "repo"},
			},
			"artifacts": []any{
				map[string]any{
					"name": "literal",
					"copy": []any{map[string]any{"from": "@repo/payload/one/**", "exclude": 1}},
				},
			},
		},
	}
	before := Fingerprint(raw)
	refs := slices.Collect(References(raw, ReferenceRelations))
	assert.Equal(t, len(refs), 2)
	assert.Equal(t, refs[0].Position, 0)
	assert.Equal(t, refs[0].Name, "")
	assert.Equal(t, refs[1].Position, 1)
	assert.Equal(
		t,
		refs[1].NamedResource,
		NamedResource{Group: SourceDomain, Kind: KindGitRepository, Namespace: "apps", Name: "repo"},
	)
	assert.Equal(t, refs[1].BindsSource(), true)
	assert.Diff(t, refs[0].Files, []string(nil))
	assert.Diff(t, refs[1].Files, []string{"payload/one"})
	assert.Equal(t, refs[1].Subtree, true)
	assert.Equal(t, refs[1].NamespaceDefault, true)
	targets := ProducerTargets(raw)
	assert.Equal(t, len(targets), 1)
	assert.Equal(t, targets[0].Name, "literal")
	decoded, err := raw.ArtifactGeneratorSpec()
	assert.Equal(t, len(decoded.Sources), len(refs))
	assert.Equal(t, decoded.Sources[refs[1].Position].Alias, "repo")
	assert.Equal(t, decoded.Sources[refs[0].Position].Alias, "")
	assert.ErrorIf(t, err == nil, "malformed copy metadata decoded without an error")
	catalog := CatalogObject(raw, raw.Named())
	assert.Equal(t, len(slices.Collect(References(catalog, ReferenceRelations))), 0)
	assert.Equal(t, len(ProducerTargets(catalog)), 1)
	for ref := range References(raw, ReferenceRelations) {
		ref.NamedResource.Name = "mutated"
		assert.Equal(t, ref.Name, "mutated")
	}
	after := slices.Collect(References(raw, ReferenceRelations))
	if diff := cmp.Diff(refs, after, cmp.AllowUnexported(Reference{})); diff != "" {
		t.Errorf("borrowed references changed (-want +got):\n%s", diff)
	}
	assert.Equal(t, Fingerprint(raw), before)
}
