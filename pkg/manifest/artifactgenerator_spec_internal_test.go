package manifest

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil/assert"
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
	Namespace      string                 `json:"namespace"`
	InputsFrom     string                 `json:"inputsFrom"`
	Revision       string                 `json:"revision"`
	OriginRevision string                 `json:"originRevision"`
	Copy           []fingerprintOperation `json:"copy"`
}

type fingerprintOperation struct {
	From     string   `json:"from"`
	To       string   `json:"to"`
	Strategy string   `json:"strategy"`
	Exclude  []string `json:"exclude"`
	Optional bool     `json:"optional"`
}

func TestArtifactGeneratorSpec_JSON(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		spec map[string]any
	}{
		{"absent", nil},
		{"empty", map[string]any{"sources": []any{}, "artifacts": []any{}}},
		{"null", map[string]any{"sources": nil, "artifacts": nil, "pathPattern": nil, "commonMetadata": nil, "managedNamespaces": nil, "serviceAccountName": ""}},
		{"incomplete", map[string]any{"sources": []any{nil, map[string]any{}, map[string]any{"kind": KindGitRepository, "name": "repo"}}}},
		{"namespaces", map[string]any{"sources": []any{map[string]any{"namespace": ""}, map[string]any{"namespace": nil}, map[string]any{"namespace": "apps"}}}},
		{"metadata", map[string]any{"commonMetadata": map[string]any{"labels": map[string]string{"app": "test"}, "annotations": map[string]any{"null": nil}}}},
		{"copies", map[string]any{"pathPattern": "@repo/{app}", "artifacts": []any{map[string]any{"name": "app-{app}", "namespace": "{app}", "inputsFrom": "@repo/settings/{app}.yaml", "revision": "@repo", "originRevision": "@repo", "copy": []any{nil, map[string]any{"from": "@repo/file", "to": "@artifact/file", "strategy": "Overwrite", "exclude": []string{"ignored"}, "optional": true}, map[string]any{"exclude": []any{}}}}}}},
		{"mixed_case_fields", map[string]any{"PathPattern": "@repo/{app}", "sources": []any{map[string]any{"Alias": "repo", "Kind": KindGitRepository, "Name": "repo", "Namespace": "apps"}}, "Artifacts": []any{map[string]any{"Name": "app", "Namespace": "apps", "InputsFrom": "@artifact/file", "Revision": "@repo", "OriginRevision": "@repo", "Copy": []any{map[string]any{"From": "@repo/file", "To": "@artifact/file", "Strategy": "Overwrite", "Exclude": []string{"ignored"}, "Optional": true}}}}}},
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
			parsed, err := parseRawObject(map[string]any{
				"apiVersion": SourceExtensionsDomain + "/v1beta1",
				"kind":       KindArtifactGenerator,
				"metadata":   map[string]any{"name": "generator"},
				"spec":       tc.spec,
			})
			assert.NoError(t, err)
			raw := &RawObject{Spec: tc.spec}
			before, directBefore := Fingerprint(parsed), Fingerprint(raw)
			got, err := parsed.ArtifactGeneratorSpec()
			direct, directErr := raw.ArtifactGeneratorSpec()
			assert.Equal(t, err == nil, priorErr == nil)
			assert.Equal(t, directErr == nil, err == nil)
			assert.Diff(t, direct, got)
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
			mutateGeneratorSpec(&got)
			mutateGeneratorSpec(&direct)
			again, err := parsed.ArtifactGeneratorSpec()
			assert.NoError(t, err)
			original, err := raw.ArtifactGeneratorSpec()
			assert.NoError(t, err)
			assert.Diff(t, again, original)
			assert.Equal(t, Fingerprint(parsed), before)
			assert.Equal(t, Fingerprint(raw), directBefore)
		})
	}
}

func mutateGeneratorSpec(spec *ArtifactGeneratorSpec) {
	if spec.CommonMetadata.Labels != nil {
		spec.CommonMetadata.Labels["app"] = "changed"
	}
	if spec.CommonMetadata.Annotations != nil {
		spec.CommonMetadata.Annotations["null"] = "changed"
	}
	if len(spec.Artifacts) > 0 && len(spec.Artifacts[0].Copy) > 1 {
		spec.Artifacts[0].Copy[1].Exclude[0] = "changed"
	}
	if len(spec.Artifacts) != 0 {
		spec.Artifacts[0].Name = "changed"
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
			raw.Kind, raw.APIVersion = KindArtifactGenerator, SourceExtensionsDomain+"/v1beta1"
			_, err := raw.ArtifactGeneratorSpec()
			assert.ErrorIf(t, err == nil, "noncanonical sources container decoded without an error")
			assert.Equal(t, raw.ReferenceCapacity().References, 0)
			assert.Equal(t, len(slices.Collect(References(raw, ReferenceRelations))), 0)
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
				map[string]any{"alias": "repo", "kind": KindGitRepository, "name": "repo", "namespace": nil},
			},
			"artifacts": []any{
				map[string]any{
					"name":       "literal",
					"inputsFrom": "@repo/config/stage/../values*.yaml",
					"copy":       []any{map[string]any{"from": "@repo/payload/one/**", "exclude": 1}},
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
	assert.Diff(t, refs[1].Files, []string{"config/values*.yaml", "payload/one"})
	assert.Equal(t, refs[1].Subtree, true)
	assert.Equal(t, refs[1].NamespaceDefault, true)
	targets := ProducerTargets(raw)
	assert.Equal(t, len(targets), 1)
	assert.Equal(t, targets[0].Name, "literal")
	decoded, err := raw.ArtifactGeneratorSpec()
	assert.Equal(t, len(decoded.Sources), len(refs))
	assert.Equal(t, decoded.Sources[refs[1].Position].Alias, "repo")
	assert.Equal(t, decoded.Sources[refs[1].Position].NamespaceSet, true)
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

func TestArtifactGeneratorSpec_MixedCaseDeclarations(t *testing.T) {
	t.Parallel()
	parsed, err := parseRawObject(map[string]any{
		"apiVersion": SourceExtensionsDomain + "/v1beta1",
		"kind":       KindArtifactGenerator,
		"metadata":   map[string]any{"name": "generator", "namespace": "apps"},
		"spec": map[string]any{
			"sources": []any{
				map[string]any{"Alias": "repo", "Kind": KindGitRepository, "Name": "repo", "Namespace": "input"},
			},
			"PathPattern": "@repo/clusters/{app}",
			"Artifacts": []any{
				map[string]any{"Name": "literal", "Copy": []any{map[string]any{"From": "@repo/common/file"}}},
				map[string]any{"Name": "bundle-{app}", "Namespace": "delivery"},
				map[string]any{"Name": "route", "Namespace": "delivery-{app}"},
			},
			"ServiceAccountName": "offline",
			"ManagedNamespaces": map[string]any{
				"AllowedNames": []any{"true"}, "Prune": true,
				"Metadata": map[string]any{
					"FromSource": map[string]any{
						"Path": "@repo/metadata/{app}.yaml", "AllowedLabels": map[string]string{"app": "true"},
						"AllowedAnnotations": map[string]string{"owner": "true"},
					},
					"From": []any{
						map[string]any{
							"Namespace": "*",
							"Strategy":  "Merge",
							"Labels":    map[string]string{"app": "one"},
						},
						map[string]any{
							"Namespace":   "delivery",
							"Strategy":    "Override",
							"Annotations": map[string]string{"owner": "two"},
						},
					},
				},
			},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, parsed.WithNamespace("other").(*RawObject).generator == parsed.generator, true)
	refs := slices.Collect(References(parsed, ReferenceRelations))
	assert.Equal(t, len(refs), 1)
	assert.Equal(t, parsed.ReferenceCapacity().References, len(refs))
	assert.Equal(
		t,
		refs[0].NamedResource,
		NamedResource{Group: SourceDomain, Kind: KindGitRepository, Namespace: "input", Name: "repo"},
	)
	assert.Equal(t, refs[0].NamespaceDefault, false)
	assert.Diff(t, refs[0].Files, []string{"clusters", "common/file", "metadata"})
	spec, err := parsed.ArtifactGeneratorSpec()
	assert.NoError(t, err)
	assert.Equal(t, spec.ServiceAccountName, "offline")
	policy := spec.ManagedNamespaces
	assert.FatalIf(
		t,
		policy == nil || policy.Metadata == nil || policy.Metadata.FromSource == nil,
		"missing namespace policy",
	)
	assert.Diff(t, policy, &ArtifactGeneratorManagedNamespaces{
		AllowedNames: []string{"true"},
		Prune:        true,
		Metadata: &ArtifactGeneratorNamespaceMetadata{
			FromSource: &ArtifactGeneratorNamespaceMetadataSource{
				Path:               "@repo/metadata/{app}.yaml",
				AllowedLabels:      map[string]string{"app": "true"},
				AllowedAnnotations: map[string]string{"owner": "true"},
			},
			From: []ArtifactGeneratorNamespaceMetadataEntry{
				{Namespace: "*", Strategy: "Merge", Labels: map[string]string{"app": "one"}},
				{Namespace: "delivery", Strategy: "Override", Annotations: map[string]string{"owner": "two"}},
			},
		},
	})
	pristine, err := parsed.ArtifactGeneratorSpec()
	assert.NoError(t, err)
	before := Fingerprint(pristine)
	policy.Prune = false
	policy.AllowedNames[0] = "false"
	policy.Metadata.FromSource.Path = "@artifact/other"
	policy.Metadata.FromSource.AllowedLabels["app"] = "false"
	policy.Metadata.FromSource.AllowedAnnotations["owner"] = "false"
	policy.Metadata.From[0].Labels["app"] = "changed"
	policy.Metadata.From[1].Annotations["owner"] = "changed"
	policy.Metadata.From[1].Namespace = "*"
	assert.Equal(t, Fingerprint(spec) == before, false)
	again, err := parsed.ArtifactGeneratorSpec()
	assert.NoError(t, err)
	assert.Diff(t, again, pristine)
	assert.Equal(t, Fingerprint(again), before)

	target := NamedResource{Group: SourceDomain, Kind: KindExternalArtifact, Namespace: "delivery", Name: "bundle-prod"}
	literal := NamedResource{Group: SourceDomain, Kind: KindExternalArtifact, Namespace: "apps", Name: "literal"}
	fixed := NamedResource{Kind: "Namespace", Name: "delivery"}
	dynamic := NamedResource{Kind: "Namespace", Name: "delivery-prod"}
	want := []ProducerTarget{
		{NamedResource: literal}, {NamedResource: target}, {NamedResource: fixed}, {NamedResource: dynamic},
	}
	catalog := CatalogObject(parsed, parsed.Named())
	data, err := EncodeCatalog(catalog)
	assert.NoError(t, err)
	decoded, err := DecodeCatalog(data)
	assert.NoError(t, err)
	for _, obj := range []BaseManifest{parsed, catalog, decoded} {
		assert.Equal(t, obj.(*RawObject).generator != nil, true)
		assert.Equal(t, ProducerNeedsCandidates(obj), true)
		assert.Diff(t, ProducerTargets(obj, target, dynamic), want)
	}
	assert.Equal(t, catalog.ReferenceCapacity().References, 0)
	assert.Equal(t, decoded.ReferenceCapacity().References, 0)
	compact, err := decoded.(*RawObject).ArtifactGeneratorSpec()
	assert.NoError(t, err)
	assert.Diff(t, compact.ManagedNamespaces, &ArtifactGeneratorManagedNamespaces{})
}
