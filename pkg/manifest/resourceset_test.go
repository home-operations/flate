package manifest

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestResourceSetInputProvider_InputPlan(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, providerType string
		status             []fluxopv1.ResourceSetInput
		kind               ProviderInputKind
	}{
		{"omitted", "", nil, ProviderInputLiteral},
		{"static", fluxopv1.InputProviderStatic, nil, ProviderInputLiteral},
		{"registry", fluxopv1.InputProviderOCIArtifactTag, nil, ProviderInputRegistry},
		{"empty_status", fluxopv1.InputProviderOCIArtifactTag, []fluxopv1.ResourceSetInput{}, ProviderInputRegistry},
		{"pre_baked_registry", fluxopv1.InputProviderOCIArtifactTag, []fluxopv1.ResourceSetInput{{"id": {Raw: []byte(`"exported-id"`)}}}, ProviderInputLiteral},
		{"pre_baked_forge", fluxopv1.InputProviderGitHubTag, []fluxopv1.ResourceSetInput{{"id": {Raw: []byte(`"exported-id"`)}}}, ProviderInputLiteral},
		{"acr", fluxopv1.InputProviderACRArtifactTag, nil, ProviderInputUnsupported},
		{"ecr", fluxopv1.InputProviderECRArtifactTag, nil, ProviderInputUnsupported},
		{"gar", fluxopv1.InputProviderGARArtifactTag, nil, ProviderInputUnsupported},
		{"forge", fluxopv1.InputProviderGitHubTag, nil, ProviderInputUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			provider := &ResourceSetInputProvider{Name: "tags", Namespace: "authored", Type: tc.providerType,
				URL: "oci://registry.test/app", SecretRef: &LocalObjectReference{Name: "auth"},
				CertSecretRef: &LocalObjectReference{Name: "cert"}, Insecure: true,
				DefaultValues: fluxopv1.ResourceSetInput{"name": {Raw: []byte(`"app"`)}},
				Status:        fluxopv1.ResourceSetInputProviderStatus{ExportedInputs: tc.status},
			}
			projected := provider.WithNamespace("effective").(*ResourceSetInputProvider)
			plan, err := projected.InputPlan()
			assert.NoError(t, err)
			assert.Equal(t, plan.Kind, tc.kind)
			switch tc.kind {
			case ProviderInputLiteral:
				assert.Equal(t, len(plan.Inputs), 1)
				if len(tc.status) != 0 {
					assert.Diff(t, plan.Inputs[0], map[string]any{"id": "exported-id"})
				} else {
					assert.Equal(t, plan.Inputs[0]["name"], any("app"))
					assert.Equal(t, len(plan.Inputs[0]["id"].(string)), 16)
				}
			case ProviderInputRegistry:
				assert.Diff(t, *plan.Registry, RegistryAccess{
					Owner: DiagnosticIdentity{ID: projected.Named(), Resource: projected.Named()},
					URL:   provider.URL, Namespace: "effective", SecretRef: provider.SecretRef,
					CertSecretRef: provider.CertSecretRef, Insecure: true, Timeout: time.Minute,
				})
			}
			assert.Equal(t, provider.Namespace, "authored")
		})
	}
}

func TestResourceSetInputProvider_InputPlanOwnsMaps(t *testing.T) {
	t.Parallel()
	for _, status := range []bool{false, true} {
		t.Run(fmt.Sprintf("status_%t", status), func(t *testing.T) {
			t.Parallel()
			values := fluxopv1.ResourceSetInput{"id": {Raw: []byte(`"authored"`)}, "nested": {Raw: []byte(`{"name":"app"}`)}}
			provider := &ResourceSetInputProvider{Type: fluxopv1.InputProviderStatic, DefaultValues: values}
			if status {
				provider.Type = fluxopv1.InputProviderOCIArtifactTag
				provider.Status.ExportedInputs = []fluxopv1.ResourceSetInput{values}
			}
			first, err := provider.InputPlan()
			assert.NoError(t, err)
			first.Inputs[0]["id"] = "mutated"
			first.Inputs[0]["nested"].(map[string]any)["name"] = "mutated"
			second, err := provider.InputPlan()
			assert.NoError(t, err)
			assert.Diff(t, second.Inputs, []map[string]any{{"id": "authored", "nested": map[string]any{"name": "app"}}})
			assert.Equal(t, string(values["nested"].Raw), `{"name":"app"}`)
		})
	}
}

func TestResourceSetInputProvider_SelectTags(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		filter *fluxopv1.ResourceSetInputFilter
		tags   []string
		want   []string
	}{
		{"alphabetical", nil, []string{"v2", "v10", "latest", "v1"}, []string{"v2", "v10", "v1", "latest"}},
		{"limit_after_order", &fluxopv1.ResourceSetInputFilter{Limit: 2}, []string{"a", "c", "b"}, []string{"c", "b"}},
		{"include_exclude_overlap", &fluxopv1.ResourceSetInputFilter{IncludeTag: "^v", ExcludeTag: "beta"}, []string{"latest", "v2-beta", "v1", "v2"}, []string{"v2", "v1"}},
		{"semver_stable_ties", &fluxopv1.ResourceSetInputFilter{Semver: ">=1.0.0 <3.0.0", ExcludeTag: "beta", Limit: 4}, []string{"1.0.0+first", "v2.0.0", "v1.0.0", "1.0.0+last", "2.1.0-beta", "latest", "3.0.0"}, []string{"v2.0.0", "1.0.0+first", "v1.0.0", "1.0.0+last"}},
		{"semver_components", &fluxopv1.ResourceSetInputFilter{Semver: ">=0.0.0"}, []string{"2", "2.0", "v2025.02.03", "3.0.0"}, []string{"v2025.02.03", "3.0.0"}},
		{"tag_overrides_branch", &fluxopv1.ResourceSetInputFilter{IncludeBranch: "^branch", IncludeTag: "^tag", ExcludeBranch: "tag", ExcludeTag: "excluded"}, []string{"branch", "tag", "tag-excluded"}, []string{"tag"}},
		{"environment_overrides_tag", &fluxopv1.ResourceSetInputFilter{IncludeBranch: "^branch", IncludeTag: "^tag", IncludeEnvironment: "^environment", ExcludeBranch: "environment", ExcludeTag: "environment", ExcludeEnvironment: "excluded"}, []string{"branch", "tag", "environment", "environment-excluded"}, []string{"environment"}},
		{"empty_selection", &fluxopv1.ResourceSetInputFilter{IncludeTag: "^release"}, []string{"latest", "v1"}, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before := slices.Clone(tc.tags)
			provider := &ResourceSetInputProvider{Filter: tc.filter}
			got, err := provider.SelectTags(tc.tags)
			assert.NoError(t, err)
			assert.Diff(t, got, tc.want)
			assert.Diff(t, tc.tags, before)
		})
	}
}

func TestResourceSetInputProvider_SelectTagsDefaultLimit(t *testing.T) {
	t.Parallel()
	tags := make([]string, 101)
	for i := range tags {
		tags[i] = fmt.Sprintf("tag-%03d", i)
	}
	for _, tc := range []struct {
		name   string
		filter *fluxopv1.ResourceSetInputFilter
	}{
		{"omitted", nil},
		{"zero", &fluxopv1.ResourceSetInputFilter{Limit: 0}},
		{"negative", &fluxopv1.ResourceSetInputFilter{Limit: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := (&ResourceSetInputProvider{Filter: tc.filter}).SelectTags(tags)
			assert.NoError(t, err)
			assert.Equal(t, len(got), 100)
			assert.Equal(t, got[0], "tag-100")
			assert.Equal(t, got[99], "tag-001")
		})
	}
}

func TestResourceSetInputProvider_SelectTagsInvalidFilter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, message string
		filter        fluxopv1.ResourceSetInputFilter
	}{
		{"include_branch", "includeBranch", fluxopv1.ResourceSetInputFilter{IncludeBranch: "[", IncludeTag: ".*"}},
		{"include_tag", "includeTag", fluxopv1.ResourceSetInputFilter{IncludeTag: "[", IncludeEnvironment: ".*"}},
		{"include_environment", "includeEnvironment", fluxopv1.ResourceSetInputFilter{IncludeEnvironment: "["}},
		{"exclude_branch", "excludeBranch", fluxopv1.ResourceSetInputFilter{ExcludeBranch: "[", ExcludeTag: ".*"}},
		{"exclude_tag", "excludeTag", fluxopv1.ResourceSetInputFilter{ExcludeTag: "[", ExcludeEnvironment: ".*"}},
		{"exclude_environment", "excludeEnvironment", fluxopv1.ResourceSetInputFilter{ExcludeEnvironment: "["}},
		{"semver", "semver", fluxopv1.ResourceSetInputFilter{Semver: "invalid"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := (&ResourceSetInputProvider{Filter: &tc.filter}).SelectTags(nil)
			if !errors.Is(err, ErrInput) || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("got %v, want input error containing %q", err, tc.message)
			}
		})
	}
}

func TestResourceSetInputProvider_TagInputs(t *testing.T) {
	t.Parallel()
	provider := &ResourceSetInputProvider{DefaultValues: fluxopv1.ResourceSetInput{
		"id": {Raw: []byte(`"default-id"`)}, "tag": {Raw: []byte(`"default-tag"`)},
		"digest": {Raw: []byte(`"default-digest"`)}, "name": {Raw: []byte(`"app"`)},
		"nested": {Raw: []byte(`{"enabled":true}`)},
	}}
	input, err := provider.TagInputs("v1.0.0", "sha256:resolved")
	assert.NoError(t, err)
	decoded, err := decodeResourceSetInputs([]fluxopv1.ResourceSetInput{input})
	assert.NoError(t, err)
	assert.Diff(t, decoded, []map[string]any{{
		"id": "93520228", "tag": "v1.0.0", "digest": "sha256:resolved", "name": "app", "nested": map[string]any{"enabled": true},
	}})
	input["nested"].Raw[0] = '['
	assert.Equal(t, string(provider.DefaultValues["nested"].Raw), `{"enabled":true}`)
	provider.DefaultValues = fluxopv1.ResourceSetInput{"bad": &apix.JSON{Raw: []byte(`invalid`)}}
	_, err = provider.TagInputs("v1.0.0", "sha256:resolved")
	assert.Equal(t, errors.Is(err, ErrInput), true)
}
