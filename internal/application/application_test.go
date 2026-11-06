package application_test

import (
	"bytes"
	"regexp"
	"testing"

	"github.com/home-operations/flate/internal/application"
	"github.com/home-operations/flate/internal/assert"
)

func TestContains_Forms(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, content string
		possible      bool
	}{
		{"empty", "", false},
		{"workload", "apiVersion: apps/v1\nkind: Deployment\n", false},
		{"kustomize group", "kustomize.toolkit.fluxcd.io/v1", true},
		{"operator group", "fluxcd.controlplane.io/v1", true},
		{"helm group", "helm.toolkit.fluxcd.io/v2", true},
		{"unrelated group", "source.toolkit.fluxcd.io/v1", false},
		{"kustomization", "kind: Kustomization", true},
		{"flux instance", "kind: FluxInstance", true},
		{"helm release", "kind: HelmRelease", true},
		{"resource set", "kind: ResourceSet", true},
		{"json", `{"kind":"Kustomization"}`, true},
		{"single quotes", "'kind': 'FluxInstance'", true},
		{"mixed quotes", "\"kind': \"ResourceSet'", true},
		{"quoted key missing opening quote", "kind': ResourceSet", false},
		{"unquoted key with opening quote", "'kind: ResourceSet", true},
		{"key suffix", "notkind: ResourceSet", true},
		{"key case", "Kind: ResourceSet", false},
		{"value case", "kind: resourceset", false},
		{"short value", "kind: Resource", false},
		{"value suffix letter", "kind: ResourceSets", false},
		{"value suffix digit", "kind: ResourceSet1", false},
		{"value suffix underscore", "kind: ResourceSet_", false},
		{"value suffix punctuation", "kind: ResourceSet-", true},
		{"value suffix unicode", "kind: ResourceSeté", true},
		{"value suffix invalid utf8", "kind: ResourceSet\xff", true},
		{"whitespace", "kind\t\n\f\r :\t\n\f\r ResourceSet", true},
		{"vertical tab before colon", "kind\v: ResourceSet", false},
		{"vertical tab after colon", "kind:\vResourceSet", false},
		{"unicode space", "kind:\u00a0ResourceSet", false},
		{"space after quote", "kind: ' ResourceSet'", false},
		{"double value quote", "kind: ''ResourceSet", false},
		{"helm kind template", "kind: {{ .Values.kind }}", true},
		{"resource set kind template", "kind: << inputs.kind >>", true},
		{"quoted kind template", `"kind": "{{ .Values.kind }}"`, true},
		{"unclosed kind template", "kind: {{", true},
		{"incomplete kind template", "kind: {", false},
		{"envsubst kind", "kind: ${KIND}", false},
		{"later match", "kind: Deployment\nkind: ResourceSet", true},
		{"overlapping quoted keys", "'kind'kind': ResourceSet", true},
		{"resource set computed group", "apiVersion: << inputs.group >>.toolkit.fluxcd.io/v1\nkind: Kustomization\n", true},
		{"helm computed group", "apiVersion: {{ printf \"%s.%s\" \"fluxcd\" \"controlplane.io/v1\" }}\nkind: FluxInstance\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, application.Contains([]byte(tc.content)), tc.possible)
		})
	}
}

func FuzzContains(f *testing.F) {
	for _, seed := range []string{
		"", "kind: Deployment", "kind: ResourceSet", `{"kind":"HelmRelease"}`,
		"'kind'kind': FluxInstance", "kind:\t{{", "kind: <<", "kind: ResourceSet\xff",
		"kustomize.toolkit.fluxcd.io", "fluxcd.controlplane.io", "helm.toolkit.fluxcd.io",
	} {
		f.Add([]byte(seed))
	}
	screen := regexp.MustCompile(
		`(?:kind\s*:|["']kind["']\s*:)\s*["']?(?:(?:Kustomization|FluxInstance|HelmRelease|ResourceSet)(?:[^A-Za-z0-9_]|$)|\{\{|<<)`,
	)
	f.Fuzz(func(t *testing.T, content []byte) {
		want := bytes.Contains(content, []byte("kustomize.toolkit.fluxcd.io")) ||
			bytes.Contains(content, []byte("fluxcd.controlplane.io")) ||
			bytes.Contains(content, []byte("helm.toolkit.fluxcd.io")) || screen.Match(content)
		assert.Equal(t, application.Contains(content), want)
	})
}
