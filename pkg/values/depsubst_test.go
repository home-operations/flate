package values

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func parsedDependency(t *testing.T, name, spec string) *manifest.Kustomization {
	t.Helper()
	docs, err := manifest.SplitDocs([]byte(testutil.KustomizationYAML(name, "apps", spec)))
	if err != nil {
		t.Fatal(err)
	}
	obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return obj.(*manifest.Kustomization)
}

func TestDependencySubstitutions_ParsedDependsOn(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, expression, namespace, want, wantNamespace string
		declarations                                     []string
		inline                                           string
	}{
		{name: "empty-supplied", expression: "${PREFIX:-}config", inline: "prod-", want: "prod-config"},
		{name: "nonempty-supplied", expression: "${PREFIX:-dev-}config", inline: "prod-", want: "prod-config"},
		{name: "separate-supplier", expression: "${PREFIX:-}config", declarations: []string{"prod-"}, want: "prod-config"},
		{name: "empty-absent", expression: "${PREFIX:-}config", want: "config"},
		{name: "nonempty-absent", expression: "${PREFIX:-dev-}config", want: "dev-config"},
		{name: "assign-supplied", expression: "${PREFIX:=}config", inline: "prod-", want: "prod-config"},
		{name: "assign-absent", expression: "${PREFIX:=dev-}config", want: "dev-config"},
		{name: "explicit-empty", expression: "${PREFIX:-dev-}config", declarations: []string{""}, want: "dev-config"},
		{name: "agreeing", expression: "${PREFIX}config", declarations: []string{"prod-", "prod-"}, want: "prod-config"},
		{name: "conflicting-bare", expression: "${PREFIX}config", declarations: []string{"prod-", "dev-", "prod-"}, want: "${PREFIX}config"},
		{name: "conflicting-default", expression: "${PREFIX:-fallback-}config", declarations: []string{"prod-", "dev-", "prod-"}, want: "fallback-config"},
		{name: "unset-bare", expression: "${PREFIX}config", want: "${PREFIX}config"},
		{name: "evaluation-error", expression: "${PREFIX", want: "${PREFIX"},
		{name: "literal", expression: "config", want: "config"},
		{name: "namespace", expression: "${PREFIX}config", namespace: "${NS}", declarations: []string{"prod-"}, want: "prod-config", wantNamespace: "shared"},
		{name: "empty-result", expression: "${PREFIX:-}", want: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			for i, value := range tt.declarations {
				st.AddObject(parsedDependency(t, fmt.Sprintf("supplier-%d", i), fmt.Sprintf("  postBuild: {substitute: {PREFIX: %q, NS: shared}}\n", value)))
			}
			spec := fmt.Sprintf("  dependsOn: [{name: %q, namespace: %q}]\n", tt.expression, tt.namespace)
			if tt.inline != "" {
				spec += fmt.Sprintf("  postBuild: {substitute: {PREFIX: %q}}\n", tt.inline)
			}
			original := parsedDependency(t, "consumer", spec)
			before := original.Clone()
			st.AddObject(original)
			d := NewDependencySubstitutions(st.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization))
			resolved := manifest.MapDependsOn(original, d.Resolve, d.Resolve).(*manifest.Kustomization)
			assert.Equal(t, resolved.DependsOn[0].Name, tt.want)
			assert.Equal(t, resolved.DependsOn[0].Namespace, tt.wantNamespace)
			st.AddObject(resolved)
			if diff := cmp.Diff(before, original, cmp.AllowUnexported(manifest.Kustomization{}), cmpopts.IgnoreFields(manifest.Kustomization{}, "projection")); diff != "" {
				t.Fatalf("stored input mutated (-want +got):\n%s", diff)
			}
			if again := manifest.MapDependsOn(resolved, d.Resolve, d.Resolve); again != resolved {
				t.Fatal("second pass replaced the resolved object")
			}
			if tt.want != "" && tt.want[0] != '$' {
				ns := "apps"
				if tt.wantNamespace != "" {
					ns = tt.wantNamespace
				}
				target := parsedDependency(t, tt.want, "  path: ./target\n")
				st.AddObject(target.WithNamespace(ns))
				for ref := range manifest.References(resolved, manifest.ReferenceRelations) {
					if ref.Use == "dependsOn" && st.GetObject(ref.NamedResource) == nil {
						t.Fatalf("resolved dependency does not match target: %s", ref.NamedResource)
					}
				}
			}
		})
	}
}

func TestDependencySubstitutions_ZeroValue(t *testing.T) {
	t.Parallel()
	var d DependencySubstitutions
	assert.Equal(t, d.Resolve("${PREFIX:-dev-}config"), "dev-config")
	assert.Equal(t, d.Resolve("${PREFIX:-}config"), "config")
	assert.Equal(t, d.Resolve("${PREFIX}config"), "${PREFIX}config")
}

func TestDependencySubstitutions_NoOpAllocations(t *testing.T) {
	original := parsedDependency(t, "plain", "  dependsOn: [{name: config}]\n")
	d := NewDependencySubstitutions(nil)
	allocs := testing.AllocsPerRun(100, func() {
		if got := manifest.MapDependsOn(original, d.Resolve, d.Resolve); got != original {
			t.Fatal("literal dependency replaced")
		}
	})
	assert.Equal(t, allocs, float64(0))
}
