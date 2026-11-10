package kustomize

import (
	"fmt"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/values"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func parsedDependency(t *testing.T, name, spec string) *manifest.Kustomization {
	t.Helper()
	docs, err := testutil.SplitDocs([]byte(testutil.KustomizationYAML(name, "apps", spec)))
	assert.NoError(t, err)
	parsed, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
	obj := parsed.Object
	assert.NoError(t, err)
	return obj.(*manifest.Kustomization)
}

func TestDocumentDependencies_ParsedDependsOn(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, expression, namespace, want, wantNamespace string
		declarations                                     []string
		inline                                           string
	}{
		{name: "empty-supplied", expression: "${PREFIX:-}config", inline: "prod-", want: "prod-config"},
		{name: "nonempty-supplied", expression: "${PREFIX:-dev-}config", inline: "prod-", want: "prod-config"},
		{name: "separate-supplier", expression: "${PREFIX:-}config", declarations: []string{"prod-"}, want: "${PREFIX:-}config"},
		{name: "empty-absent", expression: "${PREFIX:-}config", want: "${PREFIX:-}config"},
		{name: "nonempty-absent", expression: "${PREFIX:-dev-}config", want: "${PREFIX:-dev-}config"},
		{name: "assign-supplied", expression: "${PREFIX:=}config", inline: "prod-", want: "prod-config"},
		{name: "assign-absent", expression: "${PREFIX:=dev-}config", want: "${PREFIX:=dev-}config"},
		{name: "explicit-empty", expression: "${PREFIX:-dev-}config", declarations: []string{""}, want: "${PREFIX:-dev-}config"},
		{name: "agreeing", expression: "${PREFIX}config", declarations: []string{"prod-", "prod-"}, want: "${PREFIX}config"},
		{name: "conflicting-bare", expression: "${PREFIX}config", declarations: []string{"prod-", "dev-", "prod-"}, want: "${PREFIX}config"},
		{name: "conflicting-default", expression: "${PREFIX:-fallback-}config", declarations: []string{"prod-", "dev-", "prod-"}, want: "${PREFIX:-fallback-}config"},
		{name: "unset-bare", expression: "${PREFIX}config", want: "${PREFIX}config"},
		{name: "evaluation-error", expression: "${PREFIX", want: "${PREFIX"},
		{name: "literal", expression: "config", want: "config"},
		{name: "namespace", expression: "${PREFIX}config", namespace: "${NS}", declarations: []string{"prod-"}, want: "${PREFIX}config", wantNamespace: "${NS}"},
		{name: "empty-result", expression: "${PREFIX:-}", want: "${PREFIX:-}"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testDocumentDependenciesCase(
				t,
				&tt,
			)
		},
		)
	}
}

func TestDocumentDependencies_ZeroValue(t *testing.T) {
	t.Parallel()
	for _, expression := range []string{"${PREFIX:-dev-}config", "${PREFIX:-}config", "${PREFIX}config"} {
		parent := &manifest.Kustomization{}
		original := parsedDependency(t, "consumer", fmt.Sprintf("  dependsOn: [{name: %q}]\n", expression))
		resolved := shapeDependency(t, parent, original).Object.(*manifest.Kustomization)
		assert.Equal(t, resolved.DependsOn[0].Name, expression)
	}
}

//nolint:paralleltest // testing.AllocsPerRun changes process-global state.
func TestDocumentDependencies_NoOpAllocations(t *testing.T) {
	original := parsedDependency(t, "plain", "  dependsOn: [{name: config}]\n")
	occurrence := manifest.DocOccurrence{Document: testutil.ObjectDocument(original)}
	before := original
	allocs := testing.AllocsPerRun(100, func() {
		data, err := values.SubstitutionDocument(occurrence, false)
		if data != nil || err != nil || original != before {
			t.Fatal("literal dependency replaced")
		}
	})
	assert.Equal(t, allocs, float64(0))
}

func testDocumentDependenciesCase(
	t *testing.T,
	testCaseInput *struct {
		name          string
		expression    string
		namespace     string
		want          string
		wantNamespace string
		declarations  []string
		inline        string
	},
) {
	t.Helper()

	st := store.New()
	for i, value := range testCaseInput.declarations {
		st.AddObject(
			parsedDependency(
				t,
				fmt.Sprintf("supplier-%d", i),
				fmt.Sprintf("  postBuild: {substitute: {PREFIX: %q, NS: shared}}\n", value),
			),
		)
	}
	spec := fmt.Sprintf(
		"  dependsOn: [{name: %q, namespace: %q}]\n",
		testCaseInput.expression,
		testCaseInput.namespace,
	)
	original := parsedDependency(t, "consumer", spec)
	before := original.Clone()
	st.AddObject(original)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	if testCaseInput.inline != "" {
		parent.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"PREFIX": testCaseInput.inline}}
	}
	shaped := shapeDependency(t, parent, original)
	resolved, document := shaped.Object.(*manifest.Kustomization), shaped.Document
	assert.Equal(t, resolved.DependsOn[0].Name, testCaseInput.want)
	assert.Equal(t, resolved.DependsOn[0].Namespace, testCaseInput.wantNamespace)
	st.AddObject(resolved)
	{
		diff := cmp.Diff(
			before,
			original,
			cmp.AllowUnexported(manifest.Kustomization{}),
			cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
		)
		assert.FatalIf(t, diff != "", "stored input mutated (-want +got):\n%s", diff)
	}
	{
		before := manifest.Fingerprint(document)
		data, err := values.SubstitutionDocument(
			manifest.DocOccurrence{Document: document},
			values.SubstitutionActive(parent),
		)
		assert.NoError(t, err)
		assert.Equal(t, data == nil, true)
		assert.FatalIf(t, manifest.Fingerprint(document) != before, "%s", "second pass changed the resolved document")
	}
	if testCaseInput.want != "" {
		ns := "apps"
		if testCaseInput.wantNamespace != "" {
			ns = testCaseInput.wantNamespace
		}
		target := parsedDependency(t, testCaseInput.want, "  path: ./target\n")
		st.AddObject(target.WithNamespace(ns))
		for ref := range manifest.References(resolved, manifest.ReferenceRelations) {
			assert.FatalIf(
				t,
				ref.Use == "dependsOn" && st.GetObject(ref.NamedResource) == nil,
				"resolved dependency does not match target: %s",
				ref.NamedResource,
			)
		}
	}
}

func shapeDependency(
	t *testing.T,
	parent, child *manifest.Kustomization,
) manifest.ParsedDoc {
	t.Helper()
	docs := []manifest.DocOccurrence{{Document: testutil.ObjectDocument(child)}}
	assert.NoError(t, PrepareShape(&RenderRequest{Spec: parent}, docs).Apply(t.Context(), ShapeOptions{}))
	parsed, err := manifest.ParseDoc(docs[0].Document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	return parsed
}
