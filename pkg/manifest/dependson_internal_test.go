package manifest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestParseDoc_DependsOnNames(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"", "${PREFIX:-}"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			doc := mustYAML(
				t,
				fmt.Sprintf(
					"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer}\nspec:\n  dependsOn: [{name: %q}]\n",
					name,
				),
			)
			_, _, _, err := ParseDoc(doc, ParseDocOptions{})
			{
				assert.FatalIf(t, (err != nil) != (name == ""), "authored dependency %q: err=%v", name, err)
			}
		})
	}
}

func TestMapDependsOn_PreservesFields(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: apps}
spec:
  path: ./app
  dependsOn:
  - {name: plain, namespace: shared, readyExpr: 'dep.metadata.name == "plain"'}
  - {name: '${PREFIX}config', readyExpr: 'dep.metadata.name == "prod-config"'}
  - {name: '${PREFIX}tail', namespace: '${NS}'}
`)
	obj, _, _, err := ParseDoc(doc, ParseDocOptions{})
	{
		assert.NoError(t, err)
	}
	original := obj.(*Kustomization)
	before := original.Clone()
	identity := func(s string) string { return s }
	{
		assert.FatalIf(
			t,
			MapDependsOn(original, identity, identity) != original,
			"%s",
			"no-op transform replaced object",
		)
	}
	other := &RawObject{APIVersion: "v1", Kind: KindConfigMap, Name: "config"}
	{
		assert.FatalIf(
			t,
			MapDependsOn(other, func(string) string { t.Fatal("transformed another kind"); return "" }, nil) != other,
			"%s",
			"another kind replaced",
		)
	}
	resolve := func(s string) string { return strings.ReplaceAll(s, "${PREFIX}", "prod-") }
	for _, namespaces := range []bool{false, true} {
		t.Run(map[bool]string{false: "preserve-namespace", true: "resolve-namespace"}[namespaces], func(t *testing.T) {
			t.Parallel()
			testMapDependsOnPreservesFieldsCase(
				t,
				&testMapDependsOnPreservesFieldsCaseInputs{
					original:   &original,
					before:     &before,
					resolve:    &resolve,
					namespaces: &namespaces,
				},
			)
		},
		)
	}
	{
		got := MapDependsOn(
			original,
			identity,
			func(s string) string { return strings.ReplaceAll(s, "${NS}", "infra") },
		)
		assert.FatalIf(t, got == original, "%s", "namespace-only change did not clone")
	}
}

type testMapDependsOnPreservesFieldsCaseInputs struct {
	original   **Kustomization
	before     **Kustomization
	resolve    *func(s string) string
	namespaces *bool
}

func testMapDependsOnPreservesFieldsCase(t *testing.T, testCaseInput *testMapDependsOnPreservesFieldsCaseInputs) {
	t.Helper()

	var ns func(string) string
	if *testCaseInput.namespaces {
		ns = func(s string) string { return strings.ReplaceAll(s, "${NS}", "infra") }
	}
	got := MapDependsOn((*testCaseInput.original), (*testCaseInput.resolve), ns).(*Kustomization)
	want := (*testCaseInput.before).Clone()
	want.DependsOn[1].Name = "prod-config"
	want.DependsOn[2].Name = "prod-tail"
	if *testCaseInput.namespaces {
		want.DependsOn[2].Namespace = "infra"
	}
	{
		assert.FatalIf(t, got == (*testCaseInput.original), "%s", "changed dependency did not clone")
	}
	{
		diff := cmp.Diff(
			want,
			got,
			cmp.AllowUnexported(Kustomization{}),
			cmpopts.IgnoreFields(Kustomization{}, "projection"),
		)
		assert.FatalIf(t, diff != "", "transformed fields (-want +got):\n%s", diff)
	}
	{
		diff := cmp.Diff((*testCaseInput.before), (*testCaseInput.original), cmp.AllowUnexported(Kustomization{}),
			cmpopts.IgnoreFields(Kustomization{}, "projection"),
		)
		assert.FatalIf(t, diff != "", "authored object mutated (-want +got):\n%s", diff)
	}
	for ref := range References(got, ReferenceRelations) {
		{
			assert.FatalIf(t, ref.Use == "dependsOn" && ref.Position == 1 &&
				(ref.Namespace != "apps" || got.DependsOn[1].Namespace != ""), "%s", "omitted namespace lost inheritance")
		}
	}
}
