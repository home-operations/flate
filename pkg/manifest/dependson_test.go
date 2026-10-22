package manifest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestParseDoc_DependsOnNames(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"", "${PREFIX:-}"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			doc := mustYAML(t, fmt.Sprintf("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer}\nspec:\n  dependsOn: [{name: %q}]\n", name))
			_, _, _, err := ParseDoc(doc, ParseDocOptions{})
			if (err != nil) != (name == "") {
				t.Fatalf("authored dependency %q: err=%v", name, err)
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
	if err != nil {
		t.Fatal(err)
	}
	original := obj.(*Kustomization)
	before := original.Clone()
	identity := func(s string) string { return s }
	if MapDependsOn(original, identity, identity) != original {
		t.Fatal("no-op transform replaced object")
	}
	other := &RawObject{APIVersion: "v1", Kind: KindConfigMap, Name: "config"}
	if MapDependsOn(other, func(string) string { t.Fatal("transformed another kind"); return "" }, nil) != other {
		t.Fatal("another kind replaced")
	}
	resolve := func(s string) string { return strings.ReplaceAll(s, "${PREFIX}", "prod-") }
	for _, namespaces := range []bool{false, true} {
		t.Run(map[bool]string{false: "preserve-namespace", true: "resolve-namespace"}[namespaces], func(t *testing.T) {
			t.Parallel()
			var ns func(string) string
			if namespaces {
				ns = func(s string) string { return strings.ReplaceAll(s, "${NS}", "infra") }
			}
			got := MapDependsOn(original, resolve, ns).(*Kustomization)
			want := before.Clone()
			want.DependsOn[1].Name = "prod-config"
			want.DependsOn[2].Name = "prod-tail"
			if namespaces {
				want.DependsOn[2].Namespace = "infra"
			}
			if got == original {
				t.Fatal("changed dependency did not clone")
			}
			if diff := cmp.Diff(want, got, cmp.AllowUnexported(Kustomization{}), cmpopts.IgnoreFields(Kustomization{}, "projection")); diff != "" {
				t.Fatalf("transformed fields (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, original, cmp.AllowUnexported(Kustomization{}), cmpopts.IgnoreFields(Kustomization{}, "projection")); diff != "" {
				t.Fatalf("authored object mutated (-want +got):\n%s", diff)
			}
			for ref := range References(got, ReferenceRelations) {
				if ref.Use == "dependsOn" && ref.Position == 1 && (ref.Namespace != "apps" || got.DependsOn[1].Namespace != "") {
					t.Fatal("omitted namespace lost inheritance")
				}
			}
		})
	}
	if got := MapDependsOn(original, identity, func(s string) string { return strings.ReplaceAll(s, "${NS}", "infra") }); got == original {
		t.Fatal("namespace-only change did not clone")
	}
}
