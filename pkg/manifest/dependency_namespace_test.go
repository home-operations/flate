package manifest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestIsClusterScopedKind_Scope(t *testing.T) {
	for _, tc := range []struct {
		kind string
		want bool
	}{
		{"CustomResourceDefinition", true}, {"Namespace", true}, {"ClusterRole", true}, {"ClusterIssuer", true},
		{"ConfigMap", false}, {"Secret", false}, {"Kustomization", false}, {"HelmRelease", false}, {"Widget", false}, {"", false},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			assert.Equal(t, IsClusterScopedKind(tc.kind), tc.want)
			assert.Equal(t, IsClusterScoped(map[string]any{"kind": tc.kind}), tc.want)
		})
	}
}

func TestDependsOn_TypedNamespace(t *testing.T) {
	for _, kind := range []string{KindKustomization, KindHelmRelease} {
		for _, ns := range []string{"", "other"} {
			t.Run(kind+"/"+ns, func(t *testing.T) {
				api, spec := "kustomize.toolkit.fluxcd.io/v1", "sourceRef: {kind: GitRepository, name: source}"
				if kind == KindHelmRelease {
					api = "helm.toolkit.fluxcd.io/v2"
					spec = "chartRef: {kind: OCIRepository, name: source}"
				}
				text := fmt.Sprintf("apiVersion: %s\nkind: %s\nmetadata: {name: app, namespace: apps}\nspec:\n  %s\n  dependsOn:\n  - name: dependency\n    namespace: %q\n    readyExpr: 'true'\n", api, kind, spec, ns)
				docs, err := DecodeDocs(strings.NewReader(text))
				if err != nil {
					t.Fatal(err)
				}
				obj, err := ParseDoc(docs[0], ParseDocOptions{})
				if err != nil {
					t.Fatal(err)
				}
				var deps []DependencyRef
				switch x := obj.(type) {
				case *Kustomization:
					deps = x.DependsOn
				case *HelmRelease:
					deps = x.DependsOn
				default:
					t.Fatalf("unexpected %T", obj)
				}
				wantNS := ns
				if wantNS == "" {
					wantNS = "apps"
				}
				assert.Diff(t, deps, []DependencyRef{{Kind: kind, Name: "dependency", Namespace: wantNS, ReadyExpr: "true"}})
			})
		}
	}
}
