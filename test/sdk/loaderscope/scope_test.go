package loaderscope_test

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestScopeStartup(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "set.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: set, namespace: authored}
spec:
  dependsOn:
  - apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRole
    name: role
    namespace: explicit
`)
	s := store.New()
	l := loader.New(s)
	{
		_, err := l.Load(t.Context(), root)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	l.Admit(root)
	rs, ok := s.Get[*manifest.ResourceSet](
		manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      manifest.KindResourceSet,
			Name:      "set",
			Namespace: "authored",
		},
	)
	assert.FatalIf(t, !ok, "%v", "standalone loader did not admit the ResourceSet")
	for ref := range manifest.References(rs, manifest.ReferenceRelations) {
		assert.FatalIf(t, ref.Namespace != "", "cluster dependency namespace = %q", ref.Namespace)
	}
	assert.FatalIf(
		t,
		manifest.KindOf(&manifest.RawObject{APIVersion: "v1", Kind: "Namespace"}).Namespaced(),
		"%v",
		"loader startup did not install builtin scope authority",
	)
}
