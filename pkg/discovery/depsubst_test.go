package discovery_test

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRun_DependencySubstitutionsSnapshot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "flux/parent.yaml", testutil.KustomizationYAML("parent", "apps", "  path: ./children\n"))
	testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  path: ./app\n  dependsOn: [{name: '${PREFIX:-dev-}config'}]\n"))
	testutil.WriteFile(t, root, "children/supplier.yaml", testutil.KustomizationYAML("supplier", "apps", "  postBuild: {substitute: {PREFIX: prod-}}\n"))
	testutil.WriteFile(t, root, "children/target.yaml", testutil.KustomizationYAML("prod-config", "apps", "  path: ./config\n"))
	st := store.New()
	res, err := discovery.Run(t.Context(), discovery.Config{Path: filepath.Join(root, "flux"), Store: st})
	if err != nil {
		t.Fatal(err)
	}
	consumer, ok := st.Get[*manifest.Kustomization](testutil.KustomizationID("apps", "consumer"))
	if !ok {
		t.Fatal("consumer missing")
	}
	assert.Equal(t, consumer.DependsOn[0].Name, "prod-config")
	assert.Equal(t, res.DependencySubstitutions.Resolve("${PREFIX:-dev-}config"), "prod-config")
	if _, exists := res.Index.Lookup(consumer.Named()); !exists {
		t.Fatal("resolved discovery index was not published")
	}
	for ref := range manifest.References(consumer) {
		if ref.Use == "dependsOn" && st.GetObject(ref.NamedResource) == nil {
			t.Fatalf("discovered dependency target missing: %s", ref.NamedResource)
		}
	}
	supplier, _ := st.Get[*manifest.Kustomization](testutil.KustomizationID("apps", "supplier"))
	changed := supplier.Clone()
	changed.PostBuild.Substitute["PREFIX"] = "later-"
	st.AddObject(changed)
	assert.Equal(t, res.DependencySubstitutions.Resolve("${PREFIX:-dev-}config"), "prod-config")
}
