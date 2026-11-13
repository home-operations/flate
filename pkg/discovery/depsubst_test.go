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

func TestRun_DependencyDocumentsImmutable(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(
		t,
		root,
		"flux/parent.yaml",
		testutil.KustomizationYAML("parent", "apps", "  path: ./children\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"apps",
			"  path: ./app\n  dependsOn: [{name: '${PREFIX:-dev-}config'}]\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"children/supplier.yaml",
		testutil.KustomizationYAML("supplier", "apps", "  postBuild: {substitute: {PREFIX: prod-}}\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"children/target.yaml",
		testutil.KustomizationYAML("prod-config", "apps", "  path: ./config\n"),
	)
	st := store.New()
	res, err := discovery.Run(
		t.Context(),
		discovery.Config{Trees: testTrees(t), Path: filepath.Join(root, "flux"), Store: st},
	)
	assert.FatalIf(t, err != nil, "%v", err)
	consumer := testutil.KustomizationID("apps", "consumer")
	assert.Equal(t, st.GetObject(consumer).(*manifest.Kustomization).DependsOn[0].Name, "${PREFIX:-dev-}config")
	if record, exists := res.Index.Lookup(
		consumer,
	); !exists || record.Parent != (manifest.NamedResource{}) ||
		st.GetObject(consumer) == nil {
		t.Fatalf("discovery-only consumer ownership = %+v, published=%v", record, st.GetObject(consumer))
	}
	{
		_, exists := res.Index.Lookup(testutil.KustomizationID("apps", "prod-config"))
		assert.FatalIf(t, !exists, "%v", "discovered dependency target missing")
	}
	supplier, ok := st.Get[*manifest.Kustomization](testutil.KustomizationID("apps", "supplier"))
	assert.FatalIf(t, !ok, "%v", "independent inline supplier missing")
	changed := supplier.Clone()
	changed.PostBuild.Substitute["PREFIX"] = "later-"
	st.AddObject(changed)
	assert.Equal(t, st.GetObject(consumer).(*manifest.Kustomization).DependsOn[0].Name, "${PREFIX:-dev-}config")
}

func TestRun_DependsOnPreservesAdmission(t *testing.T) {
	t.Parallel()
	t.Run(
		"inactive supplier and source",
		func(t *testing.T) { t.Parallel(); testRunDependsOnPreservesAdmissionInactiveSupplierAndSource2(t) },
	)

	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [child.yaml, supplier.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/child.yaml",
		testutil.KustomizationYAML(
			"child",
			"apps",
			"  path: ./leaf\n  dependsOn: [{name: '${PREFIX:-dev-}config', namespace: '${TEAM}'}]\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"app/supplier.yaml",
		testutil.KustomizationYAML("supplier", "apps", "  postBuild: {substitute: {PREFIX: prod-, TEAM: shared}}\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"other/active.yaml",
		testutil.KustomizationYAML(
			"active",
			"apps",
			"  path: ./other\n  dependsOn: [{name: '${PREFIX:-dev-}config', namespace: '${TEAM}'}]\n",
		),
	)
	st := store.New()
	parsed, err := manifest.ParseDoc(map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "active", "namespace": "apps", "uid": "admitted"},
		"spec": map[string]any{
			"path":      "./other",
			"dependsOn": []any{map[string]any{"name": "${PREFIX:-dev-}config", "namespace": "${TEAM}"}},
			"postBuild": map[string]any{"substitute": map[string]any{"TEAM": "shared"}},
		},
	}, manifest.ParseDocOptions{})
	activeObject, activeDoc := parsed.Object, parsed.Document
	assert.FatalIf(t, err != nil, "%v", err)
	before := manifest.Fingerprint(activeDoc)
	st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if id == activeObject.Named() {
			got := payload.(*manifest.Kustomization)
			assert.Equal(t, got.DependsOn[0].Name, "${PREFIX:-dev-}config")
			assert.Equal(t, got.DependsOn[0].Namespace, "${TEAM}")
		}
	}, false)
	st.AddObject(activeObject, activeDoc)
	st.AddObject(&manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./app"})
	result, err := discovery.Run(
		t.Context(),
		discovery.Config{Trees: testTrees(t), Path: filepath.Join(root, "app"), RepoRoot: root, Store: st},
	)
	assert.FatalIf(t, err != nil, "%v", err)
	_, doc, emission := st.GetEmission(activeObject.Named())
	emission.Finish()
	assert.Equal(t, doc["metadata"].(map[string]any)["uid"], any("admitted"))
	assert.Equal(t, manifest.Fingerprint(doc), before)

	assert.FatalIf(
		t,
		st.GetObject(activeObject.Named()).(*manifest.Kustomization).DependsOn[0].Name != "${PREFIX:-dev-}config",
		"%v",
		"dependency substitutions were not resolved",
	)
	active, ok := st.Get[*manifest.Kustomization](testutil.KustomizationID("apps", "active"))
	assert.FatalIf(
		t,
		!ok || active.DependsOn[0].Name != "${PREFIX:-dev-}config",
		"admitted owner dependency changed: %+v",
		active,
	)
	assert.Equal(t, active.DependsOn[0].Namespace, "${TEAM}")
	assert.Equal(t, activeObject.(*manifest.Kustomization).DependsOn[0].Namespace, "${TEAM}")
	child := testutil.KustomizationID("apps", "child")
	{
		_, indexed := result.Index.Lookup(child)
		assert.FatalIf(
			t,
			!indexed || st.GetObject(child) != nil,
			"%v",
			"dependency substitution republished render-owned child",
		)
	}
}

func testRunDependsOnPreservesAdmissionInactiveSupplierAndSource(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"active.yaml",
		testutil.KustomizationYAML(
			"active",
			"apps",
			"  path: ./active\n  dependsOn: [{name: '${PREFIX:-dev-}config'}]\n",
		),
	)
	testutil.WriteFile(t, root, "inactive/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(
		t,
		root,
		"inactive/disabled.yaml",
		testutil.KustomizationYAML(
			"disabled",
			"apps",
			"  path: ./inactive-target\n  postBuild: {substitute: {PREFIX: inactive-}}\n  sourceRef: {kind: GitRepository, name: inactive-only}\n",
		),
	)
	testutil.WriteFile(t, root, "inactive-target/kustomization.yaml", "resources: [source.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"inactive-target/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: inactive-source, namespace: apps}\nspec: {url: https://fixture.invalid/inactive.git}\n",
	)
	st := store.New()
	result, err := discovery.Run(t.Context(), discovery.Config{Trees: testTrees(t), Path: root, Store: st})
	assert.FatalIf(t, err != nil, "%v", err)
	active := testutil.KustomizationID("apps", "active")
	if record, indexed := result.Index.Lookup(
		active,
	); !indexed || record.Parent != (manifest.NamedResource{}) ||
		st.GetObject(active) == nil {
		t.Fatalf("discovery-only active ownership = %+v, published=%v", record, st.GetObject(active))
	}
	assert.FatalIf(
		t,
		st.GetObject(active).(*manifest.Kustomization).DependsOn[0].Name != "${PREFIX:-dev-}config",
		"%v",
		"inactive declaration supplied active substitution",
	)
	for _, name := range []string{"inactive-only", "inactive-source"} {
		id := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindGitRepository,
			Namespace: "apps",
			Name:      name,
		}
		assert.FatalIf(
			t,
			st.GetObject(id) != nil || st.GetArtifact(id, store.ArtifactContent) != nil,
			"inactive evidence published a source: %s",
			id,
		)
	}
}

func testRunDependsOnPreservesAdmissionInactiveSupplierAndSource2(t *testing.T) {
	t.Helper()
	testRunDependsOnPreservesAdmissionInactiveSupplierAndSource(t)
}
