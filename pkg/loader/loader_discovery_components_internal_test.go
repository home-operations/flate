package loader

import (
	"context"
	"path/filepath"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestLoader_OrphanNestedSubtreeSkipped pins the buroa/k8s-gitops
// case: a commented-out wrapper KS in `default/kustomization.yaml`
// (resources: [./valheim/ks.yaml, # ./atuin/ks.yaml]) should leave
// BOTH the wrapper AND the deep HelmRelease under
// `default/atuin/app/helmrelease.yaml` invisible. The wrapper lives
// in a directory (`atuin/`) that has no kustomization.yaml of its
// own; the HR lives in `atuin/app/` which DOES have one but is only
// reachable via the orphan wrapper's spec.path. PR #346 fixed only
// the same-dir orphan case; this test reproduces the deeper shape.
func TestLoader_OrphanNestedSubtreeSkipped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "default/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./valheim/ks.yaml
  # - ./atuin/ks.yaml   # commented toggle stub — should be invisible
`)
	for _, app := range []string{"valheim", "atuin"} {
		testutil.WriteFile(
			t,
			dir,
			"default/"+app+"/ks.yaml",
			testutil.KustomizationYAML(
				app,
				"",
				"  path: ./default/"+app+"/app\n  sourceRef: { kind: GitRepository, name: flux-system, namespace: flux-system }\n",
			),
		)
		testutil.WriteFile(
			t,
			dir,
			"default/"+app+"/app/kustomization.yaml",
			`apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./helmrelease.yaml
`,
		)
		testutil.WriteFile(t, dir, "default/"+app+"/app/helmrelease.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: `+app+`
spec:
  chart:
    spec:
      chart: app-template
      sourceRef: { kind: HelmRepository, name: bjw-s, namespace: flux-system }
      version: "5.0.0"
`)
	}
	s := store.New()
	// Entry the loader at `default/` so its kustomization.yaml is the
	// graph root. The orphan-tree skip only applies under graph-walk;
	// see Loader.Load doc-comment.
	l := loaderFixture(s, nil)
	{
		_, err := loadForTest(context.Background(), t, l, filepath.Join(dir, "default"))
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
	// Valheim (claimed) MUST be present.
	{
		_, ok := l.Index.Lookup(testutil.KustomizationID("", "valheim"))
		assert.ErrorIf(t, !ok, "%s", "claimed Kustomization 'valheim' missing")
	}
	// Atuin (commented out) MUST be absent.
	assert.ErrorIf(
		t,
		s.GetObject(testutil.KustomizationID("", "atuin")) != nil,
		"%s",
		"orphan Kustomization 'atuin' should be skipped (#346 deep-orphan)",
	)
	// Atuin's deep HR MUST also be absent — its only kustomize-graph
	// connection is via the orphan wrapper.
	assert.ErrorIf(
		t,
		s.GetObject(testutil.HelmReleaseID("", "atuin")) != nil,
		"%s",
		"HelmRelease only reachable via orphan wrapper should be skipped",
	)
	// Valheim's HR should NOT load via the initial graph walk either
	// — it lives in `default/valheim/app/` which is only reachable
	// via the wrapper's spec.path, NOT via `default/kustomization.yaml`'s
	// resource graph. The orchestrator's discovery later calls
	// loadAt(valheim/app/) which becomes a fresh entry-point.
	// (This test stops before that step, so the HR is absent here —
	// which is the correct flux-local behavior.)
	assert.ErrorIf(
		t,
		s.GetObject(testutil.HelmReleaseID("", "valheim")) != nil,
		"%s",
		"HelmRelease in unreachable subtree leaked into initial walk",
	)
}

// TestLoader_OrphanYAMLSkipped pins the issue-#342 fix: a YAML
// file in a directory governed by a kustomization.yaml is loaded
// only when it appears in `resources:`. The unreferenced file (a
// "toggle stub" — common pattern where maintainers comment out
// resources entries to disable a wrapper without deleting the file)
// must NOT reach the store. Before the fix, the orphan was
// discovered and reconciled against the wrong overlay state,
// producing spurious "dependency not found" failures for any chart
// source the parent's namespace transform was supposed to inject.
func TestLoader_OrphanYAMLSkipped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/games/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: games
resources:
  - ./valheim.yaml
  # - ./vrising.yaml   # disabled toggle stub
`)
	testutil.WriteFile(
		t,
		dir,
		"apps/games/valheim.yaml",
		testutil.KustomizationYAML(
			"valheim",
			"",
			"  interval: 1h\n  path: ./apps/base/games/valheim\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n",
		),
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/games/vrising.yaml",
		testutil.KustomizationYAML(
			"vrising",
			"",
			"  interval: 1h\n  path: ./apps/base/games/vrising\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n",
		),
	)
	s := store.New()
	s.AddObject(&manifest.Kustomization{Name: "games", Path: "./apps/games"})
	l := New(s)
	l.SourceRoot = dir
	{
		_, err := loadForTest(t.Context(), t, l, dir)
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
	// valheim is referenced — must load. Kustomization namespace
	// is left empty by the parser (the YAML has no
	// metadata.namespace); the parent kustomization.yaml's
	// `namespace:` overlay would fill it at render time.
	{
		_, ok := l.Index.Lookup(testutil.KustomizationID("games", "valheim"))
		assert.ErrorIf(t, !ok, "%s", "referenced Kustomization 'valheim' missing from store")
	}
	// vrising is NOT referenced — must be skipped as orphan.
	assert.ErrorIf(
		t,
		s.GetObject(testutil.KustomizationID("", "vrising")) != nil,
		"%s",
		"orphan Kustomization 'vrising' should not be loaded (issue #342)",
	)
}

// Component data must retain producer evidence for substituteFrom dependencies
// without admitting templated Flux resources outside their parent's render.
func TestLoader_DiscoveryRecordsComponentDataResources(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/settings/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
resources:
  - ./cluster-settings.yaml
  - ./templated-ks.yaml
`)
	testutil.WriteFile(t, dir, "components/settings/cluster-settings.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
  namespace: flux-system
data:
  FOO: bar
`)
	testutil.WriteFile(
		t,
		dir,
		"components/settings/templated-ks.yaml",
		testutil.KustomizationYAML(
			"${APP}-leak",
			"ns",
			"  path: ./irrelevant\n  sourceRef: {kind: GitRepository, name: flux-system}\n  interval: 1h\n",
		),
	)
	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	{
		_, err := loadForTest(context.Background(), t, l, dir)
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}

	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}
	{
		_, ok := l.Index.Lookup(cmID)
		assert.ErrorIf(t, !ok, "Component-housed CM must be recorded in Existence during discovery")
	}
	{
		_, ok := filesForTest(l.Index)[cmID]
		assert.ErrorIf(t, !ok, "Component-housed CM must be recorded in SourceFiles during discovery")
	}
	assert.ErrorIf(
		t,
		s.GetObject(cmID) != nil,
		"Component-housed CM must NOT be in Store during discovery (existence-only)",
	)
	leakID := testutil.KustomizationID("ns", "${APP}-leak")
	assert.ErrorIf(
		t,
		s.GetObject(leakID) != nil,
		"templated Flux CR inside a Component must NOT leak into the Store",
	)
}

func TestLoader_DiscoveryRecordsSourceRefs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "hr.yaml")
	testutil.WriteFile(
		t,
		dir,
		"hr.yaml",
		"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: envoy-gateway, namespace: envoy-gateway}\nspec:\n  chartRef: {kind: OCIRepository, name: envoy-gateway, namespace: flux-system}\n  dependsOn: [{name: parent}]\n  valuesFrom: [{kind: ConfigMap, name: settings}, {kind: Secret, name: credentials, optional: true}]\n",
	)
	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	{
		_, err := loadForTest(t.Context(), t, l, dir)
		assert.NoError(t, err)
	}
	hrID := testutil.HelmReleaseID("envoy-gateway", "envoy-gateway")
	assert.Equal(t, s.GetObject(hrID), manifest.BaseManifest(nil))
	want := []manifest.NamedResource{
		{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindOCIRepository,
			Namespace: "flux-system",
			Name:      "envoy-gateway",
		},
		{Kind: manifest.KindConfigMap, Namespace: "envoy-gateway", Name: "settings"},
		{Kind: manifest.KindSecret, Namespace: "envoy-gateway", Name: "credentials"},
	}
	assert.Diff(t, l.Index.Records[hrID].Refs, want)
	assert.Equal(t, len(l.documents.cache[path].objects), 1)
	ksPath := filepath.Join(dir, "ks.yaml")
	testutil.WriteFile(t, dir, "ks.yaml", testutil.KustomizationYAML("app", "apps", "  {path: ./apps}\n"))
	{
		_, err := l.loadFile(t.Context(), ksPath, &l.documents)
		assert.NoError(t, err)
	}
	assert.Equal(t, len(l.documents.cache[ksPath].objects), 1)
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		file := filepath.Join(dir, kind+".yaml")
		testutil.WriteFile(
			t,
			dir,
			kind+".yaml",
			"apiVersion: v1\nkind: "+kind+"\nmetadata: {name: data, namespace: apps}\n",
		)
		{
			_, err := l.loadFile(t.Context(), file, &l.documents)
			assert.NoError(t, err)
		}
		assert.Equal(t, len(l.documents.cache[file].objects), 1)
		assert.Equal(
			t,
			s.GetObject(manifest.NamedResource{Kind: kind, Name: "data", Namespace: "apps"}),
			manifest.BaseManifest(nil),
		)
	}
	chart := &manifest.HelmChartSource{
		Name:      "chart",
		Namespace: "apps",
		SourceRef: sourcev1.LocalHelmChartSourceReference{Name: "source"},
	}
	l.recordObject(chart, filepath.Join(dir, "chart.yaml"))
	assert.Diff(
		t,
		l.Index.Records[chart.Named()].Refs,
		[]manifest.NamedResource{
			{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindHelmRepository, Namespace: "apps", Name: "source"},
		},
	)
	for _, source := range []kustomizev1.CrossNamespaceSourceReference{{Kind: manifest.KindGitRepository}, {Name: "source"}, {}} {
		ks := &manifest.Kustomization{Name: "incomplete", Namespace: "apps", SourceRef: source}
		l.recordObject(ks, ksPath)
		assert.Equal(t, len(l.Index.Records[ks.Named()].Refs), 0)
	}
	ks := &manifest.Kustomization{
		Name:      "references",
		Namespace: "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "source"},
	}
	ks.DependsOn = []kustomizev1.DependencyReference{{Name: "parent"}}
	ks.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
		{Kind: manifest.KindConfigMap, Name: "settings"},
		{Kind: manifest.KindSecret, Name: "credentials"},
		{Kind: manifest.KindConfigMap, Name: "optional", Optional: true},
	}}
	l.recordObject(ks, ksPath)
	assert.Diff(t, l.Index.Records[ks.Named()].Refs, []manifest.NamedResource{
		{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"},
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"},
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "optional"},
	})
	assert.Diff(t, l.Index.Records[hrID].Refs, want)
}

func TestLoader_DiscoveryRecordsNestedComponentData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/outer/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
components:
  - ../inner
`)
	testutil.WriteFile(t, dir, "components/inner/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
resources:
  - ./shared.yaml
`)
	testutil.WriteFile(t, dir, "components/inner/shared.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: shared-cm
  namespace: flux-system
data:
  KEY: value
`)
	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	// Drive the loader at the outer Component directly — same shape
	// as TestLoader_SkipsKustomizeComponentSubtree's entry pattern.
	{
		_, err := loadForTest(context.Background(), t, l, filepath.Join(dir, "components", "outer"))
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}

	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "shared-cm"}
	{
		_, ok := l.Index.Lookup(id)
		assert.ErrorIf(t, !ok, "inner Component CM reached via Component-of-Component must be recorded in Existence")
	}
	{
		_, ok := filesForTest(l.Index)[id]
		assert.ErrorIf(t, !ok, "inner Component CM must be recorded in SourceFiles")
	}
	for _, role := range []string{"resources", "components"} {
		t.Run("ordinary directory through "+role, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [../component, ../ordinary]\n")
			testutil.WriteFile(
				t,
				root,
				"component/kustomization.yaml",
				"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n"+role+": [../ordinary]\n",
			)
			testutil.WriteFile(t, root, "ordinary/kustomization.yaml", "resources: [ks.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"ordinary/ks.yaml",
				testutil.KustomizationYAML("ordinary", "apps", "  path: ./workload\n"),
			)
			l := New(store.New())
			l.SourceRoot = root
			count, err := loadForTest(t.Context(), t, l, filepath.Join(root, "entry"))
			assert.NoError(t, err)
			_, found := l.Index.Lookup(testutil.KustomizationID("apps", "ordinary"))
			want := role == "resources"
			wantCount := 0
			if want {
				wantCount = 1
			}
			assert.FatalIf(
				t,
				found != want || count != wantCount,
				"ordinary Resource after Component %s: found=%t, count=%d",
				role,
				found,
				count,
			)
		})
	}
}

func TestLoader_DiscoveryComponentCycleTerminates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/a/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
components:
  - ../b
`)
	testutil.WriteFile(t, dir, "components/b/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
components:
  - ../a
`)
	s := store.New()
	l := loaderFixture(s, nil)
	// No timeout context: if cycle protection fails, the test hangs
	// and the test framework's overall timeout surfaces the bug.
	{
		_, err := loadForTest(context.Background(), t, l, filepath.Join(dir, "components", "a"))
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
}
