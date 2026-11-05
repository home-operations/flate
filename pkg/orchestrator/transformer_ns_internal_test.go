package orchestrator

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
)

// Prediction must precede the leaf's first render so children receive the
// overlay's target namespace even without a structural parent.
//
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_TransformerTargetNamespace(t *testing.T) {
	dir := t.TempDir()

	testutil.WriteFile(t, dir, "transformers/kustomization.yaml", "resources:\n  - ./transformer.yaml\n")
	testutil.WriteFile(t, dir, "transformers/transformer.yaml", `apiVersion: builtin
kind: NamespaceTransformer
metadata:
  name: apply-ns
  namespace: .invalid
fieldSpecs:
  - path: metadata/name
    kind: Namespace
    create: true
  - path: spec/targetNamespace
    group: kustomize.toolkit.fluxcd.io
    kind: Kustomization
    create: true
`)

	testutil.WriteFile(t, dir, "apps/myapp/kustomization.yaml", `resources:
  - ./namespace.yaml
  - ./myapp/ks.yaml
transformers:
  - ./transformers
`)
	testutil.WriteFile(t, dir, "apps/myapp/namespace.yaml", `apiVersion: v1
kind: Namespace
metadata:
  name: .invalid
`)
	testutil.WriteFile(t, dir, "apps/myapp/transformers/kustomization.yaml", `namespace: myns
resources:
  - ../../../transformers
`)

	// Namespace prediction MUST qualify a leaf with no structural parent before its first render.
	testutil.WriteFile(
		t,
		dir,
		"apps/myapp/myapp/ks.yaml",
		testutil.KustomizationYAML(
			"myapp",
			"flux-system",
			"  path: ./apps/myapp/myapp/app\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/myapp/myapp/app/kustomization.yaml", "resources:\n  - ./helmrelease.yaml\n")
	testutil.WriteFile(t, dir, "apps/myapp/myapp/app/helmrelease.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: myapp
spec:
  interval: 1h
  chart:
    spec:
      chart: ./charts/myapp
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "charts/myapp/Chart.yaml", "apiVersion: v2\nname: myapp\nversion: 1.0.0\n")
	testutil.WriteFile(t, dir, "charts/myapp/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-output}\n")

	renderedSingleton1, err := RenderTrees(
		t.Context(),
		[]Tree{{Path: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		Config{Concurrency: 4, RecordWork: true},
	)
	assert.FatalIf(t, len(renderedSingleton1) != 1, "missing render: %v", err)
	o := renderedSingleton1[0].Orchestrator
	res := renderedSingleton1[0].Result
	assert.FatalIf(t, err != nil, "Render: %v", err)

	canonical := testutil.KustomizationID("myns", "myapp")
	assert.ErrorIf(t, o.store.GetObject(canonical) == nil, "%v", "transformer-projected Kustomization is missing")
	stale := canonical
	stale.Namespace = "flux-system"
	assert.ErrorIf(
		t,
		o.store.GetObject(stale) != nil,
		"%v",
		"authored Kustomization identity survived the namespace transformer",
	)

	namespaced := testutil.HelmReleaseID("myns", "myapp")
	assert.ErrorIf(t, o.store.GetObject(namespaced) == nil,
		"expected HelmRelease at myns/myapp; HR objects in store: %v",
		hrIDs(o))

	// The empty-namespace phantom must never reach the store.
	phantom := testutil.HelmReleaseID("", "myapp")
	assert.Equal(t, workCounts(res.Work, phantom, manifest.WorkHelm), manifest.WorkCounts{})
	assert.Equal(t, workCounts(res.Work, namespaced, manifest.WorkHelm), manifest.WorkCounts{Attempts: 1, Completed: 1})
	{
		obj := o.store.GetObject(phantom)
		assert.ErrorIf(t, obj != nil, "empty-namespace HelmRelease phantom present in store: %v", phantom)
	}

	for id := range res.Failed {
		assert.ErrorIf(t, id.Kind == manifest.KindHelmRelease,
			"unexpected HelmRelease failure: %s = %v", id, res.Failed[id])
	}

	testutil.WriteFile(
		t,
		dir,
		"apps/shared/kustomization.yaml",
		"namespace: other\nresources: [../myapp/myapp/ks.yaml]\npatches:\n- target: {kind: Kustomization}\n  patch: |-\n    - op: add\n      path: /spec/targetNamespace\n      value: other\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"cluster/ks.yaml",
		testutil.KustomizationYAML(
			"cluster-a",
			"flux-system",
			"  {path: ./apps/myapp}\n",
		)+"---\n"+testutil.KustomizationYAML(
			"cluster-b",
			"flux-system",
			"  {path: ./apps/shared}\n",
		),
	)
	path := dir + "/cluster"
	renderedSingleton2, err := RenderTrees(
		t.Context(),
		[]Tree{{Path: path, RepoRoot: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		Config{Concurrency: 4},
	)
	assert.FatalIf(t, len(renderedSingleton2) != 1, "missing render: %v", err)
	o = renderedSingleton2[0].Orchestrator
	res = renderedSingleton2[0].Result
	assert.FatalIf(t, err != nil, "%v", err)
	assert.FatalIf(t, len(res.Failed) != 0,
		"shared-context failures: %v", res.Failed)

	for _, ns := range []string{"myns", "other"} {
		for _, info := range reconcilableKinds {
			kind := info.Kind
			id := manifest.NamedResource{Group: info.Group, Kind: kind, Namespace: ns, Name: "myapp"}
			assert.ErrorIf(t, o.store.GetObject(id) == nil, "shared-context render omitted %s", id)
		}
	}

	t.Run("changed_only_second_source", func(t *testing.T) {
		testOrchestratorTransformerTargetNamespaceChangedOnlySecondSource2(
			t,
			testOrchestratorTransformerTargetNamespaceChangedOnlySecondSource2Inputs{dir: &dir, path: &path},
		)
	},
	)
}

func hrIDs(o *Orchestrator) []string {
	var out []string //nolint:prealloc // An empty ID result must retain its nil slice.
	for _, obj := range o.store.ListObjects(manifest.HelmReleaseDomain, manifest.KindHelmRelease) {
		out = append(out, obj.Named().String())
	}
	return out
}

type testOrchestratorTransformerTargetNamespaceChangedOnlySecondSourceInputs struct {
	dir  *string
	path *string
}

func testOrchestratorTransformerTargetNamespaceChangedOnlySecondSource(
	t *testing.T,
	testCaseInput testOrchestratorTransformerTargetNamespaceChangedOnlySecondSourceInputs,
) {
	t.Helper()

	testutil.WriteFile(
		t, (*testCaseInput.dir), "apps/myapp/myapp/ks.yaml",
		testutil.KustomizationYAML(
			"myapp",
			"flux-system",
			"  path: ./apps/myapp/myapp/app\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n",
		),
	)
	for _, ns := range []string{"myns", "other"} {
		testutil.WriteFile(
			t,
			(*testCaseInput.dir),
			"cluster/source-"+ns+".yaml",
			`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: `+ns+`}
spec:
  url: https://fixture.invalid/cluster
`,
		)
	}
	const dirty = "cluster/source-other.yaml"
	o, err := newTestOrchestrator(t, Tree{
		Path: (*testCaseInput.path), RepoRoot: (*testCaseInput.dir),
		SelfURLs:        []string{"https://fixture.invalid/cluster"},
		ExternalChanges: change.NewSet([]string{dirty}),
	}, Config{Concurrency: 4},
	)
	assert.FatalIf(t, err != nil, "%v", err)
	{
		localErr := o.bootstrap(t.Context())
		assert.FatalIf(t, localErr != nil, "Bootstrap: %v", localErr)
	}
	for _, ns := range []string{"myns", "other"} {
		source := manifest.NamedResource{
			Group:     "source.toolkit.fluxcd.io",
			Kind:      manifest.KindGitRepository,
			Namespace: ns,
			Name:      "flux-system",
		}
		record, ok := o.index.Lookup(source)
		assert.FatalIf(
			t,
			!ok || record.File != "cluster/source-"+ns+".yaml",
			"source fixture %s: indexed=%t record=%+v",
			source,
			ok,
			record,
		)
		ownership := o.index.OwnersOfFile(record.File)
		owners, ancestors := ownership.Owners, ownership.Ancestors
		t.Logf("source %s: File=%s owners=%v ancestors=%v", source, record.File, owners, ancestors)
		assert.FatalIf(
			t,
			len(owners) != 0 || len(ancestors) != 0,
			"source fixture %s has unrelated ownership selection",
			source,
		)
		id := testutil.KustomizationID(ns, "myapp")
		record, ok = o.index.Lookup(id)
		t.Logf(
			"consumer %s: indexed=%t File=%s Authored=%s Refs=%v",
			id,
			ok,
			record.File,
			record.Authored,
			record.Refs,
		)
		authored := testutil.KustomizationID("flux-system", "myapp")
		assert.ErrorIf(t, !ok || record.File != "apps/myapp/myapp/ks.yaml" || record.Authored != authored ||
			!slices.Equal(
				record.Refs,
				[]manifest.NamedResource{source},
			), "projected consumer %s: indexed=%t record=%+v", id, ok, record)
	}
	other := testutil.KustomizationID("other", "myapp")

	res, err := o.collectResult(o.run(t.Context()))
	assert.FatalIf(t, err != nil || len(res.Failed) != 0,
		"changed-only Render: %v; failures=%v", err, res.Failed)

	t.Logf(
		"dirty=%s ShouldReconcile(%s)=%t keep=%v",
		dirty,
		other,
		o.filter.ShouldReconcile(other),
		o.filter.KeepNames(),
	)
	assert.ErrorIf(
		t,
		!o.filter.Enabled() || !o.filter.ShouldReconcile(other),
		"second-source change did not select %s",
		other,
	)

	docs := outputByParent(res.Manifests)[other]
	assert.FatalIf(
		t,
		len(docs) != 1 || manifest.DocKind(docs[0]) != manifest.KindHelmRelease,
		"rendered children of %s = %v, want one HelmRelease",
		other,
		docs,
	)
	{
		metadata := manifest.DocMetadata(docs[0])
		name := metadata.Name
		assert.ErrorIf(t, name != "myapp", "rendered child name = %q, want myapp", name)
	}
}

type testOrchestratorTransformerTargetNamespaceChangedOnlySecondSource2Inputs struct {
	dir  *string
	path *string
}

func testOrchestratorTransformerTargetNamespaceChangedOnlySecondSource2(
	t *testing.T,
	testCaseInput testOrchestratorTransformerTargetNamespaceChangedOnlySecondSource2Inputs,
) {
	t.Helper()
	testOrchestratorTransformerTargetNamespaceChangedOnlySecondSource(
		t,
		testOrchestratorTransformerTargetNamespaceChangedOnlySecondSourceInputs(testCaseInput),
	)
}
