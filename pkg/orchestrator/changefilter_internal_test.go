package orchestrator

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/baseline"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestBuildChangeFilter_FluxExcludedWarning(t *testing.T) {
	t.Setenv("PATH", "")
	before, after := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, before, ".sops.yaml", "old")
	testutil.WriteFile(t, after, ".sops.yaml", "new")
	input, _, err := baseline.AdmitDirectory(t.Context(), after, after, manifest.WorkScope{})
	if err != nil {
		t.Fatal(err)
	}
	o, err := newTestOrchestrator(
		t,
		Tree{RepoRoot: after, Path: after, BaseInput: testDirectoryInput(t, before), Input: input},
		Config{Concurrency: 2},
	)
	assert.NoError(t, err)
	assert.NoError(t, o.bootstrap(t.Context()))
	assert.Equal(t, (o.selection != nil), true)
	warnings := o.store.Warnings()
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one", warnings)
	}
	assert.Equal(t, warnings[0].Category, manifest.WarnPathConfig)
	assert.ErrorIf(t, !strings.Contains(warnings[0].Message, "Flux-excluded files are not counted"),
		"warning does not explain Flux exclusions: %q", warnings[0].Message)
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestIntegration_DiffWidensToRepoRootForSiblingCheckouts(t *testing.T) {
	origRoot := t.TempDir()
	currRoot := t.TempDir()
	for _, root := range []string{origRoot, currRoot} {
		testutil.GitInit(t, root)
	}

	// Identical Flux bootstrap on both sides — the cluster subdir
	// content is byte-identical, so a literal subdir-vs-subdir diff
	// would return zero.
	bootstrap := "---\n" +
		`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-system
  namespace: flux-system
spec:
  url: https://example.test/cluster.git
` + "---\n" +
		testutil.KustomizationYAML("cluster-apps", "flux-system", `  interval: 10m
  path: ./kubernetes/apps/foo
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	testutil.WriteFile(t, origRoot, "kubernetes/flux/cluster.yaml", bootstrap)
	testutil.WriteFile(t, currRoot, "kubernetes/flux/cluster.yaml", bootstrap)

	// The Kustomization spec.path tree lives under apps/foo — outside
	// the flux/ subdir, exactly where real-world repos put their app
	// manifests. The change is in this tree.
	appKust := `resources:
  - ./cm.yaml
`
	testutil.WriteFile(t, origRoot, "kubernetes/apps/foo/kustomization.yaml", appKust)
	testutil.WriteFile(t, currRoot, "kubernetes/apps/foo/kustomization.yaml", appKust)
	testutil.WriteFile(t, origRoot, "kubernetes/apps/foo/cm.yaml", `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: foo-config
  namespace: flux-system
data:
  value: original
`)
	testutil.WriteFile(t, currRoot, "kubernetes/apps/foo/cm.yaml", `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: foo-config
  namespace: flux-system
data:
  value: changed
`)

	// Point --path / --path-orig at the cluster subdir, NOT the repo
	// root — this is the user-facing convention and the case the fix
	// targets. The flux/cluster directory is byte-identical between
	// the two checkouts; only kubernetes/apps/foo/cm.yaml differs.
	currCluster := filepath.Join(currRoot, "kubernetes", "flux")

	// The CLI lifts a subdir --path / --path-orig to its repo root before
	// reaching the orchestrator; here we pass the explicit
	// roots directly. change.Detect runs root-to-root, so an edit in
	// kubernetes/apps/foo — outside the scanned flux/ subdir — still
	// enters the keep set, without any .git-ancestor "widen" heuristic.
	o, err := newTestOrchestrator(t, Tree{
		Path:     currCluster,
		RepoRoot: currRoot, BaseInput: testDirectoryInput(t, origRoot),
	}, Config{},
	)

	assert.FatalIf(t, err != nil, "%v", err)

	defer func() {
		o.stop()
	}()

	{
		err := o.bootstrap(t.Context())
		assert.FatalIf(t, err != nil, "Bootstrap: %v", err)
	}

	f := o.selection

	assert.FatalIf(
		t,
		f == nil,
		"%v",
		"expected selection session when PathOrig is set",
	)

	clusterApps := testutil.KustomizationID("flux-system", "cluster-apps")

	assert.ErrorIf(
		t,
		!f.Snapshot().Reconcile(clusterApps),
		"cluster-apps owns the changed file (apps/foo/cm.yaml) but was filtered out of the keep set; diff did not widen to repo root",
	)
}

func TestBuildChangeFilter_HistoryWorkOwners(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, name := range []string{"a", "b"} {
		testutil.WriteFile(t, root, "flux/"+name+".yaml", testutil.KustomizationYAML(name, "apps",
			"  path: ./producers/"+name+"\n  postBuild: {substitute: {ROOT: apps/"+name+"}}\n"))
		testutil.WriteFile(t, root, "producers/"+name+"/kustomization.yaml", "resources: [child.yaml]\n")
		testutil.WriteFile(t, root, "producers/"+name+"/child.yaml", testutil.KustomizationYAML("child-"+name, "apps",
			"  path: ./${ROOT}\n"))
		testutil.WriteFile(t, root, "apps/"+name+"/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "apps/"+name+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {value: before}\n")
	}
	tree := Tree{
		ID:       manifest.TreeID{Cluster: "history", Side: manifest.TreeHead},
		Path:     filepath.Join(root, "flux"),
		RepoRoot: root,
	}
	cfg := Config{CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true}
	warm, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	assert.NoError(t, warm.bootstrap(t.Context()))
	assert.NoError(t, warm.run(t.Context()))
	warm.stop()
	for _, name := range []string{"a", "b"} {
		testutil.WriteFile(t, root, "apps/"+name+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {value: after}\n")
	}
	tree.ExternalChanges = change.NewSet([]string{"apps/a/cm.yaml", "apps/b/cm.yaml"})
	fresh, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	t.Cleanup(fresh.stop)
	assert.NoError(t, fresh.prepareBootstrap(t.Context()))
	fresh.cfg.Input = warm.cfg.Input
	fresh.completeBootstrap()
	assert.NoError(t, fresh.prepareInvocation(t.Context()))
	work := fresh.freezeWork()
	for _, name := range []string{"a", "b"} {
		owner := testutil.KustomizationID("apps", name)
		assert.Equal(t, fresh.history[owner].Complete, true)
		assert.Equal(t, workCounts(work, owner, manifest.WorkCache),
			manifest.WorkCounts{Attempts: 4, Completed: 4, CacheService: 3})
		assert.Equal(t, workCounts(work, owner, manifest.WorkPreparation).OwnerPrepares, uint64(1))
		for _, record := range work {
			if record.Key.Owner == owner {
				assert.Equal(t, record.Key.Tree, tree.ID)
				assert.Equal(t, record.Key.Source, manifest.BootstrapSourceID)
			}
		}
	}
}
