package orchestrator

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestComputeChangeSet_FluxExcludedWarning(t *testing.T) {
	t.Setenv("PATH", "")
	before, after := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, before, ".sops.yaml", "old")
	testutil.WriteFile(t, after, ".sops.yaml", "new")
	o := &Orchestrator{cfg: Config{PathOrig: before}, store: store.New()}
	got, err := o.computeChangeSet(after)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, got.Len(), 0)
	warnings := o.store.Warnings()
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one", warnings)
	}
	assert.Equal(t, warnings[0].Category, manifest.WarnPathConfig)
	if !strings.Contains(warnings[0].Message, "Flux-excluded files are not counted") {
		t.Errorf("warning does not explain Flux exclusions: %q", warnings[0].Message)
	}
}

func TestIntegration_DiffWidensToRepoRootForSiblingCheckouts(t *testing.T) {
	origRoot := t.TempDir()
	currRoot := t.TempDir()
	// Mark both as separate git checkouts so discovery.FindRepoRoot
	// picks them up. A bare directory is enough — the source root resolves from discovery metadata.
	testutil.WriteFile(t, origRoot, filepath.Join(".git", "HEAD"), "ref: refs/heads/main\n")
	testutil.WriteFile(t, currRoot, filepath.Join(".git", "HEAD"), "ref: refs/heads/main\n")

	// Identical Flux bootstrap on both sides — the cluster subdir
	// content is byte-identical, so a literal subdir-vs-subdir diff
	// would return zero.
	bootstrap := `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-system
  namespace: flux-system
spec:
  url: https://example.test/cluster.git
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster-apps
  namespace: flux-system
spec:
  interval: 10m
  path: ./kubernetes/apps/foo
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`
	testutil.WriteFile(t, origRoot, filepath.Join("kubernetes/flux/cluster.yaml"), bootstrap)
	testutil.WriteFile(t, currRoot, filepath.Join("kubernetes/flux/cluster.yaml"), bootstrap)

	// The Kustomization spec.path tree lives under apps/foo — outside
	// the flux/ subdir, exactly where real-world repos put their app
	// manifests. The change is in this tree.
	appKust := `resources:
  - ./cm.yaml
`
	testutil.WriteFile(t, origRoot, filepath.Join("kubernetes/apps/foo/kustomization.yaml"), appKust)
	testutil.WriteFile(t, currRoot, filepath.Join("kubernetes/apps/foo/kustomization.yaml"), appKust)
	testutil.WriteFile(t, origRoot, filepath.Join("kubernetes/apps/foo/cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: foo-config
  namespace: flux-system
data:
  value: original
`)
	testutil.WriteFile(t, currRoot, filepath.Join("kubernetes/apps/foo/cm.yaml"), `---
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
	currCluster := filepath.Join(currRoot, "kubernetes/flux")

	// The CLI lifts a subdir --path / --path-orig to its repo root before
	// reaching the orchestrator (repoRootOf); here we pass the explicit
	// roots directly. change.Detect runs root-to-root, so an edit in
	// kubernetes/apps/foo — outside the scanned flux/ subdir — still
	// enters the keep set, without any .git-ancestor "widen" heuristic.
	o, err := New(Config{
		Path:        currCluster,
		RepoRoot:    currRoot,
		PathOrig:    origRoot,
		WipeSecrets: true,
	})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	defer o.Stop()
	if err := o.Bootstrap(t.Context()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	f := o.filter
	if f == nil {
		t.Fatal("expected change.Filter to be wired (PathOrig was set), got nil — buildChangeFilter dropped the filter")
	}
	clusterApps := manifest.NamedResource{
		Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps",
	}
	if !f.ShouldReconcile(clusterApps) {
		t.Errorf("cluster-apps owns the changed file (apps/foo/cm.yaml) but was filtered out of the keep set; diff did not widen to repo root")
	}
}
