package discovery_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestRun_SmallTree exercises the discovery phase end-to-end on a
// minimal three-file repo: a parent KS that points at apps/, a child
// KS under apps/, and an unrelated GR. After Run we expect the store
// populated with both KSes + the GR + the synthetic bootstrap GR, with
// SourceFiles tracking each one and ParentOf wiring the child to the
// parent.
func TestRun_SmallTree(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	testutil.WriteFileAt(t, filepath.Join(dir, "flux", "parent.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: parent
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
  interval: 10m
`)
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "child.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: child
  namespace: flux-system
spec:
  path: ./apps/leaf
  sourceRef:
    kind: GitRepository
    name: flux-system
  interval: 10m
`)
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "leaf", "kustomization.yaml"), `resources: []
`)

	st := store.New()
	res, err := discovery.Run(context.Background(), discovery.Config{
		Path: dir, Store: st, WipeSecrets: true,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	wantRoot, _ := filepath.EvalSymlinks(dir)
	if res.RepoRoot != wantRoot {
		t.Errorf("RepoRoot = %q, want %q", res.RepoRoot, wantRoot)
	}

	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "parent"}
	child := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "child"}
	for _, id := range []manifest.NamedResource{parent, child} {
		if _, ok := res.SourceFiles[id]; !ok {
			t.Errorf("SourceFiles missing %s", id)
		}
		if st.GetObject(id) == nil {
			t.Errorf("Store missing %s", id)
		}
	}

	if got := res.ParentOf[child]; got != parent {
		t.Errorf("ParentOf[child] = %v, want %v", got, parent)
	}

	// Synthetic bootstrap GR should be Ready so KSes resolve their
	// sourceRef without an explicit GitRepository file in the tree.
	bootstrap := manifest.BootstrapSourceID
	if st.GetObject(bootstrap) == nil {
		t.Errorf("bootstrap GitRepository not seeded")
	}
}

func TestRun_NamespaceIndependentOfDiscoveryDepth(t *testing.T) {
	t.Parallel()
	for _, repo := range []struct {
		name, ref string
	}{
		{"none", ""},
		{"no_ref", ""},
		{"non_matching_ref", "  ref: {branch: main}\n"},
	} {
		t.Run(repo.name, func(t *testing.T) {
			for _, placement := range []string{"deep", "clusters"} {
				t.Run(placement, func(t *testing.T) {
					root := t.TempDir()
					if _, err := git.PlainInit(root, false); err != nil {
						t.Fatal(err)
					}
					for _, file := range []struct {
						path, name, namespace, target, extra string
					}{
						{"flux/a.yaml", "a", ", namespace: flux-system", "./apps", ""},
						{"flux/c.yaml", "c", ", namespace: flux-system", "./clusters", ""},
						{"apps/team/x.yaml", "x", "", "./xout", ""},
						{"clusters/c2.yaml", "c2", ", namespace: flux-system", "./deep", ""},
						{placement + "/b.yaml", "b", ", namespace: flux-system", "./apps/team", "  targetNamespace: team\n"},
					} {
						testutil.WriteFile(t, root, file.path, fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: %s%s}
spec:
  path: %s
%s  sourceRef: {kind: GitRepository, name: flux-system}
`, file.name, file.namespace, file.target, file.extra))
					}
					if repo.name != "none" {
						testutil.WriteFile(t, root, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: https://example.invalid/x.git
`+repo.ref)
					}
					st := store.New()
					if _, err := discovery.Run(t.Context(), discovery.Config{Path: filepath.Join(root, "flux"), Store: st}); err != nil {
						t.Fatal(err)
					}
					var ids []string
					for _, ks := range st.ListAs[*manifest.Kustomization](manifest.KindKustomization) {
						ids = append(ids, ks.Named().NamespacedName())
					}
					slices.Sort(ids)
					assert.Diff(t, ids, []string{"flux-system/a", "flux-system/b", "flux-system/c", "flux-system/c2", "team/x"})
				})
			}
		})
	}
}

func TestRun_IndexesParentBeforeHelmReleaseArrival(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/apps.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system}
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [release.yaml]\n")
	release := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: demo
  namespace: apps
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`
	testutil.WriteFile(t, dir, "apps/release.yaml", release)
	testutil.WriteFile(t, dir, "orphan.yaml", strings.Replace(release, "name: demo", "name: orphan", 1))
	st := store.New()
	res, err := discovery.Run(t.Context(), discovery.Config{Path: dir, Store: st, WipeSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	hr := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "demo"}
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	if st.GetObject(hr) != nil {
		t.Fatal("parent-owned HelmRelease must wait for rendering")
	}
	if _, indexed := res.SourceFiles[hr]; !indexed {
		t.Fatal("HelmRelease source identity must be indexed")
	}
	if got := res.ParentOf[hr]; got != parent {
		t.Errorf("ParentOf[%s] = %v, want %v", hr, got, parent)
	}
	orphan := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "orphan"}
	if _, gated := res.ParentOf[orphan]; gated || st.GetObject(orphan) == nil {
		t.Error("standalone HelmRelease must be available without a parent gate")
	}
}

// TestRun_AliasesNonDefaultNamespaceBootstrap pins issue #199: a
// Kustomization whose sourceRef points at a GitRepository in a
// non-`flux-system` namespace (typical of the flux-operator /
// FluxInstance pattern, where Flux runs in `gitops-system` and the
// root GitRepository is created out-of-band by the operator) must
// have that GitRepository aliased to the working tree so depwait
// resolves it. Without the fix, every consumer fails with
// `dependency not found: GitRepository/gitops-system/gitops-system`.
func TestRun_AliasesNonDefaultNamespaceBootstrap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	testutil.WriteFileAt(t, filepath.Join(dir, "flux", "cluster.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: flux-repositories
  namespace: gitops-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: gitops-system
    namespace: gitops-system
  interval: 1h
`)
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "kustomization.yaml"), "resources: []\n")

	st := store.New()
	if _, err := discovery.Run(context.Background(), discovery.Config{
		Path: dir, Store: st, WipeSecrets: true,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	aliased := manifest.NamedResource{
		Kind: manifest.KindGitRepository, Namespace: "gitops-system", Name: "gitops-system",
	}
	if st.GetObject(aliased) == nil {
		t.Errorf("expected GitRepository/gitops-system/gitops-system to be aliased; alias scoped only to flux-system would leave this case broken")
	}
	if st.GetArtifact(aliased) == nil {
		t.Errorf("aliased GitRepository should have a SourceArtifact so depwait resolves")
	}
	info, ok := st.GetStatus(aliased)
	if !ok || info.Status != store.StatusReady {
		t.Errorf("aliased GitRepository should be Ready; got ok=%v info=%+v", ok, info)
	}
}

// TestRun_AliasesBootstrapOCIRepository pins the mortebrume/homelab
// pattern: a Kustomization whose sourceRef points at an OCIRepository
// (flux-operator FluxInstance publishes the bootstrap source as an
// OCI artifact rather than a Git repo) must be aliased to the
// working tree the same way GitRepository sources are. Without this,
// every dependent KS fails with "dependency not found:
// OCIRepository/flux-system/flux-system".
func TestRun_AliasesBootstrapOCIRepository(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	testutil.WriteFileAt(t, filepath.Join(dir, "flux", "cluster.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster-apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: OCIRepository
    name: flux-system
    namespace: flux-system
  interval: 1h
`)
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "kustomization.yaml"), "resources: []\n")

	st := store.New()
	if _, err := discovery.Run(context.Background(), discovery.Config{
		Path: dir, Store: st, WipeSecrets: true,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	aliased := manifest.NamedResource{
		Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "flux-system",
	}
	if st.GetObject(aliased) == nil {
		t.Errorf("expected bootstrap OCIRepository to be aliased; only GitRepository aliasing would leave this case broken")
	}
	if st.GetArtifact(aliased) == nil {
		t.Errorf("aliased OCIRepository should have a SourceArtifact so depwait resolves")
	}
	info, ok := st.GetStatus(aliased)
	if !ok || info.Status != store.StatusReady {
		t.Errorf("aliased OCIRepository should be Ready; got ok=%v info=%+v", ok, info)
	}
}

// TestRun_LoadsResourceSetAndDeepRSIP pins that discovery loads a
// ResourceSet and the RSIPs behind a Kustomization's spec.path into the
// store WITHOUT expanding the RS — expansion is now a run-time concern
// owned by the ResourceSet controller (a first-class DAG node), not
// discovery. Discovery must still walk the parent KS's path so the RSIP
// is seeded; the RS-emitted child Kustomization is NOT produced here.
func TestRun_LoadsResourceSetAndDeepRSIP(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Root: a parent KS that points at apps/, plus an RS at the same
	// level. The RS selects RSIPs by label in its own namespace.
	testutil.WriteFileAt(t, filepath.Join(dir, "flux", "parent.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: parent, namespace: flux-system}
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system}
  interval: 10m
`)
	testutil.WriteFileAt(t, filepath.Join(dir, "flux", "rs.yaml"), `---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: late-rs, namespace: flux-system}
spec:
  inputsFrom:
    - apiVersion: fluxcd.controlplane.io/v1
      kind: ResourceSetInputProvider
      selector:
        matchLabels: {role: db}
  resourcesTemplate: |
    ---
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: child-<< index (index inputs "provider") "name" >>, namespace: flux-system}
    spec:
      path: ./child
      sourceRef: {kind: GitRepository, name: flux-system}
`)
	// RSIP lives BEHIND the parent KS's path — discovery must expand
	// that path so the RSIP is loaded into the store for the run-time
	// RS controller to resolve.
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "rsip.yaml"), `---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata:
  name: rsip
  namespace: flux-system
  labels: {role: db}
spec:
  type: Static
  defaultValues:
    user: alice
`)

	st := store.New()
	if _, err := discovery.Run(context.Background(), discovery.Config{
		Path: dir, Store: st, WipeSecrets: true,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The RS itself is loaded into the store as a node for the run phase.
	rsID := manifest.NamedResource{Kind: manifest.KindResourceSet, Namespace: "flux-system", Name: "late-rs"}
	if st.GetObject(rsID) == nil {
		t.Errorf("expected ResourceSet %s loaded into store", rsID)
	}
	// The deep RSIP behind the parent KS's path is loaded — discovery
	// walked the spec.path even though it no longer expands the RS.
	rsipID := manifest.NamedResource{Kind: manifest.KindResourceSetInputProvider, Namespace: "flux-system", Name: "rsip"}
	if st.GetObject(rsipID) == nil {
		t.Errorf("expected ResourceSetInputProvider %s loaded into store (parent KS path walked)", rsipID)
	}
	// Discovery does NOT expand the RS anymore — the child Kustomization
	// is produced at run time by the RS controller, not here.
	child := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "child-rsip"}
	if st.GetObject(child) != nil {
		t.Errorf("discovery should not pre-expand the RS; %s must be produced at run time", child)
	}
}

func TestRun_RequiresStoreAndLoader(t *testing.T) {
	t.Parallel()
	if _, err := discovery.Run(context.Background(), discovery.Config{Path: t.TempDir()}); err == nil {
		t.Error("Run with nil Store/Loader: want error, got nil")
	}
}

// TestRun_CrossTreeBaseEmissionGate pins the #777 gate: a Flux KS stored as a
// cross-tree kustomize base (apps/base/app-a/ks.yaml pulled in by cluster-apps
// via apps/test/app-a -> ../../base/app-a) gets cluster-apps wired as its
// structural parent in ParentOf, even though its source file sits under no KS
// spec.path. A self-emitting KS (spec.path covering its own definition file)
// must NOT be gated on itself, or it would deadlock waiting for itself.
func TestRun_CrossTreeBaseEmissionGate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	testutil.WriteFile(t, dir, "cluster/cluster-apps.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster-apps
  namespace: flux-system
spec:
  path: ./apps/test
  sourceRef:
    kind: GitRepository
    name: flux-system
`)
	testutil.WriteFile(t, dir, "apps/test/kustomization.yaml", "resources:\n  - ./app-a\n")
	testutil.WriteFile(t, dir, "apps/test/app-a/kustomization.yaml", "resources:\n  - ../../base/app-a\n")
	testutil.WriteFile(t, dir, "apps/base/app-a/kustomization.yaml", "resources:\n  - ks.yaml\n")
	// ${VAR} path — unresolvable at discovery, so app-a's own walk can't follow
	// it; only cluster-apps emits its file, giving an unambiguous gate.
	testutil.WriteFile(t, dir, "apps/base/app-a/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: app-a
  namespace: flux-system
spec:
  path: ./apps/${CLUSTER_ENV}/app-a
  sourceRef:
    kind: GitRepository
    name: flux-system
`)

	// A self-emitting KS: its spec.path (./self) holds its own ks.yaml.
	testutil.WriteFile(t, dir, "self/kustomization.yaml", "resources:\n  - ks.yaml\n")
	testutil.WriteFile(t, dir, "self/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: selfish
  namespace: flux-system
spec:
  path: ./self
  sourceRef:
    kind: GitRepository
    name: flux-system
`)

	st := store.New()
	res, err := discovery.Run(context.Background(), discovery.Config{Path: dir, Store: st, WipeSecrets: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	appA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "app-a"}
	clusterApps := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps"}
	if got := res.ParentOf[appA]; got != clusterApps {
		t.Errorf("ParentOf[app-a] = %v, want cluster-apps (cross-tree emission gate)", got)
	}

	selfish := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "selfish"}
	if got, gated := res.ParentOf[selfish]; gated {
		t.Errorf("ParentOf[selfish] = %v, want absent (a self-emit must not gate on itself)", got)
	}
}

func TestResolveScanPath_SymlinkResolved(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(dir, "real")
	if err := os.MkdirAll(target, 0o750); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	got, err := discovery.ResolveScanPath(link)
	if err != nil {
		t.Fatalf("ResolveScanPath: %v", err)
	}
	want, _ := filepath.EvalSymlinks(target)
	if got != want {
		t.Errorf("ResolveScanPath(link) = %q, want %q", got, want)
	}
}

func TestFindRepoRoot_NoGitFallsBack(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if got := discovery.FindRepoRoot(dir); got != dir {
		t.Errorf("FindRepoRoot(%q) = %q; expected unchanged when no .git ancestor", dir, got)
	}
}

// TestRun_AliasesURLMatchedInTreeGitRepository pins the Zariel/
// home-ops pattern: a GitRepository CR defined IN the tree
// whose spec.url points at the same remote the working tree
// itself clones from. Real Flux uses these with SOPS-decrypted
// SSH deploy keys; flate runs offline and can't materialize the
// key, so the fetch fails on a placeholder credential.
//
// The alias in overrideSelfReferentialGitRepositories detects that
// the URL matches the working tree's .git/config remote and
// overrides the artifact with a working-tree alias so the
// dependent KSes proceed against local files rather than the
// failed-fetch source.
func TestRun_AliasesURLMatchedInTreeGitRepository(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	repo, err := git.PlainInit(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, dir, ".git/config", `[core]
	repositoryformatversion = 0
[remote "origin"]
	url = git@github.com:Example/home-ops.git
`)
	if err := repo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))); err != nil {
		t.Fatal(err)
	}

	testutil.WriteFileAt(t, filepath.Join(dir, "k8s", "flux", "cluster.yaml"), `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: home-kubernetes
  namespace: flux-system
spec:
  url: ssh://git@github.com/example/home-ops.git
  ref:
    branch: main
  secretRef:
    name: github-deploy-key
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster
  namespace: flux-system
spec:
  path: ./k8s/apps
  sourceRef:
    kind: GitRepository
    name: home-kubernetes
  interval: 1h
`)
	testutil.WriteFileAt(t, filepath.Join(dir, "k8s", "apps", "kustomization.yaml"), "resources: []\n")

	wt, err := repo.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wt.Add("."); err != nil {
		t.Fatal(err)
	}
	if _, err := wt.Commit("fixture", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}}); err != nil {
		t.Fatal(err)
	}

	st := store.New()
	if _, err := discovery.Run(context.Background(), discovery.Config{
		Path: dir, Store: st, WipeSecrets: true,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	id := manifest.NamedResource{
		Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "home-kubernetes",
	}
	art := st.GetArtifact(id)
	if art == nil {
		t.Fatalf("expected URL-matched GitRepository to have a SourceArtifact")
	}
	src, ok := art.(*store.SourceArtifact)
	if !ok {
		t.Fatalf("expected SourceArtifact, got %T", art)
	}
	wantPrefix := "file://"
	if !strings.HasPrefix(src.URL, wantPrefix) {
		t.Errorf("expected file:// URL alias; got %q", src.URL)
	}
	info, ok := st.GetStatus(id)
	if !ok || info.Status != store.StatusReady {
		t.Errorf("expected URL-matched GitRepository to be Ready; got ok=%v info=%+v", ok, info)
	}
}

// TestRun_LeavesUnmatchedInTreeGitRepository covers the
// negative case: an in-tree GitRepository whose URL points at a
// different remote (a real shared-infra source) must NOT get
// aliased to the working tree — that would silently render the
// wrong files. The store should still have the GitRepository
// from file-load, but without an aliased SourceArtifact.
func TestRun_LeavesUnmatchedInTreeGitRepository(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFileAt(t, filepath.Join(dir, ".git", "config"), `[core]
	repositoryformatversion = 0
[remote "origin"]
	url = git@github.com:Example/home-ops.git
`)
	testutil.WriteFileAt(t, filepath.Join(dir, ".git", "HEAD"), "ref: refs/heads/main\n")

	testutil.WriteFileAt(t, filepath.Join(dir, "k8s", "flux", "shared-infra.yaml"), `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: shared-infra
  namespace: flux-system
spec:
  url: https://github.com/upstream/shared-infra.git
  ref:
    branch: main
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster
  namespace: flux-system
spec:
  path: ./k8s/apps
  sourceRef:
    kind: GitRepository
    name: shared-infra
  interval: 1h
`)
	testutil.WriteFileAt(t, filepath.Join(dir, "k8s", "apps", "kustomization.yaml"), "resources: []\n")

	st := store.New()
	if _, err := discovery.Run(context.Background(), discovery.Config{
		Path: dir, Store: st, WipeSecrets: true,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	id := manifest.NamedResource{
		Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "shared-infra",
	}
	if st.GetArtifact(id) != nil {
		t.Errorf("non-matching upstream GitRepository must NOT receive a working-tree alias artifact")
	}
}

// TestRun_ComponentGeneratorMaterializesCM mirrors home-operations/flate
// issue #396: a Flux Kustomization references a kustomize Component
// whose configMapGenerator produces cluster-settings. Without the
// generator-discovery pass, depwait can't find the CM (no on-disk
// YAML to walk to) and every downstream KS with `substituteFrom:
// [cluster-settings]` fails. The fix synthesizes the CM at the
// Flux KS's namespace and registers it in the store + ExistenceIndex.
func TestRun_ComponentGeneratorMaterializesCM(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Flux Kustomization in flux-system pointing at ./cluster
	testutil.WriteFileAt(t, filepath.Join(dir, "flux", "ks.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster-config
  namespace: flux-system
spec:
  path: ./cluster
  sourceRef: {kind: GitRepository, name: flux-system}
  interval: 10m
`)
	// cluster/kustomization.yaml references the Component
	testutil.WriteFileAt(t, filepath.Join(dir, "cluster", "kustomization.yaml"), `apiVersion: kustomize.config.k8s.io/v1
kind: Kustomization
components:
  - ../components/cluster-settings
`)
	// Component declares the configMapGenerator
	testutil.WriteFileAt(t, filepath.Join(dir, "components", "cluster-settings", "kustomization.yaml"), `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
configMapGenerator:
  - name: cluster-settings
    literals:
      - DOMAIN=example.com
      - TIMEZONE=UTC
`)

	st := store.New()
	if _, err := discovery.Run(context.Background(), discovery.Config{
		Path: dir, Store: st, WipeSecrets: true,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cmID := manifest.NamedResource{
		Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings",
	}
	cm, _ := st.GetByName[*manifest.ConfigMap](manifest.KindConfigMap, "flux-system", "cluster-settings")
	if cm == nil {
		t.Fatalf("ConfigMap %s not synthesized from configMapGenerator", cmID)
	}
	if got, _ := cm.Data["DOMAIN"].(string); got != "example.com" {
		t.Errorf("DOMAIN = %q, want example.com", got)
	}
	if got, _ := cm.Data["TIMEZONE"].(string); got != "UTC" {
		t.Errorf("TIMEZONE = %q, want UTC", got)
	}
}

// TestRun_KRMIgnoreFileScopesInitialScanOnly pins the override's reach:
// Config.KRMIgnoreFile filters the --path scan, while spec.path targets
// followed afterwards still honor their own .krmignore rather than the
// override (whose patterns are anchored at the scan root).
func TestRun_KRMIgnoreFileScopesInitialScanOnly(t *testing.T) {
	dir := t.TempDir()
	ks := func(name, path string) string {
		return `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: ` + name + `
  namespace: flux-system
spec:
  path: ` + path + `
  sourceRef:
    kind: GitRepository
    name: flux-system
  interval: 10m
`
	}
	testutil.WriteFileAt(t, filepath.Join(dir, "cluster", "keep.yaml"), ks("keep", "./apps"))
	testutil.WriteFileAt(t, filepath.Join(dir, "cluster", "drop.yaml"), ks("drop", "./apps"))
	testutil.WriteFileAt(t, filepath.Join(dir, "cluster", ".krmignore"), "keep.yaml\n")
	testutil.WriteFileAt(t, filepath.Join(dir, "override.krmignore"), "drop.yaml\n")
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "leaf.yaml"), ks("leaf", "./apps/leaf"))
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "hidden.yaml"), ks("hidden", "./apps/leaf"))
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", ".krmignore"), "hidden.yaml\n")
	testutil.WriteFileAt(t, filepath.Join(dir, "apps", "leaf", "kustomization.yaml"), "resources: []\n")

	st := store.New()
	if _, err := discovery.Run(context.Background(), discovery.Config{
		Path: filepath.Join(dir, "cluster"), RepoRoot: dir, Store: st, WipeSecrets: true,
		KRMIgnoreFile: filepath.Join(dir, "override.krmignore"),
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	id := func(name string) manifest.NamedResource {
		return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: name}
	}
	for name, want := range map[string]bool{"keep": true, "drop": false, "leaf": true, "hidden": false} {
		if got := st.GetObject(id(name)) != nil; got != want {
			t.Errorf("Store has %s = %v, want %v", name, got, want)
		}
	}
}

func TestRun_GraphOwnedOrphans(t *testing.T) {
	for _, tc := range []struct{ name, resource, namespace string }{
		{"sibling", "../shared/demo", ""},
		{"direct", "../shared/demo/bundle.yaml", ""},
		{"explicit", "../shared/demo", "demo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: flux-system}
spec: {path: ./cluster/per-cluster, sourceRef: {kind: GitRepository, name: flux-system}}
`)
			testutil.WriteFile(t, dir, "cluster/per-cluster/kustomization.yaml", "namespace: demo\nresources: ["+tc.resource+"]\n")
			testutil.WriteFile(t, dir, "cluster/shared/demo/kustomization.yaml", "namespace: demo\nresources: [bundle.yaml]\n")
			ns := ""
			if tc.namespace != "" {
				ns = ", namespace: " + tc.namespace
			}
			testutil.WriteFile(t, dir, "cluster/shared/demo/bundle.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo`+ns+`}
spec: {chart: {spec: {chart: demo, sourceRef: {kind: HelmRepository, name: demo}}}}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: demo`+ns+`}
spec: {url: "https://charts.example.invalid", interval: 1h}
`)
			testutil.WriteFile(t, dir, "standalone.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: standalone, namespace: demo}
data: {value: fixture}
`)
			st := store.New()
			res, err := discovery.Run(t.Context(), discovery.Config{Path: dir, RepoRoot: dir, Store: st, WipeSecrets: true})
			if err != nil {
				t.Fatal(err)
			}
			raw := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "demo", Namespace: tc.namespace}
			if _, ok := res.Existence.Get(raw); !ok {
				t.Error("raw release must remain indexed")
			}
			if st.GetObject(raw) != nil {
				t.Errorf("graph-owned release promoted: %s", raw)
			}
			repos := st.ListAs[*manifest.HelmRepository](manifest.KindHelmRepository)
			if len(repos) != 1 || repos[0].Name != "demo" || repos[0].Namespace != "demo" {
				t.Errorf("repositories = %+v, want only demo/demo", repos)
			}
			if st.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "demo", Name: "standalone"}) == nil {
				t.Error("standalone ConfigMap not promoted")
			}
		})
	}
}

func TestRun_StandaloneSecretWinsProducerPlaceholder(t *testing.T) {
	for _, scenario := range []string{"standalone", "preexisting", "graph-owned"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: flux-system}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: flux-system}}
`)
			resources := "producer.yaml"
			if scenario == "graph-owned" {
				resources += ", ../secret.yaml"
			}
			testutil.WriteFile(t, dir, "apps/kustomization.yaml", "namespace: secure\nresources: ["+resources+"]\n")
			testutil.WriteFile(t, dir, "apps/producer.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: producer, namespace: secure}
spec:
 target: {name: app-secret}
 data: [{secretKey: HOST, remoteRef: {key: fixture}}]
`)
			testutil.WriteFile(t, dir, "secret.yaml", `apiVersion: v1
kind: Secret
metadata: {name: app-secret, namespace: secure}
stringData: {HOST: fixture-value}
`)
			st := store.New()
			id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "secure", Name: "app-secret"}
			preexisting := &manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: map[string]any{"HOST": "preexisting-value"}}
			if scenario == "preexisting" {
				st.AddObject(preexisting)
			}
			res, err := discovery.Run(t.Context(), discovery.Config{Path: dir, RepoRoot: dir, Store: st, WipeSecrets: true})
			if err != nil {
				t.Fatal(err)
			}
			want := "fixture-value"
			if scenario == "preexisting" {
				want = "preexisting-value"
			}
			if scenario == "graph-owned" {
				want = "..PLACEHOLDER_HOST.."
			}
			secret, ok := st.GetObject(id).(*manifest.Secret)
			if !ok || secret.StringData["HOST"] != want {
				t.Fatalf("Secret = %+v, want HOST=%q", secret, want)
			}
			if scenario == "preexisting" && secret != preexisting {
				t.Error("preexisting immutable Secret replaced")
			}
			producer, ok := res.Producers.Producer(id)
			if !ok || producer != (manifest.NamedResource{Kind: "ExternalSecret", Namespace: "secure", Name: "producer"}) {
				t.Errorf("producer attribution = %v, %v", producer, ok)
			}
		})
	}
}

func TestRun_GraphOwnershipSourceClassification(t *testing.T) {
	for _, scenario := range []string{"bootstrap", "self", "external"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			sourceName := "flux-system"
			if scenario != "bootstrap" {
				sourceName = "source"
				testutil.WriteFile(t, dir, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: source, namespace: flux-system}
spec: {url: "https://example.invalid/cluster.git"}
`)
			}
			testutil.WriteFile(t, dir, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: flux-system}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: `+sourceName+`}}
`)
			testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [../shared/cm.yaml]\n")
			testutil.WriteFile(t, dir, "shared/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: config, namespace: flux-system}
data: {value: fixture}
`)
			cfg := discovery.Config{Path: dir, RepoRoot: dir, Store: store.New(), WipeSecrets: true}
			if scenario == "self" {
				cfg.SelfURLs = []string{"https://example.invalid/cluster.git"}
			}
			res, err := discovery.Run(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			owners := res.SelfProduce.OwnersOfFile("shared/cm.yaml")
			if scenario == "external" {
				if len(owners) != 0 {
					t.Errorf("external KS owns local file: %v", owners)
				}
			} else if len(owners) != 1 || owners[0].Name != "owner" {
				t.Errorf("local graph owners = %v", owners)
			}
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "config"}
			if got, want := cfg.Store.GetObject(id) != nil, scenario == "external"; got != want {
				t.Errorf("standalone promotion = %v, want %v", got, want)
			}
			src := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: sourceName}
			if cfg.Store.GetObject(src) == nil {
				t.Error("source must remain file-loaded")
			}
			if got, want := cfg.Store.GetArtifact(src) != nil, scenario != "external"; got != want {
				t.Errorf("source alias = %v, want %v", got, want)
			}
		})
	}
}

func TestRun_OnHelmReleaseBeforeAdmission(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: parent, namespace: flux-system}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: flux-system}}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: flux-system}
spec: {path: ./apps/child, sourceRef: {kind: GitRepository, name: flux-system}}
`)
	hrYAML := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: %s, namespace: flux-system}
spec: {suspend: true, chartRef: {kind: OCIRepository, name: fixture}}
`
	testutil.WriteFile(t, root, "apps/child/hr.yaml", fmt.Sprintf(hrYAML, "covered"))
	testutil.WriteFile(t, root, "flux/loose.yaml", fmt.Sprintf(hrYAML, "loose"))
	baseline, err := discovery.Run(t.Context(), discovery.Config{
		Path: filepath.Join(root, "flux"), RepoRoot: root, Store: store.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, observe := range []bool{false, true} {
		t.Run(fmt.Sprintf("observe_%t", observe), func(t *testing.T) {
			st := store.New()
			cfg := discovery.Config{Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st}
			var counts [2]int
			if observe {
				cfg.OnHelmRelease = func(hr *manifest.HelmRelease) {
					switch hr.Name {
					case "covered":
						counts[0]++
					case "loose":
						counts[1]++
					}
				}
			}
			res, err := discovery.Run(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			covered := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "covered"}
			loose := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "loose"}
			assert.Equal(t, st.GetObject(covered) == nil, true)
			assert.Equal(t, st.GetObject(loose) != nil, true)
			path, indexed := res.Existence.Get(covered)
			assert.Equal(t, indexed, true)
			assert.Equal(t, path, filepath.Join(root, "apps/child/hr.yaml"))
			wantCounts := [2]int{}
			if observe {
				wantCounts = [2]int{2, 1}
				assert.Diff(t, res.SourceFiles, baseline.SourceFiles)
			}
			assert.Equal(t, counts, wantCounts)
		})
	}
}
