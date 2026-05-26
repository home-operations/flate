package discovery_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

	mustWrite(t, filepath.Join(dir, "flux", "parent.yaml"), `---
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
	mustWrite(t, filepath.Join(dir, "apps", "child.yaml"), `---
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
	mustWrite(t, filepath.Join(dir, "apps", "leaf", "kustomization.yaml"), `resources: []
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

	mustWrite(t, filepath.Join(dir, "flux", "cluster.yaml"), `---
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
	mustWrite(t, filepath.Join(dir, "apps", "kustomization.yaml"), "resources: []\n")

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

	mustWrite(t, filepath.Join(dir, "flux", "cluster.yaml"), `---
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
	mustWrite(t, filepath.Join(dir, "apps", "kustomization.yaml"), "resources: []\n")

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

// TestRun_ResourceSetReExpandsForLateRSIPs pins a discovery-loop
// fix: a ResourceSet whose RSIP providers only become visible in a
// later iteration (because they live behind a Kustomization path
// that hasn't been walked yet) must be re-rendered, not memoized
// after its first empty render. Without the fix, the RS would
// render to zero docs on the first pass — when no RSIPs were in
// the store — and never recover when the deeper KS expansion
// produced them.
func TestRun_ResourceSetReExpandsForLateRSIPs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Root: a parent KS that points at apps/, plus an RS at the same
	// level. The RS selects RSIPs by label in its own namespace.
	mustWrite(t, filepath.Join(dir, "flux", "parent.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: parent, namespace: flux-system}
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system}
  interval: 10m
`)
	mustWrite(t, filepath.Join(dir, "flux", "rs.yaml"), `---
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
	// that path before the RSIP is in the store. Re-rendering the RS
	// on a later iter is what makes the child-rsip Kustomization show
	// up at all.
	mustWrite(t, filepath.Join(dir, "apps", "rsip.yaml"), `---
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

	// The RS render produces a Kustomization named "child-rsip" — present
	// only when the RS re-renders after the RSIP gets loaded.
	want := manifest.NamedResource{
		Kind:      manifest.KindKustomization,
		Namespace: "flux-system",
		Name:      "child-rsip",
	}
	if st.GetObject(want) == nil {
		t.Errorf("expected RS-emitted Kustomization %s in store; the RS likely rendered before its RSIP was loaded and was never retried", want)
	}
}

func TestRun_RequiresStoreAndLoader(t *testing.T) {
	t.Parallel()
	if _, err := discovery.Run(context.Background(), discovery.Config{Path: t.TempDir()}); err == nil {
		t.Error("Run with nil Store/Loader: want error, got nil")
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

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestRun_AliasesURLMatchedInTreeGitRepository pins the Zariel/
// home-ops pattern: a GitRepository CR defined IN the tree
// whose spec.url points at the same remote the working tree
// itself clones from. Real Flux uses these with SOPS-decrypted
// SSH deploy keys; flate runs offline and can't materialize the
// key, so the fetch fails on a placeholder credential.
//
// The second-pass alias in aliasBootstrapSources detects that
// the URL matches the working tree's .git/config remote and
// overrides the artifact with a working-tree alias so the
// dependent KSes proceed against local files rather than the
// failed-fetch source.
func TestRun_AliasesURLMatchedInTreeGitRepository(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Stand up a minimal .git/config so PlainOpen sees a repo and
	// readWorkingTreeRemotes returns one remote.
	mustWrite(t, filepath.Join(dir, ".git", "config"), `[core]
	repositoryformatversion = 0
[remote "origin"]
	url = git@github.com:Example/home-ops.git
`)
	mustWrite(t, filepath.Join(dir, ".git", "HEAD"), "ref: refs/heads/main\n")

	mustWrite(t, filepath.Join(dir, "k8s", "flux", "cluster.yaml"), `---
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
	mustWrite(t, filepath.Join(dir, "k8s", "apps", "kustomization.yaml"), "resources: []\n")

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
	mustWrite(t, filepath.Join(dir, ".git", "config"), `[core]
	repositoryformatversion = 0
[remote "origin"]
	url = git@github.com:Example/home-ops.git
`)
	mustWrite(t, filepath.Join(dir, ".git", "HEAD"), "ref: refs/heads/main\n")

	mustWrite(t, filepath.Join(dir, "k8s", "flux", "shared-infra.yaml"), `---
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
	mustWrite(t, filepath.Join(dir, "k8s", "apps", "kustomization.yaml"), "resources: []\n")

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
	mustWrite(t, filepath.Join(dir, "flux", "ks.yaml"), `---
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
	mustWrite(t, filepath.Join(dir, "cluster", "kustomization.yaml"), `apiVersion: kustomize.config.k8s.io/v1
kind: Kustomization
components:
  - ../components/cluster-settings
`)
	// Component declares the configMapGenerator
	mustWrite(t, filepath.Join(dir, "components", "cluster-settings", "kustomization.yaml"), `apiVersion: kustomize.config.k8s.io/v1alpha1
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
	cm, _ := store.GetByName[*manifest.ConfigMap](st, manifest.KindConfigMap, "flux-system", "cluster-settings")
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
