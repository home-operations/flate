package discovery_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
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

	parent := manifest.NamedResource{Group: "kustomize.toolkit.fluxcd.io", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "parent"}
	child := manifest.NamedResource{Group: "kustomize.toolkit.fluxcd.io", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "child"}
	for _, id := range []manifest.NamedResource{parent, child} {
		if _, ok := filesForTest(res.Index)[id]; !ok {
			t.Errorf("SourceFiles missing %s", id)
		}
		if st.GetObject(id) == nil {
			t.Errorf("Store missing %s", id)
		}
	}

	if got := parentsForTest(res.Index)[child]; got != parent {
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
		name, url, ref string
	}{
		{"none", "", ""},
		{"no_ref", "https://example.invalid/self.git", ""},
		{"non_matching_ref", "https://example.invalid/x.git", "  ref: {branch: main}\n"},
	} {
		t.Run(repo.name, func(t *testing.T) {
			t.Parallel()
			for _, placement := range []string{"deep", "clusters"} {
				t.Run(placement, func(t *testing.T) {
					t.Parallel()
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
						testutil.WriteFile(t, root, "flux/repo.yaml", fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: %s
%s`, repo.url, repo.ref))
					}
					st := store.New()
					if _, err := discovery.Run(t.Context(), discovery.Config{
						Path: filepath.Join(root, "flux"), Store: st,
						SelfURLs: []string{"https://example.invalid/self.git"},
					}); err != nil {
						t.Fatal(err)
					}
					var ids []string
					for _, ks := range st.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization) {
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
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [release.yaml, producer.yaml]\n")
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
	testutil.WriteFile(t, dir, "apps/producer.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: values, namespace: apps}
spec: {target: {name: values}}
`)

	st := store.New()
	foreign := &manifest.RawObject{APIVersion: "foreign.example/v1", Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "demo"}
	st.AddObject(foreign)
	res, err := discovery.Run(t.Context(), discovery.Config{Path: dir, Store: st, WipeSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	hr := manifest.NamedResource{Group: "helm.toolkit.fluxcd.io", Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "demo"}
	parent := manifest.NamedResource{Group: "kustomize.toolkit.fluxcd.io", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	if st.GetObject(hr) != nil {
		t.Fatal("parent-owned HelmRelease must wait for rendering")
	}
	if _, indexed := filesForTest(res.Index)[hr]; !indexed {
		t.Fatal("HelmRelease source identity must be indexed")
	}
	if got := parentsForTest(res.Index)[hr]; got != parent {
		t.Errorf("ParentOf[%s] = %v, want %v", hr, got, parent)
	}
	secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "values"}
	producer := manifest.NamedResource{Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: "apps", Name: "values"}
	if got, ok := res.Producers.Producer(secret); !ok || got != producer {
		t.Fatalf("producer not indexed before dependent arrival: %v, %t", got, ok)
	}
	foreignSecret := secret
	foreignSecret.Group = "foreign.example"
	if _, ok := res.Producers.Producer(foreignSecret); ok {
		t.Fatal("foreign secret borrowed the core producer")
	}
	if st.GetObject(foreign.Named()) != foreign {
		t.Fatal("discovery replaced the foreign HelmRelease")
	}
	if _, ok := res.Index.Lookup(foreign.Named()); ok {
		t.Fatal("foreign HelmRelease borrowed the real declaration record")
	}
	orphan := manifest.NamedResource{Group: "helm.toolkit.fluxcd.io", Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "orphan"}
	if _, gated := parentsForTest(res.Index)[orphan]; gated || st.GetObject(orphan) == nil {
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
// `dependency not found: GitRepository.source.toolkit.fluxcd.io/gitops-system/gitops-system`.
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

	aliased := manifest.NamedResource{Group: "source.toolkit.fluxcd.io",
		Kind: manifest.KindGitRepository, Namespace: "gitops-system", Name: "gitops-system",
	}
	if st.GetObject(aliased) == nil {
		t.Errorf("expected GitRepository.source.toolkit.fluxcd.io/gitops-system/gitops-system to be aliased; alias scoped only to flux-system would leave this case broken")
	}
	if st.GetArtifact(aliased) == nil {
		t.Errorf("aliased GitRepository should have a SourceArtifact so depwait resolves")
	}
	info, ok := st.GetStatus(aliased)
	if !ok || info.Status != store.StatusReady {
		t.Errorf("aliased GitRepository should be Ready; got ok=%v info=%+v", ok, info)
	}
}

func TestRun_BootstrapOCIArtifactRoot(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindBucket} {
		for _, missing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/missing_%t", kind, missing), func(t *testing.T) {
				t.Parallel()
				dir := t.TempDir()
				p := "./apps/x"
				if missing {
					p = "./absent"
				}
				testutil.WriteFile(t, dir, "kubernetes/clusters/c0/ks.yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: parent, namespace: flux-system}
spec: {path: %s, sourceRef: {kind: %s, name: flux-system}}
`, p, kind))
				if !missing {
					testutil.WriteFile(t, dir, "kubernetes/clusters/c0/constraint.yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: flux-system}
spec:
  path: ./consumer
  sourceRef: {kind: %s, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: payload}]}
`, kind))
				}
				testutil.WriteFile(t, dir, "kubernetes/apps/x/kustomization.yaml", "resources: [cm.yaml, child.yaml]\n")
				testutil.WriteFile(t, dir, "kubernetes/apps/x/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: flux-system}\ndata: {K: value}\n")
				testutil.WriteFile(t, dir, "kubernetes/apps/x/child.yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: flux-system}
spec:
  path: ./consumer
  sourceRef: {kind: %s, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: payload}]}
`, kind))
				testutil.WriteFile(t, dir, "kubernetes/consumer/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, dir, "kubernetes/consumer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {value: '${K}'}\n")
				testutil.WriteFile(t, dir, "apps/x/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n")
				st := store.New()
				res, err := discovery.Run(t.Context(), discovery.Config{Path: filepath.Join(dir, "kubernetes/clusters/c0"), RepoRoot: dir, Store: st, WipeSecrets: true})
				if err != nil {
					t.Fatal(err)
				}
				source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "flux-system", Name: "flux-system"}
				parent := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "parent"}
				child := parent
				child.Name = "child"
				if missing {
					if st.GetObject(source) != nil || st.GetArtifact(source) != nil {
						t.Fatal("unresolved source aliased")
					}
					if owners, _ := res.Index.OwnersOfFile("kubernetes/apps/x/cm.yaml"); len(owners) != 0 {
						t.Fatalf("unresolved source claimed files: %v", owners)
					}
					return
				}
				art, ok := st.GetArtifact(source).(*store.SourceArtifact)
				if !ok || art.LocalRoot != filepath.Join(dir, "kubernetes") {
					t.Fatalf("source = %+v", art)
				}
				if owners, _ := res.Index.OwnersOfFile("kubernetes/apps/x/cm.yaml"); !slices.Contains(owners, parent) {
					t.Fatalf("subtree owners = %v", owners)
				}
				if owners, _ := res.Index.OwnersOfFile("apps/x/cm.yaml"); len(owners) != 0 {
					t.Fatalf("unrelated sibling owners = %v", owners)
				}
				record, ok := res.Index.Lookup(child)
				if !ok || record.Parent != parent {
					t.Fatalf("child record = %+v", record)
				}
				payload := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "payload"}
				if producers := res.Index.DataProducers(payload); !slices.Contains(producers, parent) {
					t.Fatalf("subtree producers = %v", producers)
				}
				if readers := slices.Collect(res.Index.ReadersOfFile("kubernetes/apps/x/cm.yaml")); !slices.Contains(readers, parent) {
					t.Fatalf("subtree readers = %v", readers)
				}
				if readers := slices.Collect(res.Index.ReadersOfFile("apps/x/cm.yaml")); len(readers) != 0 {
					t.Fatalf("sibling readers = %v", readers)
				}
				for _, file := range []string{"kubernetes/apps/x/cm.yaml", "apps/x/cm.yaml"} {
					selected := change.NewFilter(change.NewSet([]string{file}), res.Index, st).KeepNames()
					consumer := child
					consumer.Name = "consumer"
					for _, id := range []manifest.NamedResource{parent, child, consumer} {
						if got := slices.Contains(selected, id.String()); got != strings.HasPrefix(file, "kubernetes/") {
							t.Fatalf("change %s selected %v; checking %s", file, selected, id)
						}
					}
				}
			})
		}
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
	rsID := manifest.NamedResource{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSet, Namespace: "flux-system", Name: "late-rs"}
	if st.GetObject(rsID) == nil {
		t.Errorf("expected ResourceSet %s loaded into store", rsID)
	}
	// The deep RSIP behind the parent KS's path is loaded — discovery
	// walked the spec.path even though it no longer expands the RS.
	rsipID := manifest.NamedResource{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSetInputProvider, Namespace: "flux-system", Name: "rsip"}
	if st.GetObject(rsipID) == nil {
		t.Errorf("expected ResourceSetInputProvider %s loaded into store (parent KS path walked)", rsipID)
	}
	// Discovery does NOT expand the RS anymore — the child Kustomization
	// is produced at run time by the RS controller, not here.
	child := manifest.NamedResource{Group: "kustomize.toolkit.fluxcd.io", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "child-rsip"}
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

	appA := manifest.NamedResource{Group: "kustomize.toolkit.fluxcd.io", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "app-a"}
	clusterApps := manifest.NamedResource{Group: "kustomize.toolkit.fluxcd.io", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps"}
	if got := parentsForTest(res.Index)[appA]; got != clusterApps {
		t.Errorf("ParentOf[app-a] = %v, want cluster-apps (cross-tree emission gate)", got)
	}

	selfish := manifest.NamedResource{Group: "kustomize.toolkit.fluxcd.io", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "selfish"}
	if got, gated := parentsForTest(res.Index)[selfish]; gated {
		t.Errorf("ParentOf[selfish] = %v, want absent (a self-emit must not gate on itself)", got)
	}
}

func TestRun_SymlinkEntryResolved(t *testing.T) {
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
	got, err := discovery.Run(t.Context(), discovery.Config{Path: link, Store: store.New()})
	if err != nil {
		t.Fatal(err)
	}
	want, _ := filepath.EvalSymlinks(target)
	if got.RepoRoot != want {
		t.Errorf("RepoRoot = %q, want %q", got.RepoRoot, want)
	}
}

func TestRun_NoRepositoryUsesScanRoot(t *testing.T) {
	t.Parallel()
	for _, layout := range []string{"absent", "empty git directory"} {
		t.Run(layout, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if layout == "empty git directory" {
				if err := os.Mkdir(filepath.Join(dir, ".git"), 0o750); err != nil {
					t.Fatal(err)
				}
			}
			st := store.New()
			result, err := discovery.Run(t.Context(), discovery.Config{Path: dir, Store: st})
			if err != nil {
				t.Fatal(err)
			}
			artifact := st.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
			if result.RepoRoot != dir || artifact.LocalPath != dir {
				t.Errorf("no-repository roots = %q, %q; want %q", result.RepoRoot, artifact.LocalPath, dir)
			}
		})
	}
}

// TestRun_AliasesURLMatchedInTreeGitRepository pins the Zariel/
// home-ops pattern: a GitRepository CR defined IN the tree
// whose spec.url points at the same remote the working tree
// itself clones from. Real Flux uses these with SOPS-decrypted
// SSH deploy keys; flate runs offline and can't materialize the
// key, so the fetch fails on a placeholder credential.
//
// Entry resolution detects that
// the URL matches the working tree's .git/config remote and
// overrides the artifact with a working-tree alias so the
// dependent KSes proceed against local files rather than the
// failed-fetch source.
func TestRun_AliasesURLMatchedInTreeGitRepository(t *testing.T) {
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
	committed, err := wt.Commit("fixture", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
	if err != nil {
		t.Fatal(err)
	}

	testutil.WriteFile(t, dir, "k8s/apps/kustomization.yaml", "resources: []\nnamePrefix: current-\n")
	if _, err := wt.Add("."); err != nil {
		t.Fatal(err)
	}
	if _, err := wt.Commit("current", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(1, 0)}}); err != nil {
		t.Fatal(err)
	}
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse_%t", reverse), func(t *testing.T) {
			st := store.New()
			pinned := &manifest.GitRepository{Name: "committed", Namespace: "flux-system", URL: "https://github.com/example/home-ops.git", Reference: &manifest.GitRepositoryRef{Commit: committed.String()}}
			st.AddObject(pinned)
			var objects []manifest.BaseManifest
			want := []string{(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "home-kubernetes"}).String()}
			for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket} {
				source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "other", Name: "missing"}
				want = append(want, source.String())
				for _, name := range []string{"one", "two"} {
					objects = append(objects, &manifest.Kustomization{Name: kind + name, Namespace: "other", Path: "./k8s/apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: kind, Name: source.Name}})
				}
			}
			if reverse {
				slices.Reverse(objects)
			}
			st.AddObjects(objects)
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			if _, err := discovery.Run(t.Context(), discovery.Config{Path: dir, Store: st, WipeSecrets: true}); err != nil {
				t.Fatal(err)
			}
			id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "home-kubernetes"}
			artifact, ok := st.GetArtifact(id).(*store.SourceArtifact)
			if !ok || !artifact.InputAlias || !strings.HasPrefix(artifact.URL, "file://") {
				t.Fatalf("matched source artifact = %+v", artifact)
			}
			if info, ok := st.GetStatus(id); !ok || info.Status != store.StatusReady {
				t.Fatalf("matched source status = %+v, %t", info, ok)
			}
			if artifact, ok := st.GetArtifact(pinned.Named()).(*store.SourceArtifact); !ok || artifact.InputAlias || artifact.Key == "" || st.GetObject(pinned.Named()) != pinned {
				t.Fatalf("committed source artifact = %+v", artifact)
			}
			slices.Sort(want)
			warnings := 0
			for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
				var entry struct {
					Message string   `json:"msg"`
					Count   int      `json:"count"`
					IDs     []string `json:"ids"`
				}
				if err := json.Unmarshal([]byte(line), &entry); err != nil {
					t.Fatal(err)
				}
				if strings.Contains(entry.Message, "aliased multiple bootstrap sources") {
					warnings++
					if entry.Count != len(want) || !slices.Equal(entry.IDs, want) {
						t.Fatalf("combined aliases = %v (%d), want %v", entry.IDs, entry.Count, want)
					}
				}
			}
			if warnings != 1 {
				t.Fatalf("combined warning count = %d, logs=%s", warnings, logs.String())
			}
		})
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
	repo, err := git.PlainInit(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"git@github.com:Example/home-ops.git"}}); err != nil {
		t.Fatal(err)
	}

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

	id := manifest.NamedResource{Group: "source.toolkit.fluxcd.io",
		Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "shared-infra",
	}
	if st.GetArtifact(id) != nil {
		t.Errorf("non-matching upstream GitRepository must NOT receive a working-tree alias artifact")
	}
}

// Generated data MUST use the render namespace, independent of the owning
// Kustomization's metadata.namespace, so dependency lookups address its output.
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
		Kind: manifest.KindConfigMap, Name: "cluster-settings",
	}
	cm, _ := st.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "cluster-settings"})
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
	t.Parallel()
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
		return manifest.NamedResource{Group: "kustomize.toolkit.fluxcd.io", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: name}
	}
	for name, want := range map[string]bool{"keep": true, "drop": false, "leaf": true, "hidden": false} {
		if got := st.GetObject(id(name)) != nil; got != want {
			t.Errorf("Store has %s = %v, want %v", name, got, want)
		}
	}
}

func TestRun_GraphOwnedOrphans(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, resource, namespace string }{
		{"sibling", "../shared/demo", ""},
		{"direct", "../shared/demo/bundle.yaml", ""},
		{"explicit", "../shared/demo", "demo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
			raw := manifest.NamedResource{Group: "helm.toolkit.fluxcd.io", Kind: manifest.KindHelmRelease, Name: "demo", Namespace: "demo"}
			if _, ok := res.Index.Lookup(raw); !ok {
				t.Error("projected release must remain indexed")
			}
			if st.GetObject(raw) != nil {
				t.Errorf("graph-owned release promoted: %s", raw)
			}
			repos := st.ListAs[*manifest.HelmRepository](manifest.SourceDomain, manifest.KindHelmRepository)
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
	t.Parallel()
	for _, scenario := range []string{"standalone", "preexisting", "graph-owned"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
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
			if !ok || producer != (manifest.NamedResource{Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: "secure", Name: "producer"}) {
				t.Errorf("producer attribution = %v, %v", producer, ok)
			}
		})
	}
}

func TestRun_GraphOwnershipSourceClassification(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"bootstrap", "self", "self_unavailable", "external", "missing_oci", "missing_bucket", "declared_oci", "declared_bucket"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			sourceName := "flux-system"
			kind := manifest.KindGitRepository
			if strings.HasSuffix(scenario, "oci") {
				kind = manifest.KindOCIRepository
			} else if strings.HasSuffix(scenario, "bucket") {
				kind = manifest.KindBucket
			}
			external := scenario == "external" || scenario == "self_unavailable" || strings.HasPrefix(scenario, "declared_")
			if strings.HasPrefix(scenario, "self") || scenario == "external" {
				sourceName = "source"
				testutil.WriteFile(t, dir, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: source, namespace: flux-system}
spec: {url: "https://example.invalid/cluster.git"}
`)
			}
			testutil.WriteFile(t, dir, "flux/ks.yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: flux-system}
spec: {path: ./apps, sourceRef: {kind: %s, name: %s}}
`, kind, sourceName))
			testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [cm.yaml, child.yaml, hr.yaml]\n")
			testutil.WriteFile(t, dir, "apps/child.yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: flux-system}
spec: {path: ./apps/leaf, sourceRef: {kind: %s, name: %s}}
`, kind, sourceName))
			testutil.WriteFile(t, dir, "apps/leaf/kustomization.yaml", "resources: []\n")
			testutil.WriteFile(t, dir, "apps/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: release, namespace: flux-system}
spec: {chartRef: {kind: OCIRepository, name: chart}}
`)
			testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: config, namespace: flux-system}
data: {value: fixture}
`)
			cfg := discovery.Config{Path: filepath.Join(dir, "flux"), RepoRoot: dir, Store: store.New(), WipeSecrets: true}
			var authored manifest.BaseManifest
			if strings.HasPrefix(scenario, "declared_") {
				spec := map[string]any{"url": "oci://registry.invalid/artifact", "ignore": "excluded/", "ref": map[string]any{"tag": "stable"}}
				if kind == manifest.KindBucket {
					spec = map[string]any{"endpoint": "minio.invalid", "bucketName": "actual-bucket", "prefix": "published/", "ignore": "excluded/"}
				}
				var err error
				authored, _, _, err = manifest.ParseDoc(map[string]any{"apiVersion": manifest.SourceDomain + "/v1", "kind": kind, "metadata": map[string]any{"name": sourceName, "namespace": "flux-system"}, "spec": spec}, manifest.ParseDocOptions{})
				if err != nil {
					t.Fatal(err)
				}
				cfg.Store.AddObject(authored)
			}
			authoredContent := manifest.Fingerprint(authored)

			foreignSource := &manifest.RawObject{APIVersion: "foreign.example/v1", Kind: kind, Namespace: "flux-system", Name: sourceName}
			foreignOwner := &manifest.RawObject{APIVersion: "foreign.example/v1", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "owner", Spec: map[string]any{"path": "./shared"}}
			cfg.Store.AddObjects([]manifest.BaseManifest{foreignSource, foreignOwner})
			if strings.HasPrefix(scenario, "self") {
				cfg.SelfURLs = []string{"https://example.invalid/cluster.git"}
			}
			if scenario == "self_unavailable" {
				testutil.WriteFile(t, dir, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: source, namespace: flux-system}
spec: {url: "https://example.invalid/cluster.git", ref: {branch: unavailable}}
`)
			}
			res, err := discovery.Run(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Store.GetObject(foreignSource.Named()) != foreignSource || cfg.Store.GetArtifact(foreignSource.Named()) != nil {
				t.Fatal("foreign source acquired the bootstrap alias")
			}
			if _, indexed := res.Index.Lookup(foreignOwner.Named()); indexed {
				t.Fatal("foreign owner borrowed a real claim")
			}
			child := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "child"}
			if cfg.Store.GetObject(child) == nil {
				t.Fatal("entry path did not discover its child Kustomization")
			}
			release := manifest.NamedResource{Group: "helm.toolkit.fluxcd.io", Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "release"}
			if got := cfg.Store.GetObject(release) != nil; got != external {
				t.Errorf("entry path HelmRelease promotion = %v, want %v", got, external)
			}
			owners := ownersForTest(res.Index, "apps/cm.yaml")
			if external {
				if len(owners) != 0 {
					t.Errorf("external KS owns local file: %v", owners)
				}
			} else if len(owners) != 1 || owners[0].Name != "owner" {
				t.Errorf("local graph owners = %v", owners)
			}
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "config"}
			if got, want := cfg.Store.GetObject(id) != nil, external; got != want {
				t.Errorf("standalone promotion = %v, want %v", got, want)
			}
			src := manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "flux-system", Name: sourceName}
			if cfg.Store.GetObject(src) == nil {
				t.Error("source must remain file-loaded")
			}
			if authored != nil && (cfg.Store.GetObject(src) != authored || manifest.Fingerprint(authored) != authoredContent) {
				t.Fatal("declared source object or spec changed")
			}
			if got, want := cfg.Store.GetArtifact(src) != nil, !external; got != want {
				t.Errorf("source alias = %v, want %v", got, want)
			}
		})
	}
}

func TestRun_ValuesFromOnlyChangeSelectsConsumer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ namespace, apiVersion string }{
		{"apps", "v1"}, {"other", "v1"}, {"apps", "foreign.example/v1"},
	} {
		t.Run(tc.namespace+"/"+tc.apiVersion, func(t *testing.T) {
			t.Parallel()
			ns := tc.namespace
			root := t.TempDir()
			for _, file := range []struct{ path, body string }{
				{"flux/producer.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: producer, namespace: flux-system}\nspec: {path: ./producer}\n"},
				{"flux/consumer.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: flux-system}\nspec: {path: ./consumer}\n"},
				{"producer/kustomization.yaml", "namespace: apps\nresources: [values.yaml]\n"},
				{"producer/values.yaml", "apiVersion: " + tc.apiVersion + "\nkind: ConfigMap\nmetadata: {name: values}\ndata: {values.yaml: fixture}\n"},
				{"consumer/kustomization.yaml", "namespace: " + ns + "\nresources: [hr.yaml]\n"},
				{"consumer/hr.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: consumer}\nspec:\n  chartRef: {kind: OCIRepository, name: chart, namespace: flux-system}\n  valuesFrom: [{kind: ConfigMap, name: values}]\n"},
			} {
				testutil.WriteFile(t, root, file.path, file.body)
			}
			st := store.New()
			res, err := discovery.Run(t.Context(), discovery.Config{Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st})
			if err != nil {
				t.Fatal(err)
			}
			filter := change.NewFilter(change.NewSet([]string{"producer/values.yaml"}), res.Index, st)
			id := manifest.NamedResource{Group: "helm.toolkit.fluxcd.io", Kind: manifest.KindHelmRelease, Namespace: ns, Name: "consumer"}
			if got := filter.ShouldReconcile(id); got != (ns == "apps" && tc.apiVersion == "v1") {
				t.Fatalf("consumer %s selected=%t", id, got)
			}
		})
	}
}

func TestRun_SubstituteFromOnlyChangeSelectsConsumer(t *testing.T) {
	t.Parallel()
	for _, optional := range []bool{false, true} {
		t.Run(fmt.Sprint(optional), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for _, file := range []struct{ path, body string }{
				{"flux/producer.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: producer, namespace: apps}\nspec: {path: ./producer}\n"},
				{"flux/consumer.yaml", fmt.Sprintf("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec:\n  path: ./consumer\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: values, optional: %t}]\n", optional)},
				{"producer/kustomization.yaml", "namespace: apps\nresources: [values.yaml]\n"},
				{"producer/values.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values}\ndata: {VALUE: fixture}\n"},
				{"consumer/kustomization.yaml", "resources: []\n"},
			} {
				testutil.WriteFile(t, root, file.path, file.body)
			}
			st := store.New()
			res, err := discovery.Run(t.Context(), discovery.Config{Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st})
			if err != nil {
				t.Fatal(err)
			}
			filter := change.NewFilter(change.NewSet([]string{"producer/values.yaml"}), res.Index, st)
			id := manifest.NamedResource{Group: "kustomize.toolkit.fluxcd.io", Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
			if got := filter.ShouldReconcile(id); got == optional {
				t.Fatalf("optional=%t consumer selected=%t", optional, got)
			}
		})
	}
}
