package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// TestDetectOrphans drives the orphan-detection logic in isolation —
// no controllers, just the store / sourceFiles wiring the orchestrator
// builds during Bootstrap. Three scenarios:
//
//  1. Truly orphaned child (file under parent path, never emitted by
//     parent's render): downgraded.
//  2. Root-level resource (no covering parent): NOT downgraded.
//  3. Child re-emitted by parent (WasRendered set): NOT downgraded.
func TestDetectOrphans(t *testing.T) {
	t.Parallel()
	parent := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		Path: "./kubernetes/apps",
	}
	orphan := &manifest.Kustomization{
		Name: "orphan", Namespace: "flux-system",
		Path: "./kubernetes/apps/orphan/app",
	}
	emittedChild := &manifest.Kustomization{
		Name: "wired", Namespace: "flux-system",
		Path: "./kubernetes/apps/wired/app",
	}
	root := &manifest.Kustomization{
		Name: "another-root", Namespace: "flux-system",
		Path: "./standalone",
	}

	o := &Orchestrator{
		store: store.New(),
		index: indexForTest(map[manifest.NamedResource]string{
			parent.Named():       "kubernetes/flux/cluster/ks.yaml",
			orphan.Named():       "kubernetes/apps/orphan/ks.yaml",
			emittedChild.Named(): "kubernetes/apps/wired/ks.yaml",
			root.Named():         "kubernetes/standalone/ks.yaml",
		}),
	}
	for _, ks := range []*manifest.Kustomization{parent, orphan, emittedChild, root} {
		o.store.AddObject(ks)
	}
	// Mark emittedChild as rendered by its parent — simulates the
	// AddObject + RecordEmitted call cluster-apps's render would make.
	publishFixture(t, o.store, parent.Named(), []manifest.NamedResource{emittedChild.Named()})

	failed := map[manifest.NamedResource]store.StatusInfo{
		orphan.Named():       {Status: store.StatusFailed, Message: "TIMEZONE undefined"},
		emittedChild.Named(): {Status: store.StatusFailed, Message: "TIMEZONE undefined"},
		root.Named():         {Status: store.StatusFailed, Message: "broken"},
	}

	orphans := o.detectOrphans(failed, loader.Claims(o.store, o.repoRoot))

	if _, ok := orphans[orphan.Named()]; !ok {
		t.Errorf("expected orphan to be detected")
	}
	if _, ok := orphans[emittedChild.Named()]; ok {
		t.Errorf("re-emitted child is not an orphan: parent's render covered it")
	}
	if _, ok := orphans[root.Named()]; ok {
		t.Errorf("root resource with no covering parent is not an orphan")
	}
	if got := len(orphans); got != 1 {
		t.Errorf("expected exactly 1 orphan, got %d: %+v", got, orphans)
	}
	o.store.AddObject(&manifest.GitRepository{Name: "external", Namespace: "flux-system"})
	o.store.AddObject(&manifest.Kustomization{
		Name: "external", Namespace: "flux-system", Path: "./",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "external", Namespace: "flux-system"},
	})
	assert.Diff(t, o.detectOrphans(failed, loader.Claims(o.store, o.repoRoot)), orphans)
}

// TestDetectOrphans_NonReconcilableKindsIgnored — ConfigMaps and
// Secrets that fail (they can't fail in practice but the failure
// map is permissive) are never reported as orphans; orphan
// detection only applies to Kustomization + HelmRelease.
func TestDetectOrphans_NonReconcilableKindsIgnored(t *testing.T) {
	t.Parallel()
	parent := &manifest.Kustomization{
		Name: "p", Namespace: "ns",
		Path: "./apps",
	}
	cm := &manifest.ConfigMap{Name: "stuck", Namespace: "ns"}
	foreign := &manifest.RawObject{APIVersion: "example.test/v1", Kind: manifest.KindKustomization, Name: cm.Name, Namespace: cm.Namespace}

	o := &Orchestrator{
		store: store.New(),
		index: indexForTest(map[manifest.NamedResource]string{
			parent.Named():  "ks.yaml",
			cm.Named():      "apps/stuck/cm.yaml",
			foreign.Named(): "apps/stuck/ks.yaml",
		}),
	}
	o.store.AddObject(parent)
	o.store.AddObject(cm)
	o.store.AddObject(foreign)

	failed := map[manifest.NamedResource]store.StatusInfo{
		cm.Named():      {Status: store.StatusFailed, Message: "bogus"},
		foreign.Named(): {Status: store.StatusFailed, Message: "foreign"},
	}
	orphans := o.detectOrphans(failed, loader.Claims(o.store, o.repoRoot))
	if len(orphans) != 0 {
		t.Errorf("orphan detection should skip non-reconcilable kinds; got %+v", orphans)
	}
}

// TestDetectOrphans_ParentFailedIsCascadeNotOrphan pins the cascade/orphan
// discriminator: when a child's covering parent itself FAILED (so it emitted
// nothing, making every child under its spec.path look unreferenced), the child
// is a blocked cascade victim — NOT an orphan. Demoting it would drop it from
// the failure set and let the report/test runner score a broken tree as passing.
// Only a child under a parent that SUCCEEDED is a genuine orphan.
func TestDetectOrphans_ParentFailedIsCascadeNotOrphan(t *testing.T) {
	t.Parallel()
	parent := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		Path: "./kubernetes/apps",
	}
	child := &manifest.Kustomization{
		Name: "plex", Namespace: "media",
		Path: "./kubernetes/apps/media/plex/app",
	}

	o := &Orchestrator{
		store: store.New(),
		index: indexForTest(map[manifest.NamedResource]string{
			parent.Named(): "kubernetes/flux/cluster/ks.yaml",
			child.Named():  "kubernetes/apps/media/plex/ks.yaml",
		}),
	}
	o.store.AddObject(parent)
	o.store.AddObject(child)

	failed := map[manifest.NamedResource]store.StatusInfo{
		parent.Named(): {Status: store.StatusFailed, Message: "kustomize build boom"}, // the build that emitted nothing
		child.Named():  {Status: store.StatusFailed, Message: "dependencies failed: cluster-apps"},
	}
	orphans := o.detectOrphans(failed, loader.Claims(o.store, o.repoRoot))
	if _, ok := orphans[child.Named()]; ok {
		t.Errorf("child under a FAILED covering parent is a cascade victim, not an orphan: %+v", orphans)
	}
	if len(orphans) != 0 {
		t.Errorf("expected no orphans when the covering parent failed, got %+v", orphans)
	}
}

func TestOrchestrator_StopBeforeBootstrapCompletion(t *testing.T) {
	for _, stage := range []string{"before-bootstrap", "input-failure"} {
		for _, injected := range []bool{false, true} {
			t.Run(stage+"/injected="+strconv.FormatBool(injected), func(t *testing.T) {
				root := filepath.Join(t.TempDir(), "cache")
				cfg := Config{Path: t.TempDir(), CacheDir: root, Concurrency: 2}
				if injected {
					owner, err := sourcecache.New(t.Context(), "")
					assert.NoError(t, err)
					t.Cleanup(func() { _ = owner.Close() })
					cfg.Cache = owner
				}
				key := sourcecache.BlobKey{Content: "blob-before-stop"}
				if cfg.Cache != nil {
					reader, _, _, err := cfg.Cache.Blob(t.Context(), key, sourcecache.Policy{MaxAge: time.Hour},
						func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
							_, err := io.WriteString(w, "caller-owned")
							return sourcecache.ProducerResult{}, err
						})
					assert.NoError(t, err)
					assert.NoError(t, reader.Close())
				}
				cfg.Input = &store.SourceArtifact{}
				o, err := New(t.Context(), cfg)
				assert.NoError(t, err)
				if o.cache == nil || o.hrc.Helm == nil || o.ksc.Trees == nil || (cfg.Cache != nil && o.cache != cfg.Cache) {
					t.Fatal("New did not wire both renderers through the selected owner")
				}
				if stage == "input-failure" && !errors.Is(o.Bootstrap(t.Context()), manifest.ErrInput) {
					t.Fatal("expected Bootstrap input failure")
				}
				if err := o.Stop(); err != nil {
					t.Fatalf("Stop: %v", err)
				}
				if err := o.Stop(); err != nil {
					t.Fatalf("Stop: %v", err)
				}
				if injected {
					if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("Stop allocated independent cache backing: %v", err)
					}
				} else {
					res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
					if err != nil || len(res.Errors) != 0 {
						t.Fatalf("Stop retained owner lock: result=%+v err=%v", res, err)
					}
				}
				if cfg.Cache != nil {
					reader, _, _, err := cfg.Cache.Blob(t.Context(), key, sourcecache.Policy{MaxAge: time.Hour}, nil)
					assert.NoError(t, err)
					assert.NoError(t, reader.Close())
				}
			})
		}
	}
}

func TestOrchestrator_SimpleCluster(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n"))
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: hello
data: {k: v}
`)

	fallback := t.TempDir()
	for _, key := range []string{"HOME", "XDG_CACHE_HOME", "TMPDIR", "TMP", "TEMP", "LOCALAPPDATA", "FLATE_CACHE_DIR"} {
		t.Setenv(key, fallback)
	}
	o, err := New(t.Context(), Config{Path: dir, WipeSecrets: true, Concurrency: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := o.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if err := o.Bootstrap(t.Context()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if err := o.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := len(o.Store().ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)); got != 1 {
		t.Errorf("expected 1 Kustomization, got %d", got)
	}
	if got := len(o.Store().ListObjects("", manifest.KindConfigMap)); got < 1 {
		t.Errorf("expected at least 1 ConfigMap after reconcile, got %d", got)
	}
	if entries, err := os.ReadDir(fallback); err != nil || len(entries) != 0 {
		t.Fatalf("empty SDK CacheDir wrote cache artifacts: %v, %v", entries, err)
	}
}

// TestOrchestrator_TwoKSSameSpecPathNoSpuriousTimeout reproduces the
// dual git-source redundancy pattern: two Kustomizations defined in the
// same file and pointing at the SAME spec.path. They are peers (neither
// renders the other), so they must not be wired as each other's
// structural parent — a mutual parent edge deadlocks the pair in
// collectDeps and stalls their dependents. The context timeout bounds
// the run if a mutual parent edge regresses.
func TestOrchestrator_TwoKSSameSpecPathNoSpuriousTimeout(t *testing.T) {
	dir := t.TempDir()
	// Two KSes, same spec.path ./flux, both defined in flux/repo.yaml so
	// both source files sit under their shared spec.path prefix — the
	// file-prefix parent index would otherwise make each the other's
	// structural parent and deadlock the pair. repo.yaml is intentionally
	// NOT listed in flux/kustomization.yaml (it's surfaced by the
	// bootstrap-sibling scan), so rendering ./flux does not re-emit the
	// peers — isolating the file-index parent edge this fix targets.
	testutil.WriteFile(t, dir, "flux/repo.yaml", testutil.KustomizationYAML("config-a", "flux-system", `  path: ./flux
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)+"---\n"+
		testutil.KustomizationYAML("config-b", "flux-system", `  path: ./flux
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`))
	testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "flux/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: flux-cm}
data: {k: v}
`)
	// Downstream consumer dependsOn one of the peers — the cascade victim
	// if config-a deadlocks.
	testutil.WriteFile(t, dir, "consumer.yaml", testutil.KustomizationYAML("consumer", "flux-system", "  path: ./apps\n  dependsOn:\n    - name: config-a\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello}
data: {k: v}
`)

	o, err := New(t.Context(), Config{Path: dir, WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	// The context timeout bounds a regressed mutual-parent deadlock.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := o.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, name := range []string{"config-a", "config-b", "consumer"} {
		id := testutil.KustomizationID("flux-system", name)
		info, ok := o.Store().GetStatus(id)
		if !ok || info.Status != store.StatusReady {
			t.Fatalf("%s status = (%+v, %v), want Ready (mutual-parent deadlock regressed?)", id, info, ok)
		}
	}
}

// TestOrchestrator_CommentedOutKSNotReconciled is the e2e fence for the
// bootstrap-sibling over-discovery: a Flux Kustomization commented out of its
// namespace's kustomization.yaml (a disabled app) that lives inside
// cluster-apps' rendered tree must NOT be discovered or reconciled — real Flux
// never deploys it. The entry is scoped to kubernetes/flux (with a git checkout
// so spec.paths resolve against the repo root) so cluster-apps is discovered
// before its subtree is scanned.
func TestOrchestrator_CommentedOutKSNotReconciled(t *testing.T) {
	dir := t.TempDir()
	testutil.GitInit(t, dir)
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster-apps.yaml", testutil.KustomizationYAML("cluster-apps", "flux-system", "  path: ./kubernetes/apps/test\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	// Bare apps/test → its network/ subdir is a kustomize base. echo is listed
	// and rendered; tunnel is commented out (a disabled app).
	testutil.WriteFile(t, dir, "kubernetes/apps/test/network/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: network
resources:
  - ./echo.yaml
  # - ./tunnel.yaml
`)
	testutil.WriteFile(t, dir, "kubernetes/apps/test/network/echo.yaml", testutil.KustomizationYAML("echo", "network", "  path: ./kubernetes/apps/base/echo\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	testutil.WriteFile(t, dir, "kubernetes/apps/test/network/tunnel.yaml", testutil.KustomizationYAML("tunnel", "network", "  path: ./kubernetes/apps/base/tunnel\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	testutil.WriteFile(t, dir, "kubernetes/apps/base/echo/kustomization.yaml", "resources:\n  - ./cm.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/base/echo/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: echo-cm}\ndata: {k: v}\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/base/tunnel/kustomization.yaml", "resources:\n  - ./cm.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/base/tunnel/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: tunnel-cm}\ndata: {k: v}\n")

	o, err := New(t.Context(), Config{Path: dir + "/kubernetes/flux", WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := o.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// echo is listed → reconciled.
	echoID := testutil.KustomizationID("network", "echo")
	if _, ok := o.Store().GetStatus(echoID); !ok {
		t.Errorf("listed KS echo should be reconciled")
	}
	// tunnel is commented out → never discovered or reconciled.
	tunnelID := testutil.KustomizationID("network", "tunnel")
	if o.Store().GetObject(tunnelID) != nil {
		t.Errorf("commented-out KS tunnel must not reach the store")
	}
	if _, ok := o.Store().GetStatus(tunnelID); ok {
		t.Errorf("commented-out KS tunnel must not be reconciled")
	}
}

// TestOrchestrator_ExplicitRepoRootNoGit proves Config.RepoRoot replaces the
// .git-ancestor walk: an extracted tree with NO .git, the scan entry point at a
// subdir (kubernetes/flux/cluster), and a repo-root-relative KS spec.path
// (./kubernetes/apps) renders correctly when RepoRoot is set — the spec.path
// resolves against RepoRoot, not the collapsed entry-point subdir. Without
// RepoRoot and no repository, discovery falls back to the subdir, ./kubernetes/apps
// doubles into a nonexistent path, and nothing renders. This is konflate's
// clusterPath bug; the explicit anchor is what fixes it.
func TestOrchestrator_ExplicitRepoRootNoGit(t *testing.T) {
	writeCluster := func(t *testing.T, dir string) {
		t.Helper()
		testutil.WriteFile(t, dir, "kubernetes/flux/cluster/ks.yaml", testutil.KustomizationYAML("cluster-apps", "flux-system", "  path: ./kubernetes/apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
		testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
		testutil.WriteFile(t, dir, "kubernetes/apps/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo, namespace: default}\ndata: {k: v}\n")
	}
	clusterApps := testutil.KustomizationID("flux-system", "cluster-apps")
	rendersDemo := func(t *testing.T, cfg Config) bool {
		t.Helper()
		o, err := New(t.Context(), cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		res, err := o.Render(context.Background())
		if res == nil {
			t.Fatalf("Render returned nil Result: %v", err)
		}
		for _, m := range res.Manifests[clusterApps] {
			if m["kind"] != "ConfigMap" {
				continue
			}
			if meta, _ := m["metadata"].(map[string]any); meta != nil && meta["name"] == "demo" {
				return true
			}
		}
		return false
	}

	t.Run("no anchor renders nothing", func(t *testing.T) {
		dir := t.TempDir()
		writeCluster(t, dir)
		if rendersDemo(t, Config{Path: dir + "/kubernetes/flux/cluster", WipeSecrets: true, Concurrency: 4}) {
			t.Error("no RepoRoot + no .git: spec.path doubles; cluster-apps should render nothing")
		}
	})
}

// The bjw-s/onedr0p self-substitute deadlock, in FULL mode: cluster-apps'
// spec.path is a BARE dir whose flux-system group base pulls in a component
// defining a namespace-less cluster-settings ConfigMap, which cluster-apps
// then lists in postBuild.substituteFrom. That CM is produced only by
// cluster-apps' own render, so a hard dependency edge would dead-lock it
// against itself ("ConfigMap/flux-system/cluster-settings: dependency not
// found"). The graph-aware self-production index must drop the self-edge so
// cluster-apps renders and emits the CM.
func TestOrchestrator_SelfSubstituteBareDirNoDeadlock(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster-apps.yaml", testutil.KustomizationYAML("cluster-apps", "flux-system", `  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substituteFrom:
      - kind: ConfigMap
        name: cluster-settings
`))
	testutil.WriteFile(t, dir, "kubernetes/apps/flux-system/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\ncomponents:\n  - ../../components/substitutions\n")
	testutil.WriteFile(t, dir, "kubernetes/components/substitutions/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources:\n  - ./cluster-settings.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/components/substitutions/cluster-settings.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cluster-settings\ndata:\n  CLUSTER_NAME: home\n")

	o, err := New(t.Context(), Config{Path: dir, WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	// The context timeout bounds a regressed self-deadlock.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := o.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ksID := testutil.KustomizationID("flux-system", "cluster-apps")
	if info, ok := o.Store().GetStatus(ksID); !ok || info.Status != store.StatusReady {
		t.Fatalf("cluster-apps status = (%+v, %v), want Ready (self-substitute deadlock regressed?)", info, ok)
	}
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}
	if got := o.Store().GetObject(cmID); got == nil {
		t.Errorf("cluster-apps render did not emit %s", cmID)
	}
}

// The guard: a substituteFrom ConfigMap that NO KS self-produces (and no
// producer emits) must keep its hard dependency edge and fail loudly. The
// graph-aware self-edge drop must not turn a genuinely-absent CM into a
// silent pass. Same shape as above but the group base pulls in no
// cluster-settings, so nothing produces ConfigMap/flux-system/cluster-settings.
func TestOrchestrator_SelfSubstituteAbsentCMFailsLoud(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster-apps.yaml", testutil.KustomizationYAML("cluster-apps", "flux-system", `  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substituteFrom:
      - kind: ConfigMap
        name: cluster-settings
`))
	testutil.WriteFile(t, dir, "kubernetes/apps/flux-system/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources:\n  - ./cm.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/flux-system/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: other\ndata:\n  k: v\n")

	o, err := New(t.Context(), Config{Path: dir, WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = o.Run(ctx) // reconcile failure is recorded in status, not returned
	ksID := testutil.KustomizationID("flux-system", "cluster-apps")
	info, ok := o.Store().GetStatus(ksID)
	if !ok || info.Status != store.StatusFailed {
		t.Fatalf("cluster-apps status = (%+v, %v), want Failed (absent substituteFrom CM must fail loud)", info, ok)
	}
	if !strings.Contains(info.Message, "cluster-settings") {
		t.Errorf("cluster-apps failure message = %q, want it to mention cluster-settings", info.Message)
	}
}

func TestRS_EmissionGrouping(t *testing.T) {
	t.Parallel()

	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			t.Parallel()

			s := store.New()
			parent := testutil.KustomizationID("flux-system", "parent")
			a := &manifest.ResourceSet{Name: "a", Namespace: "apps"}
			b := &manifest.ResourceSet{Name: "b", Namespace: "apps"}
			s.AddObject(a)
			s.AddObject(b)
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{a.Named(): {Parent: parent}, b.Named(): {Parent: parent}}}
			o := &Orchestrator{store: s, index: index}
			doc := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "shared", "namespace": "apps"}, "data": map[string]any{"value": "equal"}}
			children, docs := base.ParseChildren(parent, []map[string]any{doc}, manifest.ParseDocOptions{})
			ids := []manifest.NamedResource{a.Named(), b.Named()}
			if reverse {
				slices.Reverse(ids)
			}
			for _, id := range ids {
				s.RecordEmitted(id, &store.Emission{Children: children, Manifests: docs, Complete: true})
			}
			assert.Diff(t, o.collectManifests(), map[manifest.NamedResource][]map[string]any{parent: docs})
			second := manifest.DeepCopyMap(doc)
			second["metadata"].(map[string]any)["name"] = "second"
			for _, ordered := range [][]map[string]any{{doc, second}, {second, doc}} {
				entries, authoritative := base.ParseChildren(b.Named(), ordered, manifest.ParseDocOptions{})
				s.RecordEmitted(b.Named(), &store.Emission{Children: entries, Manifests: authoritative, Complete: true})
				if got := o.collectManifests()[parent]; len(got) != 2 || resourceset.DedupKey(got[0]) != resourceset.DedupKey(ordered[0]) {
					t.Fatalf("authoritative document order lost: %v", got)
				}
			}
			unequal := manifest.DeepCopyMap(doc)
			unequal["data"] = map[string]any{"value": "different"}
			changed, changedDocs := base.ParseChildren(parent, []map[string]any{unequal}, manifest.ParseDocOptions{})
			for _, id := range ids {
				if id == a.Named() {
					s.RecordEmitted(id, &store.Emission{Children: children, Manifests: docs, Complete: true})
				} else {
					s.RecordEmitted(id, &store.Emission{Children: changed, Manifests: changedDocs, Complete: true})
				}
			}
			assert.Diff(t, o.collectManifests(), map[manifest.NamedResource][]map[string]any{parent: changedDocs})
		})
	}
}

func TestOrchestrator_Render(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n"))
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: hello
data: {k: v}
`)

	o, err := New(t.Context(), Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if res == nil {
		t.Fatal("Render returned nil Result")
	}
	ksID := testutil.KustomizationID("flux-system", "apps")
	mans, ok := res.Manifests[ksID]
	if !ok {
		t.Fatalf("Result.Manifests missing %s; keys=%v", ksID, keysOf(res.Manifests))
	}
	if len(mans) == 0 {
		t.Errorf("expected rendered docs for %s; got empty", ksID)
	}
	if len(res.Failed) != 0 {
		t.Errorf("expected no failures; got %v", res.Failed)
	}
	if len(res.Orphans) != 0 {
		t.Errorf("expected no orphans; got %v", res.Orphans)
	}
	again, err := o.Render(t.Context())
	if err != nil {
		t.Fatalf("Render (2nd): %v", err)
	}
	if again != res {
		t.Fatal("second Render changed Result identity")
	}
}

func TestOrchestrator_TypedNullErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ field, diagnostic string }{
		{"patches", "must specify one of patch and path"},
		{"components", "new root cannot be empty"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			t.Parallel()
			dir := testutil.WriteTree(t, map[string]string{
				"ks.yaml":                 testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  "+tc.field+": [null]\n"),
				"apps/kustomization.yaml": "resources: [cm.yaml]\n",
				"apps/cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\n",
			})
			o, err := New(t.Context(), Config{Path: dir, RepoRoot: dir, CacheDir: t.TempDir(), Concurrency: 2})
			assert.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, o.Stop()) })
			res, err := o.Render(t.Context())
			if _, ok := errors.AsType[*FailuresError](err); !ok {
				t.Fatalf("render error = %v, want resource failure", err)
			}
			if !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("missing upstream diagnostic: %v", err)
			}
			if res == nil {
				t.Fatal("missing render result")
			}
			id := testutil.KustomizationID("flux-system", "apps")
			if info, ok := res.Failed[id]; !ok || !strings.Contains(info.Message, tc.diagnostic) {
				t.Fatalf("Kustomization failure = %+v, present = %t", info, ok)
			}
		})
	}
}

func TestOrchestrator_Render_RSExtensionAttributedToParentKS(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", `namespace: flux-system
resources: [rsip.yaml]
generatorOptions:
  disableNameSuffixHash: true
configMapGenerator:
  - name: acl
    literals: [seed=value]
patches:
  - target: {kind: ConfigMap, name: acl}
    patch: |
      - op: replace
        path: /apiVersion
        value: fluxcd.controlplane.io/v1
      - op: replace
        path: /kind
        value: ResourceSet
      - op: remove
        path: /data
      - op: add
        path: /spec
        value:
          inputsFrom:
            - apiVersion: fluxcd.controlplane.io/v1
              kind: ResourceSetInputProvider
              selector:
                matchLabels: {role: db}
          resourcesTemplate: |
            apiVersion: external-secrets.io/v1
            kind: ExternalSecret
            metadata: {name: acl, namespace: flux-system}
            spec:
              target: {name: acl}
`)
	testutil.WriteFile(t, dir, "apps/rsip.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata:
  name: rsip
  namespace: flux-system
  labels: {role: db}
spec:
  type: Static
  defaultValues: {user: alice}
`)

	o, err := New(t.Context(), Config{Path: dir, WipeSecrets: true, Concurrency: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	rsID := manifest.NamedResource{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSet, Namespace: "flux-system", Name: "acl"}
	if _, ok := o.index.Lookup(rsID); ok {
		t.Fatal("fixture ResourceSet must only exist in render output")
	}
	res, err := o.Render(t.Context())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	ksID := testutil.KustomizationID("flux-system", "apps")
	if owners := testutil.Owners(o.Store().Productions(rsID, nil)); !slices.Contains(owners, ksID) {
		t.Fatalf("missing independent contributor: %v", owners)
	}
	mans := res.Manifests[ksID]
	var found bool
	for _, doc := range mans {
		md, _ := doc["metadata"].(map[string]any)
		name, _ := md["name"].(string)
		kind, _ := doc["kind"].(string)
		if kind == "ExternalSecret" && name == "acl" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected ExternalSecret/acl in res.Manifests[%s] (the RS-rendered child); kinds in stream: %v",
			ksID, kindsOf(mans))
	}
}

func kindsOf(docs []map[string]any) []string {
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		k, _ := d["kind"].(string)
		out = append(out, k)
	}
	return out
}

// TestOrchestrator_AllowMissingSecretsPropagates locks the #190 fix:
// when --allow-missing-secrets is on AND a source's auth Secret isn't
// in the offline tree, the source soft-skips (Ready+"skipped:") and
// the downstream KS that consumes it propagates the skip rather than
// failing with "source artifact not found". `flate test` then reports
// SKIPPED, not FAILED.
func TestOrchestrator_AllowMissingSecretsPropagates(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "oci.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: private-app
  namespace: default
spec:
  interval: 5m
  url: oci://example.invalid/private/app
  secretRef:
    name: ghcr-creds
`)
	testutil.WriteFile(t, dir, "ks.yaml", testutil.KustomizationYAML("private-app", "default", "  interval: 5m\n  path: ./\n  sourceRef:\n    kind: OCIRepository\n    name: private-app\n    namespace: default\n"))

	o, err := New(t.Context(), Config{
		Path:                dir,
		AllowMissingSecrets: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	ociID := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindOCIRepository, Namespace: "default", Name: "private-app"}
	ksID := testutil.KustomizationID("default", "private-app")

	if _, failed := res.Failed[ociID]; failed {
		t.Errorf("OCIRepository must not be in Failed when --allow-missing-secrets is on; got %v", res.Failed[ociID])
	}
	if _, failed := res.Failed[ksID]; failed {
		t.Errorf("dependent KS must propagate skip, not fail; got %v", res.Failed[ksID])
	}

	ociInfo, ok := o.Store().GetStatus(ociID)
	if !ok || !store.IsSkipped(ociInfo) {
		t.Errorf("OCIRepository should be Ready+skipped; got %+v", ociInfo)
	}
	ksInfo, ok := o.Store().GetStatus(ksID)
	if !ok || !store.IsSkipped(ksInfo) {
		t.Errorf("KS should be Ready+skipped; got %+v", ksInfo)
	}
}

// TestOrchestrator_AllowMissingSecretsHRSkipsBeforePull locks the
// silent-anonymous-pull regression the iter-17 edge-case review
// flagged: an HR with chartRef → OCIRepository whose auth Secret is
// missing MUST propagate the skip BEFORE helm.Template runs.
// Without the pre-check, the source-controller's soft-skip leaves
// no on-disk artifact but the helm client's locateOCIChart would
// still try an oras-pull against the registry — succeeding silently
// against a public mirror, or failing with an opaque registry error.
// Either way the user's "the auth is missing" signal is lost.
func TestOrchestrator_AllowMissingSecretsHRSkipsBeforePull(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "oci.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: app-template
  namespace: flux-system
spec:
  interval: 5m
  url: oci://example.invalid/private/chart
  secretRef:
    name: ghcr-creds
`)
	testutil.WriteFile(t, dir, "hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: demo
  namespace: default
spec:
  interval: 5m
  chartRef:
    kind: OCIRepository
    name: app-template
    namespace: flux-system
`)

	o, err := New(t.Context(), Config{
		Path:                dir,
		AllowMissingSecrets: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	hrID := testutil.HelmReleaseID("default", "demo")
	if _, failed := res.Failed[hrID]; failed {
		t.Errorf("HR must propagate skip before helm.Template runs; got %v", res.Failed[hrID])
	}
	hrInfo, ok := o.Store().GetStatus(hrID)
	if !ok || !store.IsSkipped(hrInfo) {
		t.Errorf("HR should be Ready+skipped (chart source skipped); got %+v. "+
			"If status is Failed with a registry-pull error, the pre-check is missing and "+
			"helm tried an anonymous pull — exactly the silent-downgrade the pre-check exists to prevent.",
			hrInfo)
	}
}

func keysOf[V any](m map[manifest.NamedResource]V) []manifest.NamedResource {
	out := make([]manifest.NamedResource, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestOrchestrator_ChangedOnlyKeepsSubstituteFromProducer is the
// end-to-end regression fence for issue #418. A leaf HelmRelease
// change under cluster-apps must NOT cause cluster-apps's reconcile
// to fail with "ConfigMap/flux-system/cluster-settings: dependency
// not found" because the producer Kustomization (cluster-vars) that
// renders the substituteFrom ConfigMap was skipped from the keep set.
//
// Wiring under test:
//   - cluster-apps (KS, flux-system) consumes ConfigMap/cluster-settings
//     via spec.postBuild.substituteFrom.
//   - cluster-vars (KS, flux-system) renders that CM through a kustomize
//     Component subtree at kubernetes/components/cluster-settings.
//   - cluster-apps's spec.path covers an ntfy subtree whose HR file is
//     the only changed file. The HR is suspended so no chart pull is
//     needed for this offline fixture.
//
// Expectations:
//  1. cluster-vars is in the filter keep set (ancestor-only — the
//     producer must reconcile but doesn't promote its other children).
//  2. Run does NOT fail with "dependency not found" for the CM.
//  3. The producer's artifact is materialized in the store.
//  4. The rendered ConfigMap is materialized in the store.
func TestOrchestrator_ChangedOnlyKeepsSubstituteFromProducer(t *testing.T) {
	dir := t.TempDir()

	testutil.WriteFile(t, dir, "kubernetes/flux/cluster-apps.yaml", testutil.KustomizationYAML("cluster-apps", "flux-system", `  interval: 10m
  path: ./kubernetes/apps
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
  postBuild:
    substituteFrom:
      - kind: ConfigMap
        name: cluster-settings
`))
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster-vars.yaml", testutil.KustomizationYAML("cluster-vars", "flux-system", "  interval: 10m\n  path: ./kubernetes/flux/vars\n  prune: true\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n"))
	testutil.WriteFile(t, dir, "kubernetes/flux/vars/kustomization.yaml", `namespace: flux-system
components:
  - ../../components/cluster-settings
`)
	testutil.WriteFile(t, dir, "kubernetes/components/cluster-settings/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
resources:
  - cluster-settings.yaml
`)
	testutil.WriteFile(t, dir, "kubernetes/components/cluster-settings/cluster-settings.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
data:
  CLUSTER_DOMAIN: example.test
`)
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", `resources:
  - communication/ntfy/ks.yaml
`)
	testutil.WriteFile(t, dir, "kubernetes/apps/communication/ntfy/ks.yaml", testutil.KustomizationYAML("ntfy", "communication", `  interval: 10m
  path: ./kubernetes/apps/communication/ntfy/app
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`))
	testutil.WriteFile(t, dir, "kubernetes/apps/communication/ntfy/app/kustomization.yaml", `resources:
  - helmrelease.yaml
`)
	testutil.WriteFile(t, dir, "kubernetes/apps/communication/ntfy/app/helmrelease.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: ntfy
  namespace: communication
spec:
  interval: 10m
  suspend: true
  chartRef:
    kind: OCIRepository
    name: ntfy
    namespace: flux-system
`)

	o, err := New(t.Context(), Config{
		Path:        dir,
		WipeSecrets: true,
		ExternalChanges: change.NewSet([]string{
			"kubernetes/apps/communication/ntfy/app/helmrelease.yaml",
		}),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	producerID := testutil.KustomizationID("flux-system", "cluster-vars")
	f := o.filter
	if f == nil {
		t.Fatalf("filter returned nil; ExternalChanges should have activated changed-only mode")
	}
	if !f.ShouldReconcile(producerID) {
		t.Fatalf("producer %s must be in keep set; keep=%v", producerID, f.KeepNames())
	}

	if err := o.Run(context.Background()); err != nil {
		if strings.Contains(err.Error(), "ConfigMap/flux-system/cluster-settings: dependency not found") {
			t.Fatalf("changed-only skipped unchanged substituteFrom producer: %v", err)
		}
		t.Fatalf("Run: %v", err)
	}

	if got := o.Store().GetArtifact(producerID); got == nil {
		t.Errorf("producer %s artifact missing after Run; producer was kept but never reconciled", producerID)
	}
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}
	if got := o.Store().GetObject(cmID); got == nil {
		t.Errorf("rendered %s missing after Run; producer reconcile did not materialize the substituteFrom CM", cmID)
	}
}

// TestOrchestrator_ChangedOnlyKeepsResourcesEscapeOwner reproduces #833
// end-to-end: a Flux Kustomization's overlay pulls a base in from OUTSIDE
// its own spec.path via a `resources:` escape, and the only edit is to a
// plain resource under that base. The prefix-claim index alone attributes
// the file to no owner; the SelfProduceIndex file attribution must reach
// the change filter through buildChangeFilter so the owning KS is kept.
// Exercises the real wiring (SelfProduceIndex.OwnersOfFile is nil-safe, so
// a nil selfProduce would silently disable the fallback and the unit tests
// with a fake FileOwnerLookup would still pass).
func TestOrchestrator_ChangedOnlyKeepsResourcesEscapeOwner(t *testing.T) {
	writeTree := func(dir, value string) {
		testutil.WriteFile(t, dir, "clusters/prod/flux-system/apps.yaml", testutil.KustomizationYAML("apps", "flux-system", "  interval: 10m\n  path: ./apps/prod\n  prune: true\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n"))
		testutil.WriteFile(t, dir, "apps/prod/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../base/postgres
`)
		testutil.WriteFile(t, dir, "apps/base/postgres/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cluster.yaml
`)
		testutil.WriteFile(t, dir, "apps/base/postgres/cluster.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres
  namespace: default
data:
  replicas: "`+value+`"
`)
	}
	dir, origDir := t.TempDir(), t.TempDir()
	writeTree(origDir, "1")
	writeTree(dir, "2")

	o, err := New(t.Context(), Config{Path: dir, PathOrig: origDir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := o.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	f := o.filter
	if f == nil {
		t.Fatal("expected non-nil change filter when PathOrig differs from Path")
	}
	apps := testutil.KustomizationID("flux-system", "apps")
	if !f.ShouldReconcile(apps) {
		t.Fatalf("KS reading the changed file via a resources: escape must be kept; keep=%v", f.KeepNames())
	}
}

// TestOrchestrator_BootstrapIsIdempotent locks the A.1 invariant:
// Bootstrap mutates orchestrator state (sourceFiles, parentOf,
// existence, depGraph, componentCache, filter). A second call must
// be a no-op so any caller (Render, embedders, test harnesses) that
// runs through Bootstrap twice doesn't replay discovery and double-
// mutate those indexes.
//
// Probes via three signals that differ pre/post fix:
//  1. Store object counts must not change. Discovery's AddObject is
//     idempotent today but this is the hard fence against future
//     accumulation logic.
//  2. The change.Filter pointer must be stable. Pre-fix,
//     buildChangeFilter ran twice and replaced o.filter with a brand-
//     new instance — the second filter dropped any runtime keep/refire configuration the
//     first pass wired into the store's listener set.
//  3. o.bootstrapped must already be true before the second call,
//     pinning the guard's read site.
func TestOrchestrator_BootstrapIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	origDir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n"))
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: hello
data: {k: v}
`)
	// Baseline with different content forces changed-only mode, which
	// makes buildChangeFilter actually construct a filter we can pin
	// across the two Bootstrap calls.
	testutil.WriteFile(t, origDir, "ks.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./other\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n"))

	o, err := New(t.Context(), Config{Path: dir, PathOrig: origDir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := o.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap (1st): %v", err)
	}
	if !o.bootstrapped {
		t.Fatal("orchestrator.bootstrapped must be true after first Bootstrap")
	}
	ksCount1 := len(o.Store().ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization))
	cmCount1 := len(o.Store().ListObjects("", manifest.KindConfigMap))
	filter1 := o.filter
	if filter1 == nil {
		t.Fatal("expected non-nil change filter when PathOrig differs from Path")
	}

	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap (2nd): %v", err)
	}
	ksCount2 := len(o.Store().ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization))
	cmCount2 := len(o.Store().ListObjects("", manifest.KindConfigMap))
	filter2 := o.filter

	if ksCount1 != ksCount2 {
		t.Errorf("Kustomization count differs across Bootstrap calls: %d -> %d", ksCount1, ksCount2)
	}
	if cmCount1 != cmCount2 {
		t.Errorf("ConfigMap count differs across Bootstrap calls: %d -> %d", cmCount1, cmCount2)
	}
	if filter1 != filter2 {
		t.Errorf("change filter pointer must be stable across Bootstrap calls; "+
			"got %p -> %p (pre-fix buildChangeFilter re-ran and constructed a fresh filter)",
			filter1, filter2)
	}
	// Sentinel: the apps KS must be present after both passes — never
	// duplicated, never dropped. The Store is keyed by NamedResource
	// so a true duplicate would be a no-op; this check pins that the
	// canonical id is reachable post second-Bootstrap.
	appsID := testutil.KustomizationID("flux-system", "apps")
	if got := o.Store().GetObject(appsID); got == nil {
		t.Errorf("apps KS missing after second Bootstrap")
	}
}

func writeDigestTrackingRelease(t *testing.T, root, rel, text string) {
	t.Helper()
	testutil.WriteFile(t, root, rel, `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: flux-instance, namespace: flux-system}
spec:
  suspend: true
  chartRef: {kind: OCIRepository, name: fixture}
  values:
    instance:
      kustomize:
        patches:
          - target: {name: helm-controller}
            patch: "--feature-gates=DisableChartDigestTracking=`+text+`"
`)
}

func writeDigestTrackingTree(t *testing.T, root string) {
	t.Helper()
	testutil.WriteFile(t, root, "flux/ks.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild:\n    substitute: {GATE: \"true\"}\n"))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [hr.yaml]\n")
}

func TestOrchestrator_BootstrapChartDigestTracking(t *testing.T) {
	for _, covered := range []bool{false, true} {
		for _, override := range []struct {
			name  string
			value *bool
		}{
			{"auto", nil}, {"enabled", new(false)}, {"disabled", new(true)},
		} {
			for _, gate := range []bool{false, true} {
				t.Run(fmt.Sprintf("covered_%t/%s/gate_%t", covered, override.name, gate), func(t *testing.T) {
					root := t.TempDir()
					writeDigestTrackingTree(t, root)
					rel := "apps/hr.yaml"
					if !covered {
						rel = "flux/hr.yaml"
						testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: []\n")
					}
					writeDigestTrackingRelease(t, root, rel, strconv.FormatBool(gate))
					o, err := New(t.Context(), Config{Path: filepath.Join(root, "flux"), RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2,
						DisableChartDigestTracking: override.value})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := o.Stop(); err != nil {
							t.Errorf("Stop: %v", err)
						}
					})
					if err := o.Bootstrap(t.Context()); err != nil {
						t.Fatal(err)
					}
					want := gate
					if override.value != nil {
						want = *override.value
						assert.Equal(t, o.cfg.DisableChartDigestTracking == override.value, false)
						assert.Equal(t, *o.cfg.DisableChartDigestTracking, *override.value)
					} else {
						assert.Equal(t, o.cfg.DisableChartDigestTracking == nil, true)
					}
					assert.Equal(t, o.hrc.Options.DisableChartDigestTracking, want)
					id := testutil.HelmReleaseID("flux-system", "flux-instance")
					assert.Equal(t, o.store.GetObject(id) == nil, covered)
					_, indexed := o.index.Lookup(id)
					assert.Equal(t, indexed, true)
					writeDigestTrackingRelease(t, root, rel, strconv.FormatBool(!gate))
					if err := o.Bootstrap(t.Context()); err != nil {
						t.Fatal(err)
					}
					assert.Equal(t, o.hrc.Options.DisableChartDigestTracking, want)
				})
			}
		}
	}
}

func TestOrchestrator_BootstrapChartDigestTrackingFailureRetry(t *testing.T) {
	for _, stage := range []string{"discovery", "change filter"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			writeDigestTrackingTree(t, root)
			writeDigestTrackingRelease(t, root, "apps/hr.yaml", "true")
			cfg := Config{Path: filepath.Join(root, "flux"), RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2}
			if stage == "discovery" {
				if err := os.Remove(filepath.Join(root, "apps/kustomization.yaml")); err != nil {
					t.Fatal(err)
				}
				file := filepath.Join(root, "apps/z-broken.yaml")
				testutil.WriteFile(t, root, "apps/z-broken.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unreadable}\n")
				if err := os.Chmod(file, 0); err != nil {
					t.Skipf("cannot make file unreadable: %v", err)
				}
				if _, err := os.ReadFile(file); err == nil {
					t.Skip("OS or current user permits reading a file with no permissions")
				}
			} else {
				cfg.PathOrig = filepath.Join(root, "baseline-loop")
				if err := os.Symlink("baseline-loop", cfg.PathOrig); err != nil {
					t.Fatal(err)
				}
			}
			o, err := New(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := o.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			})
			firstErr := o.Bootstrap(t.Context())
			if firstErr == nil {
				t.Fatal("expected Bootstrap failure")
			}
			owner, client, trees := o.cache, o.hrc.Helm, o.ksc.Trees
			if owner == nil || client == nil || trees == nil {
				t.Fatal("Bootstrap failure discarded owner or renderer wiring")
			}
			assert.Equal(t, o.bootstrapped, false)
			assert.Equal(t, o.cfg.DisableChartDigestTracking == nil, true)
			assert.Equal(t, o.hrc.Options.DisableChartDigestTracking, false)
			writeDigestTrackingRelease(t, root, "apps/hr.yaml", "false")
			if stage == "discovery" {
				if err := os.Remove(filepath.Join(root, "apps/z-broken.yaml")); err != nil {
					t.Fatal(err)
				}
			} else {
				cfg.PathOrig = ""
			}
			if got := o.Bootstrap(t.Context()); !errors.Is(got, firstErr) {
				t.Fatalf("terminal Bootstrap error changed: first=%v next=%v", firstErr, got)
			}
			if o.cache != owner || o.hrc.Helm != client || o.ksc.Trees != trees || o.bootstrapped {
				t.Fatal("failed Bootstrap reopened its owner or rediscovered input")
			}
			if _, err := trees.FetchRemote(t.Context(), "://closed", sourcecache.RetryConfig{}, nil); !errors.Is(err, fs.ErrClosed) {
				t.Fatalf("failed Bootstrap left TreeCache open: %v", err)
			}
			fresh, err := New(t.Context(), cfg)
			assert.NoError(t, err)
			t.Cleanup(func() {
				if err := fresh.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			})
			assert.NoError(t, fresh.Bootstrap(t.Context()))
			assert.Equal(t, fresh.bootstrapped, true)
			assert.Equal(t, fresh.cfg.DisableChartDigestTracking == nil, true)
			assert.Equal(t, fresh.hrc.Options.DisableChartDigestTracking, false)
		})
	}
}

func TestOrchestrator_ChartDigestTrackingRenderedBoundary(t *testing.T) {
	for _, producer := range []string{"file", "kustomization", "resourceset"} {
		t.Run(producer, func(t *testing.T) {
			root := t.TempDir()
			writeDigestTrackingTree(t, root)
			switch producer {
			case "file":
				writeDigestTrackingRelease(t, root, "apps/hr.yaml", "true")
			case "kustomization":
				writeDigestTrackingRelease(t, root, "apps/hr.yaml", "${GATE}")
			default:
				testutil.WriteFile(t, root, "apps/hr.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: instance, namespace: flux-system}
spec:
  inputs: [{gate: "true"}]
  resourcesTemplate: |
    apiVersion: helm.toolkit.fluxcd.io/v2
    kind: HelmRelease
    metadata: {name: flux-instance, namespace: flux-system}
    spec:
      suspend: true
      chartRef: {kind: OCIRepository, name: fixture}
      values:
        instance:
          kustomize:
            patches:
              - patch: "DisableChartDigestTracking=<< inputs.gate >>"
`)
			}
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: proof}\ndata: {value: stable}\n")
			var previous []byte
			for _, resources := range []string{"[hr.yaml, cm.yaml]", "[cm.yaml, hr.yaml]"} {
				testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: "+resources+"\n")
				o, err := New(t.Context(), Config{Path: filepath.Join(root, "flux"), RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := o.Stop(); err != nil {
						t.Errorf("Stop: %v", err)
					}
				})
				if err := o.Bootstrap(t.Context()); err != nil {
					t.Fatal(err)
				}
				assert.Equal(t, o.hrc.Options.DisableChartDigestTracking, producer == "file")
				res, err := o.Render(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				id := testutil.HelmReleaseID("flux-system", "flux-instance")
				hr, ok := o.store.Get[*manifest.HelmRelease](id)
				if !ok {
					t.Fatal("qualifying rendered release missing")
				}
				instance := hr.Values["instance"].(map[string]any)
				patches := instance["kustomize"].(map[string]any)["patches"].([]any)
				assert.Equal(t, strings.Contains(patches[0].(map[string]any)["patch"].(string), "DisableChartDigestTracking=true"), true)
				assert.Equal(t, o.cfg.DisableChartDigestTracking == nil, true)
				assert.Equal(t, o.hrc.Options.DisableChartDigestTracking, producer == "file")
				ks := testutil.KustomizationID("flux-system", "apps")
				docs := slices.Clone(res.Manifests[ks])
				slices.SortFunc(docs, func(a, b map[string]any) int {
					return strings.Compare(a["kind"].(string), b["kind"].(string))
				})
				output, err := json.Marshal(docs)
				if err != nil {
					t.Fatal(err)
				}
				if previous != nil {
					assert.Equal(t, string(output), string(previous))
				}
				previous = output
				if err := o.Stop(); err != nil {
					t.Fatalf("Stop: %v", err)
				}
			}
		})
	}
}

func TestRenderTrees_ChartDigestTrackingIndependent(t *testing.T) {
	baseRoot, headRoot := t.TempDir(), t.TempDir()
	for _, root := range []string{baseRoot, headRoot} {
		writeDigestTrackingTree(t, root)
	}
	writeDigestTrackingRelease(t, baseRoot, "apps/hr.yaml", "false")
	writeDigestTrackingRelease(t, headRoot, "apps/hr.yaml", "true")
	base, head, err := RenderTrees(t.Context(),
		Tree{Path: filepath.Join(baseRoot, "flux"), RepoRoot: baseRoot},
		Tree{Path: filepath.Join(headRoot, "flux"), RepoRoot: headRoot},
		Config{CacheDir: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, base.hrc.Options.DisableChartDigestTracking, false)
	assert.Equal(t, head.hrc.Options.DisableChartDigestTracking, true)
}

func TestOrchestrator_ChartDigestTrackingCallerOwnership(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(disabled), func(t *testing.T) {
			root := t.TempDir()
			writeDigestTrackingTree(t, root)
			writeDigestTrackingRelease(t, root, "apps/hr.yaml", strconv.FormatBool(!disabled))
			requested := disabled
			cfg := Config{Path: filepath.Join(root, "flux"), RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2,
				DisableChartDigestTracking: &requested}
			first, err := New(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := first.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			})
			second, err := New(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := second.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			})
			requested = !disabled
			for _, o := range []*Orchestrator{first, second} {
				if err := o.Bootstrap(t.Context()); err != nil {
					t.Fatal(err)
				}
				assert.Equal(t, o.hrc.Options.DisableChartDigestTracking, disabled)
				assert.Equal(t, requested, !disabled)
			}
			assert.Equal(t, first.cfg.DisableChartDigestTracking == second.cfg.DisableChartDigestTracking, false)
		})
	}
}

func TestOrchestrator_BootstrapChartDigestTrackingInputOrder(t *testing.T) {
	for _, gates := range [][]string{{"true", "false"}, {"false", "true"}} {
		t.Run(strings.Join(gates, "/"), func(t *testing.T) {
			root := t.TempDir()
			writeDigestTrackingTree(t, root)
			writeDigestTrackingRelease(t, root, "flux/a.yaml", gates[0])
			writeDigestTrackingRelease(t, root, "flux/z.yaml", gates[1])
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: []\n")
			o, err := New(t.Context(), Config{Path: filepath.Join(root, "flux"), RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := o.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			})
			if err := o.Bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, o.hrc.Options.DisableChartDigestTracking, true)
		})
	}
}

func TestRenderTrees_InjectedCacheRemainsCallerOwned(t *testing.T) {
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c, err := sourcecache.New(t.Context(), root)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			calls := 0
			key := sourcecache.BlobKey{Content: "blob-owner-test"}
			produce := func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				calls++
				_, err := io.WriteString(w, "held")
				return sourcecache.ProducerResult{}, err
			}
			reader, _, _, err := c.Blob(t.Context(), key, sourcecache.Policy{MaxAge: time.Hour}, produce)
			if err != nil {
				t.Fatal(err)
			}
			_ = reader.Close()
			baseDir, headDir := t.TempDir(), t.TempDir()
			for _, dir := range []string{baseDir, headDir} {
				writeSubstitutionCluster(t, dir, "values", "value")
				testutil.WriteFile(t, dir, "values.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: apps}\ndata: {VALUE: stable}\n")
			}
			configuredRoot := t.TempDir()
			base, head, err := RenderTrees(t.Context(), Tree{RepoRoot: baseDir}, Tree{RepoRoot: headDir},
				Config{Cache: c, CacheDir: configuredRoot, Concurrency: 2})
			if err != nil {
				t.Fatal(err)
			}
			if base.cache != c || head.cache != c || base.hrc.Helm == nil || head.hrc.Helm == nil || base.ksc.Trees == nil || head.ksc.Trees == nil {
				t.Fatal("diff sides did not wire both renderers through the selected owner")
			}
			entries, err := os.ReadDir(configuredRoot)
			if err != nil || len(entries) != 0 {
				t.Fatalf("diff sides created independent cache backing: entries=%v err=%v", entries, err)
			}
			reader, _, _, err = c.Blob(t.Context(), key, sourcecache.Policy{MaxAge: time.Hour}, produce)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil || string(data) != "held" || calls != 1 {
				t.Fatalf("caller cache after both sides stop: data=%q calls=%d err=%v", data, calls, err)
			}
			assert.NoError(t, os.Remove(filepath.Join(baseDir, "app/cm.yaml")))
			base, head, err = RenderTrees(t.Context(), Tree{RepoRoot: baseDir}, Tree{RepoRoot: headDir}, Config{Cache: c, Concurrency: 2})
			if _, ok := errors.AsType[*FailuresError](err); !ok || base.Result == nil || len(base.Result.Failed) == 0 || head.Result == nil || len(head.Result.Manifests) == 0 || head.Err != nil {
				t.Fatalf("resource failure canceled healthy sibling: base=%+v head=%+v err=%v", base, head, err)
			}
			for _, side := range []Rendered{base, head} {
				if _, err := side.ksc.Trees.FetchRemote(t.Context(), "://closed", sourcecache.RetryConfig{}, nil); !errors.Is(err, fs.ErrClosed) {
					t.Fatalf("partial side left TreeCache open: %v", err)
				}
			}
			reader, _, _, err = c.Blob(t.Context(), key, sourcecache.Policy{MaxAge: time.Hour}, nil)
			assert.NoError(t, err)
			assert.NoError(t, reader.Close())

		})
	}
}

func TestRenderTrees_NetworkRunIsolation(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			c := cachetest.New(t, "", sourcecache.New)
			root := t.TempDir()
			testutil.WriteFile(t, root, "cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\n")
			tree := Tree{RepoRoot: root}
			cfg := Config{Cache: c, Concurrency: workers}
			base, head, err := RenderTrees(t.Context(), tree, tree, cfg)
			if err != nil {
				t.Fatal(err)
			}
			network := base.cfg.Network
			if network == nil || head.cfg.Network != network {
				t.Fatal("diff sides did not share one Network")
			}
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits.Add(1); _, _ = io.WriteString(w, "new run") }))
			t.Cleanup(srv.Close)
			var failing atomic.Bool
			failing.Store(true)
			testutil.StubDNS(t, func(context.Context, string) error {
				if failing.Load() {
					return os.ErrDeadlineExceeded
				}
				return nil
			})
			target := strings.Replace(srv.URL, "127.0.0.1", "example.com", 1)
			tr, err := network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
			assert.NoError(t, err)
			req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
			_, err = (&http.Client{Transport: tr}).Do(req)
			terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
			if !ok || !terminal.Attempted {
				t.Fatalf("seed: %v", err)
			}
			cause := terminal.Err
			failing.Store(false)
			for i, side := range []Rendered{base, head} {
				if _, err := side.ksc.Trees.FetchRemote(t.Context(), target, sourcecache.RetryConfig{}, nil); !errors.Is(err, fs.ErrClosed) {
					t.Fatalf("returned side %d is not terminal: %v", i, err)
				}
				trees := kustomize.NewTreeCache(c, side.cfg.Network, task.NewBounded(2))
				t.Cleanup(trees.Close)
				_, err := trees.FetchRemote(t.Context(), fmt.Sprintf("%s/%d", target, i), sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour}, nil)
				terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
				if !ok || terminal.Attempted || !errors.Is(err, cause) {
					t.Fatalf("side %d: %v", i, err)
				}
			}
			if hits.Load() != 0 {
				t.Fatalf("suppressed requests=%d", hits.Load())
			}
			next, other, err := RenderTrees(t.Context(), tree, tree, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if next.cfg.Network == network || other.cfg.Network != next.cfg.Network {
				t.Fatal("Network leaked across invocations sharing Cache")
			}
			for _, side := range []Rendered{next, other} {
				if _, err := side.ksc.Trees.FetchRemote(t.Context(), target, sourcecache.RetryConfig{}, nil); !errors.Is(err, fs.ErrClosed) {
					t.Fatalf("fresh returned side is not terminal: %v", err)
				}
			}
			trees := kustomize.NewTreeCache(c, next.cfg.Network, task.NewBounded(2))
			t.Cleanup(trees.Close)
			input, err := trees.FetchRemote(t.Context(), target+"/0", sourcecache.RetryConfig{}, nil)
			var body []byte
			if err == nil {
				body, err = fs.ReadFile(input, "content")
			}
			if err != nil || string(body) != "new run" || hits.Load() != 1 {
				t.Fatalf("fresh run: %q %v hits=%d", body, err, hits.Load())
			}
		})
	}
}

func TestOrchestrator_StopErrorPropagation(t *testing.T) {
	for _, boundary := range []string{"stop", "bootstrap-error", "bootstrap-cancel", "run", "run-error", "run-cancel", "render", "render-error"} {
		t.Run(boundary, func(t *testing.T) {
			owner, err := sourcecache.New(t.Context(), t.TempDir())
			assert.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, owner.Close()) })
			root := t.TempDir()
			writeSubstitutionCluster(t, root, "values", "value")
			testutil.WriteFile(t, root, "healthy.yaml", substitutionKS("healthy", "healthy", ""))
			testutil.WriteFile(t, root, "healthy/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "healthy/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: partial}\n")
			testutil.WriteFile(t, root, "values.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: apps}\ndata: {VALUE: stable}\n")
			o, err := New(t.Context(), Config{Path: root, Cache: owner, Concurrency: 2})
			assert.NoError(t, err)
			cleanupErr := errors.New("backing close failed")
			o.stopErr = cleanupErr
			t.Cleanup(func() {
				if got := o.Stop(); !errors.Is(got, cleanupErr) {
					t.Errorf("cached Stop error = %v", got)
				}
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var result *Result
			switch boundary {
			case "stop":
				err = o.Stop()
			case "bootstrap-error", "bootstrap-cancel":
				if boundary == "bootstrap-error" {
					o.cfg.Input = &store.SourceArtifact{}
				} else {
					cancel()
				}
				err = o.Bootstrap(ctx)
				if next := o.Bootstrap(t.Context()); !errors.Is(next, err) {
					t.Fatalf("Bootstrap lost terminal error identity: %v", next)
				}
				nextResult, next := o.Render(t.Context())
				if !errors.Is(next, err) {
					t.Fatalf("Render changed Bootstrap error: %v", next)
				}
				if nextResult != nil {
					t.Fatalf("Render returned non-nil Result on Bootstrap error: %+v", nextResult)
				}
				cause := manifest.ErrInput
				if boundary == "bootstrap-cancel" {
					cause = context.Canceled
				}
				if !errors.Is(err, cause) {
					t.Fatalf("Bootstrap operation error lost: %v", err)
				}
			default:
				if strings.HasSuffix(boundary, "error") {
					assert.NoError(t, os.Remove(filepath.Join(root, "app/cm.yaml")))
				}
				assert.NoError(t, o.Bootstrap(ctx))
				if strings.HasPrefix(boundary, "run") {
					if boundary == "run-cancel" {
						cancel()
					}
					err = o.Run(ctx)
				} else {
					result, err = o.Render(ctx)
					next, nextErr := o.Render(t.Context())
					if result == nil || len(result.Manifests) == 0 || next != result || !errors.Is(nextErr, err) {
						t.Fatalf("Render lost cached partial result/error: %v %v", result, err)
					}
				}
				if strings.HasSuffix(boundary, "error") {
					if _, ok := errors.AsType[*FailuresError](err); !ok || strings.Index(err.Error(), "backing close failed") == 0 {
						t.Fatalf("operation must precede cleanup: %v", err)
					}
				}
				if boundary == "run-cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("Run cancellation lost: %v", err)
				}
			}
			if !errors.Is(err, cleanupErr) || strings.Count(err.Error(), cleanupErr.Error()) != 1 {
				t.Fatalf("cleanup must occur once: %v", err)
			}
			stops := make(chan error, 2)
			for range 2 {
				go func() { stops <- o.Stop() }()
			}
			for range 2 {
				if got := <-stops; !errors.Is(got, cleanupErr) {
					t.Fatalf("Stop changed cached error: %v", got)
				}
			}
			if _, err := o.ksc.Trees.FetchRemote(t.Context(), "://closed", sourcecache.RetryConfig{}, nil); !errors.Is(err, fs.ErrClosed) {
				t.Fatalf("TreeCache not terminal: %v", err)
			}
			if result != nil && len(o.Store().ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)) == 0 {
				t.Fatal("Stop discarded readable store")
			}
		})
	}
	t.Run("owned-backing", func(t *testing.T) {
		o, err := New(t.Context(), Config{Path: t.TempDir(), CacheDir: t.TempDir(), Concurrency: 2})
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, o.Stop()) })
		assert.NoError(t, o.Stop())
		assert.NoError(t, o.Stop())
		res, err := sourcecache.Sweep(t.Context(), o.cfg.CacheDir, sourcecache.SweepOpts{})
		if err != nil || len(res.Errors) != 0 {
			t.Fatalf("owned backing not released after cleanup: %+v, %v", res, err)
		}
	})
}

func TestOrchestrator_RunJoinsRemoteWorkers(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, backing := range []string{"memory", "disk"} {
			t.Run(fmt.Sprintf("%d/%s", workers, backing), func(t *testing.T) {
				root := ""
				if backing == "disk" {
					root = t.TempDir()
				}
				o, err := New(t.Context(), Config{Path: t.TempDir(), CacheDir: root, Concurrency: workers})
				assert.NoError(t, err)
				entered, completed := make(chan struct{}), make(chan struct{})
				srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
					close(entered)
					<-r.Context().Done()
					close(completed)
				}))
				t.Cleanup(srv.Close)
				t.Cleanup(func() { assert.NoError(t, o.Stop()) })
				callers := make(chan error, workers)
				for range workers {
					go func() {
						_, err := o.ksc.Trees.FetchRemote(t.Context(), srv.URL, sourcecache.RetryConfig{}, nil)
						callers <- err
					}()
				}
				<-entered
				result, err := o.Render(t.Context())
				assert.NoError(t, err)
				if result == nil {
					t.Fatal("cleanup discarded successful result")
				}
				for range workers {
					if err := <-callers; !errors.Is(err, context.Canceled) && !errors.Is(err, fs.ErrClosed) {
						t.Fatalf("remote caller after Stop: %v", err)
					}
				}
				<-completed
				if _, err := o.ksc.Trees.FetchRemote(t.Context(), srv.URL, sourcecache.RetryConfig{}, nil); !errors.Is(err, fs.ErrClosed) {
					t.Fatalf("run left TreeCache live: %v", err)
				}
				if root != "" {
					res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
					if err != nil || len(res.Errors) != 0 {
						t.Fatalf("run retained backing: %+v %v", res, err)
					}
				}
			})
		}
	}
}

func TestRSRawSink_DeterministicCollisionWinner(t *testing.T) {
	t.Parallel()

	parent := testutil.KustomizationID("flux-system", "apps")
	ownerA := manifest.NamedResource{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSet, Namespace: "flux-system", Name: "rs-a"}
	ownerB := manifest.NamedResource{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSet, Namespace: "flux-system", Name: "rs-b"}
	mkDoc := func(owner string) map[string]any {
		return map[string]any{
			"apiVersion": "example.com/v1",
			"kind":       "Widget",
			"metadata":   map[string]any{"name": "shared", "namespace": "default"},
			"spec":       map[string]any{"owner": owner},
		}
	}
	docA, docB := mkDoc("a-loses"), mkDoc("b-wins")
	other := mkDoc("other-group")
	other["apiVersion"] = "other.example/v1"
	version := mkDoc("version-loses")
	version["apiVersion"] = "example.com/v2"

	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			a := &manifest.ResourceSet{Name: ownerA.Name, Namespace: ownerA.Namespace}
			b := &manifest.ResourceSet{Name: ownerB.Name, Namespace: ownerB.Namespace}
			s.AddObjects([]manifest.BaseManifest{a, b})
			o := &Orchestrator{store: s, index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{ownerA: {Parent: parent}, ownerB: {Parent: parent}}}}
			ids := []manifest.NamedResource{ownerA, ownerB}
			if reverse {
				slices.Reverse(ids)
			}
			for _, id := range ids {
				input := []map[string]any{docB, other}
				if id == ownerA {
					input = []map[string]any{docA, version, {"apiVersion": "example.com/v1", "kind": "Widget"}, {"apiVersion": "example.com/v1", "metadata": map[string]any{"name": "incomplete"}}}
				}
				children, authoritative := base.ParseChildren(id, input, manifest.ParseDocOptions{})
				s.RecordEmitted(id, &store.Emission{Children: children, Manifests: authoritative, Complete: true})
			}
			docs := o.collectManifests()[parent]
			if len(docs) != 2 {
				t.Fatalf("out[apps] = %d docs, want 2 (one winner per group)", len(docs))
			}
			if manifest.DocAPIVersion(docs[1]) != "other.example/v1" {
				t.Fatalf("foreign group lost: %v", docs)
			}
			spec, _ := docs[0]["spec"].(map[string]any)
			if got := spec["owner"]; got != "b-wins" {
				t.Fatalf("collision winner owner = %v, want b-wins", got)
			}
		})
	}
}

func TestOrchestrator_SourceValuesReadersRenderEmitter(t *testing.T) {
	t.Parallel()
	for _, form := range []string{"inline", "chartRef"} {
		t.Run(form, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [ks.yaml]\n")
			testutil.WriteFile(t, root, "flux/ks.yaml", testutil.KustomizationYAML("demo", "flux-system",
				"  path: ./apps/demo\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
			resources := "resources: [hr.yaml, sibling.yaml]\n"
			spec := `  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
      valuesFiles: [./config/prod.yaml]
`
			if form == "chartRef" {
				resources = "resources: [hr.yaml, sibling.yaml, chart.yaml]\n"
				spec = "  chartRef: {kind: HelmChart, name: chart, namespace: flux-system}\n"
				testutil.WriteFile(t, root, "apps/demo/chart.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmChart
metadata: {name: chart, namespace: flux-system}
spec:
  chart: charts/demo
  sourceRef: {kind: GitRepository, name: flux-system}
  valuesFiles: [./config/prod.yaml]
`)
			}
			testutil.WriteFile(t, root, "apps/demo/kustomization.yaml", resources)
			testutil.WriteFile(t, root, "apps/demo/hr.yaml",
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec:\n  interval: 10m\n"+spec)
			testutil.WriteFile(t, root, "apps/demo/sibling.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: sibling, namespace: apps}
spec:
  chartRef: {kind: OCIRepository, name: unavailable}
`)
			testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
			testutil.WriteFile(t, root, "charts/demo/values.yaml", "greeting: default\n")
			testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: rendered, namespace: apps}
data: {greeting: {{ .Values.greeting | quote }}}
`)
			testutil.WriteFile(t, root, "config/prod.yaml", "greeting: production\n")
			o, err := New(t.Context(), Config{
				Path: filepath.Join(root, "flux"), RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2,
				ExternalChanges: change.NewSet([]string{"config/prod.yaml"}),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { assert.NoError(t, o.Stop()) })
			assert.NoError(t, o.Bootstrap(t.Context()))
			release := testutil.HelmReleaseID("apps", "demo")
			owner := testutil.KustomizationID("flux-system", "demo")
			assert.Equal(t, o.store.GetObject(release) == nil, true)
			assert.Equal(t, o.filter.ShouldReconcile(owner), true)
			result, err := o.Render(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, len(result.Failed), 0)
			assert.Equal(t, len(result.Blocked), 0)
			assert.Equal(t, o.store.GetArtifact(owner) != nil, true)
			rendered, ok := o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "rendered"})
			if !ok {
				t.Fatal("selected release did not render")
			}
			assert.Equal(t, rendered.Data["greeting"], "production")
			assert.Equal(t, o.filter.ShouldReconcile(testutil.HelmReleaseID("apps", "sibling")), false)
			assert.Equal(t, o.store.GetArtifact(testutil.HelmReleaseID("apps", "sibling")) == nil, true)
		})
	}
}

func TestOrchestrator_NetworkOwnership(t *testing.T) {
	for _, injected := range []bool{false, true} {
		for _, failure := range []bool{false, true} {
			t.Run(fmt.Sprintf("injected=%t/bootstrap_failure=%t", injected, failure), func(t *testing.T) {
				closed := make(chan struct{}, 2)
				var opened atomic.Int32
				srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
				srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
					if state == http.StateNew {
						opened.Add(1)
					}
					if state == http.StateClosed {
						closed <- struct{}{}
					}
				}
				srv.Start()
				t.Cleanup(srv.Close)
				cfg := Config{Path: t.TempDir(), Concurrency: 2}
				if injected {
					cfg.Network = new(source.Network)
					t.Cleanup(cfg.Network.Close)
				}
				o, err := New(t.Context(), cfg)
				assert.NoError(t, err)
				t.Cleanup(func() { assert.NoError(t, o.Stop()) })
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				tr, err := o.cfg.Network.HTTPTransport(source.HTTPTransportConfig{})
				assert.NoError(t, err)
				response, err := (&http.Client{Transport: tr}).Get(srv.URL)
				assert.NoError(t, err)
				_, err = io.Copy(io.Discard, response.Body)
				assert.NoError(t, errors.Join(err, response.Body.Close()))
				if failure {
					o.cfg.Input = &store.SourceArtifact{}
					if err := o.Bootstrap(ctx); err == nil {
						t.Fatal("bootstrap unexpectedly succeeded")
					}
				} else {
					assert.NoError(t, o.Stop())
				}
				if _, err := o.ksc.Trees.FetchRemote(ctx, srv.URL, sourcecache.RetryConfig{}, nil); !errors.Is(err, fs.ErrClosed) {
					t.Fatalf("workers not closed before network cleanup: %v", err)
				}
				if injected {
					select {
					case <-closed:
						t.Fatal("borrower closed injected network")
					default:
					}
					response, err := (&http.Client{Transport: tr}).Get(srv.URL)
					assert.NoError(t, err)
					_, err = io.Copy(io.Discard, response.Body)
					assert.NoError(t, errors.Join(err, response.Body.Close()))
					if opened.Load() != 1 {
						t.Fatal("borrower discarded the caller's reusable connection")
					}
					o.cfg.Network.Close()
				}
				select {
				case <-closed:
				case <-ctx.Done():
					t.Fatal("owned network retained idle connection")
				}
			})
		}
	}
}

func TestRenderTrees_NetworkOwnership(t *testing.T) {
	for _, injected := range []bool{false, true} {
		t.Run(fmt.Sprintf("injected=%t", injected), func(t *testing.T) {
			var opened atomic.Int32
			closed := make(chan struct{}, 2)
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\n")
			}))
			srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					opened.Add(1)
				}
				if state == http.StateClosed {
					closed <- struct{}{}
				}
			}
			srv.Start()
			t.Cleanup(srv.Close)
			cfg := Config{Concurrency: 2}
			if injected {
				cfg.Network = new(source.Network)
				t.Cleanup(cfg.Network.Close)
			}
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux.yaml", substitutionKS("consumer", "app", ""))
			testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: ["+srv.URL+"/cm.yaml]\n")
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			base, head, err := RenderTrees(ctx, Tree{RepoRoot: root}, Tree{RepoRoot: root}, cfg)
			assert.NoError(t, err)
			if base.cfg.Network != head.cfg.Network || base.ownNetwork || head.ownNetwork {
				t.Fatal("diff sides did not borrow the outer Network")
			}
			if opened.Load() == 0 {
				t.Fatal("renders made no request")
			}
			if injected {
				select {
				case <-closed:
					t.Fatal("diff side closed caller's Network")
				default:
				}
				cfg.Network.Close()
			}
			for range opened.Load() {
				select {
				case <-closed:
				case <-ctx.Done():
					t.Fatal("outer owner retained idle transport")
				}
			}
		})
	}
}
