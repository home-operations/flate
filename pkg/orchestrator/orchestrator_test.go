package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
	parent := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./kubernetes/apps"},
	}
	orphan := &manifest.Kustomization{
		Name: "orphan", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./kubernetes/apps/orphan/app"},
	}
	emittedChild := &manifest.Kustomization{
		Name: "wired", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./kubernetes/apps/wired/app"},
	}
	root := &manifest.Kustomization{
		Name: "another-root", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./standalone"},
	}

	o := &Orchestrator{
		store:    store.New(),
		rendered: newRenderedSet(),
		sourceFiles: map[manifest.NamedResource]string{
			parent.Named():       "kubernetes/flux/cluster/ks.yaml",
			orphan.Named():       "kubernetes/apps/orphan/ks.yaml",
			emittedChild.Named(): "kubernetes/apps/wired/ks.yaml",
			root.Named():         "kubernetes/standalone/ks.yaml",
		},
	}
	for _, ks := range []*manifest.Kustomization{parent, orphan, emittedChild, root} {
		o.store.AddObject(ks)
	}
	// Mark emittedChild as rendered by its parent — simulates the
	// AddObject + MarkRenderedBatch call cluster-apps's render would make.
	o.rendered.MarkRenderedBatch(parent.Named(), []manifest.NamedResource{emittedChild.Named()})

	failed := map[manifest.NamedResource]store.StatusInfo{
		orphan.Named():       {Status: store.StatusFailed, Message: "TIMEZONE undefined"},
		emittedChild.Named(): {Status: store.StatusFailed, Message: "TIMEZONE undefined"},
		root.Named():         {Status: store.StatusFailed, Message: "broken"},
	}

	orphans := o.detectOrphans(failed)

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
}

// TestDetectOrphans_NonReconcilableKindsIgnored — ConfigMaps and
// Secrets that fail (they can't fail in practice but the failure
// map is permissive) are never reported as orphans; orphan
// detection only applies to Kustomization + HelmRelease.
func TestDetectOrphans_NonReconcilableKindsIgnored(t *testing.T) {
	parent := &manifest.Kustomization{
		Name: "p", Namespace: "ns",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	cm := &manifest.ConfigMap{Name: "stuck", Namespace: "ns"}

	o := &Orchestrator{
		store: store.New(),
		sourceFiles: map[manifest.NamedResource]string{
			parent.Named(): "ks.yaml",
			cm.Named():     "apps/stuck/cm.yaml",
		},
	}
	o.store.AddObject(parent)
	o.store.AddObject(cm)

	failed := map[manifest.NamedResource]store.StatusInfo{
		cm.Named(): {Status: store.StatusFailed, Message: "bogus"},
	}
	orphans := o.detectOrphans(failed)
	if len(orphans) != 0 {
		t.Errorf("orphan detection should skip non-reconcilable kinds; got %+v", orphans)
	}
}

// TestOrchestrator_WithFetcherAfterBootstrapPanics pins the
// contract that WithFetcher MUST be called before Bootstrap. A
// late swap would silently miss any source CR discovery already
// reconciled, so we fail fast instead of pretending success.
func TestOrchestrator_WithFetcherAfterBootstrapPanics(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: []\n")

	o, err := New(Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic when WithFetcher is called after Bootstrap")
		}
		msg, _ := r.(string)
		if !strings.Contains(msg, "BEFORE Bootstrap") {
			t.Errorf("panic message should name the ordering contract; got: %v", r)
		}
	}()
	o.WithFetcher(manifest.KindOCIRepository, nil)
}

func TestOrchestrator_SimpleCluster(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: hello
data: {k: v}
`)

	o, err := New(Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if err := o.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := len(o.Store().ListObjects(manifest.KindKustomization)); got != 1 {
		t.Errorf("expected 1 Kustomization, got %d", got)
	}
	if got := len(o.Store().ListObjects(manifest.KindConfigMap)); got < 1 {
		t.Errorf("expected at least 1 ConfigMap after reconcile, got %d", got)
	}
}

func TestOrchestrator_DependsOnCanArriveFromRenderedKustomization(t *testing.T) {
	dir := t.TempDir()
	produced := `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: produced
  namespace: flux-system
spec:
  path: ./produced
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(produced))
	}))
	t.Cleanup(srv.Close)
	testutil.WriteFile(t, dir, "flux/producer.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: producer
  namespace: flux-system
spec:
  path: ./producer
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "flux/consumer.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: consumer
  namespace: flux-system
spec:
  path: ./consumer
  dependsOn:
    - name: produced
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "producer/kustomization.yaml", "resources:\n- "+srv.URL+"/produced.yaml\n")
	testutil.WriteFile(t, dir, "consumer/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "consumer/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: consumer}
data: {k: v}
`)
	testutil.WriteFile(t, dir, "produced/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "produced/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: produced}
data: {k: v}
`)

	o, err := New(Config{Path: dir, WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	consumerID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "consumer"}
	if msg, ok := o.preflightFailure(consumerID); ok {
		t.Fatalf("consumer should wait for rendered dependency, not fail preflight: %s", msg)
	}
	if err := o.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, name := range []string{"produced", "consumer"} {
		id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: name}
		info, ok := o.Store().GetStatus(id)
		if !ok || info.Status != store.StatusReady {
			t.Fatalf("%s status = (%+v, %v), want Ready", id, info, ok)
		}
	}
}

// TestOrchestrator_TwoKSSameSpecPathNoSpuriousTimeout reproduces the
// dual git-source redundancy pattern: two Kustomizations defined in the
// same file and pointing at the SAME spec.path. They are peers (neither
// renders the other), so they must not be wired as each other's
// structural parent — a mutual parent edge deadlocks the pair in
// collectDeps until the 30s per-dep timeout, cascading to anything that
// dependsOn one of them. The run context is bounded so a regression
// fails fast instead of riding the full DefaultTimeout.
func TestOrchestrator_TwoKSSameSpecPathNoSpuriousTimeout(t *testing.T) {
	dir := t.TempDir()
	// Two KSes, same spec.path ./flux, both defined in flux/repo.yaml so
	// both source files sit under their shared spec.path prefix — the
	// file-prefix parent index would otherwise make each the other's
	// structural parent and deadlock the pair. repo.yaml is intentionally
	// NOT listed in flux/kustomization.yaml (it's surfaced by the
	// bootstrap-sibling scan), so rendering ./flux does not re-emit the
	// peers — isolating the file-index parent edge this fix targets.
	testutil.WriteFile(t, dir, "flux/repo.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: config-a, namespace: flux-system}
spec:
  path: ./flux
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: config-b, namespace: flux-system}
spec:
  path: ./flux
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "flux/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: flux-cm}
data: {k: v}
`)
	// Downstream consumer dependsOn one of the peers — the cascade victim
	// if config-a deadlocks.
	testutil.WriteFile(t, dir, "consumer.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: flux-system}
spec:
  path: ./apps
  dependsOn:
    - name: config-a
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello}
data: {k: v}
`)

	o, err := New(Config{Path: dir, WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	// Bounded so a regressed mutual-parent deadlock fails in seconds, not
	// the full 30s depwait.DefaultTimeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := o.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, name := range []string{"config-a", "config-b", "consumer"} {
		id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: name}
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
// never deploys it. The entry is scoped to kubernetes/flux (with a .git marker
// so spec.paths resolve against the repo root) so cluster-apps is discovered
// before its subtree is scanned.
func TestOrchestrator_CommentedOutKSNotReconciled(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, ".git/HEAD", "ref: refs/heads/main\n") // repo-root anchor
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster-apps.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: cluster-apps, namespace: flux-system}
spec:
  path: ./kubernetes/apps/test
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	// Bare apps/test → its network/ subdir is a kustomize base. echo is listed
	// and rendered; tunnel is commented out (a disabled app).
	testutil.WriteFile(t, dir, "kubernetes/apps/test/network/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: network
resources:
  - ./echo.yaml
  # - ./tunnel.yaml
`)
	testutil.WriteFile(t, dir, "kubernetes/apps/test/network/echo.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: echo, namespace: network}
spec:
  path: ./kubernetes/apps/base/echo
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "kubernetes/apps/test/network/tunnel.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: tunnel, namespace: network}
spec:
  path: ./kubernetes/apps/base/tunnel
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "kubernetes/apps/base/echo/kustomization.yaml", "resources:\n  - ./cm.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/base/echo/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: echo-cm}\ndata: {k: v}\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/base/tunnel/kustomization.yaml", "resources:\n  - ./cm.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/base/tunnel/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: tunnel-cm}\ndata: {k: v}\n")

	o, err := New(Config{Path: dir + "/kubernetes/flux", WipeSecrets: true, Concurrency: 4})
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
	echoID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "network", Name: "echo"}
	if _, ok := o.Store().GetStatus(echoID); !ok {
		t.Errorf("listed KS echo should be reconciled")
	}
	// tunnel is commented out → never discovered or reconciled.
	tunnelID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "network", Name: "tunnel"}
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
// RepoRoot and no .git, FindRepoRoot falls back to the subdir, ./kubernetes/apps
// doubles into a nonexistent path, and nothing renders. This is konflate's
// clusterPath bug; the explicit anchor is what fixes it.
func TestOrchestrator_ExplicitRepoRootNoGit(t *testing.T) {
	writeCluster := func(t *testing.T, dir string) {
		testutil.WriteFile(t, dir, "kubernetes/flux/cluster/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: cluster-apps, namespace: flux-system}
spec:
  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
		testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
		testutil.WriteFile(t, dir, "kubernetes/apps/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo, namespace: default}\ndata: {k: v}\n")
	}
	clusterApps := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps"}
	rendersDemo := func(t *testing.T, cfg Config) bool {
		t.Helper()
		o, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		// A per-resource render failure (e.g. the doubled-path case below)
		// is advisory — Result stays non-nil. Gate on the rendered docs, not
		// on err.
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

	// Explicit RepoRoot: spec.path ./kubernetes/apps resolves against the root,
	// so the ConfigMap renders even though we scanned only the entry-point subdir
	// and there is no .git to walk up to.
	t.Run("explicit RepoRoot renders", func(t *testing.T) {
		dir := t.TempDir()
		writeCluster(t, dir)
		if !rendersDemo(t, Config{Path: dir + "/kubernetes/flux/cluster", RepoRoot: dir, WipeSecrets: true, Concurrency: 4}) {
			t.Error("explicit RepoRoot: ConfigMap demo must render (spec.path resolves against RepoRoot)")
		}
	})

	// No anchor (no RepoRoot, no .git): documents the failure mode the explicit
	// anchor fixes — ./kubernetes/apps doubles under the entry-point subdir, so
	// cluster-apps renders nothing.
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
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster-apps.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: cluster-apps, namespace: flux-system}
spec:
  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substituteFrom:
      - kind: ConfigMap
        name: cluster-settings
`)
	testutil.WriteFile(t, dir, "kubernetes/apps/flux-system/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\ncomponents:\n  - ../../components/substitutions\n")
	testutil.WriteFile(t, dir, "kubernetes/components/substitutions/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources:\n  - ./cluster-settings.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/components/substitutions/cluster-settings.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cluster-settings\ndata:\n  CLUSTER_NAME: home\n")

	o, err := New(Config{Path: dir, WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	// Bounded so a regressed self-deadlock fails fast, not at the full 30s cap.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := o.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ksID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps"}
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
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster-apps.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: cluster-apps, namespace: flux-system}
spec:
  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substituteFrom:
      - kind: ConfigMap
        name: cluster-settings
`)
	testutil.WriteFile(t, dir, "kubernetes/apps/flux-system/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources:\n  - ./cm.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/flux-system/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: other\ndata:\n  k: v\n")

	o, err := New(Config{Path: dir, WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = o.Run(ctx) // reconcile failure is recorded in status, not returned
	ksID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps"}
	info, ok := o.Store().GetStatus(ksID)
	if !ok || info.Status != store.StatusFailed {
		t.Fatalf("cluster-apps status = (%+v, %v), want Failed (absent substituteFrom CM must fail loud)", info, ok)
	}
	if !strings.Contains(info.Message, "cluster-settings") {
		t.Errorf("cluster-apps failure message = %q, want it to mention cluster-settings", info.Message)
	}
}

func TestOrchestrator_RunReturnsContextCancellation(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello}
data: {k: v}
`)

	o, err := New(Config{Path: dir, WipeSecrets: true, Concurrency: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := o.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run with pre-canceled context = %v, want context.Canceled", err)
	}
}

func TestExpandResourceSetsPostRun_ReturnsRenderError(t *testing.T) {
	parent := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	rs := &manifest.ResourceSet{
		Name: "broken", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata:
  name: << inputs.nonexistent >>
`,
		},
	}
	o := &Orchestrator{
		store:    store.New(),
		rendered: newRenderedSet(),
		sourceFiles: map[manifest.NamedResource]string{
			rs.Named(): "apps/resourceset.yaml",
		},
	}
	o.store.AddObject(parent)
	o.store.AddObject(rs)

	err := o.expandResourceSetsPostRun(context.Background())
	if err == nil {
		t.Fatal("expected ResourceSet render error")
	}
	if !strings.Contains(err.Error(), "ResourceSet/flux-system/broken") {
		t.Fatalf("error should identify ResourceSet: %v", err)
	}
	info, ok := o.store.GetStatus(rs.Named())
	if !ok || info.Status != store.StatusFailed {
		t.Fatalf("ResourceSet status = (%+v, %v), want Failed", info, ok)
	}
}

func TestExpandResourceSetsPostRun_RespectsCanceledContext(t *testing.T) {
	rs := &manifest.ResourceSet{Name: "apps", Namespace: "flux-system"}
	o := &Orchestrator{store: store.New()}
	o.store.AddObject(rs)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := o.expandResourceSetsPostRun(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expandResourceSetsPostRun canceled = %v, want context.Canceled", err)
	}
}

// TestExpandResourceSetsPostRun_DeterministicCollisionWinner pins that
// when two distinct ResourceSets emit the SAME object identity
// (apiVersion|kind|ns|name) with DIFFERENT content, the global first-
// wins dedup winner is deterministic — the ResourceSet that sorts first
// in store order wins, independent of goroutine scheduling. Before the
// serial-commit rework the winner was whichever render goroutine reached
// the shared map first (an 8-way errgroup), so output could flip
// run-to-run. A Widget (unmodeled kind) is used so ParseDoc yields a
// RawObject — the only shape expandResourceSetsPostRun re-emits.
func TestExpandResourceSetsPostRun_DeterministicCollisionWinner(t *testing.T) {
	parent := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	mkRS := func(name, owner string) *manifest.ResourceSet {
		return &manifest.ResourceSet{
			Name: name, Namespace: "flux-system",
			ResourceSetSpec: fluxopv1.ResourceSetSpec{
				ResourcesTemplate: "apiVersion: example.com/v1\nkind: Widget\nmetadata:\n  name: shared\n  namespace: default\nspec:\n  owner: " + owner + "\n",
			},
		}
	}
	// rs-a sorts before rs-b (same namespace, "rs-a" < "rs-b"), so the
	// deterministic winner of the shared Widget must always be rs-a's.
	rsA := mkRS("rs-a", "a-wins")
	rsB := mkRS("rs-b", "b-loses")

	newOrch := func() *Orchestrator {
		o := &Orchestrator{
			store:    store.New(),
			rendered: newRenderedSet(),
			sourceFiles: map[manifest.NamedResource]string{
				rsA.Named(): "apps/rs-a.yaml",
				rsB.Named(): "apps/rs-b.yaml",
			},
		}
		o.store.AddObject(parent)
		o.store.AddObject(rsA)
		o.store.AddObject(rsB)
		return o
	}

	// Many iterations (fresh orchestrator each) shake out any scheduling-
	// dependent winner; the contract is that rs-a always wins.
	for iter := range 50 {
		o := newOrch()
		if err := o.expandResourceSetsPostRun(context.Background()); err != nil {
			t.Fatalf("iter %d: expandResourceSetsPostRun: %v", iter, err)
		}
		docs := o.rsExtensions[parent.Named()]
		if len(docs) != 1 {
			t.Fatalf("iter %d: rsExtensions[apps] = %d docs, want 1 (deduped to one winner)", iter, len(docs))
		}
		spec, _ := docs[0]["spec"].(map[string]any)
		if got := spec["owner"]; got != "a-wins" {
			t.Fatalf("iter %d: collision winner owner = %v, want a-wins (sorted-first RS) — non-deterministic", iter, got)
		}
	}
}

// TestOrchestrator_Render exercises the embed-friendly Render() entry:
// one call drives Bootstrap + Run + result collection, and returns a
// structured Result keyed by NamedResource. Embedders previously had
// to scrape o.Store().GetArtifact(id) per-kind after Run.
func TestOrchestrator_Render(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: hello
data: {k: v}
`)

	o, err := New(Config{Path: dir, WipeSecrets: true})
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
	ksID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
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
}

// TestOrchestrator_Render_RSExtensionAttributedToParentKS verifies
// the post-Run ResourceSet expansion lands non-Flux RS children
// (ExternalSecret, ConfigMap-as-data, etc.) in the manifest stream
// of the structural-parent Kustomization. Without this, dragonfly-
// acls-style RSes that emit ExternalSecrets from kustomize-substituted
// RSIPs (the tholinka/home-ops `dragonfly-${APP}` pattern) would
// silently drop their output — flate would render zero docs for the
// RS even though the in-cluster controller would produce one.
func TestOrchestrator_Render_RSExtensionAttributedToParentKS(t *testing.T) {
	dir := t.TempDir()
	// Parent KS at the repo root scans ./apps.
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", `resources:
- rs.yaml
- rsip.yaml
`)
	// RS with a selector-based inputsFrom emitting a RawObject
	// (ExternalSecret) — the kind flate doesn't track natively.
	testutil.WriteFile(t, dir, "apps/rs.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: acl, namespace: flux-system}
spec:
  inputsFrom:
    - apiVersion: fluxcd.controlplane.io/v1
      kind: ResourceSetInputProvider
      selector:
        matchLabels: {role: db}
  resourcesTemplate: |
    ---
    apiVersion: external-secrets.io/v1
    kind: ExternalSecret
    metadata:
      name: acl
      namespace: flux-system
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

	o, err := New(Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	ksID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
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

// TestOrchestrator_Render_AppliesSkipKinds locks the iter-16
// embed-facing follow-on to #169: HelmOptions.SkipResourceKinds()
// applies uniformly to BOTH HR and KS docs in Result.Manifests, not
// just to the CLI emit paths. Without this, an embedder calling
// orchestrator.New + Render would see HR-rendered Secrets dropped
// but KS-rendered Secrets retained — the same asymmetry the CLI
// fix patched at a different layer.
func TestOrchestrator_Render_AppliesSkipKinds(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"resources:\n- cm.yaml\n- secret.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: kept-cm, namespace: default}
data: {k: v}
`)
	testutil.WriteFile(t, dir, "apps/secret.yaml", `apiVersion: v1
kind: Secret
metadata: {name: dropped-secret, namespace: default}
stringData: {password: hunter2}
`)

	o, err := New(Config{
		Path:        dir,
		WipeSecrets: true,
		HelmOptions: helm.Options{SkipSecrets: true},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	ksID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	mans := res.Manifests[ksID]
	if len(mans) == 0 {
		t.Fatalf("Result.Manifests[apps] should retain non-Secret docs; got empty")
	}
	for _, doc := range mans {
		if manifest.DocKind(doc) == manifest.KindSecret {
			t.Errorf("Result.Manifests should not include Secret docs when SkipSecrets=true; got %v", doc)
		}
	}
	// ConfigMap must survive — sanity that the filter only drops Secret.
	var sawCM bool
	for _, doc := range mans {
		if manifest.DocKind(doc) == manifest.KindConfigMap {
			sawCM = true
		}
	}
	if !sawCM {
		t.Errorf("ConfigMap should remain in Result.Manifests; mans=%v", mans)
	}
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
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: private-app
  namespace: default
spec:
  interval: 5m
  path: ./
  sourceRef:
    kind: OCIRepository
    name: private-app
    namespace: default
`)

	o, err := New(Config{
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

	ociID := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "default", Name: "private-app"}
	ksID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "default", Name: "private-app"}

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
// missing MUST propagate the skip BEFORE helm.TemplateDocs runs.
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

	o, err := New(Config{
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

	hrID := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "default", Name: "demo"}
	if _, failed := res.Failed[hrID]; failed {
		t.Errorf("HR must propagate skip before helm.TemplateDocs runs; got %v", res.Failed[hrID])
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

// TestOrchestrator_TypedListener verifies Store.OnStatus delivers a
// typed StatusInfo payload (no `any` type-switching needed by the
// embedder).
func TestOrchestrator_TypedListener(t *testing.T) {
	s := store.New()
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "k"}

	var seen store.StatusInfo
	unsub := s.OnStatus(func(other manifest.NamedResource, info store.StatusInfo) {
		if other == id {
			seen = info
		}
	}, false)
	defer unsub()

	s.UpdateStatus(id, store.StatusFailed, "boom")
	if seen.Status != store.StatusFailed || seen.Message != "boom" {
		t.Errorf("typed listener did not receive payload: %+v", seen)
	}
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

	testutil.WriteFile(t, dir, "kubernetes/flux/cluster-apps.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster-apps
  namespace: flux-system
spec:
  interval: 10m
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
`)
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster-vars.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster-vars
  namespace: flux-system
spec:
  interval: 10m
  path: ./kubernetes/flux/vars
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
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
	testutil.WriteFile(t, dir, "kubernetes/apps/communication/ntfy/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: ntfy
  namespace: communication
spec:
  interval: 10m
  path: ./kubernetes/apps/communication/ntfy/app
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
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

	o, err := New(Config{
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

	producerID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-vars"}
	f := o.Filter()
	if f == nil {
		t.Fatalf("Filter() returned nil; ExternalChanges should have activated changed-only mode")
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
//     new instance — the second filter dropped any OnAdd hook the
//     first pass wired into the store's listener set.
//  3. o.bootstrapped must already be true before the second call,
//     pinning the guard's read site.
func TestOrchestrator_BootstrapIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	origDir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
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
	testutil.WriteFile(t, origDir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./other
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)

	o, err := New(Config{Path: dir, PathOrig: origDir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(o.Stop)

	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap (1st): %v", err)
	}
	if !o.bootstrapped {
		t.Fatal("orchestrator.bootstrapped must be true after first Bootstrap")
	}
	ksCount1 := len(o.Store().ListObjects(manifest.KindKustomization))
	cmCount1 := len(o.Store().ListObjects(manifest.KindConfigMap))
	filter1 := o.Filter()
	if filter1 == nil {
		t.Fatal("expected non-nil change filter when PathOrig differs from Path")
	}

	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap (2nd): %v", err)
	}
	ksCount2 := len(o.Store().ListObjects(manifest.KindKustomization))
	cmCount2 := len(o.Store().ListObjects(manifest.KindConfigMap))
	filter2 := o.Filter()

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
	appsID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	if got := o.Store().GetObject(appsID); got == nil {
		t.Errorf("apps KS missing after second Bootstrap")
	}
}

// TestOrchestrator_RenderCalledTwiceProducesIdenticalArtifacts pins
// the embedder contract: a second Render on the same orchestrator
// returns the same Result.Manifests. Render itself is sync.Once-
// guarded since the controllers' Configure hooks panic if invoked
// after Start; the cache returns the first call's Result/err pair.
func TestOrchestrator_RenderCalledTwiceProducesIdenticalArtifacts(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: hello
data: {k: v}
`)

	o, err := New(Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(o.Stop)

	res1, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render (1st): %v", err)
	}
	res2, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render (2nd): %v", err)
	}

	if len(res1.Manifests) != len(res2.Manifests) {
		t.Fatalf("Render.Manifests key count differs: %d -> %d", len(res1.Manifests), len(res2.Manifests))
	}
	for id, mans1 := range res1.Manifests {
		mans2, ok := res2.Manifests[id]
		if !ok {
			t.Errorf("second Render dropped key %s", id)
			continue
		}
		if len(mans1) != len(mans2) {
			t.Errorf("Render.Manifests[%s] doc count differs: %d -> %d", id, len(mans1), len(mans2))
			continue
		}
		for i := range mans1 {
			k1, _ := mans1[i]["kind"].(string)
			k2, _ := mans2[i]["kind"].(string)
			md1, _ := mans1[i]["metadata"].(map[string]any)
			md2, _ := mans2[i]["metadata"].(map[string]any)
			n1, _ := md1["name"].(string)
			n2, _ := md2["name"].(string)
			if k1 != k2 || n1 != n2 {
				t.Errorf("Render.Manifests[%s][%d] differs: (%s/%s) -> (%s/%s)",
					id, i, k1, n1, k2, n2)
			}
		}
	}
	if len(res1.Failed) != len(res2.Failed) {
		t.Errorf("Render.Failed count differs: %d -> %d", len(res1.Failed), len(res2.Failed))
	}
	if len(res1.Orphans) != len(res2.Orphans) {
		t.Errorf("Render.Orphans count differs: %d -> %d", len(res1.Orphans), len(res2.Orphans))
	}
}

// TestOrchestrator_RenderErrorPathStopsCleanly pins the A.5 invariant:
// when Render returns (nil, err) the orchestrator's Stop has already
// fired, so the staging cache + helm client + controller listeners
// don't survive in memory until process exit.
//
// Detects the bug by probing o.stopOnce: if Render fired Stop, our
// follow-up stopOnce.Do is a no-op (sync.Once already triggered).
// Pre-fix the deferred Stop didn't exist, so the probe would still
// run and flip `probeFired` to true — flagging the leak.
func TestOrchestrator_RenderErrorPathStopsCleanly(t *testing.T) {
	// Non-existent path forces Bootstrap -> discovery.Run -> os.Stat
	// to fail, which is the canonical (nil, err) embed path.
	o, err := New(Config{Path: "/nonexistent/orchestrator-render-cleanup", WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	res, err := o.Render(context.Background())
	if err == nil {
		t.Fatal("expected Render error for non-existent path")
	}
	if res != nil {
		t.Errorf("Render returned non-nil Result on Bootstrap error: %+v", res)
	}

	// Probe: if Render's deferred Stop fired, stopOnce is already
	// triggered and this Do is a no-op. Pre-fix the deferred Stop
	// didn't exist, the orchestrator's staging cache + helm client
	// would survive in memory, and probeFired would flip true.
	probeFired := false
	o.stopOnce.Do(func() { probeFired = true })
	if probeFired {
		t.Fatal("Render error path did not fire Stop; staging cache + helm client leaked")
	}

	// And a separate Stop call must still be a true no-op — the
	// sync.Once guard composes safely with the deferred Stop above
	// and any explicit caller Stop. No panic from double-Close.
	o.Stop()
}
