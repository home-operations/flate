package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log" //nolint:depguard // Restoring slog.SetDefault also requires restoring the standard logger.
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type countedDetailError struct{ calls int }

func (e *countedDetailError) Error() string {
	e.calls++
	return "upstream diagnostic"
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDiscover_FailureDetailsEvaluation(t *testing.T) {
	for _, level := range []slog.Level{slog.LevelInfo, slog.LevelDebug} {
		t.Run(level.String(), func(t *testing.T) {
			var logs bytes.Buffer
			prior, writer, flags := slog.Default(), log.Writer(), log.Flags()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: level})))
			t.Cleanup(func() {
				slog.SetDefault(prior)
				log.SetOutput(writer)
				log.SetFlags(flags)
			})
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: []\n")
			o, err := newTestOrchestrator(
				t,
				Tree{Path: root},
				Config{Concurrency: 2, Selection: manifest.Selection{Changed: true}},
			)
			assert.NoError(t, err)
			assert.NoError(t, o.prepareBootstrap(t.Context()))
			detail := new(countedDetailError)
			failure := manifest.NewError(manifest.ErrInput, detail, "entry failed")
			result, err := o.session.Freeze(t.Context())
			assert.NoError(t, err)
			result.Binding.Failure = failure
			assert.NoError(t, o.discover(t.Context()))
			assert.Equal(t, o.entryBinding.Failure, error(failure))
			assert.Equal(t, detail.calls > 0, level == slog.LevelDebug)
			assert.Equal(t, strings.Contains(logs.String(), "orchestrator: entry failed"), level == slog.LevelDebug)
			assert.Equal(t, strings.Contains(logs.String(), "upstream diagnostic"), level == slog.LevelDebug)
		})
	}
}

func TestRenderTrees_MissingPath(t *testing.T) {
	t.Parallel()
	runs, err := RenderTrees(
		t.Context(),
		[]Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		Config{},
	)
	{
		assert.FatalIf(t, len(runs) != 1 || runs[0].Orchestrator != nil || !errors.Is(err, manifest.ErrInput) ||
			!errors.Is(err, manifest.ErrFlux), "missing path identity: runs=%v err=%v", runs, err)
	}
	{
		assert.FatalIf(t, !strings.Contains(err.Error(), "tree path is required"), "missing path diagnostic: %v", err)
	}
}

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
	o.store.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	// Mark emittedChild as rendered by its parent — simulates the
	// AddObject + RecordEmitted call cluster-apps's render would make.
	publishFixture(t, o.store, parent.Named(), []manifest.NamedResource{emittedChild.Named()})

	failed := map[manifest.NamedResource]store.StatusInfo{
		orphan.Named():       {Status: store.StatusFailed, Message: "TIMEZONE undefined"},
		emittedChild.Named(): {Status: store.StatusFailed, Message: "TIMEZONE undefined"},
		root.Named():         {Status: store.StatusFailed, Message: "broken"},
	}

	orphans := o.detectOrphans(failed, o.reportOwnership(o.reportingClaims()))

	{
		{
			_, ok := orphans[orphan.Named()]
			assert.ErrorIf(t, !ok, "expected orphan to be detected")
		}
	}
	{
		{
			_, ok := orphans[emittedChild.Named()]
			assert.ErrorIf(t, ok, "re-emitted child is not an orphan: parent's render covered it")
		}
	}
	{
		{
			_, ok := orphans[root.Named()]
			assert.ErrorIf(t, ok, "root resource with no covering parent is not an orphan")
		}
	}
	{
		{
			got := len(orphans)
			assert.ErrorIf(t, got != 1, "expected exactly 1 orphan, got %d: %+v", got, orphans)
		}
	}
	o.store.AddObject(&manifest.GitRepository{Name: "external", Namespace: "flux-system"})
	o.store.AddObject(&manifest.Kustomization{
		Name:      "external",
		Namespace: "flux-system",
		Path:      "./",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      "external",
			Namespace: "flux-system",
		},
	})
	assert.Diff(t, o.detectOrphans(failed, o.reportOwnership(o.reportingClaims())), orphans)
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
	foreign := &manifest.RawObject{
		APIVersion: "example.test/v1",
		Kind:       manifest.KindKustomization,
		Name:       cm.Name,
		Namespace:  cm.Namespace,
	}

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
	orphans := o.detectOrphans(failed, o.reportOwnership(o.reportingClaims()))
	{
		assert.ErrorIf(t, len(orphans) != 0, "orphan detection should skip non-reconcilable kinds; got %+v", orphans)
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
	orphans := o.detectOrphans(failed, o.reportOwnership(o.reportingClaims()))
	{
		{
			_, ok := orphans[child.Named()]
			assert.ErrorIf(
				t,
				ok,
				"child under a FAILED covering parent is a cascade victim, not an orphan: %+v",
				orphans,
			)
		}
	}
	{
		assert.ErrorIf(t, len(orphans) != 0, "expected no orphans when the covering parent failed, got %+v", orphans)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_StopBeforeBootstrapCompletion(t *testing.T) {
	owner, err := sourcecache.New(t.Context(), "")
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, owner.Close()) })
	o, err := newTestOrchestrator(t, Tree{Path: t.TempDir()}, Config{Cache: owner, Concurrency: 2})
	assert.NoError(t, err)
	entered, completed := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(completed)
	}))
	t.Cleanup(server.Close)
	done := make(chan error, 1)
	go func() {
		_, localErr := o.inputs.Acquire(t.Context(), server.URL, sourcecache.RetryConfig{}, nil)
		done <- localErr
	}()
	<-entered
	o.stop()
	o.stop()
	{
		{
			innerLocalErr := <-done
			assert.FatalIf(
				t,
				!errors.Is(innerLocalErr, context.Canceled),
				"local cleanup did not cancel/join input: %v",
				innerLocalErr,
			)
		}
	}
	<-completed
	_, _, _, err = owner.Blob(t.Context(), sourcecache.BlobKey{Content: "after-tree-close"}, sourcecache.Policy{},
		func(_ context.Context, input sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			_, innerInnerLocalErr := io.WriteString(input, "caller-owned")
			return sourcecache.ProducerResult{}, innerInnerLocalErr
		})
	assert.NoError(t, err)
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_SimpleCluster(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"ks.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n",
		),
	)
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
	o, err := newTestOrchestrator(t, Tree{Path: dir}, Config{WipeSecrets: true, Concurrency: 2})
	{
		assert.FatalIf(t, err != nil, "New: %v", err)
	}
	t.Cleanup(func() {
		o.stop()
	})
	{
		{
			err := o.bootstrap(t.Context())
			assert.FatalIf(t, err != nil, "Bootstrap: %v", err)
		}
	}
	{
		{
			err := o.run(t.Context())
			assert.FatalIf(t, err != nil, "Run: %v", err)
		}
	}

	{
		{
			got := len(o.Store().ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization))
			assert.ErrorIf(t, got != 1, "expected one discovered Kustomization, got %d", got)
		}
	}
	{
		{
			got := len(o.Store().ListObjects("", manifest.KindConfigMap))
			assert.ErrorIf(t, got < 1, "expected at least 1 ConfigMap after reconcile, got %d", got)
		}
	}
	{
		{
			entries, err := os.ReadDir(fallback)
			assert.FatalIf(
				t,
				err != nil || len(entries) != 0,
				"empty SDK CacheDir wrote cache artifacts: %v, %v",
				entries,
				err,
			)
		}
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_TwoKSSameSpecPathNoSpuriousTimeout(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "peers/kustomization.yaml", "resources: [../flux/repo.yaml]\n")
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
	testutil.WriteFile(
		t,
		dir,
		"consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"flux-system",
			"  path: ./apps\n  dependsOn:\n    - name: config-a\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello}
data: {k: v}
`)

	o, err := newTestOrchestrator(t, Tree{Path: dir}, Config{WipeSecrets: true, Concurrency: 4})
	{
		assert.FatalIf(t, err != nil, "New: %v", err)
	}
	{
		{
			err := o.bootstrap(context.Background())
			assert.FatalIf(t, err != nil, "Bootstrap: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	{
		{
			err := o.run(ctx)
			assert.FatalIf(t, err != nil, "Run: %v", err)
		}
	}
	for _, name := range []string{"config-a", "config-b", "consumer"} {
		id := testutil.KustomizationID("flux-system", name)
		info, ok := o.Store().GetStatus(id)
		{
			assert.FatalIf(
				t,
				!ok || info.Status != store.StatusReady,
				"%s status = (%+v, %v), want Ready (mutual-parent deadlock regressed?)",
				id,
				info,
				ok,
			)
		}
	}
}

func TestOrchestrator_CommentedOutKSNotReconciled(t *testing.T) {
	t.Parallel()
	for _, entry := range []string{".", "kubernetes/flux"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%t", entry, enabled), func(t *testing.T) {
				t.Parallel()
				testOrchestratorCommentedOutKSNotReconciledCase(
					t,
					testOrchestratorCommentedOutKSNotReconciledCaseInputs{entry: &entry, enabled: &enabled},
				)
			},
			)
		}
	}
}

// TestOrchestrator_ExplicitRepoRootNoGit proves Tree.RepoRoot replaces the
// .git-ancestor walk: an extracted tree with NO .git, the scan entry point at a
// subdir (kubernetes/flux/cluster), and a repo-root-relative KS spec.path
// (./kubernetes/apps) renders correctly when RepoRoot is set — the spec.path
// resolves against RepoRoot, not the collapsed entry-point subdir. Without
// RepoRoot and no repository, discovery falls back to the subdir, ./kubernetes/apps
// doubles into a nonexistent path, and nothing renders. This is konflate's
// clusterPath bug; the explicit anchor is what fixes it.
func TestOrchestrator_ExplicitRepoRootNoGit(t *testing.T) {
	t.Parallel()

	writeCluster := func(t *testing.T, dir string) {
		t.Helper()
		testutil.WriteFile(
			t,
			dir,
			"kubernetes/flux/cluster/ks.yaml",
			testutil.KustomizationYAML(
				"cluster-apps",
				"flux-system",
				"  path: ./kubernetes/apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		)
		testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
		testutil.WriteFile(t, dir, "kubernetes/apps/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo, namespace: default}\ndata: {k: v}\n")
	}
	clusterApps := testutil.KustomizationID("flux-system", "cluster-apps")
	rendersDemo := func(t *testing.T, cfg treeConfig) bool {
		t.Helper()
		renderedSingleton2, err := RenderTrees(
			context.Background(),
			[]Tree{
				{
					RepoRoot:        cfg.RepoRoot,
					Path:            cfg.Path,
					SelfURLs:        cfg.SelfURLs,
					Input:           cfg.Input,
					BaseInput:       cfg.BaseInput,
					ExternalChanges: cfg.ExternalChanges,
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				},
			},
			cfg.Config,
		)
		{
			assert.FatalIf(t, len(renderedSingleton2) != 1, "missing render: %v", err)
		}
		res := renderedSingleton2[0].Result
		{
			assert.FatalIf(t, res == nil, "Render returned nil Result: %v", err)
		}
		for _, m := range outputByParent(res.Manifests)[clusterApps] {
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
		t.Parallel()

		dir := t.TempDir()
		writeCluster(t, dir)
		{
			assert.ErrorIf(
				t,
				rendersDemo(t, treeConfig{WipeSecrets: true, Concurrency: 4, Path: dir + "/kubernetes/flux/cluster"}),
				"%v",
				"no RepoRoot + no .git: spec.path doubles; cluster-apps should render nothing",
			)
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
//
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_SelfSubstituteBareDirNoDeadlock(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [kubernetes/flux/cluster-apps.yaml]\n")
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/flux/cluster-apps.yaml",
		testutil.KustomizationYAML("cluster-apps", "flux-system", `  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substituteFrom:
      - kind: ConfigMap
        name: cluster-settings
`),
	)
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/apps/flux-system/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\ncomponents:\n  - ../../components/substitutions\n",
	)
	testutil.WriteFile(t, dir, "kubernetes/components/substitutions/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources:\n  - ./cluster-settings.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/components/substitutions/cluster-settings.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cluster-settings\ndata:\n  CLUSTER_NAME: home\n")

	o, err := newTestOrchestrator(t, Tree{Path: dir}, Config{WipeSecrets: true, Concurrency: 4})
	{
		assert.FatalIf(t, err != nil, "New: %v", err)
	}
	{
		{
			err := o.bootstrap(context.Background())
			assert.FatalIf(t, err != nil, "Bootstrap: %v", err)
		}
	}
	// The context timeout bounds a regressed self-deadlock.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	{
		{
			err := o.run(ctx)
			assert.FatalIf(t, err != nil, "Run: %v", err)
		}
	}
	ksID := testutil.KustomizationID("flux-system", "cluster-apps")
	{
		{
			info, ok := o.Store().GetStatus(ksID)
			assert.FatalIf(
				t,
				!ok || info.Status != store.StatusReady,
				"cluster-apps status = (%+v, %v), want Ready (self-substitute deadlock regressed?)",
				info,
				ok,
			)
		}
	}
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}
	{
		{
			got := o.Store().GetObject(cmID)
			assert.ErrorIf(t, got == nil, "cluster-apps render did not emit %s", cmID)
		}
	}
}

// The guard: a substituteFrom ConfigMap that NO KS self-produces (and no
// producer emits) must keep its hard dependency edge and fail loudly. The
// graph-aware self-edge drop must not turn a genuinely-absent CM into a
// silent pass. Same shape as above but the group base pulls in no
// cluster-settings, so nothing produces ConfigMap/flux-system/cluster-settings.
//
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_SelfSubstituteAbsentCMFailsLoud(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/flux/cluster-apps.yaml",
		testutil.KustomizationYAML("cluster-apps", "flux-system", `  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substituteFrom:
      - kind: ConfigMap
        name: cluster-settings
`),
	)
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/apps/flux-system/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources:\n  - ./cm.yaml\n",
	)
	testutil.WriteFile(t, dir, "kubernetes/apps/flux-system/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: other\ndata:\n  k: v\n")

	o, err := newTestOrchestrator(t, Tree{Path: dir}, Config{WipeSecrets: true, Concurrency: 4})
	{
		assert.FatalIf(t, err != nil, "New: %v", err)
	}
	{
		{
			err := o.bootstrap(context.Background())
			assert.FatalIf(t, err != nil, "Bootstrap: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = o.run(ctx) // reconcile failure is recorded in status, not returned
	ksID := testutil.KustomizationID("flux-system", "cluster-apps")
	info, ok := o.Store().GetStatus(ksID)
	{
		assert.FatalIf(
			t,
			!ok || info.Status != store.StatusFailed,
			"cluster-apps status = (%+v, %v), want Failed (absent substituteFrom CM must fail loud)",
			info,
			ok,
		)
	}
	if !strings.Contains(info.String(), "cluster-settings") {
		t.Errorf("cluster-apps failure message = %q, want it to mention cluster-settings", info.String())
	}
}

type testOrchestratorCommentedOutKSNotReconciledCaseInputs struct {
	entry   *string
	enabled *bool
}

func testOrchestratorCommentedOutKSNotReconciledCase(
	t *testing.T,
	testCaseInput testOrchestratorCommentedOutKSNotReconciledCaseInputs,
) {
	t.Helper()

	dir := t.TempDir()
	testutil.GitInit(t, dir)
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [kubernetes/flux/cluster-apps.yaml]\n")
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/flux/cluster-apps.yaml",
		testutil.KustomizationYAML(
			"cluster-apps",
			"flux-system",
			"  path: ./kubernetes/apps/test\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	resources := "namespace: network\nresources: [echo.yaml]\n"
	if *testCaseInput.enabled {
		resources = "namespace: network\nresources: [echo.yaml, tunnel.yaml]\n"
	}
	testutil.WriteFile(t, dir, "kubernetes/apps/test/network/kustomization.yaml", resources)
	for _, name := range []string{"echo", "tunnel"} {
		spec := "  path: ./kubernetes/apps/base/" + name + "\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
		if name == "tunnel" {
			spec += "  components: [../../../components/backup]\n"
		}
		testutil.WriteFile(
			t,
			dir,
			"kubernetes/apps/test/network/"+name+".yaml",
			testutil.KustomizationYAML(name, "network", spec),
		)
		testutil.WriteFile(
			t,
			dir,
			"kubernetes/apps/base/"+name+"/kustomization.yaml",
			"resources: [cm.yaml]\n",
		)
		testutil.WriteFile(
			t,
			dir,
			"kubernetes/apps/base/"+name+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+"-cm}\n",
		)
	}
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/components/backup/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [backup.yaml]\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/components/backup/backup.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: backup-cm}\n",
	)
	renderedSingleton1, err := RenderTrees(
		t.Context(),
		[]Tree{
			{
				Path: filepath.Join(dir, (*testCaseInput.entry)),
				ID:   manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
			},
		},
		Config{WipeSecrets: true, Concurrency: 2},
	)
	{
		assert.FatalIf(t, len(renderedSingleton1) != 1, "missing render: %v", err)
	}
	o := renderedSingleton1[0].Orchestrator
	result := renderedSingleton1[0].Result
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	found := map[string]bool{}
	for _, batch := range result.Manifests {
		for _, obj := range batch.Documents {
			name, _ := manifest.DocMetadata(obj)
			found[name] = true
		}
	}
	{
		assert.ErrorIf(t, !found["echo-cm"], "%v", "listed app output is missing")
	}
	for _, name := range []string{"tunnel", "tunnel-cm", "backup-cm"} {
		if found[name] != (*testCaseInput.enabled) {
			t.Errorf("output %s present=%t, want %t", name, found[name], (*testCaseInput.enabled))
		}
	}
	id := testutil.KustomizationID("network", "tunnel")
	{
		assert.ErrorIf(
			t,
			(o.Store().GetObject(id) != nil) != (*testCaseInput.enabled),
			"disabled app reached store: %v",
			id,
		)
	}
}
