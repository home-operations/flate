package kustomization

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func newControllerWithFixture(t *testing.T) (*Controller, *store.Store, string) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFileAt(t, filepath.Join(root, "apps", "kustomization.yaml"),
		"resources:\n- cm.yaml\n")
	testutil.WriteFileAt(t, filepath.Join(root, "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: default}
data: {greeting: hi}
`)

	s := store.New()
	bootstrap := &manifest.GitRepository{
		Name: "flux-system", Namespace: "flux-system",
		URL: "file://" + root,
	}
	s.AddObject(bootstrap)
	location, err := gitrepo.Locate(t.Context(), root, "")
	assert.NoError(t, err)
	input, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	s.SetArtifact(bootstrap.Named(), input)
	s.UpdateStatus(bootstrap.Named(), store.StatusReady, "ready")

	owner := cachetest.New(t, "", sourcecache.New)
	cache := kustomize.NewTreeCache(owner, nil)
	t.Cleanup(cache.Close)

	tasks := task.NewBounded(2)
	c := New(s, tasks, cache, true)
	c.Configure(Options{}, sourcecache.RetryConfig{})
	c.StartLifecycle()
	t.Cleanup(func() {
		c.Close()
		tasks.BlockTillDone()
	})
	return c, s, root
}

const namespaceIdentityDocs = `apiVersion: v1
kind: ConfigMap
metadata: {name: hello}
data: {greeting: hi}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: chart}
spec: {url: oci://example.test/chart}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: app}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: role}
---
apiVersion: foreign.example/v1
kind: ClusterRole
metadata: {name: foreign}
`

// TestReconcile_HappyPath drives the full reconcile flow: AddObject
// fires the listener, depwait clears (no deps), kustomize renders,
// emitRenderedChildren lands the ConfigMap as a rendered artifact,
// status flips Ready, artifact carries a fingerprint.
func TestReconcile_HappyPath(t *testing.T) {
	t.Parallel()
	c, s, root := newControllerWithFixture(t)
	testutil.WriteFile(t, root, "apps/cm.yaml", namespaceIdentityDocs)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
		Contents: map[string]any{},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	if info.Message != "" {
		t.Errorf("unexpected Ready message: %q", info.Message)
	}
	art, _ := s.GetArtifact(ks.Named()).(*store.KustomizationArtifact)
	if art == nil {
		t.Fatal("KustomizationArtifact not stored")
	}
	if art.Fingerprint == "" {
		t.Error("artifact missing fingerprint")
	}
	if len(art.Manifests) == 0 {
		t.Error("artifact has no rendered manifests")
	}
	assert.Equal(t, len(art.Manifests), 5)
	for _, doc := range art.Manifests {
		name, ns := manifest.DocMetadata(doc)
		assert.Equal(t, ns, "")
		want := ks.TargetNamespace
		if manifest.DocKind(doc) == "ClusterRole" && manifest.DocAPIVersion(doc) == "rbac.authorization.k8s.io/v1" {
			want = ""
		}
		if s.GetObject(manifest.NamedResource{Group: schema.FromAPIVersionAndKind(manifest.DocAPIVersion(doc), manifest.DocKind(doc)).Group, Kind: manifest.DocKind(doc), Namespace: want, Name: name}) == nil {
			t.Errorf("missing effective child %s/%s", want, name)
		}
	}
	if filepath.Dir(art.Path) == "/" {
		t.Errorf("unexpected artifact path: %q (root=%s)", art.Path, root)
	}
}

func TestReconcile_EmptyPostBuildRespectsSubstituteStrategy(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		strategy  kustomizev1.SubstituteStrategy
		postBuild map[string]any
		disabled  string
		input     string
		want      string
	}{
		{name: "no postBuild", want: "${VAR:=default}"},
		{name: "unset", postBuild: map[string]any{}, want: "${VAR:=default}"},
		{name: "WithVariables", strategy: kustomizev1.SubstituteStrategyWithVariables, postBuild: map[string]any{}, want: "${VAR:=default}"},
		{name: "Always", strategy: kustomizev1.SubstituteStrategyAlways, postBuild: map[string]any{}, want: "default"},
		{name: "empty substitute", postBuild: map[string]any{"substitute": map[string]any{}}, want: "${VAR:=default}"},
		{
			name: "missing optional ConfigMap",
			postBuild: map[string]any{"substituteFrom": []any{
				map[string]any{"kind": "ConfigMap", "name": "missing", "optional": true},
			}},
			want: "${VAR:=default}",
		},
		{
			name: "missing optional Secret",
			postBuild: map[string]any{"substituteFrom": []any{
				map[string]any{"kind": "Secret", "name": "missing", "optional": true},
			}},
			want: "${VAR:=default}",
		},
		{
			name:      "inline substitute",
			postBuild: map[string]any{"substitute": map[string]any{"VAR": "configured"}},
			want:      "configured",
		},
		{name: "disabled label", strategy: kustomizev1.SubstituteStrategyAlways, postBuild: map[string]any{}, disabled: "labels", want: "${VAR:=default}"},
		{name: "disabled annotation", strategy: kustomizev1.SubstituteStrategyAlways, postBuild: map[string]any{}, disabled: "annotations", want: "${VAR:=default}"},
		{name: "double dollar without variables", input: "echo $$PID", want: "echo $$PID"},
		{name: "double dollar Always", input: "echo $$PID", strategy: kustomizev1.SubstituteStrategyAlways, postBuild: map[string]any{}, want: "echo $PID"},
		{name: "double dollar with variables", input: "echo $$PID", postBuild: map[string]any{"substitute": map[string]any{"VAR": "configured"}}, want: "echo $PID"},
		{name: "double dollar disabled label", input: "echo $$PID", postBuild: map[string]any{"substitute": map[string]any{"VAR": "configured"}}, disabled: "labels", want: "echo $$PID"},
		{name: "double dollar disabled annotation", input: "echo $$PID", postBuild: map[string]any{"substitute": map[string]any{"VAR": "configured"}}, disabled: "annotations", want: "echo $$PID"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, s, root := newControllerWithFixture(t)
			input := tc.input
			if input == "" {
				input = "${VAR:=default}"
			}
			optOut := ""
			if tc.disabled != "" {
				optOut = fmt.Sprintf(", %s: {kustomize.toolkit.fluxcd.io/substitute: disabled}", tc.disabled)
			}
			testutil.WriteFileAt(t, filepath.Join(root, "apps", "cm.yaml"), fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: default%s}
data: {greeting: '%s'}
`, optOut, input))

			spec := map[string]any{
				"path":      "./apps",
				"sourceRef": map[string]any{"kind": "GitRepository", "name": "flux-system"},
			}
			if tc.postBuild != nil {
				if tc.strategy != "" {
					tc.postBuild["substituteStrategy"] = string(tc.strategy)
				}
				spec["postBuild"] = tc.postBuild
			}
			obj, _, _, err := manifest.ParseDoc(map[string]any{
				"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
				"kind":       "Kustomization",
				"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
				"spec":       spec,
			}, manifest.ParseDocOptions{DefaultNamespace: ""})
			if err != nil {
				t.Fatalf("ParseDoc: %v", err)
			}
			ks := obj.(*manifest.Kustomization)
			s.AddObject(ks)
			if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
				t.Fatalf("status = %+v, want StatusReady", info)
			}
			art, _ := s.GetArtifact(ks.Named()).(*store.KustomizationArtifact)
			if art == nil || len(art.Manifests) != 1 {
				t.Fatalf("expected one rendered manifest, got %+v", art)
			}
			data := art.Manifests[0]["data"].(map[string]any)
			if got := data["greeting"]; got != tc.want {
				t.Errorf("greeting = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestReconcile_FingerprintDedup_SkipsRender locks the dedup
// short-circuit from PR #220: a second AddObject with byte-equal
// post-Prepare spec must not re-run kustomize.RenderFlux. We force
// the issue by AddObject'ing the same KS twice and verifying the
// artifact's identity persists (SetArtifact dedupes via DeepEqual).
func TestReconcile_FingerprintDedup_SkipsRender(t *testing.T) {
	t.Parallel()
	c, s, root := newControllerWithFixture(t)
	testutil.WriteFile(t, root, "apps/cm.yaml", namespaceIdentityDocs)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps", TargetNamespace: "release",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
		Contents: map[string]any{},
	}
	s.AddObject(ks)
	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	firstFP := s.GetArtifact(ks.Named()).(*store.KustomizationArtifact).Fingerprint
	art := s.GetArtifact(ks.Named()).(*store.KustomizationArtifact)
	before := make([]map[string]any, len(art.Manifests))
	for i, doc := range art.Manifests {
		before[i] = manifest.DeepCopyMap(doc)
	}

	// Re-AddObject a copy with kustomize ownership labels stamped, then drive a
	// second reconcile: the labels differ so the body re-runs, but the
	// fingerprint matches → second render skipped, artifact fingerprint
	// unchanged.
	stamped := ks.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	s.AddObject(stamped)
	c.ReconcileNode(context.Background(), ks.Named(), 0)
	secondFP := s.GetArtifact(ks.Named()).(*store.KustomizationArtifact).Fingerprint
	if firstFP != secondFP {
		t.Errorf("fingerprint changed across label-only re-AddObject: %q vs %q", firstFP, secondFP)
	}
	assert.Diff(t, art.Manifests, before)
	for _, doc := range art.Manifests {
		name, ns := manifest.DocMetadata(doc)
		want := ks.TargetNamespace
		if manifest.DocAPIVersion(doc) == "rbac.authorization.k8s.io/v1" {
			want = ""
		}
		assert.Equal(t, ns, want)
		if s.GetObject(manifest.NamedResource{Group: schema.FromAPIVersionAndKind(manifest.DocAPIVersion(doc), manifest.DocKind(doc)).Group, Kind: manifest.DocKind(doc), Namespace: want, Name: name}) == nil {
			t.Errorf("replayed child %s/%s missing", want, name)
		}
		if want != "" && s.GetObject(manifest.NamedResource{Group: schema.FromAPIVersionAndKind(manifest.DocAPIVersion(doc), manifest.DocKind(doc)).Group, Kind: manifest.DocKind(doc), Name: name}) != nil {
			t.Errorf("replay published duplicate bare child %s", name)
		}
	}

}

// TestReconcile_AlreadyReady_NoTransientPending pins Part A of the
// re-emission-churn fix: a re-reconcile of an already-Ready KS that
// turns out to be a no-op (fingerprint unchanged, dedup-skip) must NOT
// transiently flip the status Ready→Pending. That transient window is
// exactly what a dependent's quiescence-bound depwait can re-read at a
// transient pool drain and give up on ("parent Kustomization not
// ready"). Pre-fix the unconditional UpdateStatus(Pending) at the top
// of reconcile fired on every re-run; post-fix it is suppressed when
// the object is already Ready.
func TestReconcile_AlreadyReady_NoTransientPending(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
		Contents: map[string]any{},
	}
	s.AddObject(ks)
	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}

	var mu sync.Mutex
	var sawPending bool
	s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id != ks.Named() {
			return
		}
		if info, ok := payload.(store.StatusInfo); ok && info.Status == store.StatusPending {
			mu.Lock()
			sawPending = true
			mu.Unlock()
		}
	}, false)

	// Re-emit with kustomize ownership labels stamped, then drive a second
	// reconcile: the labels differ so the body re-runs, but the fingerprint
	// matches → dedup-skip no-op.
	stamped := ks.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	s.AddObject(stamped)
	c.ReconcileNode(context.Background(), ks.Named(), 0)

	mu.Lock()
	defer mu.Unlock()
	if sawPending {
		t.Error("no-op re-reconcile of an already-Ready KS transiently downgraded it to Pending (the quiescence-race window)")
	}
	if info, _ := s.GetStatus(ks.Named()); info.Status != store.StatusReady {
		t.Errorf("KS not Ready after no-op re-run: %+v", info)
	}
}

// TestReconcile_GenuineReRender_DoesDowngrade is the byte-determinism
// guard for Part A: a re-reconcile whose effective spec CHANGED
// (fingerprint differs, e.g. #102 structural-parent-injected
// targetNamespace) MUST still downgrade to Pending before re-rendering,
// so a dependent re-gates on the new output rather than reading a
// stale-Ready parent. Proves the wasReady guard suppresses only the
// pre-fingerprint progress writes, not the genuine "rendering"
// downgrade.
func TestReconcile_GenuineReRender_DoesDowngrade(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
		Contents: map[string]any{},
	}
	s.AddObject(ks)
	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	firstFP := s.GetArtifact(ks.Named()).(*store.KustomizationArtifact).Fingerprint

	var mu sync.Mutex
	var sawPending bool
	s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id != ks.Named() {
			return
		}
		if info, ok := payload.(store.StatusInfo); ok && info.Status == store.StatusPending {
			mu.Lock()
			sawPending = true
			mu.Unlock()
		}
	}, false)

	// Re-emit with a genuine spec change (targetNamespace) → fingerprint
	// differs → real re-render, not a dedup-skip. ReconcileNode runs the
	// re-render synchronously, so the new artifact has landed on return.
	changed := ks.Clone()
	changed.TargetNamespace = "elsewhere"
	s.AddObject(changed)
	c.ReconcileNode(context.Background(), ks.Named(), 0)

	var secondFP string
	if art, ok := s.GetArtifact(ks.Named()).(*store.KustomizationArtifact); ok && art.Fingerprint != firstFP {
		secondFP = art.Fingerprint
	}

	mu.Lock()
	defer mu.Unlock()
	if !sawPending {
		t.Error("genuine re-render did not downgrade to Pending; dependents would read a stale-Ready parent")
	}
	if secondFP == "" || secondFP == firstFP {
		t.Errorf("fingerprint did not change after spec mutation (%q → %q); not a genuine re-render", firstFP, secondFP)
	}
}

// TestReconcile_MissingPathFails covers the error path: spec.path
// points at a directory that doesn't exist → kustomize.RenderFlux
// errors → reconcile returns it → status flips Failed.
func TestReconcile_MissingPathFails(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	ks := &manifest.Kustomization{
		Name: "broken", Namespace: "flux-system",
		Path: "./nonexistent",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
		Contents: map[string]any{},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.Message == "" {
		t.Error("expected non-empty failure message")
	}
}

// TestReconcile_DependsOnFailed cascades a dep failure: when a KS's
// dependsOn target is in StatusFailed, reconcile must return
// DependencyFailedError without rendering.
func TestReconcile_DependsOnFailed(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	dep := &manifest.Kustomization{Name: "dep", Namespace: "flux-system"}
	s.AddObject(dep)
	s.UpdateStatus(dep.Named(), store.StatusFailed, "synthetic dep failure")

	ks := &manifest.Kustomization{
		Name: "depender", Namespace: "flux-system",
		Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
		DependsOn: []kustomizev1.DependencyReference{
			{Name: dep.Name, Namespace: dep.Namespace},
		},
		Contents: map[string]any{},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.Message == "" {
		t.Error("expected failure message from dep cascade")
	}
}

func TestReconcile_Suspension(t *testing.T) {
	t.Parallel()
	t.Run("initial suspension", func(t *testing.T) {
		t.Parallel()
		c, s, _ := newControllerWithFixture(t)
		ks := &manifest.Kustomization{
			Name: "suspended", Namespace: "flux-system",
			Path: "./apps", Suspend: true,
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
			},
		}
		s.AddObject(ks)
		info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode})
		if info.Status != store.StatusReady {
			t.Fatalf("status = %+v, want StatusReady", info)
		}
		if info.Message != "suspended" {
			t.Errorf("expected Ready/suspended, got %+v", info)
		}
		if s.GetArtifact(ks.Named()) != nil {
			t.Error("suspended KS should not write an artifact")
		}
	})
	for _, tt := range []struct {
		name      string
		suspended bool
		cascade   bool
	}{
		{name: "refreshed/suspended=false"},
		{name: "refreshed/suspended=true", suspended: true},
		{name: "refreshed/oci_cascade", suspended: true, cascade: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			suspended := tt.suspended
			c, st, root := newControllerWithFixture(t)
			testutil.WriteFile(t, root, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: ks-refresh-output}
data: {greeting: hi}
`)
			parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system"}
			st.AddObject(parent)
			st.UpdateStatus(parent.Named(), store.StatusReady, "")
			doc := map[string]any{
				"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": manifest.KindKustomization,
				"metadata": map[string]any{"name": "apps", "namespace": "flux-system"},
				"spec": map[string]any{
					"path": "./apps", "sourceRef": map[string]any{"kind": manifest.KindGitRepository, "name": "flux-system"},
					"dependsOn": []any{map[string]any{"name": parent.Name}},
				},
			}
			level := manifest.DrainNone
			if tt.cascade {
				doc["spec"].(map[string]any)["sourceRef"] = map[string]any{"kind": manifest.KindOCIRepository, "name": "missing"}
				st.UpdateStatus(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: parent.Namespace, Name: "missing"}, store.StatusReady, "")
				level = manifest.DrainCascade
			}
			obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: ""})
			if err != nil {
				t.Fatal(err)
			}
			ks := obj.(*manifest.Kustomization)
			original := ks.Clone()
			st.AddObject(ks)
			st.UpdateStatus(ks.Named(), store.StatusPending, "")
			replacement := manifest.DeepCopyMap(doc)
			spec := replacement["spec"].(map[string]any)
			spec["suspend"] = suspended
			spec["targetNamespace"] = "refreshed"
			var observations atomic.Int64
			var fired, published atomic.Bool
			unsub := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
				info, ok := payload.(store.StatusInfo)
				if id != ks.Named() || !ok || info.Message != "resolving dependencies" {
					return
				}
				observations.Add(1)
				if fired.Swap(true) {
					return
				}
				c.Emit(parent.Named(), []map[string]any{replacement}, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: parent.Named().Namespace}, true)
				published.Store(true)
			}, false)
			t.Cleanup(unsub)
			c.Tasks.Go(t.Context(), "refresh", func(ctx context.Context) {
				if blocked := c.ReconcileNode(ctx, ks.Named(), level); len(blocked) != 0 {
					t.Errorf("blocked on %v", blocked)
				}
			})
			c.Tasks.BlockTillDone()
			if observations.Load() != 1 || !published.Load() || c.Tasks.Failures() != 0 {
				t.Fatalf("observations=%d published=%t panics=%d", observations.Load(), published.Load(), c.Tasks.Failures())
			}
			fresh, ok := st.Get[*manifest.Kustomization](ks.Named())
			if !ok || fresh == ks || fresh.Suspend != suspended || fresh.TargetNamespace != "refreshed" {
				t.Fatalf("canonical replacement = %+v", fresh)
			}
			if diff := cmp.Diff(original, ks); diff != "" {
				t.Fatalf("original mutated (-want +got):\n%s", diff)
			}
			if owner, ok := st.EmittedParent(ks.Named()); !ok || owner != parent.Named() {
				t.Fatalf("replacement owner = %v, found=%t", owner, ok)
			}
			info, _ := st.GetStatus(ks.Named())
			art, _ := st.GetArtifact(ks.Named()).(*store.KustomizationArtifact)
			output := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "refreshed", Name: "ks-refresh-output"}
			if suspended {
				if info.Status != store.StatusReady || info.Message != store.MsgSuspended {
					t.Errorf("status = %+v, want Ready/suspended", info)
				}
				if art != nil || st.GetObject(output) != nil || len(st.EmittedChildrenByParent()[ks.Named()]) != 0 {
					t.Errorf("suspended replacement rendered: artifact=%+v output=%v", art, st.GetObject(output))
				}
			} else if info.Status != store.StatusReady || art == nil || len(art.Manifests) != 1 || st.GetObject(output) == nil {
				t.Fatalf("active replacement did not render %s: status=%+v artifact=%+v", output, info, art)
			}
		})
	}
}

func TestReconcile_ReleasedPinWaitsForSource(t *testing.T) {
	t.Parallel()
	c, st, root := newControllerWithFixture(t)
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps", Contents: map[string]any{}}
	st.AddObject(ks)
	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: ks.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
		t.Fatalf("first reconcile: %+v", info)
	}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, Key: "released"})
	_, _, err := c.resolveSource(ks, manifest.DrainNone)
	if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); !ok || len(blocked.Deps) != 1 || blocked.Deps[0] != manifest.BootstrapSourceID {
		t.Fatalf("released pin did not wait for source acquisition: %v", err)
	}
	if _, _, err := c.resolveSource(ks, manifest.DrainForce); !errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("unavailable content at fixpoint: %v", err)
	}
}
