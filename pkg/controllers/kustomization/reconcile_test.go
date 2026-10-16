package kustomization

import (
	"context"
	"errors"
	"fmt"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// newControllerWithFixture wires a controller against a real on-disk
// fixture so reconcile can drive kustomize end-to-end. Returns the
// controller, a bootstrap GitRepository pointing at root, and the
// repo root path so callers can seed downstream artifacts.
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
	input, err := gittree.AdmitDirectory(t.Context(), root)
	assert.NoError(t, err)
	s.SetArtifact(bootstrap.Named(), input)
	s.UpdateStatus(bootstrap.Named(), store.StatusReady, "ready")

	owner, err := sourcecache.New(t.Context(), "")
	assert.NoError(t, err)
	t.Cleanup(func() { _ = owner.Close() })
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

// dispatchToFixpoint drives id through ReconcileNode with escalating drain
// levels (none→cascade→force), mirroring the scheduler's structural fixpoint,
// until the node terminalizes (no blocked deps) or drain is exhausted, then
// returns the final store status. Synchronous — replaces the event engine's
// AddObject→listener→status-poll dispatch in unit tests.
func dispatchToFixpoint(t *testing.T, c *Controller, st *store.Store, id manifest.NamedResource) store.StatusInfo {
	t.Helper()
	for _, drain := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade, manifest.DrainForce} {
		if blocked := c.ReconcileNode(context.Background(), id, drain); len(blocked) == 0 {
			break
		}
	}
	info, _ := st.GetStatus(id)
	return info
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
	info := dispatchToFixpoint(t, c, s, ks.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	if info.Message != "" {
		t.Errorf("unexpected Ready message: %q", info.Message)
	}
	art, _ := s.GetArtifact(ks.Named()).(*store.Emission)
	if art == nil {
		t.Fatal("Emission not stored")
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
}

func TestReconcile_EmptyPostBuildRespectsSubstituteStrategy(t *testing.T) {
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
			if info := dispatchToFixpoint(t, c, s, ks.Named()); info.Status != store.StatusReady {
				t.Fatalf("status = %+v, want StatusReady", info)
			}
			art, _ := s.GetArtifact(ks.Named()).(*store.Emission)
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
	if info := dispatchToFixpoint(t, c, s, ks.Named()); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	firstFP := s.GetArtifact(ks.Named()).(*store.Emission).Fingerprint
	art := s.GetArtifact(ks.Named()).(*store.Emission)
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
	secondFP := s.GetArtifact(ks.Named()).(*store.Emission).Fingerprint
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
	if info := dispatchToFixpoint(t, c, s, ks.Named()); info.Status != store.StatusReady {
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
	if info := dispatchToFixpoint(t, c, s, ks.Named()); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	firstFP := s.GetArtifact(ks.Named()).(*store.Emission).Fingerprint

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
	if art, ok := s.GetArtifact(ks.Named()).(*store.Emission); ok && art.Fingerprint != firstFP {
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

// TestReconcile_SuspendShortCircuits covers the PreGate path: a
// suspended KS never enters reconcile, status flips Ready+suspended
// without an artifact write.
func TestReconcile_SuspendShortCircuits(t *testing.T) {
	c, s, _ := newControllerWithFixture(t)
	ks := &manifest.Kustomization{
		Name: "suspended", Namespace: "flux-system",
		Path: "./apps", Suspend: true,
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	info := dispatchToFixpoint(t, c, s, ks.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	if info.Message != "suspended" {
		t.Errorf("expected Ready/suspended, got %+v", info)
	}
	if s.GetArtifact(ks.Named()) != nil {
		t.Error("suspended KS should not write an artifact")
	}
}

// TestReconcile_MissingPathFails covers the error path: spec.path
// points at a directory that doesn't exist → kustomize.RenderFlux
// errors → reconcile returns it → status flips Failed.
func TestReconcile_MissingPathFails(t *testing.T) {
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
	info := dispatchToFixpoint(t, c, s, ks.Named())
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
	info := dispatchToFixpoint(t, c, s, ks.Named())
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.Message == "" {
		t.Error("expected failure message from dep cascade")
	}
}

func TestReconcile_SuspensionAfterRefresh(t *testing.T) {
	for _, tt := range []struct {
		name      string
		suspended bool
		cascade   bool
	}{
		{name: "suspended=false"},
		{name: "suspended=true", suspended: true},
		{name: "oci_cascade", suspended: true, cascade: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
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
			c.Emit(parent.Named(), &store.Emission{Children: []store.Child{{Obj: ks, Doc: doc}}, Complete: true})
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
				publishDocuments(t, c, parent.Named(), []map[string]any{replacement}, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: parent.Named().Namespace})
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
			if owners := testutil.Owners(st.Productions(ks.Named(), nil)); !slices.Contains(owners, parent.Named()) {
				t.Fatalf("missing independent contributor: %v", owners)
			}
			info, _ := st.GetStatus(ks.Named())
			art, _ := st.GetArtifact(ks.Named()).(*store.Emission)
			output := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "refreshed", Name: "ks-refresh-output"}
			if suspended {
				if info.Status != store.StatusReady || info.Message != store.MsgSuspended {
					t.Errorf("status = %+v, want Ready/suspended", info)
				}
				_, inventory := st.GetEmission(ks.Named())
				if art != nil || st.GetObject(output) != nil || len(inventory.Children) != 0 {
					t.Errorf("suspended replacement rendered: artifact=%+v output=%v", art, st.GetObject(output))
				}
			} else if info.Status != store.StatusReady || art == nil || len(art.Manifests) != 1 || st.GetObject(output) == nil {
				t.Fatalf("active replacement did not render %s: status=%+v artifact=%+v", output, info, art)
			}
		})
	}
}

func TestReconcile_ReleasedPinWaitsForSource(t *testing.T) {
	c, st, root := newControllerWithFixture(t)
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps", Contents: map[string]any{}}
	st.AddObject(ks)
	if info := dispatchToFixpoint(t, c, st, ks.Named()); info.Status != store.StatusReady {
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

type guardedSourceFS struct {
	fs.FS
	once   sync.Once
	opened atomic.Int64
	deny   atomic.Bool
	onOpen func()
}

func (s *guardedSourceFS) Open(name string) (fs.File, error) {
	s.opened.Add(1)
	if s.deny.Load() {
		return nil, fs.ErrPermission
	}
	s.once.Do(func() {
		if s.onOpen != nil {
			s.onOpen()
		}
	})
	return s.FS.Open(name)
}

func TestReconcile_SourceCoverageUsesRenderedArtifact(t *testing.T) {
	for _, swapDuringRender := range []bool{false, true} {
		t.Run(fmt.Sprint(swapDuringRender), func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [objects.yaml]\n")
			testutil.WriteFile(t, root, "apps/objects.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: apps}\ndata: {value: original}\n---\napiVersion: v1\nkind: Secret\nmetadata: {name: exclusive, namespace: apps}\nstringData: {value: original}\n")
			s := store.New()
			sourceID := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}
			input, err := gittree.AdmitDirectory(t.Context(), root)
			assert.NoError(t, err)
			guard := &guardedSourceFS{FS: input.FS}
			initial := *input
			initial.Key, initial.FS, initial.Revision = "initial-pin", guard, "initial-pin"
			next := initial
			next.Key, next.FS = "different-pin", input.FS
			s.AddObject(&manifest.GitRepository{Name: sourceID.Name, Namespace: sourceID.Namespace, URL: "file://" + root})
			s.SetArtifact(sourceID, &initial)
			s.UpdateStatus(sourceID, store.StatusReady, "")
			docs, err := manifest.DecodeDocs(strings.NewReader("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: producer, namespace: apps}\nspec: {path: apps, namePrefix: 'first-', sourceRef: {kind: GitRepository, name: source, namespace: apps}}\n"))
			assert.NoError(t, err)
			obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
			assert.NoError(t, err)
			ks := obj.(*manifest.Kustomization)
			s.AddObject(ks)
			l := loader.New(s)
			l.SourceRoot, l.Options.WipeSecrets = root, false
			_, err = l.Load(t.Context(), filepath.Join(root, "apps"))
			assert.NoError(t, err)
			l.FinalizeIndex(root)
			shared := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "shared"}
			exclusive := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "exclusive"}
			if len(slices.Collect(s.Productions(exclusive, l.Index))) == 0 {
				t.Fatal("missing frozen coverage")
			}
			for _, id := range []manifest.NamedResource{shared, exclusive} {
				for p := range s.Productions(id, l.Index) {
					if p.Path != "" {
						loader.Promote(s, id, p, loader.PromoteOptions{Admit: func(manifest.BaseManifest) bool { return true }})
					}
				}
				if s.GetObject(id) == nil {
					t.Fatalf("missing promoted seed %s", id)
				}
			}

			q := &manifest.Kustomization{Name: "q", Namespace: "apps"}
			s.AddObject(q)
			s.RecordEmitted(q.Named(), &store.Emission{Children: []store.Child{{Obj: s.GetObject(shared)}}, Complete: true})
			owner, err := sourcecache.New(t.Context(), t.TempDir())
			assert.NoError(t, err)
			t.Cleanup(func() { _ = owner.Close() })
			trees := kustomize.NewTreeCache(owner, nil)
			t.Cleanup(trees.Close)
			c := New(s, task.NewBounded(2), trees, false)
			c.Configure(Options{Index: l.Index}, sourcecache.RetryConfig{})
			if swapDuringRender {
				guard.once = sync.Once{}
				guard.onOpen = func() { s.SetArtifact(sourceID, &next) }
			} else {
				s.SetArtifact(sourceID, &next)
			}
			if err := c.reconcile(t.Context(), ks, manifest.DrainNone); err != nil {
				t.Fatal(err)
			}
			if got := s.GetObject(exclusive) != nil; got == swapDuringRender {
				t.Fatalf("retirement followed store source rather than rendered source: retained=%t", got)
			}
			if s.GetObject(shared) == nil {
				t.Fatal("coverage retired Q support")
			}
			if !swapDuringRender {
				s.SetArtifact(sourceID, &initial)
				if err := c.reconcile(t.Context(), ks, manifest.DrainNone); err != nil {
					t.Fatal(err)
				}
				if s.GetObject(exclusive) != nil {
					t.Fatal("matching source view did not retire its whole prediction")
				}
			}
			_, empty := s.GetEmission(ks.Named())
			empty.Children, empty.Manifests, empty.Fingerprint, empty.Complete = nil, nil, "", true
			s.RecordEmitted(ks.Named(), &empty)
			empty.Finish()
			s.SetArtifact(sourceID, &initial)
			probe := sourcecache.BlobKey{Content: "source-coverage-persistence-probe", PersistOnly: true}
			probeReader, _, _, err := owner.Blob(t.Context(), probe, sourcecache.Policy{}, func(context.Context, io.Writer) (ocispec.Descriptor, error) { return ocispec.Descriptor{}, nil })
			assert.NoError(t, err)
			assert.NoError(t, probeReader.Close())
			probeReader, _, _, probeErr := owner.Blob(t.Context(), probe, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
			persistent := probeErr == nil
			if persistent {
				assert.NoError(t, probeReader.Close())
				guard.deny.Store(true)
			} else {
				t.Log("raw render persistence unavailable for this build identity")
			}
			before := guard.opened.Load()
			if err := c.reconcile(t.Context(), ks, manifest.DrainNone); err != nil {
				t.Fatalf("raw cache hit: %v", err)
			}
			if persistent && guard.opened.Load() != before {
				t.Fatal("expected immutable-source raw cache hit without input reads")
			}
			guard.deny.Store(false)
			fresh := ks.Clone()
			fresh.NamePrefix = "arbitrary-second-"
			fresh.Contents["spec"].(map[string]any)["namePrefix"] = fresh.NamePrefix
			s.AddObject(fresh)
			if err := c.reconcile(t.Context(), fresh, manifest.DrainNone); err != nil {
				t.Fatal(err)
			}
			old := shared
			old.Name = "first-shared"
			newID := shared
			newID.Name = "arbitrary-second-shared"
			if s.GetObject(old) != nil || s.GetObject(newID) == nil {
				t.Fatal("repeated rename retained obsolete output")
			}
			failed := initial
			failed.Key = "failed-pin"
			failed.FS = &guardedSourceFS{FS: input.FS}
			failed.FS.(*guardedSourceFS).deny.Store(true)
			s.SetArtifact(sourceID, &failed)
			if err := c.reconcile(t.Context(), fresh, manifest.DrainNone); err == nil || s.GetObject(newID) == nil {
				t.Fatal("failed successor withdrew completed output")
			}
			_, empty = s.GetEmission(ks.Named())
			empty.Children, empty.Manifests, empty.Complete = nil, nil, true
			s.RecordEmitted(ks.Named(), &empty)
			if s.GetObject(newID) != nil || s.GetObject(shared) == nil {
				t.Fatal("complete empty did not retire only its contribution")
			}
		})
	}
}
