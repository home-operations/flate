package kustomization

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
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
	mustWriteFile(t, filepath.Join(root, "apps", "kustomization.yaml"),
		"resources:\n- cm.yaml\n")
	mustWriteFile(t, filepath.Join(root, "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: default}
data: {greeting: hi}
`)

	s := store.New()
	bootstrap := &manifest.GitRepository{
		Name: "flux-system", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + root},
	}
	s.AddObject(bootstrap)
	s.SetArtifact(bootstrap.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + root, LocalPath: root,
	})
	s.UpdateStatus(bootstrap.Named(), store.StatusReady, "ready")

	cache, err := kustomize.NewStagingCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	tasks := task.New()
	c := New(s, tasks, cache, true)
	c.Configure(Options{})
	c.Start(context.Background())
	t.Cleanup(func() {
		c.Close()
		tasks.BlockTillDone()
	})
	return c, s, root
}

func mustWriteFile(t *testing.T, p, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitForStatus(t *testing.T, s *store.Store, id manifest.NamedResource, want store.Status) store.StatusInfo {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if info, ok := s.GetStatus(id); ok && info.Status == want {
			return info
		}
		time.Sleep(5 * time.Millisecond)
	}
	info, _ := s.GetStatus(id)
	t.Fatalf("status %v not reached; last=%+v", want, info)
	return info
}

// TestReconcile_HappyPath drives the full reconcile flow: AddObject
// fires the listener, depwait clears (no deps), kustomize renders,
// emitRenderedChildren lands the ConfigMap as a rendered artifact,
// status flips Ready, artifact carries a fingerprint.
func TestReconcile_HappyPath(t *testing.T) {
	_, s, root := newControllerWithFixture(t)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "./apps",
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
			},
		},
		SourceKind: manifest.KindGitRepository, SourceName: "flux-system", SourceNamespace: "flux-system",
		Contents: map[string]any{},
	}
	s.AddObject(ks)
	info := waitForStatus(t, s, ks.Named(), store.StatusReady)
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
	if filepath.Dir(art.Path) == "/" {
		t.Errorf("unexpected artifact path: %q (root=%s)", art.Path, root)
	}
}

// TestReconcile_FingerprintDedup_SkipsRender locks the dedup
// short-circuit from PR #220: a second AddObject with byte-equal
// post-Prepare spec must not re-run kustomize.RenderFlux. We force
// the issue by AddObject'ing the same KS twice and verifying the
// artifact's identity persists (SetArtifact dedupes via DeepEqual).
func TestReconcile_FingerprintDedup_SkipsRender(t *testing.T) {
	c, s, _ := newControllerWithFixture(t)
	_ = c
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "./apps",
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
			},
		},
		SourceKind: manifest.KindGitRepository, SourceName: "flux-system", SourceNamespace: "flux-system",
		Contents: map[string]any{},
	}
	s.AddObject(ks)
	waitForStatus(t, s, ks.Named(), store.StatusReady)
	firstFP := s.GetArtifact(ks.Named()).(*store.KustomizationArtifact).Fingerprint

	// Re-AddObject a copy with kustomize ownership labels stamped.
	// AddObject's DeepEqual gate fails (labels differ), the listener
	// re-fires, but the fingerprint matches → second render skipped,
	// artifact fingerprint unchanged.
	stamped := ks.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	s.AddObject(stamped)
	// Give the coalescer's pending-re-run a moment to fire.
	time.Sleep(50 * time.Millisecond)
	secondFP := s.GetArtifact(ks.Named()).(*store.KustomizationArtifact).Fingerprint
	if firstFP != secondFP {
		t.Errorf("fingerprint changed across label-only re-AddObject: %q vs %q", firstFP, secondFP)
	}
}

// TestReconcile_SuspendShortCircuits covers the PreGate path: a
// suspended KS never enters reconcile, status flips Ready+suspended
// without an artifact write.
func TestReconcile_SuspendShortCircuits(t *testing.T) {
	c, s, _ := newControllerWithFixture(t)
	_ = c
	ks := &manifest.Kustomization{
		Name: "suspended", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "./apps", Suspend: true,
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
			},
		},
		SourceKind: manifest.KindGitRepository, SourceName: "flux-system", SourceNamespace: "flux-system",
	}
	s.AddObject(ks)
	info := waitForStatus(t, s, ks.Named(), store.StatusReady)
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
	_ = c
	ks := &manifest.Kustomization{
		Name: "broken", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "./nonexistent",
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
			},
		},
		SourceKind: manifest.KindGitRepository, SourceName: "flux-system", SourceNamespace: "flux-system",
		Contents: map[string]any{},
	}
	s.AddObject(ks)
	info := waitForStatus(t, s, ks.Named(), store.StatusFailed)
	if info.Message == "" {
		t.Error("expected non-empty failure message")
	}
}

// TestReconcile_DependsOnFailed cascades a dep failure: when a KS's
// dependsOn target is in StatusFailed, reconcile must return
// DependencyFailedError without rendering.
func TestReconcile_DependsOnFailed(t *testing.T) {
	c, s, _ := newControllerWithFixture(t)
	_ = c
	dep := &manifest.Kustomization{Name: "dep", Namespace: "flux-system"}
	s.AddObject(dep)
	s.UpdateStatus(dep.Named(), store.StatusFailed, "synthetic dep failure")

	ks := &manifest.Kustomization{
		Name: "depender", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "./apps",
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
			},
			Timeout: &metav1.Duration{Duration: 100 * time.Millisecond},
		},
		SourceKind: manifest.KindGitRepository, SourceName: "flux-system", SourceNamespace: "flux-system",
		DependsOn: []manifest.DependencyRef{
			{NamedResource: dep.Named()},
		},
		Contents: map[string]any{},
	}
	s.AddObject(ks)
	info := waitForStatus(t, s, ks.Named(), store.StatusFailed)
	if info.Message == "" {
		t.Error("expected failure message from dep cascade")
	}
}
