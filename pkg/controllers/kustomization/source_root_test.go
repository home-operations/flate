package kustomization

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestReconcile_OCICascadeDiagnostic(t *testing.T) {
	t.Parallel()
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "failed"}[failed], func(t *testing.T) {
			t.Parallel()
			c, st, _ := newControllerWithFixture(t)
			ks := &manifest.Kustomization{
				Name: "consumer", Namespace: "flux-system",
				Path: "./apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindOCIRepository, Name: "missing", Namespace: "flux-system",
				},
			}
			st.AddObject(ks)
			st.UpdateStatus(ks.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: ""})
			if failed {
				st.UpdateStatus(ks.SourceID(), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "fetch failed"})
			}
			c.Tasks.Go(t.Context(), "oci cascade", func(ctx context.Context) {
				if blocked := c.ReconcileNode(ctx, ks.Named(), manifest.DrainCascade).Blocked; len(blocked) != 0 {
					t.Errorf("blocked on %v", blocked)
				}
			})
			c.Tasks.BlockTillDone()
			info, _ := st.GetStatus(ks.Named())
			if info.Status != store.StatusFailed || !strings.Contains(info.Message, "unknown OCI artifact root") {
				t.Fatalf("OCI cascade status = %+v", info)
			}
			if st.GetArtifact(ks.Named(), store.ArtifactContent) != nil || c.Tasks.Failures() != 0 {
				t.Fatal("failed OCI cascade rendered or panicked")
			}
		})
	}
}

// TestResolveSourceRoot_FallsBackToBootstrapWhenSourceRefEmpty covers
// the #105 path: child Kustomizations whose YAML declares no sourceRef
// (relying on a parent KS's render-time patches to inject one) used to
// have resolveSourceRoot return ks.Path itself, which Render then
// joined against ks.Path again — producing the X/X doubled path.
//
// With the seeded bootstrap GitRepository in the store, the fallback
// now returns the working-tree root so the first reconcile resolves
// correctly even before any parent render lands.
func TestResolveSourceRoot_FallsBackToBootstrapWhenSourceRefEmpty(t *testing.T) {
	t.Parallel()
	s := store.New()
	bootstrap := &manifest.GitRepository{
		Name: "flux-system", Namespace: "flux-system",
		URL: "file:///repo",
	}
	s.AddObject(bootstrap)
	s.SetArtifact(bootstrap.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file:///repo", LocalPath: "/repo", FS: fstest.MapFS{},
	})

	c := New(s, nil, nil, false)
	ks := &manifest.Kustomization{
		Name: "foo", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "./kubernetes/apps/foo/app",
			// SourceRef intentionally absent — modelling onedr0p-style
			// children whose sourceRef is injected only by the parent
			// render's `patches:` block.
		},
	}
	got, _, err := c.newExecution(ks.Named()).resolveSource(ks)
	if err != nil {
		t.Fatalf("resolveSourceRoot: %v", err)
	}
	if got.LocalPath != "/repo" {
		t.Errorf("resolveSourceRoot=%q, want %q (bootstrap LocalPath)", got.LocalPath, "/repo")
	}
}

// TestResolveSourceRoot_ExplicitSourceRefUnchanged guards against the
// fallback accidentally overriding an explicit sourceRef.
func TestResolveSourceRoot_ExplicitSourceRefUnchanged(t *testing.T) {
	t.Parallel()
	s := store.New()
	gitrepo := &manifest.GitRepository{
		Name: "external", Namespace: "flux-system",
		URL: "file:///external",
	}
	s.AddObject(gitrepo)
	s.SetArtifact(gitrepo.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file:///external", LocalPath: "/external", FS: fstest.MapFS{},
	})
	// Seed bootstrap too — the fallback must not preempt the explicit ref.
	bootstrap := &manifest.GitRepository{
		Name: "flux-system", Namespace: "flux-system",
		URL: "file:///repo",
	}
	s.AddObject(bootstrap)
	s.SetArtifact(bootstrap.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file:///repo", LocalPath: "/repo", FS: fstest.MapFS{},
	})

	c := New(s, nil, nil, false)
	ks := &manifest.Kustomization{
		Name: "foo", Namespace: "flux-system",
		Path:      "./app",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "external", Namespace: "flux-system"},
	}
	got, _, err := c.newExecution(ks.Named()).resolveSource(ks)
	if err != nil {
		t.Fatalf("resolveSourceRoot: %v", err)
	}
	if got.LocalPath != "/external" {
		t.Errorf("resolveSourceRoot=%q, want %q (explicit ref)", got.LocalPath, "/external")
	}
}

// TestResolveSourceRoot_NoBootstrapNoSourceRefErrors keeps the safety
// net for setups that genuinely have neither — error rather than mis-
// resolve.
func TestResolveSourceRoot_NoBootstrapNoSourceRefErrors(t *testing.T) {
	t.Parallel()
	c := New(store.New(), nil, nil, false)
	ks := &manifest.Kustomization{
		Name: "foo", Namespace: "flux-system",
		Path: "./app",
	}
	_, _, err := c.newExecution(ks.Named()).resolveSource(ks)
	if err == nil {
		t.Fatalf("expected error when bootstrap source is absent")
	}
}

func TestKustomization_ContentOnlySource(t *testing.T) {
	t.Parallel()
	c, st, _ := newControllerWithFixture(t)
	freezeSourceForTest(t, st)
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "flux-system", Path: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "flux-system"}}
	st.AddObject(ks)
	var closeFirst func()
	c.Tasks.Go(t.Context(), "first", func(ctx context.Context) {
		result := c.ReconcileNode(ctx, ks.Named(), manifest.DrainNone)
		closeFirst = result.Close
	})
	c.Tasks.BlockTillDone()
	if closeFirst != nil {
		defer closeFirst()
	}
	first := st.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
	source := st.GetArtifact(ks.SourceID(), store.ArtifactContent).(*store.SourceArtifact)
	next := *source
	next.Revision = "metadata-only"
	next.Metadata = map[string]string{"org.opencontainers.image.revision": "next"}
	st.SetArtifact(ks.SourceID(), &next)
	if !first.Reads.Valid() {
		t.Fatal("metadata invalidated content-only Kustomization")
	}
	c.Tasks.Go(t.Context(), "replay", func(ctx context.Context) {
		result := c.ReconcileNode(ctx, ks.Named(), manifest.DrainNone)
		defer result.Close()
		if result.Progressed() {
			t.Error("metadata entered the renderer")
		}
	})
	c.Tasks.BlockTillDone()
	after := st.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
	if after.Fingerprint != first.Fingerprint {
		t.Fatal("metadata changed the content replay key")
	}
	replacement := next
	replacement.Content = "replacement"
	st.SetArtifact(ks.SourceID(), &replacement)
	if after.Reads.Valid() {
		t.Fatal("content replacement did not invalidate Kustomization")
	}
}
