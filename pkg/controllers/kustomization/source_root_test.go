package kustomization

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestResolveSourceRoot_FallsBackToBootstrapWhenSourceRefEmpty covers
// the #105 path: child Kustomizations whose YAML declares no sourceRef
// (relying on a parent KS's render-time patches to inject one) used to
// have resolveSourceRoot return ks.Path itself, which RenderFlux then
// joined against ks.Path again — producing the X/X doubled path.
//
// With the seeded bootstrap GitRepository in the store, the fallback
// now returns the working-tree root so the first reconcile resolves
// correctly even before any parent render lands.
func TestResolveSourceRoot_FallsBackToBootstrapWhenSourceRefEmpty(t *testing.T) {
	s := store.New()
	bootstrap := &manifest.GitRepository{
		Name: "flux-system", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file:///repo"},
	}
	s.AddObject(bootstrap)
	s.SetArtifact(bootstrap.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file:///repo", LocalPath: "/repo",
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
	got, _, err := c.resolveSource(ks)
	if err != nil {
		t.Fatalf("resolveSourceRoot: %v", err)
	}
	if got != "/repo" {
		t.Errorf("resolveSourceRoot=%q, want %q (bootstrap LocalPath)", got, "/repo")
	}
}

// TestResolveSourceRoot_ExplicitSourceRefUnchanged guards against the
// fallback accidentally overriding an explicit sourceRef.
func TestResolveSourceRoot_ExplicitSourceRefUnchanged(t *testing.T) {
	s := store.New()
	gitrepo := &manifest.GitRepository{
		Name: "external", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file:///external"},
	}
	s.AddObject(gitrepo)
	s.SetArtifact(gitrepo.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file:///external", LocalPath: "/external",
	})
	// Seed bootstrap too — the fallback must not preempt the explicit ref.
	bootstrap := &manifest.GitRepository{
		Name: "flux-system", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file:///repo"},
	}
	s.AddObject(bootstrap)
	s.SetArtifact(bootstrap.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file:///repo", LocalPath: "/repo",
	})

	c := New(s, nil, nil, false)
	ks := &manifest.Kustomization{
		Name: "foo", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./app"},
		SourceKind:        manifest.KindGitRepository,
		SourceName:        "external",
		SourceNamespace:   "flux-system",
	}
	got, _, err := c.resolveSource(ks)
	if err != nil {
		t.Fatalf("resolveSourceRoot: %v", err)
	}
	if got != "/external" {
		t.Errorf("resolveSourceRoot=%q, want %q (explicit ref)", got, "/external")
	}
}

// TestResolveSourceRoot_NoBootstrapNoSourceRefErrors keeps the safety
// net for setups that genuinely have neither — error rather than mis-
// resolve.
func TestResolveSourceRoot_NoBootstrapNoSourceRefErrors(t *testing.T) {
	c := New(store.New(), nil, nil, false)
	ks := &manifest.Kustomization{
		Name: "foo", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./app"},
	}
	_, _, err := c.resolveSource(ks)
	if err == nil {
		t.Fatalf("expected error when bootstrap source is absent")
	}
}
