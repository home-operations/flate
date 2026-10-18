package store_test

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestGet_ReturnsTypedObject locks the happy path: an object stored
// under id returns successfully as its concrete pointer type.
func TestGet_ReturnsTypedObject(t *testing.T) {
	t.Parallel()
	s := store.New()
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system"}
	s.AddObject(ks)

	got, ok := s.Get[*manifest.Kustomization](ks.Named())
	if !ok {
		t.Fatalf("expected hit; got miss")
	}
	if got != ks {
		t.Errorf("Get returned a different pointer; want %p got %p", ks, got)
	}
}

// TestGet_MissReturnsZeroFalse covers the absent case: nothing stored
// at id yields (zero, false) without panic.
func TestGet_MissReturnsZeroFalse(t *testing.T) {
	t.Parallel()
	s := store.New()
	id := testutil.KustomizationID("flux-system", "ghost")

	got, ok := s.Get[*manifest.Kustomization](id)
	if ok {
		t.Fatalf("expected miss; got hit")
	}
	if got != nil {
		t.Errorf("missed Get should return nil pointer; got %v", got)
	}
}

// TestGet_WrongTypeReturnsZeroFalse covers the type-mismatch case:
// id exists but stored as a different concrete type. Returns
// (zero, false) rather than panicking, matching the comma-ok shape
// of a raw type assertion.
func TestGet_WrongTypeReturnsZeroFalse(t *testing.T) {
	t.Parallel()
	s := store.New()
	hr := &manifest.HelmRelease{Name: "demo", Namespace: "default"}
	s.AddObject(hr)

	id := testutil.HelmReleaseID("default", "demo")
	got, ok := s.Get[*manifest.Kustomization](id)
	if ok {
		t.Errorf("Get with wrong type should miss; got hit %v", got)
	}
}

// TestGet_NilStoreSafe is the defensive guard — embedder constructed
// a struct holding a *Store that was never initialized. Returns
// (zero, false) without panic.
func TestGet_NilStoreSafe(t *testing.T) {
	t.Parallel()
	id := testutil.KustomizationID("flux-system", "x")
	if got, ok := (*store.Store)(nil).Get[*manifest.Kustomization](id); ok || got != nil {
		t.Errorf("nil store: want (nil, false); got (%v, %v)", got, ok)
	}
}

// TestListAs_TypedSlice locks the typed-iteration contract: every
// object stored under the requested kind comes back as the typed
// pointer slice in deterministic store order.
func TestListAs_TypedSlice(t *testing.T) {
	t.Parallel()
	s := store.New()
	for _, name := range []string{"a", "b", "c"} {
		s.AddObject(&manifest.Kustomization{Name: name, Namespace: "flux-system"})
	}
	// Add a HelmRelease to confirm kind filtering excludes other kinds.
	s.AddObject(&manifest.HelmRelease{Name: "noisy", Namespace: "flux-system"})

	s.AddObject(&manifest.RawObject{APIVersion: "example.test/v1", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "a"})
	foreign := manifest.NamedResource{Group: "example.test", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "a"}
	if got := s.ListAs[manifest.BaseManifest](foreign.Group, foreign.Kind); len(got) != 1 || got[0].Named() != foreign {
		t.Fatalf("foreign collection = %v", got)
	}
	if got := s.ListAs[manifest.BaseManifest]("", manifest.KindKustomization); len(got) != 0 {
		t.Fatalf("core collection borrowed noncore objects: %v", got)
	}
	core := &manifest.RawObject{APIVersion: "v1", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "a"}
	s.AddObject(core)
	if got := s.ListAs[*manifest.RawObject]("", manifest.KindKustomization); len(got) != 1 || got[0] != core {
		t.Fatalf("exact core collection = %v", got)
	}
	if got := s.ListAs[manifest.BaseManifest]("", ""); len(got) != 6 {
		t.Fatalf("typed inventory = %v", got)
	}
	if got := s.ListAs[manifest.BaseManifest](foreign.Group, ""); len(got) != 0 {
		t.Fatalf("group-only collection = %v", got)
	}
	got := s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)
	if len(got) != 3 {
		t.Fatalf("expected 3 KSes; got %d", len(got))
	}
	seen := map[string]bool{}
	for _, ks := range got {
		seen[ks.Name] = true
	}
	for _, n := range []string{"a", "b", "c"} {
		if !seen[n] {
			t.Errorf("missing KS %q", n)
		}
	}
}

// TestListAs_NilStoreReturnsNil mirrors Get's nil-store guard.
func TestListAs_NilStoreReturnsNil(t *testing.T) {
	t.Parallel()
	if got := (*store.Store)(nil).ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization); got != nil {
		t.Errorf("nil store: want nil slice; got %v", got)
	}
}

// TestListAs_EmptyKindReturnsEmpty covers a kind with no entries.
// Should return a non-nil but length-0 slice, suitable for direct
// iteration without nil-check.
func TestListAs_EmptyKindReturnsEmpty(t *testing.T) {
	t.Parallel()
	s := store.New()
	got := s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)
	if got == nil {
		t.Errorf("want empty slice, not nil")
	}
	if len(got) != 0 {
		t.Errorf("want length 0; got %d", len(got))
	}
}
