package store_test

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"

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
	{
		assert.FatalIf(t, !ok, "expected hit; got miss")
	}
	{
		assert.ErrorIf(t, got != ks, "Get returned a different pointer; want %p got %p", ks, got)
	}
}

// TestGet_MissReturnsZeroFalse covers the absent case: nothing stored
// at id yields (zero, false) without panic.
func TestGet_MissReturnsZeroFalse(t *testing.T) {
	t.Parallel()
	s := store.New()
	id := testutil.KustomizationID("flux-system", "ghost")

	got, ok := s.Get[*manifest.Kustomization](id)
	{
		assert.FatalIf(t, ok, "expected miss; got hit")
	}
	{
		assert.ErrorIf(t, got != nil, "missed Get should return nil pointer; got %v", got)
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
	{
		assert.ErrorIf(t, ok, "Get with wrong type should miss; got hit %v", got)
	}
}

// TestGet_NilStoreSafe is the defensive guard — embedder constructed
// a struct holding a *Store that was never initialized. Returns
// (zero, false) without panic.
func TestGet_NilStoreSafe(t *testing.T) {
	t.Parallel()
	id := testutil.KustomizationID("flux-system", "x")
	{
		got, ok := (*store.Store)(nil).Get[*manifest.Kustomization](id)
		assert.ErrorIf(t, ok || got != nil, "nil store: want (nil, false); got (%v, %v)", got, ok)
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

	s.AddObject(
		&manifest.RawObject{
			APIVersion: "example.test/v1",
			Kind:       manifest.KindKustomization,
			Namespace:  "flux-system",
			Name:       "a",
		},
	)
	foreign := manifest.NamedResource{
		Group:     "example.test",
		Kind:      manifest.KindKustomization,
		Namespace: "flux-system",
		Name:      "a",
	}
	{
		got := s.ListAs[manifest.BaseManifest](foreign.Group, foreign.Kind)
		assert.FatalIf(t, len(got) != 1 || got[0].Named() != foreign, "foreign collection = %v", got)
	}
	{
		got := s.ListAs[manifest.BaseManifest]("", manifest.KindKustomization)
		assert.FatalIf(t, len(got) != 0, "core collection borrowed noncore objects: %v", got)
	}
	core := &manifest.RawObject{APIVersion: "v1", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "a"}
	s.AddObject(core)
	{
		got := s.ListAs[*manifest.RawObject]("", manifest.KindKustomization)
		assert.FatalIf(t, len(got) != 1 || got[0] != core, "exact core collection = %v", got)
	}
	{
		got := s.ListAs[manifest.BaseManifest]("", "")
		assert.FatalIf(t, len(got) != 6, "typed inventory = %v", got)
	}
	{
		got := s.ListAs[manifest.BaseManifest](foreign.Group, "")
		assert.FatalIf(t, len(got) != 0, "group-only collection = %v", got)
	}
	got := s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)
	{
		assert.FatalIf(t, len(got) != 3, "expected 3 KSes; got %d", len(got))
	}
	seen := map[string]bool{}
	for _, ks := range got {
		seen[ks.Name] = true
	}
	for _, n := range []string{"a", "b", "c"} {
		{
			assert.ErrorIf(t, !seen[n], "missing KS %q", n)
		}
	}
}

// TestListAs_NilStoreReturnsNil mirrors Get's nil-store guard.
func TestListAs_NilStoreReturnsNil(t *testing.T) {
	t.Parallel()
	{
		got := (*store.Store)(nil).ListAs[*manifest.Kustomization](
			manifest.FluxKustomizeDomain,
			manifest.KindKustomization,
		)
		assert.ErrorIf(t, got != nil, "nil store: want nil slice; got %v", got)
	}
}

// TestListAs_EmptyKindReturnsEmpty covers a kind with no entries.
// Should return a non-nil but length-0 slice, suitable for direct
// iteration without nil-check.
func TestListAs_EmptyKindReturnsEmpty(t *testing.T) {
	t.Parallel()
	s := store.New()
	got := s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)
	{
		assert.ErrorIf(t, got == nil, "want empty slice, not nil")
	}
	{
		assert.ErrorIf(t, len(got) != 0, "want length 0; got %d", len(got))
	}
}
