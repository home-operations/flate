package store_test

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestGet_ReturnsTypedObject locks the happy path: an object stored
// under id returns successfully as its concrete pointer type.
func TestGet_ReturnsTypedObject(t *testing.T) {
	s := store.New()
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system"}
	s.AddObject(ks)

	got, ok := store.Get[*manifest.Kustomization](s, ks.Named())
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
	s := store.New()
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "ghost"}

	got, ok := store.Get[*manifest.Kustomization](s, id)
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
	s := store.New()
	hr := &manifest.HelmRelease{Name: "demo", Namespace: "default"}
	s.AddObject(hr)

	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "default", Name: "demo"}
	got, ok := store.Get[*manifest.Kustomization](s, id)
	if ok {
		t.Errorf("Get with wrong type should miss; got hit %v", got)
	}
}

// TestGet_NilStoreSafe is the defensive guard — embedder constructed
// a struct holding a *Store that was never initialized. Returns
// (zero, false) without panic.
func TestGet_NilStoreSafe(t *testing.T) {
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "x"}
	if got, ok := store.Get[*manifest.Kustomization](nil, id); ok || got != nil {
		t.Errorf("nil store: want (nil, false); got (%v, %v)", got, ok)
	}
}

// TestGetByName_ReturnsTypedObject covers the (kind, namespace,
// name) lookup path: GetByName resolves the store's secondary index
// and asserts to T in one step.
func TestGetByName_ReturnsTypedObject(t *testing.T) {
	s := store.New()
	sec := &manifest.Secret{Name: "creds", Namespace: "flux-system"}
	s.AddObject(sec)

	got, ok := store.GetByName[*manifest.Secret](s, manifest.KindSecret, "flux-system", "creds")
	if !ok {
		t.Fatalf("expected hit; got miss")
	}
	if got != sec {
		t.Errorf("GetByName returned a different pointer; want %p got %p", sec, got)
	}
}

// TestGetByName_NilStoreSafe mirrors Get's defensive nil guard.
func TestGetByName_NilStoreSafe(t *testing.T) {
	if got, ok := store.GetByName[*manifest.Secret](nil, manifest.KindSecret, "ns", "name"); ok || got != nil {
		t.Errorf("nil store: want (nil, false); got (%v, %v)", got, ok)
	}
}

// TestListAs_TypedSlice locks the typed-iteration contract: every
// object stored under the requested kind comes back as the typed
// pointer slice, in store-iteration order (unsorted, but we just
// verify membership).
func TestListAs_TypedSlice(t *testing.T) {
	s := store.New()
	for _, name := range []string{"a", "b", "c"} {
		s.AddObject(&manifest.Kustomization{Name: name, Namespace: "flux-system"})
	}
	// Add a HelmRelease to confirm kind filtering excludes other kinds.
	s.AddObject(&manifest.HelmRelease{Name: "noisy", Namespace: "flux-system"})

	got := store.ListAs[*manifest.Kustomization](s, manifest.KindKustomization)
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
	if got := store.ListAs[*manifest.Kustomization](nil, manifest.KindKustomization); got != nil {
		t.Errorf("nil store: want nil slice; got %v", got)
	}
}

// TestListAs_EmptyKindReturnsEmpty covers a kind with no entries.
// Should return a non-nil but length-0 slice, suitable for direct
// iteration without nil-check.
func TestListAs_EmptyKindReturnsEmpty(t *testing.T) {
	s := store.New()
	got := store.ListAs[*manifest.Kustomization](s, manifest.KindKustomization)
	if got == nil {
		t.Errorf("want empty slice, not nil")
	}
	if len(got) != 0 {
		t.Errorf("want length 0; got %d", len(got))
	}
}
