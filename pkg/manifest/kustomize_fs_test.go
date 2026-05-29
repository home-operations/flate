package manifest

import (
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

// writeKustomization is a tiny inline fixture writer; manifest can't
// import internal/testutil (cycle: testutil imports manifest).
func writeKustomization(t *testing.T, root, rel, body string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(p), err)
	}
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
}

// TestComponentCache_HitAndMiss asserts ComponentCache.Get returns
// the same result on the first (miss) and second (hit) call for the
// same (repoRoot, base) — pinning the cache's "stable observable
// content" contract from the perf refactor plan's cache-invalidation
// risk note.
func TestComponentCache_HitAndMiss(t *testing.T) {
	root := t.TempDir()
	base := "apps/main"
	writeKustomization(t, root, filepath.Join(base, "kustomization.yaml"), `
apiVersion: kustomize.config.k8s.io/v1
kind: Kustomization
components:
  - ../components/cluster-settings
  - ./component-a
`)

	cache := NewComponentCache()
	miss := cache.Get(root, base)
	want := []string{"../components/cluster-settings", "./component-a"}
	if !reflect.DeepEqual(miss, want) {
		t.Fatalf("miss = %v, want %v", miss, want)
	}
	hit := cache.Get(root, base)
	if !reflect.DeepEqual(hit, want) {
		t.Fatalf("hit = %v, want %v", hit, want)
	}
	// Cache stores by-reference; the two calls must hand out the
	// same backing slice (verifies no per-call re-read).
	if len(miss) > 0 && &miss[0] != &hit[0] {
		t.Errorf("hit returned a fresh slice; expected the cached entry to be reused")
	}
}

// TestComponentCache_NilReceiverFallsThrough confirms the nil-cache
// short-circuit: callers that don't want caching pass nil and get
// the bare ReadKustomizeComponents result.
func TestComponentCache_NilReceiverFallsThrough(t *testing.T) {
	root := t.TempDir()
	base := "apps/x"
	writeKustomization(t, root, filepath.Join(base, "kustomization.yaml"), `
apiVersion: kustomize.config.k8s.io/v1
kind: Kustomization
components: [./c1]
`)
	var nilCache *ComponentCache
	got := nilCache.Get(root, base)
	want := []string{"./c1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("nil-receiver Get = %v, want %v", got, want)
	}
}

// TestComponentCache_SharedAcrossConsumers asserts two consumers
// reading the same (repoRoot, base) through one cache observe the
// same data even when the second one would have read a different
// answer from disk — i.e. the cache is the source of truth once
// populated.
//
// We mutate the on-disk kustomization.yaml between the two calls;
// the second call must still return the cached value. This is the
// "cache shared across consumers" contract from the plan: loader,
// discovery's orphan pass, and the change Filter all read through
// the same instance and must agree.
func TestComponentCache_SharedAcrossConsumers(t *testing.T) {
	root := t.TempDir()
	base := "apps/shared"
	writeKustomization(t, root, filepath.Join(base, "kustomization.yaml"), `
apiVersion: kustomize.config.k8s.io/v1
kind: Kustomization
components: [./first]
`)
	cache := NewComponentCache()

	consumerA := cache.Get(root, base)
	wantA := []string{"./first"}
	if !reflect.DeepEqual(consumerA, wantA) {
		t.Fatalf("consumerA = %v, want %v", consumerA, wantA)
	}

	// Mutate the on-disk file; without the shared cache, consumerB
	// would observe the new contents. With it, consumerB must see
	// what consumerA saw.
	writeKustomization(t, root, filepath.Join(base, "kustomization.yaml"), `
apiVersion: kustomize.config.k8s.io/v1
kind: Kustomization
components: [./second]
`)
	consumerB := cache.Get(root, base)
	if !reflect.DeepEqual(consumerB, wantA) {
		t.Errorf("consumerB = %v, want shared (cached) %v", consumerB, wantA)
	}
}

// TestComponentCache_ConcurrentReads exercises the RWMutex
// guarantees: many goroutines hitting Get on the same key see
// consistent results and the cache holds one canonical entry.
func TestComponentCache_ConcurrentReads(t *testing.T) {
	root := t.TempDir()
	base := "apps/race"
	writeKustomization(t, root, filepath.Join(base, "kustomization.yaml"), `
apiVersion: kustomize.config.k8s.io/v1
kind: Kustomization
components: [./alpha, ./beta]
`)
	cache := NewComponentCache()
	want := []string{"./alpha", "./beta"}

	const goroutines = 32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			got := cache.Get(root, base)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("concurrent Get = %v, want %v", got, want)
			}
		}()
	}
	wg.Wait()
}
