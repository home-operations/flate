package helm

import (
	"context"
	"strings"
	"sync"
	"testing"

	chartcommon "helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// TestTemplateCache_GetMissReturnsFalse pins the trivial-but-load-bearing
// invariant: an empty cache miss is (zero, false), not a panic. Caller-
// facing contract — Template's "cached, ok := c.Get(key)" branch relies
// on it.
func TestTemplateCache_GetMissReturnsFalse(t *testing.T) {
	c := newTemplateCache(1024, nil)
	if _, ok := c.Get("nope"); ok {
		t.Fatalf("expected miss on empty cache, got hit")
	}
}

// TestTemplateCache_NilReceiverNoops locks the "disabled cache" sentinel
// contract: a nil receiver returns clean misses from Get and silently
// swallows Put. The render path relies on this so Template's branches
// don't need an explicit "is cache wired" guard at every call site.
func TestTemplateCache_NilReceiverNoops(t *testing.T) {
	var c *templateCache
	if _, ok := c.Get("anything"); ok {
		t.Fatalf("nil receiver Get must miss")
	}
	c.Put("anything", "value") // must not panic
	if got := c.Size(); got != 0 {
		t.Fatalf("nil receiver Size should report 0, got %d", got)
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("nil receiver Len should report 0, got %d", got)
	}
}

// TestTemplateCache_DisabledOnZeroLimit pins the constructor contract:
// a <=0 limit returns nil, which the render path treats as "disabled".
// Distinct from a positive but tiny limit (which would still construct
// a cache that just evicts everything).
func TestTemplateCache_DisabledOnZeroLimit(t *testing.T) {
	if newTemplateCache(0, nil) != nil {
		t.Errorf("limit=0 should disable the cache (nil)")
	}
	if newTemplateCache(-1, nil) != nil {
		t.Errorf("negative limit should disable the cache (nil)")
	}
	if newTemplateCache(1, nil) == nil {
		t.Errorf("positive limit must construct a real cache")
	}
}

// TestTemplateCache_LRUEvictionByLimit covers the size-bounded eviction
// pinned in the plan: insert 5 entries with cumulative cost > limit;
// the oldest entries fall off the back until total ≤ limit. The exact
// "oldest 2 evicted" claim from the plan parametrizes here as: with
// 100-byte entries and a 300-byte limit, only the last 3 inserts
// survive (matching plan §2.2 step 1).
func TestTemplateCache_LRUEvictionByLimit(t *testing.T) {
	c := newTemplateCache(300, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		// Each value is exactly 100 bytes so the math is obvious.
		c.Put(k, strings.Repeat("x", 100))
	}
	for _, k := range []string{"c", "d", "e"} {
		if _, ok := c.Get(k); !ok {
			t.Errorf("expected %q in cache, got eviction", k)
		}
	}
	for _, k := range []string{"a", "b"} {
		if _, ok := c.Get(k); ok {
			t.Errorf("expected %q evicted, still in cache", k)
		}
	}
	if got := c.Len(); got != 3 {
		t.Errorf("Len after eviction = %d, want 3", got)
	}
	if got := c.Size(); got != 300 {
		t.Errorf("Size after eviction = %d, want 300", got)
	}
}

// TestTemplateCache_GetPromotesToFront pins the LRU ordering invariant:
// Get on an existing key moves it to the front, so a subsequent eviction
// drops a DIFFERENT entry than it would have without the Get. The plan
// codifies this exact case: insert A B C, Get A, insert D — D's
// insertion evicts B (the new LRU tail), not A (now the MRU).
func TestTemplateCache_GetPromotesToFront(t *testing.T) {
	c := newTemplateCache(300, nil)
	c.Put("a", strings.Repeat("x", 100))
	c.Put("b", strings.Repeat("x", 100))
	c.Put("c", strings.Repeat("x", 100))
	if _, ok := c.Get("a"); !ok {
		t.Fatalf("setup: a should be in cache before promotion test")
	}
	c.Put("d", strings.Repeat("x", 100))

	if _, ok := c.Get("a"); !ok {
		t.Errorf("a was promoted via Get; insertion of d must NOT evict it")
	}
	if _, ok := c.Get("c"); !ok {
		t.Errorf("c should still be present (it was not the LRU at d-insert)")
	}
	if _, ok := c.Get("d"); !ok {
		t.Errorf("d should be present (just inserted)")
	}
	if _, ok := c.Get("b"); ok {
		t.Errorf("b should have been evicted (it was the LRU at d-insert)")
	}
}

// TestTemplateCache_SizeTracking pins the running-total accounting:
// inserts add their byte cost, evictions subtract it, and a key
// replacement only counts the new entry's cost (not the sum of both).
func TestTemplateCache_SizeTracking(t *testing.T) {
	c := newTemplateCache(1024, nil)
	c.Put("x", strings.Repeat("y", 100))
	if got := c.Size(); got != 100 {
		t.Errorf("Size after one insert = %d, want 100", got)
	}
	// Replace with a larger value — old 100 dropped, new 250 added.
	c.Put("x", strings.Repeat("y", 250))
	if got := c.Size(); got != 250 {
		t.Errorf("Size after replacement = %d, want 250 (not 350)", got)
	}
	if got := c.Len(); got != 1 {
		t.Errorf("Len after replacement = %d, want 1", got)
	}
}

// TestTemplateCache_OversizedEntryRejected pins the "single entry
// exceeds limit" guard: such entries are dropped silently rather than
// thrashing every other entry out to make room (the entry would just
// be the next eviction target anyway).
func TestTemplateCache_OversizedEntryRejected(t *testing.T) {
	c := newTemplateCache(100, nil)
	c.Put("small", strings.Repeat("y", 50))
	c.Put("huge", strings.Repeat("y", 200))
	if _, ok := c.Get("huge"); ok {
		t.Errorf("oversized entry should be rejected, but was cached")
	}
	if _, ok := c.Get("small"); !ok {
		t.Errorf("oversized-entry rejection must NOT churn existing entries")
	}
	if got := c.Size(); got != 50 {
		t.Errorf("Size after rejection = %d, want 50", got)
	}
}

// TestTemplateCache_ReplaceDoesNotDuplicate pins that re-Putting the
// same key under the same value doesn't grow the cache twice — the
// stale entry is removed before the fresh one lands.
func TestTemplateCache_ReplaceDoesNotDuplicate(t *testing.T) {
	c := newTemplateCache(1024, nil)
	c.Put("k", "value")
	c.Put("k", "value")
	if got := c.Len(); got != 1 {
		t.Errorf("Len after duplicate Put = %d, want 1", got)
	}
	if got := c.Size(); got != int64(len("value")) {
		t.Errorf("Size after duplicate Put = %d, want %d", got, len("value"))
	}
}

// TestTemplateCache_ConcurrentSafety drives a few hundred parallel
// Get/Put pairs through the cache to surface any mutex-protected
// invariant the LRU might be violating. The pass condition is "no
// race detector / panic"; the cached values themselves are arbitrary.
func TestTemplateCache_ConcurrentSafety(t *testing.T) {
	c := newTemplateCache(64<<10, nil)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			for j := range 200 {
				key := string(rune('a' + (i+j)%26))
				c.Put(key, strings.Repeat(key, 8))
				_, _ = c.Get(key)
			}
		})
	}
	wg.Wait()
	// Size should never exceed the limit even under contention.
	if got := c.Size(); got > 64<<10 {
		t.Errorf("Size after concurrent churn = %d, exceeds limit", got)
	}
}

// TestComputeTemplateKey_StableSameInputs pins the determinism
// guarantee: the same (chartFP, values, opts, hr) tuple produces the
// same key on every call. encoding/json sorts map keys, so even maps
// with different in-memory insertion orders should collide. Without
// this, the cache would be a write-only structure (every render misses).
func TestComputeTemplateKey_StableSameInputs(t *testing.T) {
	ch := minimalChart()
	hr := minimalHR()
	values1 := map[string]any{"a": 1, "b": 2}
	values2 := map[string]any{"b": 2, "a": 1} // different in-memory order
	opts := Options{KubeVersion: "1.33"}

	k1 := computeTemplateKey("fp", ch, values1, opts, hr)
	k2 := computeTemplateKey("fp", ch, values2, opts, hr)
	if k1 != k2 {
		t.Errorf("same logical values must produce same key:\nk1=%s\nk2=%s", k1, k2)
	}
}

// TestComputeTemplateKey_DifferingFieldsDiverge pins that every keyed
// input dimension actually affects the digest. A regression where a
// new render-affecting field gets added without being mixed in would
// allow the cache to serve stale renders — the symptom we MUST catch.
//
// Each subtest mutates one input from the baseline and asserts the
// key changes.
func TestComputeTemplateKey_DifferingFieldsDiverge(t *testing.T) {
	baseChart := minimalChart()
	baseHR := minimalHR()
	baseValues := map[string]any{"k": "v"}
	baseOpts := Options{KubeVersion: "1.33"}
	baseKey := computeTemplateKey("fp", baseChart, baseValues, baseOpts, baseHR)

	t.Run("ChartFingerprint", func(t *testing.T) {
		if got := computeTemplateKey("fp-different", baseChart, baseValues, baseOpts, baseHR); got == baseKey {
			t.Error("different chart fingerprint did not change the key")
		}
	})

	t.Run("Values", func(t *testing.T) {
		altValues := map[string]any{"k": "different"}
		if got := computeTemplateKey("fp", baseChart, altValues, baseOpts, baseHR); got == baseKey {
			t.Error("different values did not change the key")
		}
	})

	t.Run("OptsKubeVersion", func(t *testing.T) {
		alt := baseOpts
		alt.KubeVersion = "1.32"
		if got := computeTemplateKey("fp", baseChart, baseValues, alt, baseHR); got == baseKey {
			t.Error("different KubeVersion did not change the key")
		}
	})

	t.Run("OptsSkipCRDs", func(t *testing.T) {
		alt := baseOpts
		alt.SkipCRDs = !baseOpts.SkipCRDs
		if got := computeTemplateKey("fp", baseChart, baseValues, alt, baseHR); got == baseKey {
			t.Error("different SkipCRDs did not change the key")
		}
	})

	t.Run("OptsAPIVersions", func(t *testing.T) {
		alt := baseOpts
		alt.APIVersions = "v1,apps/v1"
		if got := computeTemplateKey("fp", baseChart, baseValues, alt, baseHR); got == baseKey {
			t.Error("different APIVersions did not change the key")
		}
	})

	t.Run("OptsShowOnly", func(t *testing.T) {
		alt := baseOpts
		alt.ShowOnly = []string{"templates/cm.yaml"}
		if got := computeTemplateKey("fp", baseChart, baseValues, alt, baseHR); got == baseKey {
			t.Error("different ShowOnly did not change the key")
		}
	})

	t.Run("HRReleaseName", func(t *testing.T) {
		alt := *baseHR
		alt.HelmReleaseSpec.ReleaseName = "different"
		if got := computeTemplateKey("fp", baseChart, baseValues, baseOpts, &alt); got == baseKey {
			t.Error("different ReleaseName did not change the key")
		}
	})

	t.Run("HRChartValuesFiles", func(t *testing.T) {
		alt := *baseHR
		alt.ChartValuesFiles = []string{"values-prod.yaml"}
		if got := computeTemplateKey("fp", baseChart, baseValues, baseOpts, &alt); got == baseKey {
			t.Error("different ChartValuesFiles did not change the key")
		}
	})

	t.Run("HRCRDsPolicy", func(t *testing.T) {
		alt := *baseHR
		alt.CRDsPolicy = "Skip"
		if got := computeTemplateKey("fp", baseChart, baseValues, baseOpts, &alt); got == baseKey {
			t.Error("different CRDsPolicy did not change the key")
		}
	})
}

// TestTemplateCache_TemplateIntegration covers the on-the-render-path
// behavior: a Template call against the same HR/values/opts populates
// the cache, and a second call serves a byte-identical result without
// re-running action.Install.RunWithContext. The byte-identity assertion
// is the load-bearing one: any divergence between cached and uncached
// output would be a correctness bug.
func TestTemplateCache_TemplateIntegration(t *testing.T) {
	// Stage a chart whose template references .Values.greeting so
	// different values actually produce different rendered output —
	// the helmChartFixture chart hardcodes `k: v` and would not
	// surface a stale-cache regression on value mutation.
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml",
		"apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: t\n")
	testutil.WriteFile(t, dir, "mychart/values.yaml", "greeting: hi\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
data:
  greeting: {{ .Values.greeting }}
`)
	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
	hr := newHR()
	ctx := context.Background()

	first, err := cli.Template(ctx, hr, map[string]any{"greeting": "hi"}, Options{})
	if err != nil {
		t.Fatalf("first Template: %v", err)
	}
	if cli.templateCache.Len() != 1 {
		t.Errorf("cache should hold 1 entry after first render, got %d", cli.templateCache.Len())
	}
	second, err := cli.Template(ctx, hr, map[string]any{"greeting": "hi"}, Options{})
	if err != nil {
		t.Fatalf("second Template: %v", err)
	}
	if first != second {
		t.Errorf("cached render diverged from initial render:\nfirst:\n%s\nsecond:\n%s", first, second)
	}
	if cli.templateCache.Len() != 1 {
		t.Errorf("cache should still hold 1 entry after second render, got %d", cli.templateCache.Len())
	}

	// Different values must produce a distinct cache entry, not
	// silently serve the original.
	third, err := cli.Template(ctx, hr, map[string]any{"greeting": "different"}, Options{})
	if err != nil {
		t.Fatalf("third Template: %v", err)
	}
	if third == first {
		t.Errorf("different values served same cached output (cache key broken)")
	}
	if cli.templateCache.Len() != 2 {
		t.Errorf("cache should hold 2 entries after distinct render, got %d", cli.templateCache.Len())
	}
}

// TestTemplateCache_DisabledViaOptions pins that NewClientWithOptions(
// TemplateCacheBytes=0) actually wires nil through to c.templateCache —
// the render path still works and produces correct output, just
// without caching.
func TestTemplateCache_DisabledViaOptions(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml",
		"apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: t\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  k: v\n")

	cli, err := NewClientWithOptions(cacheroot.New(t.TempDir()), ClientOptions{TemplateCacheBytes: 0})
	if err != nil {
		t.Fatalf("NewClientWithOptions: %v", err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
	if cli.templateCache != nil {
		t.Fatalf("TemplateCacheBytes=0 must leave templateCache nil")
	}

	hr := newHR()
	if _, err := cli.Template(context.Background(), hr, nil, Options{}); err != nil {
		t.Fatalf("Template with disabled cache: %v", err)
	}
}

// minimalChart returns a tiny *chart.Chart fixture for the key-derivation
// tests; it shares the same shape the real LoadChart path produces.
func minimalChart() *chart.Chart {
	return &chart.Chart{
		Metadata: &chart.Metadata{Name: "test", Version: "0.1.0", APIVersion: "v2"},
		Templates: []*chartcommon.File{
			{Name: "templates/cm.yaml", Data: []byte("kind: ConfigMap\n")},
		},
		Values: map[string]any{"default": true},
	}
}

// minimalHR returns a tiny *manifest.HelmRelease for the key-derivation
// tests. Mirrors the structural shape Template's call sites observe.
func minimalHR() *manifest.HelmRelease {
	return &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "default",
	}
}
