package helm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	chartcommon "helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/diskcache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

func TestTemplateCache_OCIIdentity(t *testing.T) {
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	const next = "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	shared := digest[:len(digest)-1] + "9"
	initial := store.SourceArtifact{Digest: digest, Revision: "6.15.0@" + digest}
	for _, cache := range []struct {
		name         string
		memory, disk bool
	}{
		{"disabled", false, false}, {"memory", true, false}, {"disk", false, true}, {"memory and disk", true, true},
	} {
		for _, disable := range []bool{false, true} {
			for _, tc := range []struct {
				name                                   string
				initial, next                          store.SourceArtifact
				wantSameOutput, wantFingerprintChanged bool
				marker                                 string
			}{
				{name: "unchanged", initial: initial, next: initial, wantSameOutput: true},
				{name: "same suffix", initial: initial, next: store.SourceArtifact{Digest: shared, Revision: "6.15.0@" + shared}, wantSameOutput: true, wantFingerprintChanged: true},
				{name: "digest only", initial: initial, next: store.SourceArtifact{Digest: shared, Revision: initial.Revision}, wantSameOutput: true, wantFingerprintChanged: true},
				{name: "revision only", initial: initial, next: store.SourceArtifact{Digest: digest, Revision: "latest@" + digest}, wantSameOutput: true, wantFingerprintChanged: true},
				{name: "changed suffix", initial: initial, next: store.SourceArtifact{Digest: next, Revision: "6.15.0@" + next}, wantSameOutput: disable, wantFingerprintChanged: true},
				{name: "new values", initial: initial, next: store.SourceArtifact{Digest: shared, Revision: "6.15.0@" + shared}, marker: "updated", wantFingerprintChanged: true},
			} {
				t.Run(fmt.Sprintf("%s/disabled_%t/%s", cache.name, disable, tc.name), func(t *testing.T) {
					st, hr, dir := ociRenderFixture(t)
					original := hr.Clone()
					layout := cacheroot.New(t.TempDir())
					clientOpts := ClientOptions{}
					if cache.memory {
						clientOpts.TemplateCacheBytes = 1 << 20
					}
					if cache.disk {
						clientOpts.RenderCacheBytes = 1 << 20
						clientOpts.RenderCacheRoot = layout.RenderHelmCache()
					}
					newClient := func() *Client {
						cli, err := NewClientWithOptions(layout, clientOpts)
						if err != nil {
							t.Fatal(err)
						}
						cli.SetSourceResolver(NewStoreSourceResolver(st))
						return cli
					}
					cli := newClient()
					opts := Options{DisableChartDigestTracking: disable}
					render := func(identity store.SourceArtifact) (string, string, string, string) {
						t.Helper()
						identity.Kind, identity.LocalPath = manifest.KindOCIRepository, dir
						st.SetArtifact(manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &identity)
						out, err := cli.Template(t.Context(), hr, nil, opts)
						if err != nil {
							t.Fatal(err)
						}
						loaded, err := cli.LoadChart(t.Context(), hr)
						if err != nil {
							t.Fatal(err)
						}
						assert.Equal(t, loaded.Chart.Metadata.Version, "6.15.0")
						assert.Equal(t, loaded.Chart.Dependencies()[0].Metadata.Version, "1.2.3+child")
						if loaded.Fingerprint == "" {
							t.Fatal("missing chart content fingerprint")
						}
						fp := ociChartFingerprint(loaded.Fingerprint, &identity, disable)
						valuesKey := chartValuesCacheKey(fp, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
						merged, ok := cli.chartValuesCache[valuesKey]
						if !ok {
							t.Fatal("render did not cache values under its loaded fingerprint")
						}
						key := computeTemplateKey(fp, loaded.Chart, merged, opts, hr)
						if cli.templateCache != nil {
							cached, ok := cli.templateCache.Get(key)
							if !ok || cached != out {
								t.Fatal("render was not cached under its effective key")
							}
						}
						repeated, err := cli.Template(t.Context(), hr, nil, opts)
						if err != nil || repeated != out {
							t.Fatalf("repeat render differs: %v", err)
						}
						return out, fp, key, valuesKey
					}
					first, fp, key, valuesKey := render(tc.initial)
					if !strings.Contains(first, `marker: "original"`) {
						t.Fatal(first)
					}
					const sentinel = "cached render proof"
					if cli.templateCache != nil {
						cli.templateCache.Put(key, sentinel)
					}
					if tc.marker != "" {
						template, err := os.ReadFile(filepath.Join(dir, "templates/cm.yaml"))
						if err != nil {
							t.Fatal(err)
						}
						dir = t.TempDir()
						writeChartFiles(t, dir, "podinfo", "6.15.0")
						testutil.WriteFile(t, dir, "values.yaml", "marker: default\n")
						testutil.WriteFile(t, dir, "prod.yaml", "marker: "+tc.marker+"\n")
						testutil.WriteFile(t, dir, "charts/child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.2.3+child\n")
						testutil.WriteFile(t, dir, "templates/cm.yaml", string(template))
					}
					second, nextFP, nextKey, nextValuesKey := render(tc.next)
					assert.Equal(t, second == sentinel, cli.templateCache != nil && !tc.wantFingerprintChanged)
					if cache.disk {
						fresh := newClient()
						assert.Equal(t, fresh.templateCache.Len(), 0)
						persisted, err := fresh.Template(t.Context(), hr, nil, opts)
						if err != nil {
							t.Fatal(err)
						}
						assert.Equal(t, persisted, second)
						assert.Equal(t, persisted == sentinel, !tc.wantFingerprintChanged)
					}
					if cli.templateCache != nil {
						cli.templateCache.Put(key, first)
						if !tc.wantFingerprintChanged {
							second, _, _, _ = render(tc.next)
						}
					}
					assert.Equal(t, first == second, tc.wantSameOutput)
					assert.Equal(t, fp != nextFP, tc.wantFingerprintChanged)
					assert.Equal(t, key != nextKey, tc.wantFingerprintChanged)
					assert.Equal(t, valuesKey != nextValuesKey, tc.wantFingerprintChanged)
					if tc.marker != "" && !strings.Contains(second, `marker: "`+tc.marker+`"`) {
						t.Fatal(second)
					}
					assert.Diff(t, hr, original)
					for _, revision := range []string{"6.14.0@" + next, "invalid"} {
						st.SetArtifact(manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: dir, Digest: next, Revision: revision})
						if _, err := cli.Template(t.Context(), hr, nil, opts); !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
							t.Fatalf("invalid revision bypassed validation: %v", err)
						}
					}
				})
			}
		}
	}
}

func TestTemplateCache_NonOCITrackingMode(t *testing.T) {
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindGitRepository, manifest.KindBucket, manifest.KindExternalArtifact, manifest.KindHelmChart} {
		t.Run(kind, func(t *testing.T) {
			st, hr, dir := ociRenderFixture(t)
			hr = hr.Clone()
			hr.ChartRef = nil
			hr.Chart.RepoKind = kind
			if kind == manifest.KindHelmChart {
				hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: kind, Name: "podinfo"}
			}
			st.SetArtifact(manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "podinfo"}, &store.SourceArtifact{Kind: kind, LocalPath: dir})
			cli, err := NewClient(cacheroot.New(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			cli.SetSourceResolver(NewStoreSourceResolver(st))
			var first, key string
			for _, disabled := range []bool{false, true} {
				opts := Options{DisableChartDigestTracking: disabled}
				out, err := cli.Template(t.Context(), hr, nil, opts)
				if err != nil {
					t.Fatal(err)
				}
				loaded, err := cli.LoadChart(t.Context(), hr)
				if err != nil {
					t.Fatal(err)
				}
				merged, err := cli.mergeChartValuesFiles(loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
				if err != nil {
					t.Fatal(err)
				}
				nextKey := computeTemplateKey(loaded.Fingerprint, loaded.Chart, merged, opts, hr)
				if first != "" {
					assert.Equal(t, out, first)
					assert.Equal(t, nextKey, key)
				}
				if !strings.Contains(out, `version: "6.15.0"`) {
					t.Fatal(out)
				}
				first, key = out, nextKey
			}
			assert.Equal(t, cli.templateCache.Len(), 1)
		})
	}
}

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

	t.Run("OptsDisableChartDigestTracking", func(t *testing.T) {
		alt := baseOpts
		alt.DisableChartDigestTracking = true
		if got := computeTemplateKey("fp", baseChart, baseValues, alt, baseHR); got != baseKey {
			t.Error("digest tracking option changed a non-OCI key")
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

// TestComputeTemplateKey_CommonMetadataExcluded pins the cache boundary:
// spec.commonMetadata is applied DOWNSTREAM of Template (the controller's
// helm.ApplyHRCommonMetadata, on both the fresh-render and dedup-replay
// paths), never reaches the cached render, and so must NOT participate in
// the key — otherwise every CommonMetadata-only change forces a spurious
// full re-render. The control subtest asserts PostRenderers (which DOES
// change the rendered bytes) still diverges the key, so this test cannot
// pass by accident if a genuinely render-affecting input were dropped.
func TestComputeTemplateKey_CommonMetadataExcluded(t *testing.T) {
	ch := minimalChart()
	values := map[string]any{"k": "v"}
	opts := Options{KubeVersion: "1.33"}
	base := minimalHR()
	baseKey := computeTemplateKey("fp", ch, values, opts, base)

	t.Run("CommonMetadata does not change the key", func(t *testing.T) {
		alt := *base
		alt.CommonMetadata = &helmv2.CommonMetadata{
			Labels:      map[string]string{"team": "platform"},
			Annotations: map[string]string{"note": "x"},
		}
		if got := computeTemplateKey("fp", ch, values, opts, &alt); got != baseKey {
			t.Errorf("CommonMetadata must NOT affect the cache key (applied downstream of Template):\nbase=%s\nalt =%s", baseKey, got)
		}
	})

	t.Run("control: PostRenderers still diverges", func(t *testing.T) {
		alt := *base
		alt.PostRenderers = []helmv2.PostRenderer{{Kustomize: &helmv2.Kustomize{}}}
		if got := computeTemplateKey("fp", ch, values, opts, &alt); got == baseKey {
			t.Error("PostRenderers changes rendered output and must still diverge the key (guards against dropping a real input)")
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

// TestTemplateCache_PromotesFromDisk pins the in-memory layer's fall-through to
// disk: a Get that misses memory but hits disk returns the disk payload AND
// populates the LRU so the second same-process Get stays a memory hit. The Len()
// probe before and after verifies the promotion happened.
func TestTemplateCache_PromotesFromDisk(t *testing.T) {
	dir := t.TempDir()
	disk := diskcache.NewStore(dir, 1<<20)
	key := strings.Repeat("e", 64)
	disk.Put(key, []byte("from-disk"))

	mem := newTemplateCache(1<<20, disk)
	if got := mem.Len(); got != 0 {
		t.Fatalf("fresh cache should be empty, has %d entries", got)
	}

	v, ok := mem.Get(key)
	if !ok {
		t.Fatalf("Get should hit via disk fall-through")
	}
	if v != "from-disk" {
		t.Fatalf("disk fall-through returned wrong value: %q", v)
	}
	if got := mem.Len(); got != 1 {
		t.Fatalf("disk hit must promote to memory; Len=%d, want 1", got)
	}
	// Second Get serves from memory; the Len invariant should still hold (no
	// double-insert).
	if _, ok := mem.Get(key); !ok {
		t.Fatalf("second Get on a promoted key must hit")
	}
	if got := mem.Len(); got != 1 {
		t.Fatalf("Len after second Get = %d, want 1", got)
	}
}

// TestTemplateCache_PutWritesThroughToDisk pins the write-through contract: a Put
// against the in-memory layer must persist to disk so a subsequent fresh cache
// instance reads the same value. Without this, the cross-process hit rate would
// stay at zero.
func TestTemplateCache_PutWritesThroughToDisk(t *testing.T) {
	dir := t.TempDir()
	disk1 := diskcache.NewStore(dir, 1<<20)
	mem1 := newTemplateCache(1<<20, disk1)
	key := strings.Repeat("f", 64)
	mem1.Put(key, "persistent")

	// Fresh stack — no shared state with mem1/disk1 — but pointing at the same
	// on-disk root.
	disk2 := diskcache.NewStore(dir, 1<<20)
	mem2 := newTemplateCache(1<<20, disk2)
	got, ok := mem2.Get(key)
	if !ok {
		t.Fatalf("fresh cache must read the disk-write-through value")
	}
	if got != "persistent" {
		t.Fatalf("fresh cache returned wrong value: %q", got)
	}
}

// TestTemplateCache_TemplateIntegrationCrossProcess covers the on-the-render-path
// behavior end-to-end: render with cache instance A, instantiate cache instance
// B pointing at the same disk root, render again — second render returns
// byte-identical output via the disk cache without re-running action.Install.
// The byte-identity assertion catches any divergence between cached and uncached
// output.
func TestTemplateCache_TemplateIntegrationCrossProcess(t *testing.T) {
	// Chart fixture mirrors the in-process integration test so values churn
	// meaningfully on each render.
	chartDir := t.TempDir()
	testutil.WriteFile(t, chartDir, "mychart/Chart.yaml",
		"apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: t\n")
	testutil.WriteFile(t, chartDir, "mychart/values.yaml", "greeting: hi\n")
	testutil.WriteFile(t, chartDir, "mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
data:
  greeting: {{ .Values.greeting }}
`)

	cacheRoot := t.TempDir()
	layout := cacheroot.New(cacheRoot)

	// First client: render and persist to disk.
	first, err := NewClientWithOptions(layout, ClientOptions{
		TemplateCacheBytes: 1 << 20,
		RenderCacheBytes:   1 << 20,
		RenderCacheRoot:    layout.RenderHelmCache(),
	})
	if err != nil {
		t.Fatalf("NewClientWithOptions first: %v", err)
	}
	first.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", chartDir))

	hr := newHR()
	ctx := context.Background()
	expected, err := first.Template(ctx, hr, map[string]any{"greeting": "hi"}, Options{})
	if err != nil {
		t.Fatalf("first Template: %v", err)
	}

	// Second client: fresh in-memory cache, same disk root. The render must hit
	// disk and return byte-identical output.
	second, err := NewClientWithOptions(layout, ClientOptions{
		TemplateCacheBytes: 1 << 20,
		RenderCacheBytes:   1 << 20,
		RenderCacheRoot:    layout.RenderHelmCache(),
	})
	if err != nil {
		t.Fatalf("NewClientWithOptions second: %v", err)
	}
	second.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", chartDir))

	got, err := second.Template(ctx, hr, map[string]any{"greeting": "hi"}, Options{})
	if err != nil {
		t.Fatalf("second Template: %v", err)
	}
	if got != expected {
		t.Fatalf("cross-process disk-cached render diverged from initial render:\nfirst:\n%s\nsecond:\n%s", expected, got)
	}
	// Second client's in-memory cache should have exactly one entry — the disk
	// fall-through promotion.
	if got := second.templateCache.Len(); got != 1 {
		t.Errorf("expected 1 in-memory entry after disk-cached render, got %d", got)
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

func TestTemplateCache_ChartDigestTrackingOptions(t *testing.T) {
	st, hr, dir := ociRenderFixture(t)
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	st.SetArtifact(manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo"},
		&store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: dir, Digest: digest, Revision: "6.15.0@" + digest})
	layout := cacheroot.New(t.TempDir())
	clientOpts := ClientOptions{TemplateCacheBytes: 1 << 20, RenderCacheBytes: 1 << 20, RenderCacheRoot: layout.RenderHelmCache()}
	newClient := func() *Client {
		cli, err := NewClientWithOptions(layout, clientOpts)
		if err != nil {
			t.Fatal(err)
		}
		cli.SetSourceResolver(NewStoreSourceResolver(st))
		return cli
	}
	cli := newClient()
	for _, disable := range []bool{false, true, false, true, false} {
		opts := Options{DisableChartDigestTracking: disable}
		out, err := cli.Template(t.Context(), hr, nil, opts)
		if err != nil {
			t.Fatal(err)
		}
		version := "6.15.0"
		if !disable {
			version += "+ff3d3e14728f"
		}
		if !strings.Contains(out, `version: "`+version+`"`) {
			t.Fatalf("disable=%v returned incompatible cached output:\n%s", disable, out)
		}
		fresh := newClient()
		persisted, err := fresh.Template(t.Context(), hr, nil, opts)
		if err != nil || persisted != out {
			t.Fatalf("persisted disable=%v result differs: err=%v output=%s", disable, err, persisted)
		}
	}
	assert.Equal(t, cli.templateCache.Len(), 2)
}
