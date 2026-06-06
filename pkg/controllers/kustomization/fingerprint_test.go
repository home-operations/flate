package kustomization

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

// TestKustomizationFingerprint_StableAcrossLabelStamping locks the
// dedup contract: a KS re-AddObject'd with kustomize ownership
// labels (the typical pattern when the parent KS emits a re-stamped
// child) must produce the same fingerprint as the file-loaded
// original — otherwise the dedup short-circuit can't fire and
// kustomize.RenderFlux runs twice for one logical Kustomization.
func TestKustomizationFingerprint_StableAcrossLabelStamping(t *testing.T) {
	base := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path:            "./apps",
			TargetNamespace: "apps",
		},
	}
	stamped := base.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent-ks",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	stamped.Annotations = map[string]string{"reconcile.fluxcd.io/requestedAt": "now"}

	if got, want := kustomizationFingerprint(stamped, "/repo"), kustomizationFingerprint(base, "/repo"); got != want {
		t.Errorf("fingerprint changed under label/annotation stamping; got %q want %q", got, want)
	}
}

// TestKustomizationFingerprint_DifferentOnSpecChange flips the
// invariant: when a parent KS injects spec mutations via patches /
// replacements (TargetNamespace, postBuild.substitute, etc.), the
// fingerprint MUST differ so the controller renders the canonical
// post-patch values.
func TestKustomizationFingerprint_DifferentOnSpecChange(t *testing.T) {
	base := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps", TargetNamespace: "apps"},
	}
	patched := base.Clone()
	patched.TargetNamespace = "production"

	if got := kustomizationFingerprint(base, "/repo"); got == kustomizationFingerprint(patched, "/repo") {
		t.Errorf("fingerprint should differ when spec.targetNamespace mutates; both = %q", got)
	}
}

// TestKustomizationFingerprint_SourceRootInputs guards that a KS
// resolving to a different on-disk root (e.g. one bootstrap-GR vs.
// a sibling GitRepository) does NOT collide with the file-loaded
// sibling at the same spec.path — the source content differs, so
// the render output differs too.
func TestKustomizationFingerprint_SourceRootInputs(t *testing.T) {
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	if a, b := kustomizationFingerprint(ks, "/repo-a"), kustomizationFingerprint(ks, "/repo-b"); a == b {
		t.Errorf("fingerprint must differ across distinct sourceRoots; both = %q", a)
	}
}

// seedWorkingTree lays out a small tree under t.TempDir()-style root
// for workingTreeFingerprint tests. Returns the absolute root. Takes
// testing.TB so benchmarks can share it.
func seedWorkingTree(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	mustMkdir := func(p string) {
		if err := os.MkdirAll(filepath.Join(root, p), 0o750); err != nil {
			t.Fatalf("mkdir %s: %v", p, err)
		}
	}
	mustWrite := func(p, body string) {
		if err := os.WriteFile(filepath.Join(root, p), []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
	mustMkdir("a/b")
	mustWrite("a/b/c.yaml", "x: 1\n")
	mustWrite("top.yaml", "y: 2\n")
	return root
}

// TestWorkingTreeFingerprint_NestedFileChangeBustsCache is the
// regression fence for #ceph-csi-drivers: adding a deeply-nested
// file MUST change the fingerprint so the persistent stage cache
// doesn't reuse a stale (structurally broken) copy of the tree.
func TestWorkingTreeFingerprint_NestedFileChangeBustsCache(t *testing.T) {
	root := seedWorkingTree(t)
	before := workingTreeFingerprint(root)

	if err := os.MkdirAll(filepath.Join(root, "a/b/d"), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "a/b/d/new.yaml"), []byte("z: 3\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if after := workingTreeFingerprint(root); after == before {
		t.Errorf("fingerprint must change when a nested file is added; got %q before and after", after)
	}
}

// TestWorkingTreeFingerprint_NestedFileEditBustsCache locks the
// editor-save invalidation case: rewriting an existing nested file
// (same path, different size + mtime) must invalidate the cache.
func TestWorkingTreeFingerprint_NestedFileEditBustsCache(t *testing.T) {
	root := seedWorkingTree(t)
	before := workingTreeFingerprint(root)

	if err := os.WriteFile(filepath.Join(root, "a/b/c.yaml"), []byte("x: 1\nadded: true\n"), 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	if after := workingTreeFingerprint(root); after == before {
		t.Errorf("fingerprint must change when a nested file is edited; got %q before and after", after)
	}
}

// TestWorkingTreeFingerprint_StableOnDotPrefixedNoise locks the
// existing dotfile-skip contract: edits to `.git/`, `.flate-cache/`,
// IDE state etc. don't influence kustomize input, so the stage cache
// must survive them. Mirrors copyTreeInto's `strings.HasPrefix(base, ".")`
// rule.
func TestWorkingTreeFingerprint_StableOnDotPrefixedNoise(t *testing.T) {
	root := seedWorkingTree(t)
	for _, p := range []string{".git", ".flate-cache", ".vscode"} {
		if err := os.MkdirAll(filepath.Join(root, p), 0o750); err != nil {
			t.Fatalf("mkdir %s: %v", p, err)
		}
	}
	before := workingTreeFingerprint(root)

	for _, p := range []string{".git/HEAD", ".flate-cache/blob", ".vscode/settings.json"} {
		if err := os.WriteFile(filepath.Join(root, p), []byte("noise\n"), 0o600); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}

	if after := workingTreeFingerprint(root); after != before {
		t.Errorf("fingerprint must be stable across dot-prefixed dir noise; before=%q after=%q", before, after)
	}
}

// TestWorkingTreeFingerprint_StableOnNodeModules mirrors copyTreeInto's
// node_modules skip: front-end deps land in node_modules/ but are
// never kustomize input, so the cache key must ignore them.
func TestWorkingTreeFingerprint_StableOnNodeModules(t *testing.T) {
	root := seedWorkingTree(t)
	if err := os.MkdirAll(filepath.Join(root, "node_modules/pkg"), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	before := workingTreeFingerprint(root)

	if err := os.WriteFile(filepath.Join(root, "node_modules/pkg/index.js"), []byte("module.exports = 1\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if after := workingTreeFingerprint(root); after != before {
		t.Errorf("fingerprint must be stable across node_modules/ writes; before=%q after=%q", before, after)
	}
}

// TestWorkingTreeFingerprint_BrokenSymlinkDoesNotEmpty guards against
// a regression mode where a dangling editor lockfile or stray symlink
// caused the walk to error, returning "" — which silently downgrades
// every Stage call to per-process scratch and tanks repeat-run perf.
// Matches copyTreeInto's broken-symlink tolerance (copytree.go:79).
func TestWorkingTreeFingerprint_BrokenSymlinkDoesNotEmpty(t *testing.T) {
	root := seedWorkingTree(t)
	if err := os.Symlink(filepath.Join(root, "does-not-exist"), filepath.Join(root, "a/dangling")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	if fp := workingTreeFingerprint(root); fp == "" {
		t.Errorf("fingerprint must remain non-empty in the presence of a broken symlink")
	}
}

// TestWorkingTreeFingerprint_EmptyOnMissingPath locks the defensive
// degradation path: empty input or a nonexistent root returns "" so
// the caller falls back to per-process scratch staging instead of
// keying the persistent cache on garbage.
func TestWorkingTreeFingerprint_EmptyOnMissingPath(t *testing.T) {
	if fp := workingTreeFingerprint(""); fp != "" {
		t.Errorf(`workingTreeFingerprint("") = %q, want ""`, fp)
	}
	if fp := workingTreeFingerprint(filepath.Join(t.TempDir(), "no-such-dir")); fp != "" {
		t.Errorf("workingTreeFingerprint(nonexistent) = %q, want \"\"", fp)
	}
}

// TestCachedWorkingTreeFingerprint_SingleFlight is the regression fence
// for the warm-run thundering herd: dozens of KSes share one bootstrap
// working-tree source and all reconcile at once under --concurrency.
// The memo MUST collapse them to a single tree walk — a plain
// Load/miss/Store memo lets every caller miss before the first Store
// lands and re-walk the whole repo in parallel, which dominated warm-run
// CPU. Asserts the walk runs exactly once and every caller agrees on the
// value.
func TestCachedWorkingTreeFingerprint_SingleFlight(t *testing.T) {
	root := seedWorkingTree(t)
	want := workingTreeFingerprint(root) // reference value, computed off the memo

	c := &Controller{}
	const callers = 64
	results := make([]string, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			<-start // line every goroutine up so they all race the Load
			results[i] = c.cachedWorkingTreeFingerprint(root)
		})
	}
	close(start)
	wg.Wait()

	if got := c.walks.Load(); got != 1 {
		t.Errorf("workingTreeFingerprint walked %d times under %d concurrent callers; want 1 (single-flight broken)", got, callers)
	}
	for i, got := range results {
		if got != want {
			t.Fatalf("caller %d got fingerprint %q; want %q", i, got, want)
		}
	}

	// A later call for the same path stays a memo hit — no extra walk.
	if got := c.cachedWorkingTreeFingerprint(root); got != want {
		t.Fatalf("post-herd call got %q; want %q", got, want)
	}
	if got := c.walks.Load(); got != 1 {
		t.Errorf("walk count rose to %d on a memo hit; want 1", got)
	}
}

// seedBenchTree lays out a wider tree (nested dirs + many files) so the
// per-walk cost is meaningful — the cold-herd benchmark measures how
// many of those walks the memo elides under concurrency.
func seedBenchTree(b *testing.B, files int) string {
	b.Helper()
	root := b.TempDir()
	for i := range files {
		dir := filepath.Join(root, fmt.Sprintf("ns%02d", i%16), fmt.Sprintf("app%02d", i%8))
		if err := os.MkdirAll(dir, 0o750); err != nil {
			b.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("r%04d.yaml", i)), []byte("kind: ConfigMap\n"), 0o600); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
	return root
}

// BenchmarkCachedWorkingTreeFingerprint_ColdHerd measures the realistic
// access pattern a fresh `flate build` run hits: a fresh controller (cold
// memo) with many concurrent reconciles racing on one shared bootstrap
// source. Each iteration is a fresh herd, so single-flight shows as one
// walk per iteration versus up to `callers` walks pre-fix.
func BenchmarkCachedWorkingTreeFingerprint_ColdHerd(b *testing.B) {
	root := seedBenchTree(b, 256)
	const callers = 64
	b.ReportAllocs()
	for b.Loop() {
		c := &Controller{}
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				_ = c.cachedWorkingTreeFingerprint(root)
			})
		}
		wg.Wait()
	}
}
