package kustomization

import (
	"os"
	"path/filepath"
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
// for workingTreeFingerprint tests. Returns the absolute root.
func seedWorkingTree(t *testing.T) string {
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
