package change

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func writeFile(t *testing.T, dir, rel, body string) {
	t.Helper()
	p := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(p), err)
	}
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
}

func TestDetect_BothPathsRequired(t *testing.T) {
	if _, err := Detect("", "/tmp/x"); err == nil {
		t.Errorf("expected error for empty before")
	}
	if _, err := Detect("/tmp/x", ""); err == nil {
		t.Errorf("expected error for empty after")
	}
}

func TestDetect_IdenticalTreesEmpty(t *testing.T) {
	before := t.TempDir()
	after := t.TempDir()
	writeFile(t, before, "a.yaml", "x")
	writeFile(t, after, "a.yaml", "x")
	got, err := Detect(before, after)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if got.Len() != 0 {
		t.Errorf("expected 0 changes, got %v", got.Paths())
	}
}

func TestDetect_AddedRemovedModified(t *testing.T) {
	before := t.TempDir()
	after := t.TempDir()
	// unchanged: ident.yaml on both sides with same content
	writeFile(t, before, "ident.yaml", "same")
	writeFile(t, after, "ident.yaml", "same")
	// added: only on after side
	writeFile(t, after, "added.yaml", "new")
	// removed: only on before side
	writeFile(t, before, "removed.yaml", "gone")
	// modified-same-size: trigger the hash path with same byte length
	writeFile(t, before, "mod.yaml", "AAA")
	writeFile(t, after, "mod.yaml", "BBB")
	// Bump mtime so the mtime equality bypass doesn't short-circuit.
	if err := os.Chtimes(filepath.Join(after, "mod.yaml"), time.Now(), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	got, err := Detect(before, after)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	want := []string{"added.yaml", "mod.yaml", "removed.yaml"}
	if got := got.Paths(); !slices.Equal(got, want) {
		t.Errorf("paths = %v, want %v", got, want)
	}
}

func TestDetect_SizeDifferAlwaysChanged(t *testing.T) {
	// Different sizes: detector should flag without hashing.
	before := t.TempDir()
	after := t.TempDir()
	writeFile(t, before, "f.yaml", "small")
	writeFile(t, after, "f.yaml", "this is a larger payload")
	got, err := Detect(before, after)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if !got.Contains("f.yaml") {
		t.Errorf("expected f.yaml in change set, got %v", got.Paths())
	}
}

func TestDetect_SkipsDotDirsAndVendor(t *testing.T) {
	before := t.TempDir()
	after := t.TempDir()
	// Modifications inside dot-prefixed and well-known noise dirs are
	// ignored by the walk.
	writeFile(t, before, ".git/HEAD", "a")
	writeFile(t, after, ".git/HEAD", "b")
	writeFile(t, before, "node_modules/foo/index.js", "a")
	writeFile(t, after, "node_modules/foo/index.js", "b")
	writeFile(t, before, "vendor/dep/file.go", "a")
	writeFile(t, after, "vendor/dep/file.go", "b")
	// Sanity check: a real file change still surfaces. Use
	// different-sized content so the detector flags via size diff —
	// same-size + same-mtime would be (correctly) skipped on
	// filesystems with coarse mtime resolution.
	writeFile(t, before, "real.yaml", "short")
	writeFile(t, after, "real.yaml", "a longer payload")
	got, err := Detect(before, after)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if paths := got.Paths(); len(paths) != 1 || paths[0] != "real.yaml" {
		t.Errorf("expected just real.yaml, got %v", paths)
	}
}

func TestShouldSkipDir(t *testing.T) {
	yes := []string{".git", ".cache", "node_modules", "vendor"}
	no := []string{"src", "kubernetes", "apps", "tests"}
	for _, n := range yes {
		if !shouldSkipDir(n) {
			t.Errorf("shouldSkipDir(%q) = false, want true", n)
		}
	}
	for _, n := range no {
		if shouldSkipDir(n) {
			t.Errorf("shouldSkipDir(%q) = true, want false", n)
		}
	}
}

func TestHashFile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a", "hello")
	writeFile(t, dir, "b", "hello")
	writeFile(t, dir, "c", "different")
	ha, err := hashFile(filepath.Join(dir, "a"))
	if err != nil {
		t.Fatalf("hashFile a: %v", err)
	}
	hb, err := hashFile(filepath.Join(dir, "b"))
	if err != nil {
		t.Fatalf("hashFile b: %v", err)
	}
	hc, err := hashFile(filepath.Join(dir, "c"))
	if err != nil {
		t.Fatalf("hashFile c: %v", err)
	}
	if ha != hb {
		t.Errorf("identical content should hash equal: %q vs %q", ha, hb)
	}
	if ha == hc {
		t.Errorf("different content should hash differently: %q vs %q", ha, hc)
	}
}

func TestSet_RerootPrependsPrefix(t *testing.T) {
	s := NewSet([]string{"app/foo.yaml", "bar.yaml"})
	r := s.Reroot("kubernetes")
	if !r.Contains("kubernetes/app/foo.yaml") || !r.Contains("kubernetes/bar.yaml") {
		t.Errorf("Reroot did not prepend: %v", r.Paths())
	}
	// Empty / "." prefix is a no-op (returns same set).
	if got := s.Reroot(""); got != s {
		t.Errorf("Reroot(\"\") returned a new set, want same")
	}
	if got := s.Reroot("."); got != s {
		t.Errorf("Reroot(\".\") returned a new set, want same")
	}
}
