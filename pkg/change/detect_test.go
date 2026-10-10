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

func TestDetect_HiddenScanRoot(t *testing.T) {
	parent := t.TempDir()
	before := filepath.Join(parent, ".before")
	after := filepath.Join(parent, ".after")
	writeFile(t, before, "same.yaml", "x")
	writeFile(t, after, "same.yaml", "x")
	writeFile(t, before, "mod.yaml", "old")
	writeFile(t, after, "mod.yaml", "new")

	got, err := Detect(before, after)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if paths := got.Paths(); !slices.Equal(paths, []string{"mod.yaml"}) {
		t.Errorf("hidden scan root paths = %v, want [mod.yaml]", paths)
	}
}

// TestDetect_SameSizeSameMtime_StillDetected pins the correctness
// fix: the old (size, mtime) fast path silently treated two distinct
// same-sized files written within the same coarse-mtime tick as
// identical, dropping real changes. Removing the fast path means
// same-size files always get content-compared, so a write-write
// pattern that happens within an HFS+ 1-second tick is still
// reported. We simulate the worst case by explicitly stamping
// identical mtimes on both files post-write.
func TestDetect_SameSizeSameMtime_StillDetected(t *testing.T) {
	before := t.TempDir()
	after := t.TempDir()
	writeFile(t, before, "mod.yaml", "OLD")
	writeFile(t, after, "mod.yaml", "NEW")
	// Exact same mtime — what the mtime-fast-path used to short-circuit on.
	stamp := time.Now()
	if err := os.Chtimes(filepath.Join(before, "mod.yaml"), stamp, stamp); err != nil {
		t.Fatalf("chtimes before: %v", err)
	}
	if err := os.Chtimes(filepath.Join(after, "mod.yaml"), stamp, stamp); err != nil {
		t.Fatalf("chtimes after: %v", err)
	}

	got, err := Detect(before, after)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if !got.Contains("mod.yaml") {
		t.Errorf("Detect missed same-size same-mtime modification — fast-path regression. Paths: %v", got.Paths())
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

func TestDetect_Symlinks(t *testing.T) {
	t.Setenv("PATH", "")
	cases := []struct {
		name                      string
		beforeTarget, afterTarget string
		afterRegular              bool
	}{
		{name: "same_length_retarget", beforeTarget: "target-A", afterTarget: "target-B"},
		{name: "different_length_retarget", beforeTarget: "target", afterTarget: "other-target"},
		{name: "regular_type_swap", beforeTarget: "target", afterTarget: "target", afterRegular: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before, after := t.TempDir(), t.TempDir()
			if err := os.Symlink(tc.beforeTarget, filepath.Join(before, "link")); err != nil {
				t.Fatal(err)
			}
			if tc.afterRegular {
				writeFile(t, after, "link", tc.afterTarget)
			} else if err := os.Symlink(tc.afterTarget, filepath.Join(after, "link")); err != nil {
				t.Fatal(err)
			}
			for _, roots := range [][2]string{{before, after}, {after, before}} {
				got, err := Detect(roots[0], roots[1])
				if err != nil {
					t.Fatal(err)
				}
				if paths := got.Paths(); !slices.Equal(paths, []string{"link"}) {
					t.Errorf("paths = %v, want [link]", paths)
				}
			}
		})
	}
}
