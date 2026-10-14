package change

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
	if _, err := Detect(t.Context(), testInput(""), testInput("/tmp/x")); err == nil {
		t.Errorf("expected error for empty before")
	}
	if _, err := Detect(t.Context(), testInput("/tmp/x"), testInput("")); err == nil {
		t.Errorf("expected error for empty after")
	}
}

func TestDetect_IdenticalTreesEmpty(t *testing.T) {
	before := t.TempDir()
	after := t.TempDir()
	writeFile(t, before, "a.yaml", "x")
	writeFile(t, after, "a.yaml", "x")
	got, err := Detect(t.Context(), testInput(before), testInput(after))
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

	got, err := Detect(t.Context(), testInput(before), testInput(after))
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
	got, err := Detect(t.Context(), testInput(before), testInput(after))
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

	got, err := Detect(t.Context(), testInput(before), testInput(after))
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

	got, err := Detect(t.Context(), testInput(before), testInput(after))
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
	ha, err := hashFile(t.Context(), os.DirFS(dir), "a")
	if err != nil {
		t.Fatalf("hashFile a: %v", err)
	}
	hb, err := hashFile(t.Context(), os.DirFS(dir), "b")
	if err != nil {
		t.Fatalf("hashFile b: %v", err)
	}
	hc, err := hashFile(t.Context(), os.DirFS(dir), "c")
	if err != nil {
		t.Fatalf("hashFile c: %v", err)
	}
	if ha != hb {
		t.Errorf("identical content should hash equal: %x vs %x", ha, hb)
	}
	if ha == hc {
		t.Errorf("different content should hash differently: %x vs %x", ha, hc)
	}
}

func testInput(root string) *store.SourceArtifact {
	if root == "" {
		return nil
	}
	return &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
}

func TestDetect_MissingInputsWrapInput(t *testing.T) {
	for _, input := range []*store.SourceArtifact{nil, {}} {
		_, err := Detect(t.Context(), input, &store.SourceArtifact{FS: fstest.MapFS{}})
		if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
			t.Fatalf("missing input error=%v", err)
		}
	}
}

func TestDetect_LastBodyCancellation(t *testing.T) {
	for _, side := range []string{"before", "after"} {
		t.Run(side, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			input := fstest.MapFS{"file.yaml": {Data: []byte("same")}}
			before := &store.SourceArtifact{FS: input}
			after := &store.SourceArtifact{FS: input}
			cancelled := &cancellingInput{FS: input, cancel: cancel}
			if side == "before" {
				before.FS = cancelled
			} else {
				after.FS = cancelled
			}
			set, err := Detect(ctx, before, after)
			if !errors.Is(err, context.Canceled) || set != nil {
				t.Fatalf("cancelled last body=%v,%v", set, err)
			}
		})
	}
}

type cancellingInput struct {
	fs.FS
	cancel context.CancelFunc
}

func (c *cancellingInput) Open(name string) (fs.File, error) {
	f, err := c.FS.Open(name)
	if err != nil || name != "file.yaml" {
		return f, err
	}
	return &cancellingFile{File: f, cancel: c.cancel}, nil
}

type cancellingFile struct {
	fs.File
	cancel context.CancelFunc
}

func (c *cancellingFile) Read(p []byte) (int, error) {
	n, err := c.File.Read(p)
	if n > 0 {
		c.cancel()
	}
	return n, err
}
