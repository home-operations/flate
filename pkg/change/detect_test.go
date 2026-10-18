package change

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestDetect_BothPathsRequired(t *testing.T) {
	t.Parallel()
	if _, err := Detect(t.Context(), testInput(""), testInput("/tmp/x")); err == nil {
		t.Errorf("expected error for empty before")
	}
	if _, err := Detect(t.Context(), testInput("/tmp/x"), testInput("")); err == nil {
		t.Errorf("expected error for empty after")
	}
}

func TestDetect_IdenticalTreesEmpty(t *testing.T) {
	t.Parallel()
	before := t.TempDir()
	after := t.TempDir()
	testutil.WriteFile(t, before, "a.yaml", "x")
	testutil.WriteFile(t, after, "a.yaml", "x")
	got, err := Detect(t.Context(), testInput(before), testInput(after))
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if got.Len() != 0 {
		t.Errorf("expected 0 changes, got %v", got.Paths())
	}
}

func TestDetect_AddedRemovedModified(t *testing.T) {
	t.Parallel()
	before := t.TempDir()
	after := t.TempDir()
	// unchanged: ident.yaml on both sides with same content
	testutil.WriteFile(t, before, "ident.yaml", "same")
	testutil.WriteFile(t, after, "ident.yaml", "same")
	// added: only on after side
	testutil.WriteFile(t, after, "added.yaml", "new")
	// removed: only on before side
	testutil.WriteFile(t, before, "removed.yaml", "gone")
	// modified-same-size: trigger the hash path with same byte length
	testutil.WriteFile(t, before, "mod.yaml", "AAA")
	testutil.WriteFile(t, after, "mod.yaml", "BBB")
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
	t.Parallel()
	// Different sizes: detector should flag without hashing.
	before := t.TempDir()
	after := t.TempDir()
	testutil.WriteFile(t, before, "f.yaml", "small")
	testutil.WriteFile(t, after, "f.yaml", "this is a larger payload")
	got, err := Detect(t.Context(), testInput(before), testInput(after))
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if !got.Contains("f.yaml") {
		t.Errorf("expected f.yaml in change set, got %v", got.Paths())
	}
}

func TestDetect_HiddenScanRoot(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	before := filepath.Join(parent, ".before")
	after := filepath.Join(parent, ".after")
	testutil.WriteFile(t, before, "same.yaml", "x")
	testutil.WriteFile(t, after, "same.yaml", "x")
	testutil.WriteFile(t, before, "mod.yaml", "old")
	testutil.WriteFile(t, after, "mod.yaml", "new")

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
	t.Parallel()
	before := t.TempDir()
	after := t.TempDir()
	testutil.WriteFile(t, before, "mod.yaml", "OLD")
	testutil.WriteFile(t, after, "mod.yaml", "NEW")
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
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "a", "hello")
	testutil.WriteFile(t, dir, "b", "hello")
	testutil.WriteFile(t, dir, "c", "different")
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
	t.Parallel()
	for _, input := range []*store.SourceArtifact{nil, {}} {
		_, err := Detect(t.Context(), input, &store.SourceArtifact{FS: fstest.MapFS{}})
		if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
			t.Fatalf("missing input error=%v", err)
		}
	}
}

func TestDetect_LastBodyCancellation(t *testing.T) {
	t.Parallel()
	for _, side := range []string{"before", "after"} {
		t.Run(side, func(t *testing.T) {
			t.Parallel()
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

func TestDetect_HashLimit(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"changed", "canceled", "read error"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				reads := &hashReads{release: make(chan struct{})}
				released := false
				defer func() {
					if !released {
						close(reads.release)
					}
				}()
				readErr := errors.New("hash read failed")
				if scenario == "read error" {
					reads.err = readErr
				}
				before, after := fstest.MapFS{}, fstest.MapFS{}
				var want []string
				for i := range 17 {
					name := fmt.Sprintf("file-%02d.yaml", i)
					before[name] = &fstest.MapFile{Data: []byte("old")}
					after[name] = &fstest.MapFile{Data: []byte("new")}
					want = append(want, name)
				}
				type result struct {
					set *Set
					err error
				}
				done := make(chan result, 1)
				go func() {
					set, err := Detect(ctx, &store.SourceArtifact{FS: &hashInput{FS: before, reads: reads, before: true}}, &store.SourceArtifact{FS: &hashInput{FS: after, reads: reads}})
					done <- result{set, err}
				}()
				synctest.Wait()
				if reads.active.Load() != 8 || reads.maximum.Load() != 8 || reads.before.Load() != 8 {
					t.Fatalf("blocked jobs: active=%d peak=%d before=%d, want eight overlapping jobs", reads.active.Load(), reads.maximum.Load(), reads.before.Load())
				}
				switch scenario {
				case "canceled":
					cancel()
				case "read error":
					reads.release <- struct{}{}
				}
				synctest.Wait()
				select {
				case got := <-done:
					t.Fatalf("Detect returned before blocked readers drained: %+v", got)
				default:
				}
				if scenario == "read error" && (reads.active.Load() != 7 || reads.before.Load() != 8) {
					t.Fatalf("failed submission started extra jobs: active=%d before=%d", reads.active.Load(), reads.before.Load())
				}
				close(reads.release)
				released = true
				got := <-done
				if reads.active.Load() != 0 || reads.opened.Load() != reads.closed.Load() || reads.maximum.Load() > 8 {
					t.Fatalf("readers not drained: active=%d opened=%d closed=%d peak=%d", reads.active.Load(), reads.opened.Load(), reads.closed.Load(), reads.maximum.Load())
				}
				if scenario == "changed" {
					if got.err != nil || got.set == nil || !slices.Equal(got.set.Paths(), want) || reads.before.Load() != 17 || reads.opened.Load() != 34 {
						t.Fatalf("hash results: set=%v error=%v before=%d opened=%d", got.set, got.err, reads.before.Load(), reads.opened.Load())
					}
				} else {
					cause := readErr
					if scenario == "canceled" {
						cause = context.Canceled
					}
					if !errors.Is(got.err, cause) || got.set != nil || reads.before.Load() != 8 {
						t.Fatalf("failed hash results: set=%v error=%v before=%d", got.set, got.err, reads.before.Load())
					}
				}
			})
		})
	}
}

type hashReads struct {
	release                                 chan struct{}
	err                                     error
	failed                                  atomic.Bool
	active, maximum, opened, closed, before atomic.Int32
}

type hashInput struct {
	fs.FS
	reads  *hashReads
	before bool
}

func (f *hashInput) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if err != nil || filepath.Ext(name) != ".yaml" {
		return file, err
	}
	if f.before {
		f.reads.before.Add(1)
	}
	f.reads.opened.Add(1)
	active := f.reads.active.Add(1)
	for maximum := f.reads.maximum.Load(); active > maximum; maximum = f.reads.maximum.Load() {
		if f.reads.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	return &hashReadFile{File: file, reads: f.reads, block: f.before}, nil
}

type hashReadFile struct {
	fs.File
	reads   *hashReads
	started bool
	block   bool
}

func (f *hashReadFile) Read(p []byte) (int, error) {
	if !f.started {
		f.started = true
		if f.block {
			<-f.reads.release
			if f.reads.err != nil && f.reads.failed.CompareAndSwap(false, true) {
				return 0, f.reads.err
			}
		}
	}
	return f.File.Read(p)
}

func (f *hashReadFile) Close() error {
	f.reads.closed.Add(1)
	f.reads.active.Add(-1)
	return f.File.Close()
}
