package source

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

func TestSweep_AgePrunesStaleSlots(t *testing.T) {
	root := t.TempDir()
	stale := filepath.Join(root, "sources", "old-repo", "deadbeef")
	fresh := filepath.Join(root, "sources", "new-repo", "cafefeed")
	for _, d := range []string{stale, fresh} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "marker"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-7 * 24 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}

	res, err := Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.Removed) != 1 || res.Removed[0] != stale {
		t.Errorf("Removed = %v, want [%q]", res.Removed, stale)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale slot still exists: %v", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh slot removed: %v", err)
	}
}

func TestSweep_IgnoresSourceSlotLockFiles(t *testing.T) {
	root := t.TempDir()
	lockFile := filepath.Join(root, "sources", "repo", "deadbeef.lock")
	if err := os.MkdirAll(filepath.Dir(lockFile), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lockFile, []byte{}, 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-7 * 24 * time.Hour)
	if err := os.Chtimes(lockFile, old, old); err != nil {
		t.Fatal(err)
	}

	res, err := Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if slices.Contains(res.Removed, lockFile) {
		t.Errorf("lock file should not be swept as a source slot: %v", res.Removed)
	}
	if _, err := os.Stat(lockFile); err != nil {
		t.Errorf("lock file removed from disk: %v", err)
	}
}

func TestSweep_Baselines(t *testing.T) {
	root := t.TempDir()
	baseline := filepath.Join(root, "baselines", "abc123")
	for _, d := range []string{baseline} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-30 * 24 * time.Hour)
	for _, d := range []string{baseline} {
		if err := os.Chtimes(d, old, old); err != nil {
			t.Fatal(err)
		}
	}

	res, _ := Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour})
	if len(res.Removed) != 1 {
		t.Errorf("Removed = %v, want 1 entry", res.Removed)
	}
}

func TestSweep_MirrorsPreservedByDefault(t *testing.T) {
	root := t.TempDir()
	mirror := filepath.Join(root, "git-mirrors", "abc123")
	if err := os.MkdirAll(mirror, 0o750); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-365 * 24 * time.Hour)
	if err := os.Chtimes(mirror, old, old); err != nil {
		t.Fatal(err)
	}

	res, _ := Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour})
	if slices.Contains(res.Removed, mirror) {
		t.Error("mirror swept without IncludeMirrors")
	}

	res, _ = Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour, IncludeMirrors: true})
	if !slices.Contains(res.Removed, mirror) {
		t.Errorf("mirror not swept with IncludeMirrors: %v", res.Removed)
	}
}

func TestSweep_DryRunReports(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sources", "repo", "hash")
	if err := os.MkdirAll(slot, 0o750); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-30 * 24 * time.Hour)
	if err := os.Chtimes(slot, old, old); err != nil {
		t.Fatal(err)
	}

	res, _ := Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour, DryRun: true})
	if !slices.Contains(res.Removed, slot) {
		t.Errorf("DryRun didn't report stale slot: %v", res.Removed)
	}
	if _, err := os.Stat(slot); err != nil {
		t.Errorf("DryRun removed slot on disk: %v", err)
	}
}

func TestSweep_GitGenerations(t *testing.T) {
	for _, scenario := range []string{"unheld", "held", "dry-run", "bad-index"} {
		t.Run(scenario, func(t *testing.T) {
			layout := cacheroot.New(t.TempDir())
			c, err := cache.New(t.Context(), layout)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			produce := func(storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
				for _, input := range []billy.Filesystem{storer.(*filesystem.Storage).Filesystem(), worktree} {
					file, err := input.Create("marker")
					if err != nil {
						return plumbing.ZeroHash, err
					}
					if _, err := file.Write([]byte("retained")); err != nil {
						_ = file.Close()
						return plumbing.ZeroHash, err
					}
					if err := file.Close(); err != nil {
						return plumbing.ZeroHash, err
					}
				}
				return plumbing.ZeroHash, nil
			}
			key := cache.GitKey{ID: "source-git-sweep", RecurseSubmodules: true}
			first, err := c.Git(t.Context(), key, 0, produce)
			if err != nil {
				t.Fatal(err)
			}
			second, err := c.Git(t.Context(), key, 0, produce)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "held" {
				other, err := cache.New(t.Context(), layout)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = other.Close() })
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			if scenario == "bad-index" {
				if err := os.WriteFile(filepath.Join(layout.OCI(), "index.json"), []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			res, err := Sweep(layout, SweepOpts{DryRun: scenario == "dry-run"})
			if scenario == "bad-index" {
				if err != nil || len(res.Errors) != 1 || len(res.Removed) != 0 {
					t.Fatalf("unreadable index: %+v, %v", res, err)
				}
			} else if err != nil || len(res.Errors) != 0 {
				t.Fatalf("Sweep: %v, %v", err, res.Errors)
			}
			old := filepath.Dir(first.Backing.Root())
			if scenario == "held" || scenario == "bad-index" {
				if slices.Contains(res.Removed, old) {
					t.Fatal("sweep selected a held generation")
				}
			} else if !slices.Contains(res.Removed, old) || res.Bytes < 16 {
				t.Fatalf("orphan not reclaimed: %+v", res)
			}
			for _, input := range []billy.Filesystem{first.Backing, first.Worktree} {
				_, err := input.Stat("marker")
				if scenario == "unheld" {
					if !errors.Is(err, fs.ErrNotExist) {
						t.Fatalf("unheld generation remains: %v", err)
					}
				} else if err != nil {
					t.Fatalf("protected generation removed: %v", err)
				}
			}
			for _, input := range []billy.Filesystem{second.Backing, second.Worktree} {
				if _, err := input.Stat("marker"); err != nil {
					t.Fatalf("indexed generation removed: %v", err)
				}
			}
		})
	}
}
