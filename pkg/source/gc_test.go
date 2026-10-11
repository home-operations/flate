package source

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

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
