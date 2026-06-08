package kustomize

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// TestOverlayFS_WalkPropagatesSkipDir pins the contract that a directory the
// caller SkipDirs during the memory-layer walk is NOT re-descended on the disk
// layer. Regression for the cnpg-system bug (issue: a kustomization.yaml decoded
// as a resource): preflight populates the memory layer under an app subdir,
// scanManifests SkipDirs that subdir (adding it once as a kustomize package),
// and the disk walk must then skip its subtree too — otherwise it picks up the
// subdir's kustomization.yaml as a stray resource. The walk must still surface
// disk-only children of directories that were NOT skipped (no over-skip).
func TestOverlayFS_WalkPropagatesSkipDir(t *testing.T) {
	root := writeTree(t, map[string]string{
		"app/kustomization.yaml": existingKustomization("./inner.yaml"),
		"app/inner.yaml":         cmYAML("inner"),
		"lib/disk.yaml":          cmYAML("libdisk"),
	})
	fsys := testOverlayFS(t, root)
	// Mirror preflightRemoteResources: a rewritten file under app/ (so app/
	// exists in BOTH layers and is walked from memory) and one under lib/.
	for _, rel := range []string{"app/mem.yaml", "lib/mem.yaml"} {
		if err := fsys.WriteFile(filepath.Join(root, rel), []byte(cmYAML("mem"))); err != nil {
			t.Fatalf("seed memory %s: %v", rel, err)
		}
	}

	var visited []string
	err := fsys.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		visited = append(visited, rel)
		if rel == "app" && info.IsDir() {
			return filepath.SkipDir // scanManifests treats app/ as a self-contained package
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	// app/ was SkipDir'd: none of its children — disk OR memory — may be delivered.
	for _, bad := range []string{"app/kustomization.yaml", "app/inner.yaml", "app/mem.yaml"} {
		if slices.Contains(visited, bad) {
			t.Errorf("SkipDir not honored on the disk layer: walk descended into %q\nvisited=%v", bad, visited)
		}
	}
	// lib/ was NOT skipped: its disk-only child must still surface.
	if !slices.Contains(visited, "lib/disk.yaml") {
		t.Errorf("over-skip: disk-only file lib/disk.yaml was not visited\nvisited=%v", visited)
	}
}
