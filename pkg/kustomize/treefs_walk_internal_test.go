package kustomize

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
)

// SkipDir must suppress both layers' children while other merged directories
// still expose disk-only entries to scanManifests.
func TestTreeFS_WalkPropagatesSkipDir(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"app/kustomization.yaml": existingKustomization("./inner.yaml"),
		"app/inner.yaml":         cmYAML("inner"),
		"lib/disk.yaml":          cmYAML("libdisk"),
	})
	fsys := testTreeFS(t, root)
	// Mirror preflightRemoteResources: a rewritten file under app/ (so app/
	// exists in BOTH layers and is walked from memory) and one under lib/.
	for _, rel := range []string{"app/mem.yaml", "lib/mem.yaml"} {
		{
			err := fsys.WriteFile(filepath.Join(root, rel), []byte(cmYAML("mem")))
			assert.FatalIf(t, err != nil, "seed memory %s: %v", rel, err)
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
	{
		assert.FatalIf(t, err != nil, "walk: %v", err)
	}

	// app/ was SkipDir'd: none of its children — disk OR memory — may be delivered.
	for _, bad := range []string{"app/kustomization.yaml", "app/inner.yaml", "app/mem.yaml"} {
		{
			assert.ErrorIf(
				t,
				slices.Contains(visited, bad),
				"SkipDir not honored on the disk layer: walk descended into %q\nvisited=%v",
				bad,
				visited,
			)
		}
	}
	// lib/ was NOT skipped: its disk-only child must still surface.
	{
		assert.ErrorIf(
			t,
			!slices.Contains(visited, "lib/disk.yaml"),
			"over-skip: disk-only file lib/disk.yaml was not visited\nvisited=%v",
			visited,
		)
	}
}
