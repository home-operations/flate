package kustomize

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/sourceignore"
)

// allowlistIgnore is the mono-repo .sourceignore shape from #936: exclude
// everything at the root, re-include the manifest directories.
const allowlistIgnore = "# Exclude all\n/*\n# Include manifest directories\n!/clusters/\n!/charts/x/\n"

func testIgnoreFS(t *testing.T, root string) ignoreFS {
	t.Helper()
	matcher, err := sourceignore.New(root, nil, true)
	if err != nil {
		t.Fatalf("matcher: %v", err)
	}
	disk := testOverlayFS(t, root).(overlayFS).disk
	return newIgnoreFS(disk, root, matcher).(ignoreFS)
}

func TestIgnoreFS_HidesExcludedPaths(t *testing.T) {
	root := writeTree(t, map[string]string{
		".sourceignore":         allowlistIgnore,
		"clusters/dev/cm.yaml":  cmYAML("dev"),
		"bundles/common/a.yaml": cmYAML("a"),
		"charts/x/Chart.yaml":   "name: x\n",
		"charts/y/Chart.yaml":   "name: y\n",
		"README.md":             "hi\n",
	})
	ig := testIgnoreFS(t, root)
	abs := func(rel string) string { return filepath.Join(root, rel) }

	cases := []struct {
		rel     string
		visible bool
	}{
		{"clusters", true},
		{"clusters/dev", true},
		{"clusters/dev/cm.yaml", true},
		{"bundles", false},
		{"bundles/common", false},
		{"bundles/common/a.yaml", false},
		{"README.md", false},
		// charts/ matches /* but a deeper `!` keeps charts/x/**, so the parent
		// still exists in the artifact while its excluded sibling does not.
		{"charts", true},
		{"charts/x", true},
		{"charts/x/Chart.yaml", true},
		{"charts/y", false},
		{"charts/y/Chart.yaml", false},
	}
	for _, tc := range cases {
		t.Run(tc.rel, func(t *testing.T) {
			p := abs(tc.rel)
			if got := ig.Exists(p); got != tc.visible {
				t.Errorf("Exists = %v, want %v", got, tc.visible)
			}
			isDir := ig.disk.IsDir(p)
			if got := ig.IsDir(p); got != (tc.visible && isDir) {
				t.Errorf("IsDir = %v, want %v", got, tc.visible && isDir)
			}
			_, _, err := ig.CleanedAbs(p)
			if (err == nil) != tc.visible {
				t.Errorf("CleanedAbs err = %v, want visible=%v", err, tc.visible)
			}
			if !isDir {
				_, err := ig.ReadFile(p)
				if (err == nil) != tc.visible {
					t.Errorf("ReadFile err = %v, want visible=%v", err, tc.visible)
				}
				if !tc.visible && !errors.Is(err, os.ErrNotExist) {
					t.Errorf("hidden ReadFile error should be not-exist, got %v", err)
				}
			}
		})
	}

	// The root itself is never hidden, and its listing drops hidden children.
	if !ig.IsDir(root) {
		t.Fatal("root must stay visible")
	}
	entries, err := ig.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir root: %v", err)
	}
	slices.Sort(entries)
	if want := []string{"charts", "clusters"}; !slices.Equal(entries, want) {
		t.Errorf("ReadDir root = %v, want %v", entries, want)
	}

	var walked []string
	if err := ig.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, _ := filepath.Rel(root, p)
			walked = append(walked, filepath.ToSlash(rel))
		}
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	slices.Sort(walked)
	if want := []string{"charts/x/Chart.yaml", "clusters/dev/cm.yaml"}; !slices.Equal(walked, want) {
		t.Errorf("Walk files = %v, want %v", walked, want)
	}
}

// TestRenderFlux_SourceignoreAllowlist is the regression for #936: a root
// .sourceignore allowlist that leaves a directory out of the artifact must make
// a working-tree render fail the way Flux does, whether the directory is the
// Kustomization's spec.path or a base an in-tree kustomization.yaml pulls in.
// A fetched artifact (applyIgnore=false) is left alone: its fetcher already
// filtered it.
func TestRenderFlux_SourceignoreAllowlist(t *testing.T) {
	root := writeTree(t, map[string]string{
		".sourceignore":                       allowlistIgnore,
		"bundles/common/kustomization.yaml":   existingKustomization("cm.yaml"),
		"bundles/common/cm.yaml":              cmYAML("common"),
		"clusters/dev/app/kustomization.yaml": existingKustomization("../../../bundles/common"),
		"clusters/dev/ok/cm.yaml":             cmYAML("ok"),
	})
	ctx := context.Background()
	spec := func(path string) map[string]any {
		return ksDoc(map[string]any{"path": path})
	}

	t.Run("spec.path excluded", func(t *testing.T) {
		_, err := RenderFlux(ctx, NewTreeCache(), root, true, "bundles/common", spec("./bundles/common"))
		if !errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), "not a directory") {
			t.Fatalf("expected the excluded spec.path to be reported missing, got %v", err)
		}
	})

	t.Run("kustomization base excluded", func(t *testing.T) {
		_, err := RenderFlux(ctx, NewTreeCache(), root, true, "clusters/dev/app", spec("./clusters/dev/app"))
		if err == nil || !strings.Contains(err.Error(), "bundles/common") {
			t.Fatalf("expected the build to fail resolving the excluded base, got %v", err)
		}
	})

	t.Run("allowlisted path renders", func(t *testing.T) {
		out, err := RenderFlux(ctx, NewTreeCache(), root, true, "clusters/dev/ok", spec("./clusters/dev/ok"))
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if !strings.Contains(string(out), "name: ok") {
			t.Fatalf("expected rendered ConfigMap, got:\n%s", out)
		}
	})

	t.Run("fetched artifact not refiltered", func(t *testing.T) {
		out, err := RenderFlux(ctx, NewTreeCache(), root, false, "bundles/common", spec("./bundles/common"))
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if !strings.Contains(string(out), "name: common") {
			t.Fatalf("expected rendered ConfigMap, got:\n%s", out)
		}
	})
}

// TestRenderFlux_SourceignoreCacheReplaysFilteredView pins that a cached render
// of a working-tree source validates against the filtered disk view it was
// recorded through: a probe of a hidden path is recorded as absent and must
// still read as absent on replay, or every such render would miss forever.
func TestRenderFlux_SourceignoreCacheReplaysFilteredView(t *testing.T) {
	root := writeTree(t, map[string]string{
		".sourceignore": "b.yaml\n",
		"a.yaml":        cmYAML("a"),
		"b.yaml":        cmYAML("b"),
	})
	cache := NewTreeCache()
	cache.SetRenderCache(t.TempDir(), 1<<30)
	ctx := context.Background()

	out1, err := RenderFlux(ctx, cache, root, true, ".", demoRawSpec)
	if err != nil {
		t.Fatalf("first render: %v", err)
	}
	if strings.Contains(string(out1), "name: b") {
		t.Fatalf("b.yaml should be excluded; got %s", out1)
	}
	dr, err := cache.diskRootFor(root)
	if err != nil {
		t.Fatal(err)
	}
	ig, err := dr.ignored()
	if err != nil {
		t.Fatal(err)
	}
	key := renderKey(demoRawSpec, dr.root, ".", ig.key)
	snap, _, ok := cache.render.get(key)
	if !ok {
		t.Fatal("first render did not populate the cache")
	}
	cache.render.put(key, snap, []byte("SENTINEL\n"))
	out2, err := RenderFlux(ctx, cache, root, true, ".", demoRawSpec)
	if err != nil {
		t.Fatalf("second render: %v", err)
	}
	if string(out2) != "SENTINEL\n" {
		t.Errorf("expected a cache hit against the filtered view; got a re-render:\n%s", out2)
	}
}
