package change

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestDetect_FluxIgnoreRules(t *testing.T) {
	t.Setenv("PATH", "")
	cases := []struct {
		name                      string
		before, after             map[string]string
		want                      []string
		nestedBefore, nestedAfter bool
	}{
		{name: "dot_file", before: map[string]string{".envrc": "old"}, after: map[string]string{".envrc": "new"}, want: []string{".envrc"}},
		{name: "dot_directory", before: map[string]string{"apps/.overlays/patch.yaml": "old"}, after: map[string]string{"apps/.overlays/patch.yaml": "new"}, want: []string{"apps/.overlays/patch.yaml"}},
		{name: "vendor", before: map[string]string{"vendor/file.yaml": "old"}, after: map[string]string{"vendor/file.yaml": "new"}, want: []string{"vendor/file.yaml"}},
		{name: "node_modules", before: map[string]string{"node_modules/file.yaml": "old"}, after: map[string]string{"node_modules/file.yaml": "new"}, want: []string{"node_modules/file.yaml"}},
		{name: "sops_defaults", before: map[string]string{"apps/.sops.yaml": "old"}, after: map[string]string{"apps/.sops.yaml": "new"}},
		{name: "ci_defaults", before: map[string]string{".github/workflows/ci.yaml": "old"}, after: map[string]string{".github/workflows/ci.yaml": "new"}},
		{name: "extension_defaults", before: map[string]string{"image.png": "old"}, after: map[string]string{"image.png": "new"}},
		{name: "vcs_defaults", before: map[string]string{".gitignore": "old", ".gitmodules": "old", ".gitattributes": "old"}, after: map[string]string{".gitignore": "new", ".gitmodules": "new", ".gitattributes": "new"}},
		{name: "sourceignore_edit", before: map[string]string{".sourceignore": "# old\n"}, after: map[string]string{".sourceignore": "# new\n"}, want: []string{".sourceignore"}},
		{name: "excluded_on_both_sides", before: map[string]string{".sourceignore": "skip.yaml\n", "skip.yaml": "old"}, after: map[string]string{".sourceignore": "skip.yaml\n", "skip.yaml": "new"}},
		{name: "one_sided_pattern", before: map[string]string{"file.yaml": "same"}, after: map[string]string{".sourceignore": "file.yaml\n", "file.yaml": "same"}, want: []string{".sourceignore", "file.yaml"}},
		{name: "one_sided_pattern_edit", before: map[string]string{"file.yaml": "old"}, after: map[string]string{".sourceignore": "file.yaml\n", "file.yaml": "new"}, want: []string{".sourceignore", "file.yaml"}},
		{name: "pattern_only_before_file_absent_after", before: map[string]string{".sourceignore": "x.yaml\n", "x.yaml": "old"}, after: map[string]string{}, want: []string{".sourceignore"}},
		{name: "pattern_removed_file_kept", before: map[string]string{".sourceignore": "x.yaml\n", "x.yaml": "same"}, after: map[string]string{"x.yaml": "same"}, want: []string{".sourceignore", "x.yaml"}},
		{name: "nested_reinclude", before: map[string]string{".sourceignore": "apps/*\n!apps/keep.yaml\n", "apps/drop.yaml": "old", "apps/keep.yaml": "old"}, after: map[string]string{".sourceignore": "apps/*\n!apps/keep.yaml\n", "apps/drop.yaml": "new", "apps/keep.yaml": "new"}, want: []string{"apps/keep.yaml"}},
		{name: "reinclude_under_excluded_dir", before: map[string]string{".sourceignore": "vendor/\n!vendor/keep.yaml\n", "vendor/drop.yaml": "old", "vendor/keep.yaml": "old"}, after: map[string]string{".sourceignore": "vendor/\n!vendor/keep.yaml\n", "vendor/drop.yaml": "new", "vendor/keep.yaml": "new"}, want: []string{"vendor/keep.yaml"}},
		{name: "reinclude_via_nested_sourceignore", before: map[string]string{".sourceignore": "sub/\n", "sub/.sourceignore": "!keep.yaml\n", "sub/drop.yaml": "old", "sub/keep.yaml": "old"}, after: map[string]string{".sourceignore": "sub/\n", "sub/.sourceignore": "!keep.yaml\n", "sub/drop.yaml": "new", "sub/keep.yaml": "new"}, want: []string{"sub/keep.yaml"}},
		{name: "nested_sourceignore", before: map[string]string{"apps/.sourceignore": "skip.yaml\n", "apps/skip.yaml": "old"}, after: map[string]string{"apps/.sourceignore": "skip.yaml\n", "apps/skip.yaml": "new"}},
		{name: "user_patterns_replace_defaults", before: map[string]string{".sourceignore": "skip.yaml\n", ".sops.yaml": "old"}, after: map[string]string{".sourceignore": "skip.yaml\n", ".sops.yaml": "new"}, want: []string{".sops.yaml"}},
		{name: "baseline_nested_ignore_rules", nestedBefore: true, before: map[string]string{".sourceignore": "*.tmp\n"}, after: map[string]string{"image.png": "current"}, want: []string{".sourceignore"}},
		{name: "current_nested_ignore_rules", nestedAfter: true, before: map[string]string{"image.png": "baseline"}, after: map[string]string{".sourceignore": "*.tmp\n"}, want: []string{".sourceignore"}},
		{name: "excluded_add_delete", before: map[string]string{".github/old.yaml": "old"}, after: map[string]string{".github/new.yaml": "new"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before, after := t.TempDir(), t.TempDir()
			if tc.nestedBefore {
				before = filepath.Join(after, "baseline")
			}
			if tc.nestedAfter {
				after = filepath.Join(before, "current")
			}
			for rel, body := range tc.before {
				testutil.WriteFile(t, before, rel, body)
			}
			for rel, body := range tc.after {
				testutil.WriteFile(t, after, rel, body)
			}
			for _, reverse := range []bool{false, true} {
				left, right := before, after
				if reverse {
					left, right = after, before
				}
				got, err := Detect(left, right)
				if err != nil {
					t.Fatal(err)
				}
				assert.Diff(t, got.Paths(), tc.want)
			}
		})
	}
}

func TestDetect_GitlinkFile(t *testing.T) {
	t.Setenv("PATH", "")
	before, after := t.TempDir(), t.TempDir()
	for _, rel := range []string{".git", "apps/.git", "apps/nested/.git/config"} {
		testutil.WriteFile(t, before, rel, "old")
		testutil.WriteFile(t, after, rel, "new")
	}
	testutil.WriteFile(t, before, "links/keep.yaml", "same")
	testutil.WriteFile(t, after, "links/keep.yaml", "same")
	if err := os.Symlink("old", filepath.Join(before, "links/.git")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("new", filepath.Join(after, "links/.git")); err != nil {
		t.Fatal(err)
	}
	for _, roots := range [][2]string{{before, after}, {after, before}} {
		got, err := Detect(roots[0], roots[1])
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, got.Len(), 0)
	}
}

func TestDetect_NestedOppositeRoot(t *testing.T) {
	t.Setenv("PATH", "")
	outer := t.TempDir()
	inner := filepath.Join(outer, "baseline")
	testutil.WriteFile(t, outer, "same.yaml", "same")
	testutil.WriteFile(t, inner, "same.yaml", "same")
	testutil.WriteFile(t, outer, "mod.yaml", "old")
	testutil.WriteFile(t, inner, "mod.yaml", "new")
	for _, roots := range [][2]string{{outer, inner}, {inner, outer}} {
		got, err := Detect(roots[0], roots[1])
		if err != nil {
			t.Fatal(err)
		}
		assert.Diff(t, got.Paths(), []string{"mod.yaml"})
	}
}

func TestDetect_Concurrent(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			before, after := t.TempDir(), t.TempDir()
			var want []string
			for i := range 100 {
				rel := fmt.Sprintf("apps/app-%03d/config.yaml", i)
				testutil.WriteFile(t, before, rel, "old")
				testutil.WriteFile(t, after, rel, "new")
				want = append(want, rel)
			}
			var g errgroup.Group
			for range concurrency {
				g.Go(func() error {
					got, err := Detect(before, after)
					if err != nil {
						return err
					}
					if paths := got.Paths(); !slices.Equal(paths, want) {
						return fmt.Errorf("paths = %v, want %v", paths, want)
					}
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
