package sourceignore

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestNewFS_Parity(t *testing.T) {
	cases := []struct {
		name  string
		files map[string]string
	}{
		{name: "no_rules", files: map[string]string{"image.png": "image", "app/keep.yaml": "keep"}},
		{name: "empty_rules", files: map[string]string{".sourceignore": "# comment\n", "app/.sourceignore": "\n"}},
		{
			name: "nested_negations",
			files: map[string]string{
				".sourceignore":          "*.tmp\n",
				"app/.sourceignore":      "!keep.tmp\n",
				"app/deep/.sourceignore": "keep.tmp\n",
				"app/keep.tmp":           "keep",
				"app/deep/keep.tmp":      "drop",
				"other/cache.tmp":        "drop",
			},
		},
		{
			name: "ignored_directory_rules",
			files: map[string]string{
				".sourceignore":        "vendor/\n",
				"vendor/.sourceignore": "!keep.yaml\n",
				"vendor/keep.yaml":     "keep",
				"vendor/drop.yaml":     "drop",
			},
		},
		{
			name: "parent_before_lexically_earlier_child",
			files: map[string]string{
				".sourceignore":               "!**/keep.yaml\n",
				"!early/.sourceignore":        "keep.yaml\n",
				"!early/.child/.sourceignore": "!keep.yaml\n",
				"!early/keep.yaml":            "drop",
				"!early/.child/keep.yaml":     "keep",
			},
		},
		{name: "vcs_rules_skipped", files: map[string]string{".git/.sourceignore": "*.yaml\n", "keep.yaml": "keep"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for rel, body := range tc.files {
				testutil.WriteFile(t, root, rel, body)
			}
			var files, paths []string
			err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if path == root {
					return nil
				}
				rel, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				paths = append(paths, rel)
				if d.IsDir() && d.Name() == ".git" {
					return filepath.SkipDir
				}
				if !d.IsDir() && d.Name() == ".sourceignore" {
					files = append(files, path)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			slices.Reverse(files)
			for _, withDefaults := range []bool{false, true} {
				for _, extra := range []*string{nil, new("!app/keep.tmp\n")} {
					want, err := New(root, extra, withDefaults)
					assert.NoError(t, err)
					var relative []string
					for _, file := range files {
						rel, err := filepath.Rel(root, file)
						assert.NoError(t, err)
						relative = append(relative, filepath.ToSlash(rel))
					}
					rules := ""
					if extra != nil {
						rules = *extra
					}
					orig := slices.Clone(relative)
					listed, err := NewFS(os.DirFS(root), relative, rules, withDefaults)
					assert.NoError(t, err)
					assert.Diff(t, relative, orig)
					walked, err := NewFS(os.DirFS(root), nil, rules, withDefaults)
					assert.NoError(t, err)
					for _, rel := range paths {
						for _, isDir := range []bool{false, true} {
							assert.Equal(t, listed.Match(rel, isDir), want.Match(rel, isDir))
							assert.Equal(t, walked.Match(rel, isDir), want.Match(rel, isDir))
						}
					}
				}
			}
		})
	}
}
