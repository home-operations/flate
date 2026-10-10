package sourceignore

import (
	"io/fs"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestNewFromFiles_Parity(t *testing.T) {
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
				write(t, root, rel, body)
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
					if err != nil {
						t.Fatal(err)
					}
					got, err := NewFromFiles(root, files, extra, withDefaults)
					if err != nil {
						t.Fatal(err)
					}
					for _, rel := range paths {
						for _, isDir := range []bool{false, true} {
							assert.Equal(t, got.Match(rel, isDir), want.Match(rel, isDir))
						}
					}
				}
			}
		})
	}
}
