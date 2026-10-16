package sourceignore

import (
	"errors"
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
			name: "sibling_rules_at_different_depths",
			files: map[string]string{
				".sourceignore":             "*.tmp\n",
				"app/.sourceignore":         "!keep.tmp\n",
				"app/deep/.sourceignore":    "keep.tmp\n",
				"app/keep.tmp":              "keep",
				"app/deep/keep.tmp":         "drop",
				"other/deep/.sourceignore":  "!keep.tmp\n",
				"other/deep/keep.tmp":       "keep",
				"other/deep/drop.tmp":       "drop",
				"other/deep/child/keep.tmp": "keep",
				"other/keep.tmp":            "drop",
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

func TestNewFS_ReadErrors(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, ".sourceignore", "*.tmp\n")
	input := os.DirFS(root)
	cases := []struct {
		name    string
		input   fs.FS
		files   []string
		wantErr error
	}{
		{name: "missing_supplied_file", input: input, files: []string{"missing/.sourceignore", ".sourceignore"}},
		{name: "supplied_read_error", input: ignoreReadErrorFS{FS: input}, files: []string{".sourceignore"}, wantErr: fs.ErrPermission},
		{name: "discovered_read_error", input: ignoreReadErrorFS{FS: input}, wantErr: fs.ErrPermission},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			matcher, err := NewFS(tc.input, tc.files, "!keep.tmp\n", true)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("NewFS error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil {
				assert.Equal(t, matcher.Match("cache.tmp", false), true)
				assert.Equal(t, matcher.Match("keep.tmp", false), false)
			}
		})
	}
}

type ignoreReadErrorFS struct {
	fs.FS
}

func (f ignoreReadErrorFS) ReadFile(name string) ([]byte, error) {
	return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrPermission}
}
