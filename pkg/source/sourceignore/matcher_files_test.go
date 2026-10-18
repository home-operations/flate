package sourceignore

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	flux "github.com/fluxcd/pkg/sourceignore"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestNewFS_Parity(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
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
					patterns, err := flux.LoadIgnorePatterns(root, nil)
					assert.NoError(t, err)
					if extra != nil {
						patterns = append(patterns, flux.ReadPatterns(strings.NewReader(*extra), nil)...)
					}
					want := flux.NewMatcher(patterns)
					if withDefaults {
						if len(patterns) == 0 {
							want = flux.NewDefaultMatcher(nil, nil)
						} else {
							want = flux.NewMatcher(append(flux.VCSPatterns(nil), patterns...))
						}
					}
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
							assert.Equal(t, listed.Match(rel, isDir), want.Match(strings.Split(rel, string(filepath.Separator)), isDir))
							assert.Equal(t, walked.Match(rel, isDir), want.Match(strings.Split(rel, string(filepath.Separator)), isDir))
						}
					}
				}
			}
		})
	}
}

func TestNewFS_ReadErrors(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, ".sourceignore", "*.tmp\n")
	testutil.WriteFile(t, root, "app/.sourceignore", "!keep.tmp\n")
	input := os.DirFS(root)
	cases := []struct {
		name     string
		input    fs.FS
		files    []string
		wantErr  error
		wantPath string
	}{
		{name: "missing_supplied_file", input: input, files: []string{"missing/.sourceignore", ".sourceignore"}},
		{name: "supplied_read_error", input: ignoreReadErrorFS{FS: input}, files: []string{".sourceignore"}, wantErr: fs.ErrPermission},
		{name: "discovered_read_error", input: ignoreReadErrorFS{FS: input}, wantErr: fs.ErrPermission},
		{name: "root_read_precedes_listing", input: ignoreReadErrorFS{FS: input, denied: ".sourceignore"}, wantErr: fs.ErrPermission, wantPath: ".sourceignore"},
		{name: "nested_read_precedes_listing", input: ignoreReadErrorFS{FS: input, denied: "app/.sourceignore"}, wantErr: fs.ErrPermission, wantPath: "app/.sourceignore"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matcher, err := NewFS(tc.input, tc.files, "!keep.tmp\n", true)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("NewFS error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantPath != "" {
				pathErr, ok := errors.AsType[*fs.PathError](err)
				if !ok {
					t.Fatalf("NewFS error = %v, want PathError", err)
				}
				assert.Equal(t, pathErr.Path, tc.wantPath)
				assert.Equal(t, pathErr.Op, "read")
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
	denied string
}

func (f ignoreReadErrorFS) ReadFile(name string) ([]byte, error) {
	if f.denied == "" || name == f.denied {
		return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrPermission}
	}
	return fs.ReadFile(f.FS, name)
}

func (f ignoreReadErrorFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if f.denied != "" && name == path.Dir(f.denied) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrPermission}
	}
	return fs.ReadDir(f.FS, name)
}

func TestNewFS_Evidence(t *testing.T) {
	t.Parallel()
	files := []string{"z/.sourceignore", "app/deep/.sourceignore", "app/.sourceignore", ".sourceignore", "missing/.sourceignore"}
	want := []struct{ Path, Data string }{
		{".sourceignore", "# root \xff\r\n*.tmp\r\nignored.yaml\r\n"},
		{"app/.sourceignore", "!keep.tmp\n"},
		{"z/.sourceignore", "!keep.tmp\n"},
		{"app/deep/.sourceignore", "keep.tmp\n"},
	}
	for _, tc := range []struct {
		name  string
		files []string
	}{{"walked", nil}, {"listed", files}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := fstest.MapFS{".git/.sourceignore": {Data: []byte("*.yaml\n")}}
			for _, file := range want {
				data[file.Path] = &fstest.MapFile{Data: []byte(file.Data)}
			}
			input := &ignoreEvidenceFS{FS: data, reads: make(map[string]int)}
			matcher, err := NewFS(input, tc.files, "!app/deep/final.tmp\n", true)
			assert.NoError(t, err)
			assert.Diff(t, matcher.Evidence(), want)
			assert.Equal(t, matcher.Match("cache.tmp", false), true)
			assert.Equal(t, matcher.Match("app/keep.tmp", false), false)
			assert.Equal(t, matcher.Match("z/keep.tmp", false), false)
			assert.Equal(t, matcher.Match("app/deep/keep.tmp", false), true)
			assert.Equal(t, matcher.Match("app/deep/final.tmp", false), false)
			replaced := matcher.WithExtra("!replacement.tmp\n")
			assert.Diff(t, replaced.Evidence(), want)
			assert.Equal(t, replaced.Match("replacement.tmp", false), false)
			assert.Equal(t, replaced.Match("app/deep/final.tmp", false), true)
			assert.Equal(t, matcher.Match("replacement.tmp", false), true)
			assert.Equal(t, matcher.Match("app/deep/final.tmp", false), false)
			results := make(chan bool, 2)
			for _, name := range []string{"left.tmp", "right.tmp"} {
				go func() {
					policy := matcher.WithExtra("!" + name + "\n")
					results <- !policy.Match(name, false) && matcher.Match(name, false)
				}()
			}
			for range 2 {
				assert.Equal(t, <-results, true)
			}
			for _, file := range want {
				assert.Equal(t, input.reads[file.Path], 1)
			}
			evidence := matcher.Evidence()
			evidence[0].Path, evidence[0].Data = "replaced", "!*\n"
			assert.Diff(t, matcher.Evidence(), want)
			assert.Equal(t, matcher.Match("cache.tmp", false), true)
		})
	}
}

type ignoreEvidenceFS struct {
	fs.FS
	reads map[string]int
}

func (f *ignoreEvidenceFS) ReadFile(name string) ([]byte, error) {
	f.reads[name]++
	if f.reads[name] > 1 {
		return []byte("!*\n"), nil
	}
	return fs.ReadFile(f.FS, name)
}
