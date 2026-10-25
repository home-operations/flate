package chartinput

import (
	"context"
	"errors"
	"io/fs"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
)

type observedFS struct {
	fs.FS
	opens map[string]int
	deny  string
}

func (f observedFS) Open(name string) (fs.File, error) {
	f.opens[name]++
	if name == f.deny {
		return nil, fs.ErrPermission
	}
	return f.FS.Open(name)
}

func TestWalk_Admission(t *testing.T) {
	t.Parallel()
	for _, policy := range []string{"", "excluded/\n*.tmp\n", ".helmignore\nexcluded/\n*.tmp\n"} {
		t.Run(policy, func(t *testing.T) {
			t.Parallel()
			input := fstest.MapFS{
				"Chart.yaml": {Data: []byte("metadata")}, "templates/cm.yaml": {Data: []byte("payload")},
				"charts/child.tgz": {Data: []byte("archive")}, "excluded/bad": {Mode: fs.ModeSymlink},
				"drop.tmp": {Data: []byte("excluded")}, "templates/.hidden": {Data: []byte("default exclusion")},
			}
			want := []string{"Chart.yaml", "charts/child.tgz", "templates/cm.yaml"}
			if policy == "" {
				delete(input, "excluded/bad")
				want = append(want, "drop.tmp")
			} else {
				input[".helmignore"] = &fstest.MapFile{Data: []byte(policy)}
				if policy[0] != '.' {
					want = append(want, ".helmignore")
				}
			}
			opens := map[string]int{}
			var names []string
			err := Walk(t.Context(), observedFS{FS: input, opens: opens}, func(name string, info fs.FileInfo, data []byte) error {
				names = append(names, name)
				assert.Equal(t, info.Mode().IsRegular(), true)
				if name == ".helmignore" {
					assert.Equal(t, string(data), policy)
				} else {
					assert.Equal(t, data == nil, true)
				}
				return nil
			})
			assert.NoError(t, err)
			slices.Sort(want)
			assert.Diff(t, names, want)
			assert.Equal(t, opens[".helmignore"], 1)
			for name := range input {
				if name != ".helmignore" {
					assert.Equal(t, opens[name], 0)
				}
			}
		})
	}
}

func TestWalk_Errors(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"canceled", "cancel during visit", "visitor", "policy parse", "policy open", "irregular"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			input := fstest.MapFS{"a": {}, "b": {}}
			observed := observedFS{FS: input, opens: map[string]int{}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := errors.New("visitor failed")
			switch failure {
			case "canceled":
				cancel()
			case "cancel during visit":
				delete(input, "b")
			case "policy parse":
				input[".helmignore"] = &fstest.MapFile{Data: []byte("**\n")}
			case "policy open":
				observed.deny = ".helmignore"
			case "irregular":
				input["a"].Mode = fs.ModeSymlink
			}
			err := Walk(ctx, observed, func(string, fs.FileInfo, []byte) error {
				if failure == "cancel during visit" {
					cancel()
				}
				if failure == "visitor" {
					return want
				}
				return nil
			})
			assert.Equal(t, err != nil, true)
			switch failure {
			case "canceled", "cancel during visit":
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			case "visitor":
				assert.Equal(t, errors.Is(err, want), true)
			case "policy open":
				assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
			}
		})
	}
}
