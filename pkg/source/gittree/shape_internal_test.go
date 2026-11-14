package gittree

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/source/sourceignore"
)

func TestSourceVisible_ReusesDirectoryMatches(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, ignore string
		want         []string
	}{
		{
			name: "visible",
			want: []string{".", "ignored", "ignored/empty", "kept", "kept/empty", "kept/empty/child", "kept/file"},
		},
		{
			name: "ignored parent", ignore: "ignored/\n",
			want: []string{".", "kept", "kept/empty", "kept/empty/child", "kept/file"},
		},
		{
			name: "reincluded parent", ignore: "ignored/\n!ignored/\nkept/empty/\n",
			want: []string{".", "ignored", "ignored/empty", "kept", "kept/file"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matcher, err := sourceignore.NewFS(fstest.MapFS{}, []string{}, tc.ignore, false)
			assert.NoError(t, err)
			calls := map[string]int{}
			visible := sourceVisible(func(name string, directory bool) bool {
				if directory {
					calls[name]++
				}
				return matcher.Match(name, directory)
			})
			var got []string
			for _, name := range []string{
				".", "ignored", "ignored/empty", "kept", "kept/empty", "kept/empty/child", "kept/file", "kept/link",
			} {
				mode := fs.ModeDir
				switch name {
				case "kept/file":
					mode = 0
				case "kept/link":
					mode = fs.ModeSymlink
				}
				if visible(nil, name, mode, nil) {
					got = append(got, name)
				}
			}
			assert.Diff(t, got, tc.want)
			assert.Equal(t, calls["kept"], 1)
			assert.Equal(t, calls["kept/empty"], 1)
		})
	}
}
