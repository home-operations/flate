package gittree

import (
	"maps"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/source/sourceignore"
	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestShape_AdmitsDirectories(t *testing.T) {
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
			kept := map[string]bool{".": true}
			for _, name := range []string{
				"ignored", "ignored/empty", "kept", "kept/empty", "kept/empty/child", "kept/file",
			} {
				matcher.Admit(kept, name, name != "kept/file")
			}
			assert.Diff(t, slices.Sorted(maps.Keys(kept)), tc.want)
		})
	}
}
