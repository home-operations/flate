package manifest

import (
	"slices"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestReferences_ChartPaths(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, want string }{
		{".", "."},
		{"./", "./"},
		{"./charts/demo", "./charts/demo"},
		{"charts/demo.tgz", "charts/demo.tgz"},
		{"", ""},
		{"../escape", ""},
		{"charts/../../escape", ""},
		{"/absolute", ""},
		{"bad\x00chart", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			obj := &HelmChartSource{
				Name:      "chart",
				Chart:     tc.name,
				SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: KindGitRepository, Name: "source"},
			}
			refs := slices.Collect(References(obj, ReferenceRelations))
			assert.Equal(t, refs[0].ChartPath, tc.want)
			assert.Equal(t, obj.Chart, tc.name)
		})
	}
}
