package manifest

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
)

func assertReferenceEdges(t *testing.T, got, want []Reference) {
	t.Helper()
	{
		diff := cmp.Diff(
			want,
			got,
			cmp.AllowUnexported(Reference{}),
			cmpopts.IgnoreFields(DependencyRef{}, "Artifact", "ArtifactOnly", "ChartSource"),
			cmpopts.IgnoreFields(
				Reference{},
				"Gate",
				"ArtifactStages",
				"Ordering",
				"Selector",
				"Key",
				"NamespaceDefault",
			),
		)
		assert.FatalIf(t, diff != "", "reference edges (-want +got):\n%s", diff)
	}
}
