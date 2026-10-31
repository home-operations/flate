package change

import (
	"maps"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSet_SourceInputsIgnoreUnqualifiedCollision(t *testing.T) {
	t.Parallel()
	supplier := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "infra", Name: "supplier"}
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "optional"}
	changes := &Set{
		inputs: map[manifest.NamedResource]InputChange{input: {File: "inputs/optional.yaml", Unbound: true}},
		sources: map[manifest.NamedResource]sourceDelta{
			manifest.BootstrapSourceID: {changes: NewSet(nil)},
			supplier:                   {changes: NewSet(nil)},
		},
	}
	changes.groupSources()
	for source := range changes.Sources() {
		delta, _, _ := changes.ForSource(source)
		assert.Equal(t, len(maps.Collect(delta.Inputs())), 0)
	}
}
