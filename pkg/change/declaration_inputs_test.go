package change

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFilter_DeclarationInputs(t *testing.T) {
	t.Parallel()
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	consumer := testutil.KustomizationID("apps", "consumer")
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		input:    {File: "settings.yaml", Inputs: []string{"patch.yaml"}},
		consumer: {File: "consumer.yaml", Refs: []manifest.NamedResource{input}},
	}}
	filter := NewFilter(NewSet([]string{"patch.yaml"}), index, nil, "/repo")
	assert.Diff(t, filter.KeepNames(), []string{input.String(), consumer.String()})
	assert.Equal(t, filter.keep[input], primary)
	assert.Equal(t, filter.keep[consumer], primary)
}
