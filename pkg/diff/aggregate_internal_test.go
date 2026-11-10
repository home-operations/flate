package diff

import (
	"testing"

	"github.com/homeport/dyff/pkg/dyff"

	"github.com/home-operations/flate/internal/assert"
)

func TestClusterReport_NativeAggregateIndexes(t *testing.T) {
	t.Parallel()
	doc := func(name, value string) Doc {
		return Doc{Manifest: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": name}, "data": map[string]any{"value": value},
		}}
	}
	groups := make([]dyff.Report, 2)
	var err error
	groups[0], err = prepare([]Doc{doc("same", "old"), doc("removed", "old")}, []Doc{doc("same", "new")}, &Options{}).
		native()
	assert.NoError(t, err)
	groups[1], err = prepare([]Doc{doc("same", "old")}, []Doc{doc("same", "new"), doc("added", "new")}, &Options{}).
		native()
	assert.NoError(t, err)
	aggregate := aggregateNative(groups)
	assert.Equal(t, len(aggregate.From.Documents), 3)
	assert.Equal(t, len(aggregate.To.Documents), 3)
	assert.Equal(t, len(aggregate.Diffs), 4)
	assert.Diff(t, aggregate.From.Names, []string{"v1/ConfigMap/same", "v1/ConfigMap/removed", "v1/ConfigMap/same"})
	assert.Diff(t, aggregate.To.Names, []string{"v1/ConfigMap/same", "v1/ConfigMap/same", "v1/ConfigMap/added"})
	for i, want := range []struct {
		to    bool
		index int
	}{{false, 0}, {false, 1}, {false, 2}, {true, 2}} {
		path := aggregate.Diffs[i].Path
		root := &aggregate.From
		if want.to {
			root = &aggregate.To
		}
		assert.FatalIf(
			t,
			path.Root != root || path.DocumentIdx != want.index,
			"diff %d has wrong side or offset: %+v",
			i,
			path,
		)
	}
	assert.Equal(t, groups[1].Diffs[0].Path.DocumentIdx, 0)
	assert.FatalIf(t, groups[1].Diffs[0].Path.Root == &aggregate.From, "%v", "aggregation mutated its input paths")
}
