package change

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFilter_UniversalAndRestricted(t *testing.T) {
	t.Parallel()
	id := testutil.KustomizationID("apps", "app")
	var universal *Filter
	membership := universal.Membership(id)
	needed, selected, dirty, revision := membership.Needed, membership.Selected, membership.Dirty, membership.Revision
	assert.Equal(t, needed && selected && dirty, true)
	assert.Equal(t, revision, uint64(0))
	var restricted Filter
	assert.Equal(t, restricted.Enabled(), true)
	assert.Equal(t, restricted.ShouldReconcile(id), false)
}

func TestFilter_UnionRejectsStaleProposal(t *testing.T) {
	t.Parallel()
	selected := testutil.KustomizationID("apps", "selected")
	support := testutil.KustomizationID("inputs", "support")
	rejected := testutil.KustomizationID("apps", "rejected")
	f := NewFilter()
	added, retry := f.AddEmitted(
		[]manifest.NamedResource{support, selected},
		[]manifest.NamedResource{selected},
		[]manifest.NamedResource{selected},
		0,
	)
	want := []manifest.NamedResource{selected, support}
	slices.SortFunc(want, manifest.NamedResource.Compare)
	assert.Diff(t, added, want)
	assert.Equal(t, retry, false)
	membership2 := f.Membership(support)
	needed, chosen, dirty, revision := membership2.Needed, membership2.Selected, membership2.Dirty, membership2.Revision
	assert.Equal(t, needed && !chosen && !dirty, true)
	assert.Equal(t, revision, uint64(1))
	assert.Diff(t, f.KeepNamespaces(), map[string]struct{}{"apps": {}})
	added, retry = f.AddEmitted([]manifest.NamedResource{rejected}, nil, nil, 0)
	assert.Equal(t, retry, true)
	assert.Equal(t, len(added), 0)
	assert.Equal(t, f.ShouldReconcile(rejected), false)
	added, retry = f.AddEmitted(nil, []manifest.NamedResource{support}, nil, revision)
	assert.Equal(t, retry, false)
	assert.Equal(t, len(added), 0)
	membership3 := f.Membership(support)
	needed, chosen, dirty, revision = membership3.Needed, membership3.Selected, membership3.Dirty, membership3.Revision
	assert.Equal(t, needed && chosen && !dirty, true)
	assert.Equal(t, revision, uint64(2))
	assert.Diff(t, f.KeepNamespaces(), map[string]struct{}{"apps": {}, "inputs": {}})
	attrs := f.LogValue().Group()
	assert.Equal(t, attrs[0].Value.Int64(), int64(2))
	assert.Diff(t, attrs[1].Value.Any().([]string), f.KeepNames())
	hidden := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
	_, retry = f.AddEmitted(nil, []manifest.NamedResource{hidden}, nil, revision)
	assert.Equal(t, retry, false)
	assert.Diff(t, f.KeepNamespaces(), map[string]struct{}{"apps": {}, "inputs": {}})
}
