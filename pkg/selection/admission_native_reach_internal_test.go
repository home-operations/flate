package selection

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_NativeConstructionReachExpires(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		root NativeRootFacts
		want bool
	}{
		{name: "pending", root: NativeRootFacts{Path: "entry", Entry: true}, want: true},
		{name: "built", root: NativeRootFacts{Path: "entry", Entry: true, Built: true}},
		{name: "selected", root: NativeRootFacts{Path: "entry", Entry: true, Selected: true}},
		{name: "claimed", root: NativeRootFacts{Path: "entry", Entry: true, Claims: []manifest.KSClaim{{Prefix: "entry/"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, frozen := range []bool{false, true} {
				st := store.New()
				root := t.TempDir()
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
				entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{})
				id := entry.Named()
				session := newSolverSession(t, st, change.NewSet([]string{"other/child.yaml"}), Request{Changed: true})
				facts := session.facts.(*solverFacts)
				facts.context.Root, facts.context.Frozen = root, frozen
				facts.context.Roots = []NativeRootFacts{tc.root}
				facts.index.Records = map[manifest.NamedResource]manifest.DiscoveryRecord{id: {
					ContentRoots: map[manifest.NamedResource][]string{
						manifest.BootstrapSourceID: {"entry/kustomization.yaml", "other/child.yaml"},
					},
				}}
				c := solverClosure(t, st, facts)
				c.context = facts.context
				c.proposal.Request.Changes = facts.changes
				if frozen {
					c.catalog = session.catalogSnapshot(facts.context.Roots)
				}
				assert.Equal(t, c.affected(id, entry), tc.want)
				assert.Equal(t, len(facts.index.Records[id].ContentRoots[manifest.BootstrapSourceID]), 2)
			}
		})
	}
}

func TestAdmission_NativeConstructionGeneration(t *testing.T) {
	t.Parallel()
	st := store.New()
	facts := &solverFacts{store: st}
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).Named()
	facts.index.Records = map[manifest.NamedResource]manifest.DiscoveryRecord{owner: {
		ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"child.yaml"}},
	}}
	c := solverClosure(t, st, facts)
	for _, built := range []bool{false, true, false} {
		c.context.Roots = []NativeRootFacts{{Path: "entry", Entry: true, Built: built}}
		assert.Equal(t, len(c.contentRecord(owner).ContentRoots) != 0, !built)
		assert.Equal(t, len(c.contentRecord(owner).ContentRoots) != 0, !built)
	}
	c.context.Roots = nil
	assert.Equal(t, len(c.contentRecord(owner).ContentRoots), 1)
}
