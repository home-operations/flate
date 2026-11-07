package selection

import (
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // AllocsPerRun changes process-global runtime state.
func TestAdmission_HistoryProductionAllocations(t *testing.T) {
	for _, matching := range []bool{false, true} {
		t.Run(strconv.FormatBool(matching), func(t *testing.T) {
			st := store.New()
			facts := &solverFacts{
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
			}
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
			owner := testutil.KustomizationID("apps", "owner")
			other := testutil.KustomizationID("apps", "other")
			want := []manifest.Production{{Owner: owner, Authored: id, File: "apps/input.yaml"}}
			facts.index.Records[id] = manifest.DiscoveryRecord{Productions: want}
			c := solverClosure(t, st, facts)
			c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{other: {}}
			c.replace = map[manifest.NamedResource]bool{other: true}

			if matching {
				c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{
					owner: {id: {Productions: want}},
				}
				c.replace = map[manifest.NamedResource]bool{owner: true}
			}
			assert.Diff(t, c.productions(id), want)
			if allocations := testing.AllocsPerRun(100, func() { c.productions(id) }); allocations != 0 {
				t.Fatalf("accepted history allocates %g times per repeated production lookup", allocations)
			}
			if !matching {
				if allocations := testing.AllocsPerRun(100, func() {
					c.historyProductions = nil
					c.productions(id)
				}); allocations != 0 {
					t.Fatalf("unrelated history allocates %g times per cold production lookup", allocations)
				}
			}
			assert.Diff(t, facts.index.Records[id].Productions, want)
		})
	}
}

func TestAdmission_HistoryProductionRefresh(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{
			LocalPath: root,
			FS: fstest.MapFS{
				"owner.yaml": {Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./payload\n"))},
			},
		},
	)
	changes := change.NewSet([]string{"owner.yaml"})
	facts := &solverFacts{
		store:   st,
		changes: changes,
		index:   manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
	}
	facts.context.Root = root
	ownerObject := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "payload"}
	st.AddObject(ownerObject)
	facts.index.Records[ownerObject.Named()] = manifest.DiscoveryRecord{File: "owner.yaml"}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	owner := testutil.KustomizationID("apps", "owner")
	current := manifest.Production{Owner: owner, Authored: id, File: "current.yaml"}
	accepted := manifest.Production{Owner: owner, Authored: id, File: "accepted.yaml"}
	facts.index.Records[id] = manifest.DiscoveryRecord{Productions: []manifest.Production{current}}
	c := solverClosure(t, st, facts)
	c.proposal.Request.Changes = changes
	c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{
		owner: {id: {Productions: []manifest.Production{accepted}}},
	}
	c.replace = map[manifest.NamedResource]bool{owner: true}

	assert.Diff(t, c.productions(id), []manifest.Production{accepted})
	assert.NoError(t, c.start(t.Context(), func(materializerDemand, manifest.NamedResource) bool { return false }))
	assert.Equal(t, c.states[owner].generation > 1, true)
	assert.Equal(t, c.replace[owner], false)
	assert.Diff(t, c.productions(id), []manifest.Production{current, accepted})
}
