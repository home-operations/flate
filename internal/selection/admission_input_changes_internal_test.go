package selection

import (
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdmission_InputChangesQualifiedOnce(t *testing.T) {
	t.Parallel()
	for _, changed := range []string{"selected.yaml", "unrelated.yaml"} {
		t.Run(changed, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			changes := change.NewSet([]string{changed})
			facts := &solverFacts{
				store:   st,
				changes: changes,
				index:   manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
			}
			facts.context.Root = root
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
			inputs := make([]string, 100)
			for i := range inputs {
				inputs[i] = "other-" + strings.Repeat("x", i) + ".yaml"
			}
			inputs[len(inputs)-1] = "selected.yaml"
			facts.index.Records[id] = manifest.DiscoveryRecord{File: "settings.yaml", Inputs: inputs}
			var captured store.ReadSet
			captured.Init(st, id, nil)
			t.Cleanup(captured.Close)
			var work manifest.WorkRecorder
			captured.SetWork(&work)
			reads := &admissionArtifactReads{admissionReader: &captured}
			facts.reader = reads
			c := solverClosure(t, st, facts)
			c.context = facts.context
			c.objects = reads
			c.proposal.reads = &captured
			c.proposal.Request.Changes = changes
			assert.Equal(t, c.affected(id, nil), changed == "selected.yaml")
			calls := sourceCaptureCount(&captured)
			assert.Equal(t, work.Scope().Admission().Counts.FileEntries, uint64(1))
			assert.Equal(t, c.affected(id, nil), changed == "selected.yaml")
			assert.Equal(t, sourceCaptureCount(&captured), calls)
			assert.Equal(t, work.Scope().Admission().Counts.FileEntries, uint64(1))
			if sourceCaptureCount(&captured) > 3 {
				t.Fatalf(
					"qualified one changed path %d times for %d recorded inputs; want at most 3 artifact reads",
					sourceCaptureCount(&captured),
					len(inputs),
				)
			}
		})
	}
}

func sourceCaptureCount(reads *store.ReadSet) int {
	count := 0
	for id := range reads.Inputs() {
		if id.KindInfo().SourceArtifact {
			count++
		}
	}
	return count
}

func TestAdmission_ContentPathsSettleBeforeReach(t *testing.T) {
	t.Parallel()
	for _, result := range []string{"unchanged", "changed", "unavailable"} {
		t.Run(result, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			changes := change.NewSet([]string{"changed/file.yaml"})
			facts := &solverFacts{store: st, changes: changes}
			facts.context.Root = root
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
			facts.index.Records = map[manifest.NamedResource]manifest.DiscoveryRecord{id: {
				ContentRoots: map[manifest.NamedResource][]string{
					manifest.BootstrapSourceID: {"one/../first", "two/../second"},
				},
			}}
			var captured store.ReadSet
			captured.Init(st, id, nil)
			defer captured.Retire()
			c := solverClosure(t, st, facts)
			c.context = facts.context
			c.objects = &captured
			c.proposal.reads = &captured
			c.proposal.Request.Changes = changes
			c.proposal.preparation = &Preparation{}
			assert.Equal(t, c.affected(id, nil), false)
			assert.Equal(t, c.states[id].affectedKnown, false)
			assert.Equal(t, len(c.proposal.demands), 2)
			acquired := make([]Acquisition, 0, len(c.proposal.demands))
			for i, demand := range c.proposal.demands {
				resolved := "unrelated"
				if i == 1 && result == "changed" {
					resolved = "changed"
				}
				acquired = append(
					acquired,
					Acquisition{Demand: demand, Root: resolved, Available: result != "unavailable", Reads: &captured},
				)
			}
			c.proposal.preparation.acceptAcquired(acquired)
			c.proposal.demands = nil
			assert.Equal(t, c.affected(id, nil), result != "unchanged")
			assert.Equal(t, c.states[id].affectedKnown, true)
			assert.Equal(t, len(c.proposal.demands), 0)
		})
	}
}

func TestAdmission_DirectoryReadersRespectContentRoots(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, file string
		changed    bool
	}{
		{name: "declared subtree", file: "apps/owner/new.yaml", changed: true},
		{name: "observed external input", file: "shared/input.yaml", changed: true},
		{name: "unobserved external descendant", file: "shared/nested/unrelated.yaml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./apps/owner"}
			st.AddObject(owner)
			reader := manifest.FileReader{Reader: owner.Named(), Source: manifest.BootstrapSourceID}
			facts := &solverFacts{
				store: st, changes: change.NewSet([]string{tc.file}),
				context: SourceContext{Root: root},
				index: manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{owner.Named(): {}},
					Readers: map[string][]manifest.FileReader{
						"apps/owner": {reader}, "shared": {reader}, "shared/input.yaml": {reader},
					},
				},
			}
			facts.occurrences = map[manifest.NamedResource]OccurrenceFacts{owner.Named(): {
				Identity: owner.Named(), Authored: owner.Named(), CatalogKnown: true,
				Source: manifest.BootstrapSourceID, Roots: []string{"apps/owner"},
			}}
			c := solverClosure(t, st, facts)
			counted := &countedAdmissionFacts{census: facts, rootPaths: map[manifest.NamedResource]int{}}
			c.facts = counted
			c.context = facts.context
			c.proposal.Request.Changes = facts.changes
			assert.Equal(t, c.changedClaim(&reader, tc.file), tc.changed)
			expectedRoots := 1
			if tc.file == "shared/input.yaml" {
				expectedRoots = 0
			}
			assert.Equal(t, counted.rootPaths[owner.Named()], expectedRoots)
		})
	}
}
