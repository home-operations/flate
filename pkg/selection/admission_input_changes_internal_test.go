package selection

import (
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
