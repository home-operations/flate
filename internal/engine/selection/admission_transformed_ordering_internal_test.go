package selection

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdmission_TransformedOrderingRemovalQualification(t *testing.T) {
	t.Parallel()
	source := manifest.BootstrapSourceID
	producer := testutil.KustomizationID("apps", "reader-producer")
	for _, tc := range []struct {
		name                                           string
		unpaired, unknown, unfrozen, present, excluded bool
		base, namespace, nonrender, multiple           bool
		want                                           int
	}{
		{name: "removed", want: 1},
		{name: "removed on base", base: true, want: 1},
		{name: "removed in another namespace", namespace: true, want: 1},
		{name: "multiple removals", multiple: true, want: 1},
		{name: "non-render kind", nonrender: true},
		{name: "present", present: true},
		{name: "excluded kind", excluded: true},
		{name: "unpaired source", unpaired: true},
		{name: "unknown side", unknown: true},
		{name: "unfrozen context", unfrozen: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var target manifest.BaseManifest = &manifest.Kustomization{Name: "target", Namespace: "apps"}
			if tc.namespace {
				target = &manifest.Kustomization{Name: "target", Namespace: "other"}
			}
			if tc.nonrender {
				target = &manifest.ConfigMap{Name: "target", Namespace: "apps"}
			}
			side := manifest.TreeHead
			objects := [2][]manifest.NamedResource{{target.Named()}, nil}
			if tc.base {
				side = manifest.TreeBase
				objects[manifest.TreeHead] = []manifest.NamedResource{testutil.KustomizationID("apps", "renamed")}
			}
			if tc.multiple {
				objects[manifest.TreeBase] = append(
					objects[manifest.TreeBase], testutil.KustomizationID("apps", "second"),
				)
			}
			changes := orderingRemovalChanges(t, objects)
			st := store.New()
			if tc.present {
				st.AddObject(target)
			}
			c := solverClosure(t, st, &solverFacts{
				store:   st,
				changes: changes,
				sources: map[manifest.NamedResource]SourceFacts{source: {Paired: !tc.unpaired}},
			})
			switch {
			case tc.excluded:
				c.session.request.Roots = []manifest.NamedResource{testutil.HelmReleaseID("", "")}
			case tc.namespace:
				c.session.request.Roots = []manifest.NamedResource{testutil.KustomizationID("apps", "")}
			}
			c.context = SourceContext{
				Frozen: !tc.unfrozen, SideKnown: !tc.unknown, Side: side,
				TransformingProducers: []manifest.NamedResource{producer},
			}
			c.proposal.Request.Changes = changes
			c.admitTransformedOrdering()
			assert.Equal(t, len(c.queue), tc.want)
		})
	}
}

func orderingRemovalChanges(t *testing.T, objects [2][]manifest.NamedResource) *change.Set {
	t.Helper()
	source := manifest.BootstrapSourceID
	var indexes [2]*manifest.DiscoveryIndex
	var inputs [2]*store.SourceArtifact
	for i, identities := range objects {
		files := fstest.MapFS{}
		index := &manifest.DiscoveryIndex{
			Records:         map[manifest.NamedResource]manifest.DiscoveryRecord{},
			CompleteSources: map[manifest.NamedResource]bool{source: true},
			Declarations:    map[manifest.NamedResource]map[string]manifest.FileDeclarations{source: {}},
		}
		for _, id := range identities {
			file := id.Name + ".yaml"
			files[file] = &fstest.MapFile{Data: []byte(id.Name)}
			index.Records[id] = manifest.DiscoveryRecord{File: file}
			index.Declarations[source][file] = manifest.FileDeclarations{
				Complete: true, Present: true, Objects: map[manifest.NamedResource]string{id: id.Name},
			}
		}
		inputs[i] = &store.SourceArtifact{FS: files}
		indexes[i] = index
	}
	changes, err := change.Detect(t.Context(), inputs[0], inputs[1], change.DetectConfig{})
	assert.NoError(t, err)
	changes, err = changes.WithReaders(
		t.Context(), indexes, [2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	return changes
}
