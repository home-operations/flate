package loader

import (
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSourceFacts_FrozenComparisonSide(t *testing.T) {
	t.Parallel()
	for side := range 2 {
		t.Run(strconv.Itoa(side), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st, peer := store.New(), store.New()
			input := &store.SourceArtifact{LocalPath: root, Content: "initial", FS: fstest.MapFS{}}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			peer.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: root, Content: "peer", FS: fstest.MapFS{}},
			)
			l := New(st)
			l.SourceRoot = root
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			assert.Equal(t, l.SourceFacts(manifest.BootstrapSourceID, nil).Context.SideKnown, false)
			stores := [2]*store.Store{st, peer}
			if side == 1 {
				stores[0], stores[1] = stores[1], stores[0]
			}
			l.Options.Changes, err = change.Qualify(
				t.Context(),
				change.NewSet([]string{"changed.yaml"}),
				stores,
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{}, nil)
			defer reads.Retire()
			facts := l.SourceFacts(manifest.BootstrapSourceID, &reads)
			assert.Equal(t, facts.Context.SideKnown, true)
			assert.Equal(t, facts.Context.Side, manifest.TreeSide(side))
			assert.Equal(t, facts.Paired, true)
			assert.Equal(t, facts.InitialKnown, true)
			assert.Equal(t, facts.Initial, input)
			assert.Equal(t, facts.ConfiguredInput, (*store.SourceArtifact)(nil))
			l.Options.ConfiguredInput = input
			configured := l.SourceFacts(manifest.BootstrapSourceID, nil)
			assert.Equal(t, configured.ConfiguredInput, input)
			assert.Equal(t, configured.Initial, input)
			assert.Equal(t, configured.Derived, false)
			l.Options.ConfiguredInput = nil
			unconfigured := l.SourceFacts(manifest.BootstrapSourceID, nil)
			assert.Equal(t, unconfigured.ConfiguredInput, (*store.SourceArtifact)(nil))
			assert.Equal(t, unconfigured.Initial, input)
			assert.Equal(t, unconfigured.Derived, false)
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: root, Content: "replacement", FS: fstest.MapFS{}},
			)
			assert.Equal(t, reads.Valid(), false)
			replacement := l.SourceFacts(manifest.BootstrapSourceID, nil)
			assert.Equal(t, replacement.Paired, false)
			assert.Equal(t, replacement.Initial, input)
		})
	}
}
