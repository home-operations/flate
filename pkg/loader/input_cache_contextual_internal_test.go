package loader

import (
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:gocognit // Accepted replay MUST retain authored facts and match fresh selection in both diff directions.
func TestInputCache_ContextualSelectorReplay(t *testing.T) {
	t.Parallel()
	for _, direction := range []string{"forward", "reverse"} {
		t.Run(direction, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			owner, err := cache.New(t.Context(), t.TempDir())
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			if !owner.CanPersistRenders() {
				t.Fatal("input cache witness requires a clean stamped test binary")
			}
			before, after := contextualSelectorFiles(), contextualSelectorFiles()
			before["providers/provider.yaml"] = &fstest.MapFile{Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: provider, namespace: apps, labels: {role: y}, annotations: {revision: before}}
spec: {type: Static}
`)}
			after["providers/provider.yaml"] = &fstest.MapFile{Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: provider, namespace: apps, labels: {role: y}, annotations: {revision: after}}
spec: {type: Static}
`)}
			if direction == "reverse" {
				before, after = after, before
			}
			first := cachedFixtureLoader(t, root, before, owner)
			assert.NoError(t, first.PersistInputs(t.Context()))
			first.Close()
			reader := manifest.NamedResource{
				Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "reader",
			}
			load := func(cached bool) *Loader {
				view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash,
					gittree.ViewOptions{Mode: gittree.Direct, Current: after})
				assert.NoError(t, err)
				st := store.New()
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: view})
				l := New(st)
				t.Cleanup(l.Close)
				l.SourceRoot = root
				if cached {
					l.Options.Cache = owner
				}
				l.Options.Changes = change.NewSet([]string{"providers/provider.yaml"})
				configureSelection(t, l, selection.Request{Changed: true})
				l.documents.compact = true
				l.mountSources()
				assert.NoError(t, l.restoreCachedInputs(t.Context()))
				file := filepath.Join(root, "readers", "set.yaml")
				if cached {
					doc, known := l.documents.cache[file]
					assert.FatalIf(t, !known || doc.deferred == nil, "reader cache frame was not accepted")
					assert.Equal(t, len(doc.objects), 0)
					objects, censusErr := l.documents.censusFile(t.Context(), file, manifest.ParseDocOptions{})
					assert.NoError(t, censusErr)
					assert.Equal(t, len(objects), 1)
					assert.Equal(t, objects[0].fact.Authored, reader)
					assert.Equal(t, objects[0].fact.facts.structural == nil, true)
					assert.Equal(t, objects[0].fact.facts.refs[0].Selector.MatchExpressions[0].Key, "${KEY}")
					t.Log("accepted reader frame restored authored selector facts before resolution")
				}
				_, err = l.Load(t.Context(), filepath.Join(root, "root"), manifest.NamedResource{}, nil)
				assert.NoError(t, err)
				objects, err := l.documents.censusFile(t.Context(), file, manifest.ParseDocOptions{})
				assert.NoError(t, err)
				authored := objects[0].fact.facts.refs[0].Selector.DeepCopy()
				assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
				assert.Diff(t, objects[0].fact.facts.refs[0].Selector, authored)
				assert.Equal(t, authored.MatchExpressions[0].Key, "${KEY}")
				return l
			}
			fresh, restored := load(false), load(true)
			want, known := fresh.Occurrence(reader)
			assert.Equal(t, known, true)
			got, known := restored.Occurrence(reader)
			assert.Equal(t, known, true)
			assert.Equal(t, want.Refs[0].Selector.MatchExpressions[0].Key, "role")
			assert.Equal(t, string(want.Refs[0].Selector.MatchExpressions[0].Operator), "NotIn")
			assert.Diff(t, want.Refs[0].Selector.MatchExpressions[0].Values, []string{"x"})
			if diff := cmp.Diff(want.Refs, got.Refs, cmp.AllowUnexported(manifest.Reference{})); diff != "" {
				t.Error(diff)
			}
			var selected [2][]manifest.NamedResource
			for i, l := range []*Loader{fresh, restored} {
				proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
				assert.NoError(t, err)
				selected[i] = slices.Clone(proposal.Selected)
				assert.Equal(t, slices.Contains(proposal.Selected, reader), true)
				proposal.Retire()
			}
			assert.Diff(t, selected[1], selected[0])
		})
	}
}
