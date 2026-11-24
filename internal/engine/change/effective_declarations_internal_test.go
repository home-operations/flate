package change

import (
	"cmp"
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSet_WithEffectiveRemovals(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, headSource                                string
		moved, indexed, partial, stale, unchanged, want bool
		targets                                         [2]manifest.BaseManifest
	}{
		{name: "renamed producers", want: true},
		{
			name: "non-render children",
			targets: [2]manifest.BaseManifest{
				&manifest.ConfigMap{Namespace: "apps", Name: "db"},
				&manifest.ConfigMap{Namespace: "apps", Name: "database"},
			},
			want: true,
		},
		{name: "different sources", headSource: "other", unchanged: true},
		{name: "target moved to another producer", moved: true},
		{name: "target survives only in the index", indexed: true},
		{name: "partial inventory", partial: true, unchanged: true},
		{name: "stale output reads", stale: true, unchanged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stores := [2]*store.Store{store.New(), store.New()}
			var owners [2]manifest.NamedResource
			targets := cmp.Or(tc.targets, [2]manifest.BaseManifest{
				&manifest.HelmRelease{Namespace: "apps", Name: "db"},
				&manifest.HelmRelease{Namespace: "apps", Name: "database"},
			})
			old := targets[0]
			set := &Set{sources: make(map[manifest.NamedResource]sourceDelta)}
			source := manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "shared",
			}
			otherSource := source
			otherSource.Name = "other"
			var outputReads store.ReadSet
			values := &manifest.ConfigMap{Namespace: "apps", Name: "values", Data: map[string]any{"key": "before"}}
			for side, st := range stores {
				for _, id := range []manifest.NamedResource{source, otherSource} {
					input := &store.SourceArtifact{LocalPath: t.TempDir()}
					st.SetArtifact(id, input)
					evidence := set.sources[id]
					evidence.artifacts[side] = input
					set.sources[id] = evidence
				}
				owner := &manifest.Kustomization{
					Namespace: "apps", Name: [2]string{"before", "after"}[side], Path: "./db",
				}
				owner.SourceRef = kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindGitRepository, Name: [2]string{"shared", cmp.Or(tc.headSource, "shared")}[side],
				}
				owners[side] = owner.Named()
				st.AddObject(owner)
				target := targets[side]
				emission := &store.Emission{
					Complete: !tc.partial || side == 0, Children: []store.Child{{Obj: target}},
				}
				if tc.stale && side == 1 {
					st.AddObject(values)
					outputReads.Init(st, owner.Named(), nil)
					t.Cleanup(outputReads.Retire)
					outputReads.GetObject(values.Named())
					outputReads.Bind(emission)
				}
				st.RecordEmitted(owner.Named(), emission, nil)
			}
			if tc.stale {
				head := stores[1]
				assert.Equal(t, outputReads.Valid(), true)
				replacement := *values
				replacement.Data = map[string]any{"key": "after"}
				head.AddObject(&replacement)
				accepted, _ := head.GetArtifact(owners[1], store.ArtifactContent).(*store.Emission)
				assert.FatalIf(t, accepted == nil, "%s", "fixture must retain the accepted inventory")
				assert.Equal(t, accepted.Complete, true)
				assert.Equal(t, accepted.OutputReads, &outputReads)
				assert.Equal(t, outputReads.Valid(), false)
			}
			if tc.moved {
				head := stores[1]
				other := &manifest.Kustomization{Namespace: "apps", Name: "other", Path: "./other"}
				head.AddObject(other)
				head.RecordEmitted(other.Named(), &store.Emission{
					Complete: true, Children: []store.Child{{Obj: old}},
				}, nil)
				assert.Equal(t, head.GetDeclaration(old.Named()) != nil, true)
			}
			indexedFile := effectiveRemovalIndex(t, stores[manifest.TreeHead], &manifest.Production{
				Source: source, Authored: old.Named(), File: "other/db.yaml",
			}, tc.indexed)
			result := set.WithEffectiveRemovals(stores, [][2]manifest.NamedResource{owners}, indexedFile)
			input := set.sources[source].artifacts[1]
			removed := slices.Collect(result.RemovedDeclarations(manifest.TreeHead, source, input, true))
			assert.Equal(t, slices.Contains(removed, old.Named()), tc.want)
			assert.Equal(t, result == set, tc.unchanged)
		})
	}
}

func effectiveRemovalIndex(
	t *testing.T, head *store.Store, production *manifest.Production, indexed bool,
) func(int, manifest.NamedResource) string {
	t.Helper()
	id := production.Authored
	if indexed {
		head.SetDiscoveryIndex(&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Productions: []manifest.Production{*production}},
			},
		})
		assert.Equal(t, head.GetDeclaration(id), nil)
	}
	return func(side int, target manifest.NamedResource) string {
		if indexed && side == int(manifest.TreeHead) && target == id {
			return production.File
		}
		return ""
	}
}
