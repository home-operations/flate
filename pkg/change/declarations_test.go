package change

import (
	"maps"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestWithDeclarations_OneSidedInput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		side              int
		changed, produced bool
	}{
		{name: "added", side: 1, changed: true},
		{name: "removed", side: 0, changed: true},
		{name: "unchanged", side: 1},
		{name: "unaccepted", side: 1, changed: true, produced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stores := [2]*store.Store{store.New(), store.New()}
			input := &manifest.OCIRepository{Name: "chart", Namespace: "apps"}
			input.URL = "oci://fixture.invalid/chart"
			for side, st := range stores {
				files := fstest.MapFS{}
				if side == tc.side {
					files["input.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart, namespace: apps}\nspec: {url: oci://fixture.invalid/chart}\n")}
					production := manifest.Production{Authored: input.Named(), Source: manifest.BootstrapSourceID, File: "input.yaml"}
					if tc.produced {
						production.Owner = manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "emitter"}
					}
					st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{input.Named(): {Productions: []manifest.Production{production}}}})
					st.AddObject(input)
				}
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: files})
			}
			paths := []string{}
			if tc.changed {
				paths = append(paths, "input.yaml")
			}
			changes, err := Qualify(t.Context(), NewSet(paths), stores[0], stores[1])
			assert.NoError(t, err)
			frozen, err := changes.WithDeclarations(t.Context(), stores[0], stores[1], [2]map[manifest.NamedResource]store.Child{})
			assert.NoError(t, err)
			delta, _, _ := frozen.ForSource(manifest.BootstrapSourceID)
			got := maps.Collect(delta.Inputs())
			want := map[manifest.NamedResource]InputChange{}
			if tc.changed && !tc.produced {
				want[input.Named()] = InputChange{File: "input.yaml"}
			}
			assert.Diff(t, got, want)
			original, _, _ := changes.ForSource(manifest.BootstrapSourceID)
			assert.Equal(t, len(maps.Collect(original.Inputs())), 0)
		})
	}
}

func TestWithDeclarations_LocalApplicabilityAndSelectedKey(t *testing.T) {
	t.Parallel()
	stores := [2]*store.Store{store.New(), store.New()}
	owners := [2]*manifest.HelmRelease{}
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"}
	for side, st := range stores {
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
		owners[side] = &manifest.HelmRelease{Name: "app", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{{Kind: input.Kind, Name: input.Name, ValuesKey: "chosen"}}}
		st.AddObject(owners[side])
		st.AddObject(&manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace, Data: map[string]any{"chosen": "equal", "unrelated": []any{side}}})
	}
	changes, err := Qualify(t.Context(), NewSet([]string{"values.yaml"}), stores[0], stores[1])
	assert.NoError(t, err)
	frozen, err := changes.WithDeclarations(t.Context(), stores[0], stores[1], [2]map[manifest.NamedResource]store.Child{})
	assert.NoError(t, err)
	for side, owner := range owners {
		var ref manifest.Reference
		for candidate := range manifest.References(owner, manifest.ReferenceRelations) {
			if candidate.Use == "valuesFrom" {
				ref = candidate
			}
		}
		assert.Equal(t, frozen.OppositeDeclaration(manifest.TreeSide(side), manifest.BootstrapSourceID, owner), manifest.BaseManifest(owners[1-side]))
		foreign := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "foreign"}
		assert.Equal(t, frozen.OppositeDeclaration(manifest.TreeSide(side), foreign, owner), nil)
		assert.Equal(t, frozen.OppositeDeclaration(manifest.TreeSide(side), manifest.BootstrapSourceID, owner.Clone()), nil)
		changed, known := frozen.OutputChanged(manifest.TreeSide(side), manifest.BootstrapSourceID, ref, owner)
		assert.Equal(t, known, true)
		assert.Equal(t, changed, false)
		_, known = frozen.OutputChanged(manifest.TreeSide(side), manifest.BootstrapSourceID, ref, owner.Clone())
		assert.Equal(t, known, false)
		ref.Key = "unobserved"
		_, known = frozen.OutputChanged(manifest.TreeSide(side), manifest.BootstrapSourceID, ref, owner)
		assert.Equal(t, known, false)
		replacement := owner.Clone()
		replacement.ValuesFrom[0].Name = "replacement"
		for ref := range manifest.References(replacement, manifest.ReferenceRelations) {
			_, known = frozen.OutputChanged(manifest.TreeSide(side), manifest.BootstrapSourceID, ref, replacement)
			assert.Equal(t, known, false)
		}
	}
	unknown, err := changes.WithDeclarations(t.Context(), stores[0], nil, [2]map[manifest.NamedResource]store.Child{})
	assert.NoError(t, err)
	assert.Equal(t, unknown.OppositeDeclaration(manifest.TreeBase, manifest.BootstrapSourceID, owners[0]), nil)
	_, known := unknown.OutputChanged(manifest.TreeBase, manifest.BootstrapSourceID, manifest.Reference{NamedResource: owners[0].Named()}, owners[0])
	assert.Equal(t, known, false)
	assert.Equal(t, len(changes.declarations), 0)
}

func TestWithDeclarations_ProducerOwnedRawIsUnknown(t *testing.T) {
	t.Parallel()
	stores := [2]*store.Store{store.New(), store.New()}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	owner := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "parent"}
	for _, st := range stores {
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
		st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{child.Named(): {Productions: []manifest.Production{{Owner: owner, Source: manifest.BootstrapSourceID}}}}})
		st.AddObject(child)
	}
	changes, err := Qualify(t.Context(), NewSet(nil), stores[0], stores[1])
	assert.NoError(t, err)
	frozen, err := changes.WithDeclarations(t.Context(), stores[0], stores[1], [2]map[manifest.NamedResource]store.Child{})
	assert.NoError(t, err)
	assert.Equal(t, frozen.OppositeDeclaration(manifest.TreeBase, manifest.BootstrapSourceID, child), nil)
	_, known := frozen.OutputChanged(manifest.TreeBase, manifest.BootstrapSourceID, manifest.Reference{NamedResource: child.Named()}, child)
	assert.Equal(t, known, false)
}

func TestWithDeclarations_DoesNotRetainSourceCaptures(t *testing.T) {
	t.Parallel()
	stores := [2]*store.Store{store.New(), store.New()}
	inputs := [2]*store.SourceArtifact{}
	for side, st := range stores {
		reads := &store.ReadSet{}
		reads.Init(st, manifest.BootstrapSourceID, nil)
		t.Cleanup(reads.Close)
		inputs[side] = &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}, Reads: reads, Metadata: map[string]string{"key": "original"}, Warnings: []manifest.Warning{{Detail: []string{"original"}}}}
		st.SetArtifact(manifest.BootstrapSourceID, inputs[side])
	}
	qualified, err := Qualify(t.Context(), NewSet(nil), stores[0], stores[1])
	assert.NoError(t, err)
	for _, other := range []*store.Store{stores[1], nil} {
		frozen, err := qualified.WithDeclarations(t.Context(), stores[0], other, [2]map[manifest.NamedResource]store.Child{})
		assert.NoError(t, err)
		delta, descriptors, _ := frozen.ForSource(manifest.BootstrapSourceID)
		for side, snapshot := range descriptors {
			assert.Equal(t, snapshot, frozen.origins[side])
			if snapshot == nil || snapshot.Reads != nil || snapshot.ContentFingerprint != nil {
				t.Fatal("source capture retained in frozen comparison")
			}
			if snapshot == inputs[side] {
				t.Fatal("source descriptor was borrowed")
			}
			snapshot.Metadata["key"] = "private"
			snapshot.Warnings[0].Detail[0] = "private"
			assert.Equal(t, inputs[side].Metadata["key"], "original")
			assert.Equal(t, inputs[side].Warnings[0].Detail[0], "original")
		}
		_, snapshots, _ := delta.ForSource(manifest.BootstrapSourceID)
		for _, snapshot := range snapshots {
			if snapshot != nil && snapshot.Reads != nil {
				t.Fatal("source delta retained a capture")
			}
		}
	}
}

func TestWithDeclarations_PreparedProduction(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                     string
		output, foreign, missing bool
	}{{name: "same output"}, {name: "unknown spec", output: true}, {name: "foreign source", foreign: true}, {name: "missing opposite preparation", missing: true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stores := [2]*store.Store{store.New(), store.New()}
			var prepared [2]map[manifest.NamedResource]store.Child
			var children [2]*manifest.Kustomization
			source := manifest.BootstrapSourceID
			if tc.foreign {
				source = manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "foreign"}
			}
			owner := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "parent"}
			for side, st := range stores {
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
				children[side] = &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "./app"}
				id := children[side].Named()
				st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Productions: []manifest.Production{{Owner: owner, Source: source}}}}})
				st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "./raw"})
				value := 0
				if tc.output {
					value = side
				}
				doc := map[string]any{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": manifest.KindKustomization, "metadata": map[string]any{"name": id.Name, "namespace": id.Namespace}, "spec": map[string]any{"path": "./app", "unknown": value}}
				prepared[side] = map[manifest.NamedResource]store.Child{id: {Obj: children[side], Doc: doc}}
			}
			if tc.missing {
				prepared[1] = nil
			}
			changes, err := Qualify(t.Context(), NewSet(nil), stores[0], stores[1])
			assert.NoError(t, err)
			frozen, err := changes.WithDeclarations(t.Context(), stores[0], stores[1], prepared)
			assert.NoError(t, err)
			for side, child := range children {
				changed, known := frozen.OutputChanged(manifest.TreeSide(side), source, manifest.Reference{NamedResource: child.Named()}, child)
				var opposite manifest.BaseManifest
				if !tc.foreign && !tc.missing {
					opposite = children[1-side]
				}
				assert.Equal(t, frozen.OppositeDeclaration(manifest.TreeSide(side), source, child), opposite)
				assert.Equal(t, known, !tc.foreign && !tc.missing)
				assert.Equal(t, changed, tc.output)
			}
		})
	}
}
