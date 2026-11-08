package change

import (
	"context"
	"encoding/json"
	"maps"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/pkg/manifest"
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
			testWithDeclarationsOneSidedInputCase(t, &tc)
		},
		)
	}
}

func TestInputs_QualifiedIdentity(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	unqualified, foreign, other := id, id, id
	unqualified.Namespace, foreign.Namespace, other.Name = "", "other", "unrelated"
	set := &Set{inputs: map[manifest.NamedResource]InputChange{
		id: {File: "apps.yaml"}, unqualified: {File: "implicit.yaml"},
		foreign: {File: "foreign.yaml"}, other: {File: "unrelated.yaml"},
	}}
	for _, tc := range []struct {
		name  string
		query manifest.NamedResource
		want  map[manifest.NamedResource]InputChange
	}{
		{
			name: "namespace", query: id,
			want: map[manifest.NamedResource]InputChange{id: set.inputs[id], unqualified: set.inputs[unqualified]},
		},
		{
			name: "unknown namespace", query: unqualified,
			want: map[manifest.NamedResource]InputChange{
				id: set.inputs[id], unqualified: set.inputs[unqualified], foreign: set.inputs[foreign],
			},
		},
		{name: "all", want: set.inputs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inputs := set.Inputs(tc.query)
			assert.Diff(t, maps.Collect(inputs), tc.want)
			assert.Diff(t, maps.Collect(inputs), tc.want)
			calls := 0
			inputs(func(manifest.NamedResource, InputChange) bool { calls++; return false })
			assert.Equal(t, calls, 1)
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
		owners[side] = &manifest.HelmRelease{
			Name: "app", Namespace: "apps",
			ValuesFrom: []manifest.ValuesReference{{Kind: input.Kind, Name: input.Name, ValuesKey: "chosen"}},
		}
		st.AddObject(owners[side])
		st.AddObject(&manifest.ConfigMap{
			Name: input.Name, Namespace: input.Namespace,
			Data: map[string]any{"chosen": "equal", "unrelated": []any{side}},
		})
	}
	changes, err := Qualify(
		t.Context(),
		NewSet([]string{"values.yaml"}),
		[2]*store.Store{stores[0], stores[1]},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	frozen, err := changes.WithDeclarations(
		t.Context(), DeclarationInputs{Stores: [2]*store.Store{
			stores[0],
			stores[1],
		}, Prepared: [2]map[manifest.NamedResource]store.Child{}},
	)
	assert.NoError(t, err)
	for side, owner := range owners {
		var ref manifest.Reference
		for candidate := range manifest.References(owner, manifest.ReferenceRelations) {
			if candidate.Use == "valuesFrom" {
				ref = candidate
			}
		}
		assert.Equal(t,
			frozen.OppositeDeclaration(manifest.TreeSide(side), manifest.BootstrapSourceID, owner.Named(), owner),
			manifest.BaseManifest(owners[1-side]),
		)
		foreign := manifest.NamedResource{
			Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "foreign",
		}
		assert.Equal(t, frozen.OppositeDeclaration(manifest.TreeSide(side), foreign, owner.Named(), owner), nil)
		assert.Equal(
			t,
			frozen.OppositeDeclaration(
				manifest.TreeSide(side),
				manifest.BootstrapSourceID,
				owner.Named(),
				owner.Clone(),
			),
			nil,
		)
		comparison := frozen.OutputChanged(manifest.TreeSide(side), manifest.BootstrapSourceID, ref, owner)
		changed, known := comparison.Changed, comparison.Known
		assert.Equal(t, known, true)
		assert.Equal(t, changed, false)
		known = frozen.OutputChanged(manifest.TreeSide(side), manifest.BootstrapSourceID, ref, owner.Clone()).Known
		assert.Equal(t, known, false)
		ref.Key = "unobserved"
		known = frozen.OutputChanged(manifest.TreeSide(side), manifest.BootstrapSourceID, ref, owner).Known
		assert.Equal(t, known, false)
		replacement := owner.Clone()
		replacement.ValuesFrom[0].Name = "replacement"
		for ref := range manifest.References(replacement, manifest.ReferenceRelations) {
			known = frozen.OutputChanged(manifest.TreeSide(side), manifest.BootstrapSourceID, ref, replacement).Known
			assert.Equal(t, known, false)
		}
	}
	unknown, err := changes.WithDeclarations(
		t.Context(),
		DeclarationInputs{
			Stores:   [2]*store.Store{stores[0], nil},
			Prepared: [2]map[manifest.NamedResource]store.Child{},
		},
	)
	assert.NoError(t, err)
	assert.Equal(
		t,
		unknown.OppositeDeclaration(manifest.TreeBase, manifest.BootstrapSourceID, owners[0].Named(), owners[0]),
		nil,
	)
	known := unknown.OutputChanged(
		manifest.TreeBase, manifest.BootstrapSourceID, manifest.Reference{NamedResource: owners[0].Named()}, owners[0],
	).Known
	assert.Equal(t, known, false)
	assert.Equal(t, len(changes.declarations), 0)
}

func TestWithDeclarations_ProducerOwnedRawIsUnknown(t *testing.T) {
	t.Parallel()
	stores := [2]*store.Store{store.New(), store.New()}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	owner := manifest.NamedResource{
		Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "parent",
	}
	for _, st := range stores {
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
		st.SetDiscoveryIndex(&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				child.Named(): {Productions: []manifest.Production{{Owner: owner, Source: manifest.BootstrapSourceID}}},
			},
		})
		st.AddObject(child)
	}
	changes, err := Qualify(t.Context(), NewSet(nil), [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	frozen, err := changes.WithDeclarations(
		t.Context(), DeclarationInputs{Stores: [2]*store.Store{
			stores[0],
			stores[1],
		}, Prepared: [2]map[manifest.NamedResource]store.Child{}},
	)
	assert.NoError(t, err)
	assert.Equal(
		t,
		frozen.OppositeDeclaration(manifest.TreeBase, manifest.BootstrapSourceID, child.Named(), child),
		nil,
	)
	known := frozen.OutputChanged(
		manifest.TreeBase,
		manifest.BootstrapSourceID,
		manifest.Reference{NamedResource: child.Named()},
		child,
	).Known
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
		inputs[side] = &store.SourceArtifact{
			LocalPath: t.TempDir(),
			FS:        fstest.MapFS{},
			Reads:     reads,
			Metadata: map[string]string{
				"key": "original",
			},
			Warnings: []manifest.Warning{{Detail: []string{"original"}}},
		}
		st.SetArtifact(manifest.BootstrapSourceID, inputs[side])
	}
	qualified, err := Qualify(t.Context(), NewSet(nil), [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	for _, other := range []*store.Store{stores[1], nil} {
		frozen, err := qualified.WithDeclarations(
			t.Context(), DeclarationInputs{Stores: [2]*store.Store{
				stores[0],
				other,
			}, Prepared: [2]map[manifest.NamedResource]store.Child{}},
		)
		assert.NoError(t, err)
		delta, descriptors, _ := frozen.ForSource(manifest.BootstrapSourceID)
		for side, snapshot := range descriptors {
			assert.Equal(t, snapshot, frozen.origins[side])
			assert.FatalIf(
				t,
				snapshot == nil || snapshot.Reads != nil || snapshot.ContentFingerprint != nil,
				"%v",
				"source capture retained in frozen comparison",
			)
			assert.FatalIf(t, snapshot == inputs[side], "%v", "source descriptor was borrowed")
			snapshot.Metadata["key"] = "private"
			snapshot.Warnings[0].Detail[0] = "private"
			assert.Equal(t, inputs[side].Metadata["key"], "original")
			assert.Equal(t, inputs[side].Warnings[0].Detail[0], "original")
		}
		_, snapshots, _ := delta.ForSource(manifest.BootstrapSourceID)
		for _, snapshot := range snapshots {
			assert.FatalIf(t, snapshot != nil && snapshot.Reads != nil, "%v", "source delta retained a capture")
		}
	}
}

func TestWithDeclarations_PreparedProduction(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                     string
		output, foreign, missing bool
	}{
		{name: "same output"},
		{name: "unknown spec", output: true},
		{name: "foreign source", foreign: true},
		{name: "missing opposite preparation", missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testWithDeclarationsPreparedProductionCase(t, &tc)
		},
		)
	}
}

func TestWithReaders_UnchangedAncestorOutput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		output, missing bool
	}{
		{name: "reconciliation field"},
		{name: "render field", output: true},
		{name: "unprepared ancestor", missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stores := [2]*store.Store{store.New(), store.New()}
			indexes := [2]*manifest.DiscoveryIndex{}
			parent := manifest.NamedResource{
				Group:     manifest.FluxKustomizeDomain,
				Kind:      manifest.KindKustomization,
				Namespace: "apps",
				Name:      "parent",
			}
			child := parent
			child.Name = "child"
			for side, st := range stores {
				input := fstest.MapFS{}
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: input})
				spec := map[string]any{"path": "./app", "timeout": []string{"1m", "2m"}[side]}
				if tc.output && side == 1 {
					spec["path"] = "./other"
				}
				doc := map[string]any{
					"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": parent.Kind,
					"metadata": map[string]any{"name": parent.Name, "namespace": parent.Namespace},
					"spec":     spec,
				}
				data, err := json.Marshal(doc)
				assert.NoError(t, err)
				input["parent.yaml"] = &fstest.MapFile{Data: data}
				childDoc, err := manifest.ObjectDocument(
					child,
					&manifest.Kustomization{Name: child.Name, Namespace: child.Namespace},
				)
				assert.NoError(t, err)
				data, err = json.Marshal(childDoc)
				assert.NoError(t, err)
				input["child.yaml"] = &fstest.MapFile{Data: data}
				indexes[side] = &manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						parent: {File: "parent.yaml", Authored: parent},
						child:  {File: "child.yaml", Authored: child, Parent: parent},
					},
					CompleteSources: map[manifest.NamedResource]bool{manifest.BootstrapSourceID: true},
				}
				st.SetDiscoveryIndex(indexes[side])
				if !tc.missing || side == 0 {
					st.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace}, doc)
				}
			}
			changes, err := Qualify(
				t.Context(),
				NewSet([]string{"parent.yaml"}),
				[2]*store.Store{stores[0], stores[1]},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			changes, err = changes.WithReaders(
				t.Context(),
				[2]*manifest.DiscoveryIndex{indexes[0], indexes[1]},
				[2]map[manifest.NamedResource]inputprojection.Projection{},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			changes, err = changes.WithDeclarations(
				t.Context(), DeclarationInputs{Stores: [2]*store.Store{
					stores[0],
					stores[1],
				}, Prepared: [2]map[manifest.NamedResource]store.Child{}},
			)
			assert.NoError(t, err)
			changes, err = changes.WithReaders(
				t.Context(),
				[2]*manifest.DiscoveryIndex{indexes[0], indexes[1]},
				[2]map[manifest.NamedResource]inputprojection.Projection{},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			delta, _, _ := changes.ForSource(manifest.BootstrapSourceID)
			inputs := maps.Collect(delta.Inputs(manifest.NamedResource{}))
			_, found := inputs[child]
			assert.Equal(t, found, tc.output || tc.missing)
			_, found = inputs[parent]
			assert.Equal(t, found, tc.output || tc.missing)
		})
	}
}

func TestWithDeclarations_EffectiveInputApplicability(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"unused", "used", "missing projection", "invalidated"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testWithDeclarationsEffectiveInputApplicabilityCase(
				t,
				&scenario,
			)
		},
		)
	}
}

type declarationInputFixture struct {
	stores      [2]*store.Store
	indexes     [2]*manifest.DiscoveryIndex
	projections [2]map[manifest.NamedResource]inputprojection.Projection
	owners      [2]*manifest.Kustomization
	inputs      [2]*manifest.ConfigMap
}

func newDeclarationInputFixture(t *testing.T, scenario string) declarationInputFixture {
	t.Helper()
	f := declarationInputFixture{stores: [2]*store.Store{store.New(), store.New()}}
	inputID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	for side, st := range f.stores {
		input := fstest.MapFS{}
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: input})
		f.owners[side] = &manifest.Kustomization{Name: "reader", Namespace: "apps", Path: "./app"}
		f.owners[side].PostBuild = &kustomizev1.PostBuild{
			SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: inputID.Kind, Name: inputID.Name}},
		}
		f.inputs[side] = &manifest.ConfigMap{
			Name: inputID.Name, Namespace: inputID.Namespace,
			Data: map[string]any{"OTHER": "same", "CHANGED": []string{"old", "new"}[side]},
		}
		st.AddObject(f.inputs[side])
		st.AddObject(f.owners[side])
		prepared := f.owners[side].Clone()
		prepared.PostBuild.Substitute = map[string]string{"OTHER": "same", "CHANGED": []string{"old", "new"}[side]}
		name := "OTHER"
		if scenario == "used" {
			name = "CHANGED"
		}
		projection := inputprojection.Projection{
			Content: prepared.Content(nil, map[string]struct{}{name: {}}, true),
			Inputs:  map[manifest.NamedResource]manifest.Data{inputID: f.inputs[side]},
		}
		if scenario != "missing projection" || side != 1 {
			f.projections[side] = map[manifest.NamedResource]inputprojection.Projection{
				f.owners[side].Named(): projection,
			}
		}
		doc, err := manifest.ObjectDocument(f.owners[side].Named(), f.owners[side])
		assert.NoError(t, err)
		data, err := json.Marshal(doc)
		assert.NoError(t, err)
		input["reader.yaml"] = &fstest.MapFile{Data: data}
		f.indexes[side] = &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				f.owners[side].Named(): {File: "reader.yaml", Authored: f.owners[side].Named()},
			},
			CompleteSources: map[manifest.NamedResource]bool{manifest.BootstrapSourceID: true},
		}
		st.SetDiscoveryIndex(f.indexes[side])
	}
	return f
}

func testWithDeclarationsOneSidedInputCase(t *testing.T, testCaseInput *struct {
	name string

	side     int
	changed  bool
	produced bool
},
) {
	t.Helper()

	stores := [2]*store.Store{store.New(), store.New()}
	input := &manifest.OCIRepository{Name: "chart", Namespace: "apps"}
	input.URL = "oci://fixture.invalid/chart"
	for side, st := range stores {
		files := fstest.MapFS{}
		if side == testCaseInput.side {
			files["input.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\n" +
				"kind: OCIRepository\nmetadata: {name: chart, namespace: apps}\n" +
				"spec: {url: oci://fixture.invalid/chart}\n")}
			production := manifest.Production{
				Authored: input.Named(),
				Source:   manifest.BootstrapSourceID,
				File:     "input.yaml",
			}
			if testCaseInput.produced {
				production.Owner = manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "emitter"}
			}
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					input.Named(): {Productions: []manifest.Production{production}},
				},
			})
			st.AddObject(input)
		}
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: files})
	}
	paths := []string{}
	if testCaseInput.changed {
		paths = append(paths, "input.yaml")
	}
	changes, err := Qualify(t.Context(), NewSet(paths), [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	frozen, err := changes.WithDeclarations(
		t.Context(), DeclarationInputs{Stores: [2]*store.Store{
			stores[0],
			stores[1],
		}, Prepared: [2]map[manifest.NamedResource]store.Child{}},
	)
	assert.NoError(t, err)
	delta, _, _ := frozen.ForSource(manifest.BootstrapSourceID)
	got := maps.Collect(delta.Inputs(manifest.NamedResource{}))
	want := map[manifest.NamedResource]InputChange{}
	if testCaseInput.changed && !testCaseInput.produced {
		want[input.Named()] = InputChange{File: "input.yaml"}
	}
	assert.Diff(t, got, want)
	original, _, _ := changes.ForSource(manifest.BootstrapSourceID)
	assert.Equal(t, len(maps.Collect(original.Inputs(manifest.NamedResource{}))), 0)
}

func testWithDeclarationsPreparedProductionCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		output  bool
		foreign bool
		missing bool
	},
) {
	t.Helper()

	stores := [2]*store.Store{store.New(), store.New()}
	var prepared [2]map[manifest.NamedResource]store.Child
	var children [2]*manifest.Kustomization
	source := manifest.BootstrapSourceID
	if testCaseInput.foreign {
		source = manifest.NamedResource{
			Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "foreign",
		}
	}
	owner := manifest.NamedResource{
		Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "parent",
	}
	for side, st := range stores {
		st.SetArtifact(
			manifest.BootstrapSourceID,
			&store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}},
		)
		children[side] = &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "./app"}
		id := children[side].Named()
		st.SetDiscoveryIndex(&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Productions: []manifest.Production{{Owner: owner, Source: source}}},
			},
		})
		st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "./raw"})
		value := 0
		if testCaseInput.output {
			value = side
		}
		doc := map[string]any{
			"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": manifest.KindKustomization,
			"metadata": map[string]any{"name": id.Name, "namespace": id.Namespace},
			"spec":     map[string]any{"path": "./app", "unknown": value},
		}
		prepared[side] = map[manifest.NamedResource]store.Child{id: {Obj: children[side], Doc: doc}}
	}
	if testCaseInput.missing {
		prepared[1] = nil
	}
	changes, err := Qualify(t.Context(), NewSet(nil), [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	frozen, err := changes.WithDeclarations(
		t.Context(),
		DeclarationInputs{Stores: [2]*store.Store{stores[0], stores[1]}, Prepared: prepared},
	)
	assert.NoError(t, err)
	for side, child := range children {
		comparison7 := frozen.OutputChanged(
			manifest.TreeSide(side), source, manifest.Reference{NamedResource: child.Named()}, child,
		)
		changed, known := comparison7.Changed, comparison7.Known
		var opposite manifest.BaseManifest
		if !testCaseInput.foreign && !testCaseInput.missing {
			opposite = children[1-side]
		}
		assert.Equal(
			t,
			frozen.OppositeDeclaration(manifest.TreeSide(side), source, child.Named(), child),
			opposite,
		)
		assert.Equal(t, known, !testCaseInput.foreign && !testCaseInput.missing)
		assert.Equal(t, changed, testCaseInput.output)
	}
}

func testWithDeclarationsEffectiveInputApplicabilityCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	f := newDeclarationInputFixture(t, (*testCaseInput))
	inputID := f.inputs[0].Named()
	changes, err := Qualify(
		t.Context(),
		NewSet([]string{"settings.yaml"}),
		[2]*store.Store{f.stores[0], f.stores[1]},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	changes, err = changes.WithReaders(
		t.Context(),
		[2]*manifest.DiscoveryIndex{f.indexes[0], f.indexes[1]},
		f.projections,
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	var frozen *Set
	freeze := func(ctx context.Context) {
		var err error
		frozen, err = changes.WithDeclarations(
			ctx, DeclarationInputs{Stores: [2]*store.Store{
				f.stores[0],
				f.stores[1],
			}, Prepared: [2]map[manifest.NamedResource]store.Child{}},
		)
		assert.NoError(t, err)
	}
	if (*testCaseInput) == "invalidated" {
		captured, invalidated := make(chan struct{}), make(chan struct{})
		tasks := task.NewBounded(2)
		tasks.Go(t.Context(), "freeze", func(ctx context.Context) {
			close(captured)
			<-invalidated
			freeze(ctx)
		})
		tasks.Go(t.Context(), "invalidate", func(context.Context) {
			<-captured
			replacement := *f.inputs[1]
			replacement.Data = maps.Clone(replacement.Data)
			replacement.Data["OTHER"] = "changed after projection"
			f.stores[1].AddObject(&replacement)
			close(invalidated)
		})
		tasks.BlockTillDone()
		assert.Equal(t, tasks.Failures(), int64(0))
	} else {
		freeze(t.Context())
	}
	for side, owner := range f.owners {
		for ref := range manifest.References(owner, manifest.ReferenceRelations) {
			if ref.NamedResource != inputID {
				continue
			}
			comparison8 := frozen.OutputChanged(
				manifest.TreeSide(side),
				manifest.BootstrapSourceID,
				ref,
				owner,
			)
			changed, known := comparison8.Changed, comparison8.Known
			assert.Equal(t, known, true)
			assert.Equal(t, changed, (*testCaseInput) != "unused")
		}
	}
}
