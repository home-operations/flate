package loader

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_SelectorInputs(t *testing.T) {
	t.Parallel()
	t.Run("visibility", func(t *testing.T) {
		t.Parallel()
		testSelectorVisibility(t, false)
	})
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"set.yaml": {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\n" + "kind: ResourceSet\n" +
			"metadata: {name: app, namespace: apps}\n" +
			"spec:\n" +
			"  inputsFrom: [{kind: ResourceSetInputProvider, selector: {matchLabels: {app: selected}}}]\n" +
			"  resourcesTemplate: 'apiVersion: v1'\n")},
		"chosen.yaml": {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\n" + "kind: ResourceSetInputProvider\n" +
			"metadata: {name: chosen, namespace: apps, labels: {app: selected}}\n" +
			"spec: {type: Static}\n")},
		"other.yaml": {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\n" + "kind: ResourceSetInputProvider\n" +
			"metadata: {name: other, namespace: apps, labels: {app: other}}\n" +
			"spec: {type: Static}\n")},
		"foreign.yaml": {Data: []byte("apiVersion: foreign.example/v1\n" + "kind: ResourceSetInputProvider\n" +
			"metadata: {name: foreign, namespace: apps, labels: {app: selected}}\n" +
			"spec: {type: Static}\n")},
	}})
	l := New(st)
	l.SourceRoot = root

	app := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "app",
	}
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{app}})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	names := make([]string, 0, len(proposal.Objects))
	for _, child := range proposal.Objects {
		names = append(names, child.Obj.Named().Name)
	}
	slices.Sort(names)
	assert.Diff(t, names, []string{"app", "chosen"})
}

func TestAdmission_LateSelectorInput(t *testing.T) {
	t.Parallel()
	t.Run("visibility", func(t *testing.T) {
		t.Parallel()
		testSelectorVisibility(t, true)
	})
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"parent.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: parent, namespace: apps}\nspec: {path: ./workload}\n",
			),
		},
		"workload/kustomization.yaml": {Data: []byte("resources: []\n")},
		"set.yaml": {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\n" + "kind: ResourceSet\n" +
			"metadata: {name: app, namespace: apps}\n" +
			"spec:\n" +
			"  inputsFrom: [{kind: ResourceSetInputProvider, selector: {matchLabels: {app: selected}}}]\n" +
			"  resourcesTemplate: 'apiVersion: v1'\n")},
	}})
	l := New(st)
	l.SourceRoot = root

	l.Options.Changes = change.NewSet([]string{"parent.yaml"})
	configureSelection(t, l, selection.Request{Changed: true})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	parent := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "parent",
	}
	selectionProposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	_, committed, err := l.commitSelection(t.Context(), t, selectionProposal, selection.Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	reads := &store.ReadSet{}
	reads.Init(st, parent, nil)
	t.Cleanup(reads.Retire)
	provider := &manifest.ResourceSetInputProvider{
		Name:      "late",
		Namespace: "apps",
		Labels:    map[string]string{"app": "selected"},
	}
	proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{
		Changes:    l.Options.Changes,
		Generation: []store.Child{{Obj: provider, Productions: []manifest.Production{{Owner: parent}}}},
		Reads:      reads,
	})
	assert.NoError(t, err)
	app := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "app",
	}
	assert.Equal(t, slices.Contains(proposal.Needed, app), true)
	assert.Equal(t, slices.Contains(proposal.Selected, app), true)
	if len(proposal.Objects) != 1 {
		t.Fatalf("objects=%+v needed=%+v selected=%+v", proposal.Objects, proposal.Needed, proposal.Selected)
	}
	assert.Equal(t, proposal.Objects[0].Obj.Named(), app)
}

func TestAdmission_ExcludedSelectorKeepsQueryObservation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
	l := New(st)
	l.SourceRoot = root

	l.Options.Changes = change.NewSet(nil)
	configureSelection(t, l, selection.Request{Changed: true})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	parent := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "support",
	}
	_, committed, err := requireSelection(t, l, []manifest.NamedResource{parent}, false)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	reads := &store.ReadSet{}
	reads.Init(st, parent, nil)
	defer reads.Retire()
	consumer := &manifest.ResourceSet{
		Name:      "late",
		Namespace: "apps",
		InputsFrom: []fluxopv1.InputProviderReference{
			{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "selected"}}},
		},
	}
	proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{
		Changes:    l.Options.Changes,
		Generation: []store.Child{{Obj: consumer, Productions: []manifest.Production{{Owner: parent}}}},
		Reads:      reads,
	})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, consumer.Named()), false)
	assert.Equal(t, reads.Valid(), true)
	st.AddObject(
		&manifest.ResourceSetInputProvider{
			Name:      "provider",
			Namespace: "apps",
			Labels:    map[string]string{"app": "selected"},
		},
	)
	assert.Equal(t, reads.Valid(), false)
}

func testSelectorVisibility(t *testing.T, incoming bool) {
	t.Helper()
	for _, visibleSide := range []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead} {
		for _, representation := range []string{"store", "compact", "candidate"} {
			t.Run(

				fmt.Sprintf("visible=%d/%s", visibleSide, representation),

				func(t *testing.T) {
					t.Parallel()
					testSelectorVisibilityCase(t, incoming, visibleSide, representation)
				},
			)
		}
	}
}

func selectorAdmissionPair(t *testing.T, selectedSide manifest.TreeSide) (
	[2]*Loader, [2]*manifest.ResourceSetInputProvider, [2]*manifest.ResourceSet, *change.Set,
) {
	t.Helper()
	stores := [2]*store.Store{store.New(), store.New()}
	loaders := [2]*Loader{}
	var prepared [2]map[manifest.NamedResource]store.Child
	providers := [2]*manifest.ResourceSetInputProvider{}
	consumers := [2]*manifest.ResourceSet{}
	for side, st := range stores {
		root := t.TempDir()
		label := "other"
		if manifest.TreeSide(side) == selectedSide {
			label = "selected"
		}
		artifact := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
			"chosen.yaml": {
				Data: []byte(
					fmt.Sprintf(
						"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSetInputProvider\nmetadata: {name: chosen, namespace: apps, labels: {app: %s}}\nspec: {}\n",
						label,
					),
				),
			},
			"set.yaml": {
				Data: []byte(
					"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: app, namespace: apps}\nspec:\n  inputsFrom: [{selector: {matchLabels: {app: selected}}}]\n",
				),
			},
		}}
		st.SetArtifact(manifest.BootstrapSourceID, artifact)
		provider := &manifest.ResourceSetInputProvider{
			Name:      "chosen",
			Namespace: "apps",
			Labels:    map[string]string{"app": label},
		}
		consumer := &manifest.ResourceSet{
			Name:      "app",
			Namespace: "apps",
			InputsFrom: []fluxopv1.InputProviderReference{
				{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "selected"}}},
			},
		}
		providers[side], consumers[side] = provider, consumer
		prepared[side] = map[manifest.NamedResource]store.Child{
			provider.Named(): {Obj: provider},
			consumer.Named(): {Obj: consumer},
		}
		l := New(st)
		l.SourceRoot = root
		configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{consumer.Named()}, Changed: true})

		l.candidates = prepared[side]
		_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		l.sideKnown, l.side = true, manifest.TreeSide(side)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
		loaders[side] = l
	}
	changes, err := change.Qualify(

		t.Context(),

		change.NewSet([]string{"chosen.yaml"}), [2]*store.Store{stores[0], stores[1]},

		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	changes, err = changes.WithDeclarations(
		t.Context(),
		change.DeclarationInputs{Stores: [2]*store.Store{stores[0], stores[1]}, Prepared: prepared},
	)
	assert.NoError(t, err)
	return loaders, providers, consumers, changes
}

func TestAdmission_PairedSelectorInputs(t *testing.T) {
	t.Parallel()
	app := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "app",
	}
	for _, tc := range []struct {
		name string
		side manifest.TreeSide
	}{{"base-only match", manifest.TreeBase}, {"head-only match", manifest.TreeHead}} {
		t.Run(

			tc.name,

			func(t *testing.T) {
				t.Parallel()
				loaders, providers, consumers, changes := selectorAdmissionPair(t, tc.side)
				stores := [2]*store.Store{loaders[0].Store, loaders[1].Store}
				for side, l := range loaders {
					l.Options.Changes = changes
					binding := l.sourceBucket(manifest.BootstrapSourceID).Load()
					for binding.source != manifest.BootstrapSourceID {
						binding = binding.next
					}
					binding.current.Store(&admissionSource{
						initial: stores[side].GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact),
					})
					assert.Equal(t, len(stores[side].Objects()), 0)
					proposal, err := prepareSelection(
						t.Context(),
						t,
						l,
						selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
					)
					assert.NoError(t, err)
					assert.Equal(t, slices.Contains(proposal.Selected, app), true)
					assert.Equal(t, slices.Contains(proposal.Dirty, app), true)
					assert.Equal(
						t,
						slices.Contains(proposal.Needed, providers[side].Named()),
						manifest.TreeSide(side) == tc.side,
					)

					proposal, err = prepareSelection(t.Context(), t, l, selection.PrepareRequest{
						Changes:    l.Options.Changes,
						Generation: []store.Child{{Obj: consumers[side]}},
					})
					assert.NoError(t, err)
					assert.Equal(t, slices.Contains(proposal.Selected, app), true)
					assert.Equal(t, slices.Contains(proposal.Dirty, app), true)

					_, committed, err := requireSelection(
						t,
						l,
						[]manifest.NamedResource{providers[side].Named()},
						false,
					)
					assert.NoError(t, err)
					assert.Equal(t, committed, true)
					proposal, err = prepareSelection(t.Context(), t, l, selection.PrepareRequest{
						Changes:    l.Options.Changes,
						Generation: []store.Child{{Obj: providers[side]}},
					})
					assert.NoError(t, err)
					assert.Equal(t, slices.Contains(proposal.Selected, app), true)
					assert.Equal(t, slices.Contains(proposal.Dirty, app), true)

					producer := &manifest.HelmRelease{Name: "producer", Namespace: "apps"}
					stores[side].AddObject(producer)
					stores[side].RecordEmitted(
						producer.Named(),
						&store.Emission{Complete: true, Children: []store.Child{{Obj: consumers[side]}}},
						nil,
					)
					stores[side].UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
					historyOwners := map[manifest.NamedResource][]manifest.SourceEvidence{
						producer.Named(): {
							{
								Source: stores[side].GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact).HistoryIdentity(
									manifest.BootstrapSourceID,
								),
								Path: "input.yaml",
							},
						},
					}
					proposal, err = prepareSelection(t.Context(), t, l, selection.PrepareRequest{
						HistoryOwners: historyOwners,
						Changes:       l.Options.Changes,
						Invocation:    true,
					})
					assert.NoError(t, err)
					assert.Equal(t, slices.Contains(proposal.Needed, producer.Named()), true)

					if manifest.TreeSide(side) != tc.side {
						stores[side].SetArtifact(
							manifest.BootstrapSourceID,
							&store.SourceArtifact{LocalPath: l.SourceRoot, FS: fstest.MapFS{}},
						)
						proposal, err = prepareSelection(t.Context(), t, l, selection.PrepareRequest{
							Changes:    l.Options.Changes,
							Generation: []store.Child{{Obj: consumers[side]}},
						})
						assert.NoError(t, err)
						assert.Equal(t, slices.Contains(proposal.Selected, app), false)
					}
				}
			},
		)
	}
}

func selectorVisibilityPair(t *testing.T, incoming bool, visibleSide manifest.TreeSide, representation string) (
	[2]*Loader, [2]map[manifest.NamedResource]store.Child, []*manifest.ResourceSet, []manifest.BaseManifest,
) {
	t.Helper()
	stores := [2]*store.Store{store.New(), store.New()}
	var loaders [2]*Loader
	var prepared [2]map[manifest.NamedResource]store.Child
	app := &manifest.ResourceSet{
		Name:      "consumer",
		Namespace: "apps",
		InputsFrom: []fluxopv1.InputProviderReference{
			{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "selected"}}},
		},
	}
	other := *app
	other.InputsFrom = slices.Clone(app.InputsFrom)
	other.Name = "nonmatching"
	other.InputsFrom[0].Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "absent"}}
	provider := &manifest.ResourceSetInputProvider{
		Name:      "chosen",
		Namespace: "apps",
		Labels:    map[string]string{"app": "selected"},
	}
	controls := []manifest.BaseManifest{
		provider,
		&manifest.ResourceSetInputProvider{Name: "other", Namespace: "apps", Labels: map[string]string{"app": "other"}},
		&manifest.ResourceSetInputProvider{
			Name:      "namespace",
			Namespace: "elsewhere",
			Labels:    map[string]string{"app": "selected"},
		},
		&manifest.RawObject{
			APIVersion: "foreign.example/v1",
			Kind:       manifest.KindResourceSetInputProvider,
			Name:       "foreign",
			Namespace:  "apps",
		},
	}
	for side, st := range stores {
		root := t.TempDir()
		input := fstest.MapFS{}
		if !incoming {
			for _, consumer := range []*manifest.ResourceSet{app, &other} {
				label := consumer.InputsFrom[0].Selector.MatchLabels["app"]
				input[consumer.Name+".yaml"] = &fstest.MapFile{Data: []byte(fmt.Sprintf(
					"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: %s, namespace: apps}\nspec:\n  inputsFrom: [{selector: {matchLabels: {app: %s}}}]\n",
					consumer.Name,
					label,
				))}
			}
		}
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
		l := New(st)
		l.SourceRoot = root
		configureSelection(t, l, selection.Request{
			Roots:   []manifest.NamedResource{app.Named(), other.Named()},
			Changed: true,
		})

		l.sideKnown, l.side = true, manifest.TreeSide(side)
		l.locations = map[manifest.NamedResource]authoredOccurrence{}
		l.candidates = map[manifest.NamedResource]store.Child{}
		prepared[side] = map[manifest.NamedResource]store.Child{}
		for _, consumer := range []*manifest.ResourceSet{app, &other} {
			object := consumer.WithNamespace(consumer.Namespace)
			prepared[side][object.Named()] = store.Child{Obj: object}
		}
		if !incoming {
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
		}
		observation := manifest.InputVisibility{
			Source:   manifest.BootstrapSourceID,
			Root:     ".",
			Complete: true,
			Inputs:   map[manifest.NamedResource][]string{},
		}
		if manifest.TreeSide(side) == visibleSide {
			for _, object := range controls {
				id := object.Named()
				prepared[side][id] = store.Child{Obj: object}
				observation.Inputs[id] = []string{id.Name + ".yaml"}
				l.Index.Records[id] = manifest.DiscoveryRecord{File: id.Name + ".yaml"}
				switch representation {
				case "store":
					st.AddObject(object)
				case "compact":
					occurrence := compactOccurrence(object, projectedProducerForTest(object))
					occurrence.Declaration = occurrenceID{Source: root, File: id.Name + ".yaml"}
					l.locations[id] = occurrence
				case "candidate":
					l.candidates[id] = store.Child{Obj: object}
				}
			}
		}
		l.Index.Visibility = []manifest.InputVisibility{observation}
		st.SetDiscoveryIndex(l.Index)
		loaders[side] = l
	}
	return loaders, prepared, []*manifest.ResourceSet{app, &other}, controls
}

func TestAdmission_CompactSelectorInputs(t *testing.T) {
	t.Parallel()
	t.Run("renamed_after_freeze", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
			"kustomization.yaml": {Data: []byte("resources: [a.yaml, z.yaml]\nnamePrefix: built-\n")},
			"a.yaml": {
				Data: []byte(
					"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSetInputProvider\nmetadata: {name: a, namespace: apps, labels: {app: selected}}\nspec: {type: Static}\n",
				),
			},
			"z.yaml": {
				Data: []byte(
					"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSetInputProvider\nmetadata: {name: z, namespace: apps, labels: {app: selected}}\nspec: {type: Static}\n",
				),
			},
		}})
		l := New(st)
		l.SourceRoot = root
		configureSelection(t, l, selection.Request{Changed: true})
		_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
		assert.Equal(t, l.scopes, nil)
		query := manifest.Reference{
			Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "apps",
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "selected"}},
		}
		configureSelection(
			t,
			l,
			selection.Request{
				Roots:  []manifest.NamedResource{query.NamedResource},
				Labels: map[string]string{"app": "selected"},
			},
		)
		proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
		assert.NoError(t, err)
		got := proposal.Selected
		want := []manifest.NamedResource{
			{Group: query.Group, Kind: query.Kind, Namespace: query.Namespace, Name: "built-a"},
			{Group: query.Group, Kind: query.Kind, Namespace: query.Namespace, Name: "built-z"},
		}
		assert.Diff(t, got, want)
		for _, id := range want {
			_, authored := l.occurrence(id)
			assert.Equal(t, authored, false)
			assert.Equal(t, l.CatalogObject(id).Named(), id)
			assert.Equal(t, l.Index.Records[id].Path, filepath.Join(root, id.Name[len("built-"):]+".yaml"))
		}
	})
}

func testSelectorVisibilityCase(t *testing.T, incoming bool, visibleSide manifest.TreeSide, representation string) {
	t.Helper()

	loaders, prepared, consumers, controls := selectorVisibilityPair(
		t,
		incoming,
		visibleSide,
		representation,
	)
	stores := [2]*store.Store{loaders[0].Store, loaders[1].Store}
	indexes := [2]*manifest.DiscoveryIndex{loaders[0].Index, loaders[1].Index}
	app, other := consumers[0], consumers[1]
	provider := controls[0]
	changes, err := change.Qualify(
		t.Context(),
		change.NewSet([]string{".krmignore"}), [2]*store.Store{stores[0], stores[1]},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	changes, err = changes.WithReaders(t.Context(),
		[2]*manifest.DiscoveryIndex{indexes[0], indexes[1]},
		[2]map[manifest.NamedResource]inputprojection.Projection{},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	changes, err = changes.WithDeclarations(
		t.Context(),
		change.DeclarationInputs{Stores: stores, Prepared: prepared},
	)
	assert.NoError(t, err)
	for side, l := range loaders {
		l.Options.Changes = changes
		l.ensureScopes()
		l.freezeConstruction()
		if manifest.TreeSide(side) != visibleSide {
			assert.Equal(t, stores[side].GetObject(provider.Named()), nil)
			_, located := l.locations[provider.Named()]
			assert.Equal(t, located, false)
			assert.Equal(t, l.candidates[provider.Named()].Obj, nil)
		}
		child := store.Child{}
		if incoming {
			child = prepared[side][app.Named()]
		}
		proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{
			Changes:    l.Options.Changes,
			Generation: []store.Child{child},
			Invocation: child.Obj == nil,
		})
		assert.NoError(t, err)
		assert.Equal(t, slices.Contains(proposal.Selected, app.Named()), true)
		assert.Equal(t, slices.Contains(proposal.Dirty, app.Named()), true)
		assert.Equal(
			t,
			slices.Contains(proposal.Needed, provider.Named()),
			manifest.TreeSide(side) == visibleSide,
		)
		assert.Equal(t, slices.Contains(proposal.Selected, other.Named()), false)
		assert.Equal(t, slices.Contains(proposal.Dirty, other.Named()), false)
		for _, control := range controls[1:] {
			assert.Equal(t, slices.Contains(proposal.Needed, control.Named()), false)
		}
	}
}
