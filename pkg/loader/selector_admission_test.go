package loader

import (
	"slices"
	"testing"
	"testing/fstest"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_SelectorInputs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"set.yaml":     {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: app, namespace: apps}\nspec:\n  inputsFrom: [{kind: ResourceSetInputProvider, selector: {matchLabels: {app: selected}}}]\n  resourcesTemplate: 'apiVersion: v1'\n")},
		"chosen.yaml":  {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSetInputProvider\nmetadata: {name: chosen, namespace: apps, labels: {app: selected}}\nspec: {type: Static}\n")},
		"other.yaml":   {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSetInputProvider\nmetadata: {name: other, namespace: apps, labels: {app: other}}\nspec: {type: Static}\n")},
		"foreign.yaml": {Data: []byte("apiVersion: foreign.example/v1\nkind: ResourceSetInputProvider\nmetadata: {name: foreign, namespace: apps, labels: {app: selected}}\nspec: {type: Static}\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	app := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "app"}
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{app}}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	names := []string{}
	for _, child := range proposal.Objects {
		names = append(names, child.Obj.Named().Name)
	}
	slices.Sort(names)
	assert.Diff(t, names, []string{"app", "chosen"})

	t.Run("compact records", func(t *testing.T) {
		t.Parallel()
		st := store.New()
		l := New(st)
		l.locations = map[manifest.NamedResource]Occurrence{}
		for _, name := range []string{"z", "a"} {
			provider := &manifest.ResourceSetInputProvider{Name: name, Labels: map[string]string{"app": "selected"}}
			occurrence := compactOccurrence(provider)
			occurrence.Identity.Namespace = "apps"
			l.locations[occurrence.Identity] = occurrence
		}
		query := manifest.Reference{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "selected"}}}
		closure := admissionClosure{loader: l, objects: st}
		got, err := closure.matchingInputs(query, false)
		assert.NoError(t, err)
		assert.Diff(t, got, []manifest.NamedResource{
			{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "a"},
			{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "z"},
		})
		assert.Equal(t, len(st.Objects()), 0)
	})

	for _, tc := range []struct {
		name string
		side manifest.TreeSide
	}{{"base-only match", manifest.TreeBase}, {"head-only match", manifest.TreeHead}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stores := [2]*store.Store{store.New(), store.New()}
			loaders := [2]*Loader{}
			var prepared [2]map[manifest.NamedResource]store.Child
			providers := [2]*manifest.ResourceSetInputProvider{}
			consumers := [2]*manifest.ResourceSet{}
			for side, st := range stores {
				root := t.TempDir()
				artifact := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
				st.SetArtifact(manifest.BootstrapSourceID, artifact)
				label := "other"
				if manifest.TreeSide(side) == tc.side {
					label = "selected"
				}
				provider := &manifest.ResourceSetInputProvider{Name: "chosen", Namespace: "apps", Labels: map[string]string{"app": label}}
				consumer := &manifest.ResourceSet{Name: "app", Namespace: "apps", InputsFrom: []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "selected"}}}}}
				providers[side], consumers[side] = provider, consumer
				prepared[side] = map[manifest.NamedResource]store.Child{provider.Named(): {Obj: provider}, consumer.Named(): {Obj: consumer}}
				l := New(st)
				l.SourceRoot = root
				l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{consumer.Named()}, Changed: true}
				l.Options.Filter = change.NewFilter()
				l.candidates = prepared[side]
				l.Index.Records[provider.Named()] = manifest.DiscoveryRecord{File: "chosen.yaml"}
				l.Index.Records[consumer.Named()] = manifest.DiscoveryRecord{File: "set.yaml", Declaration: consumer}
				l.sideKnown, l.side = true, manifest.TreeSide(side)
				l.freezeConstruction()
				loaders[side] = l
			}
			changes, err := change.Qualify(t.Context(), change.NewSet([]string{"chosen.yaml"}), stores[0], stores[1])
			assert.NoError(t, err)
			changes, err = changes.WithDeclarations(t.Context(), stores[0], stores[1], prepared)
			assert.NoError(t, err)
			for side, l := range loaders {
				l.Options.Changes = changes
				l.bindings = map[manifest.NamedResource]*store.SourceArtifact{manifest.BootstrapSourceID: stores[side].GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)}
				assert.Equal(t, len(stores[side].Objects()), 0)
				proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
				assert.NoError(t, err)
				assert.Equal(t, slices.Contains(proposal.Selected, app), true)
				assert.Equal(t, slices.Contains(proposal.Dirty, app), true)
				assert.Equal(t, slices.Contains(proposal.Needed, providers[side].Named()), manifest.TreeSide(side) == tc.side)

				proposal, err = l.Prepare(t.Context(), store.Child{Obj: consumers[side]}, nil, nil)
				assert.NoError(t, err)
				assert.Equal(t, slices.Contains(proposal.Selected, app), true)
				assert.Equal(t, slices.Contains(proposal.Dirty, app), true)

				_, retry := l.Options.Filter.AddEmitted([]manifest.NamedResource{providers[side].Named()}, nil, []manifest.NamedResource{providers[side].Named()}, 0)
				assert.Equal(t, retry, false)
				proposal, err = l.Prepare(t.Context(), store.Child{Obj: providers[side]}, nil, nil)
				assert.NoError(t, err)
				assert.Equal(t, slices.Contains(proposal.Selected, app), true)
				assert.Equal(t, slices.Contains(proposal.Dirty, app), true)

				producer := &manifest.HelmRelease{Name: "producer", Namespace: "apps"}
				stores[side].AddObject(producer)
				stores[side].RecordEmitted(producer.Named(), &store.Emission{Complete: true, Children: []store.Child{{Obj: consumers[side]}}}, nil)
				stores[side].UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
				occurrence := compactOccurrence(producer)
				occurrence.Declaration.Source = l.SourceRoot
				closure := admissionClosure{loader: l, objects: stores[side]}
				closure.materializers(materializerDemand{target: providers[side].Named(), source: l.SourceRoot, readers: true}, providers[side].Named(), true, [2][]Occurrence{{occurrence}})
				assert.Equal(t, closure.role(producer.Named()).needed, true)

				if manifest.TreeSide(side) != tc.side {
					l.Options.Filter = change.NewFilter()
					stores[side].SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: l.SourceRoot, FS: fstest.MapFS{}})
					proposal, err = l.Prepare(t.Context(), store.Child{Obj: consumers[side]}, nil, nil)
					assert.NoError(t, err)
					assert.Equal(t, slices.Contains(proposal.Selected, app), false)
				}
			}
		})
	}
}

func TestAdmission_LateSelectorInput(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"set.yaml": {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: app, namespace: apps}\nspec:\n  inputsFrom: [{kind: ResourceSetInputProvider, selector: {matchLabels: {app: selected}}}]\n  resourcesTemplate: 'apiVersion: v1'\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet(nil)
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	parent := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "parent"}
	_, retry := l.Options.Filter.AddEmitted([]manifest.NamedResource{parent}, []manifest.NamedResource{parent}, []manifest.NamedResource{parent}, 0)
	assert.Equal(t, retry, false)
	reads := &store.ReadSet{}
	reads.Init(st, parent, nil)
	defer reads.Retire()
	provider := &manifest.ResourceSetInputProvider{Name: "late", Namespace: "apps", Labels: map[string]string{"app": "selected"}}
	proposal, err := l.Prepare(t.Context(), store.Child{Obj: provider, Productions: []manifest.Production{{Owner: parent}}}, reads, nil)
	assert.NoError(t, err)
	app := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "app"}
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
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet(nil)
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	parent := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "support"}
	_, retry := l.Options.Filter.AddEmitted([]manifest.NamedResource{parent}, nil, nil, 0)
	assert.Equal(t, retry, false)
	reads := &store.ReadSet{}
	reads.Init(st, parent, nil)
	defer reads.Retire()
	consumer := &manifest.ResourceSet{Name: "late", Namespace: "apps", InputsFrom: []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "selected"}}}}}
	proposal, err := l.Prepare(t.Context(), store.Child{Obj: consumer, Productions: []manifest.Production{{Owner: parent}}}, reads, nil)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, consumer.Named()), false)
	assert.Equal(t, reads.Valid(), true)
	st.AddObject(&manifest.ResourceSetInputProvider{Name: "provider", Namespace: "apps", Labels: map[string]string{"app": "selected"}})
	assert.Equal(t, reads.Valid(), false)
}
