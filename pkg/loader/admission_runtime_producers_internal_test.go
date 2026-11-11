package loader

import (
	"slices"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestFilter_SourceFileReaderKeepsEmitter(t *testing.T) {
	t.Parallel()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system", Path: "apps"}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "flux-system", Path: "workloads/demo"}
	release := testutil.HelmReleaseID("apps", "demo")
	sibling := testutil.HelmReleaseID("apps", "sibling")
	objects := filtertest.MapLister{parent.Named(): parent, owner.Named(): owner}
	idx := new(filterFixture{
		files: map[manifest.NamedResource]string{
			parent.Named(): "flux/parent.yaml",
			owner.Named():  "apps/owner.yaml",
			release:        "workloads/demo/hr.yaml",
			sibling:        "workloads/demo/sibling.yaml",
		},
		objects: objects,
	}).index()
	idx.Readers["config/prod.yaml"] = []manifest.FileReader{{Reader: release, Source: manifest.BootstrapSourceID}}
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"config/prod.yaml"}), index: idx, inventory: objects, root: ""},
	)
	for _, id := range []manifest.NamedResource{release, owner.Named(), parent.Named()} {
		assert.Equal(t, f.Reconcile(id), true)
	}
	assert.Equal(t, f.Reconcile(sibling), false)
	for _, emitter := range []manifest.NamedResource{owner.Named(), parent.Named()} {
		assert.Equal(t, len(f.AddEmitted(emitter, new(store.Child{Obj: refObj(sibling)}), f.objs)), 0)
		assert.Equal(t, f.Reconcile(sibling), false)
	}
	assert.Equal(t, f.loader.Store.GetDeclaration(release), nil)
	releaseObject := &manifest.HelmRelease{Name: release.Name, Namespace: release.Namespace}
	f.prepare(selection.PrepareRequest{Invocation: true, Generation: []store.Child{{
		Obj: releaseObject, Productions: []manifest.Production{{Owner: owner.Named()}},
	}}}, false)
	f.settle()
	f.loader.Store.RecordEmitted(owner.Named(), &store.Emission{
		Children: []store.Child{{Obj: releaseObject}}, Complete: true,
	}, nil)
	child := testutil.HelmReleaseID("apps", "rendered")
	assert.Diff(t, f.AddEmitted(release, new(store.Child{Obj: refObj(child)}), f.objs), []manifest.NamedResource{child})
}

func TestFilter_AcceptedChildren(t *testing.T) {
	t.Parallel()
	for _, parent := range []manifest.BaseManifest{
		&manifest.RawObject{APIVersion: manifest.FluxKustomizeDomain + "/v1", Kind: manifest.KindKustomization, Name: "owner"},
		&manifest.RawObject{APIVersion: manifest.HelmReleaseDomain + "/v2", Kind: manifest.KindHelmRelease, Name: "owner"},
		&manifest.RawObject{APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: manifest.KindResourceSet, Name: "owner"},
	} {
		t.Run("raw_"+parent.Named().Kind, func(t *testing.T) {
			t.Parallel()
			document := map[string]any{
				"apiVersion": "v1",
				"kind":       manifest.KindConfigMap,
				"metadata":   map[string]any{"name": "child"},
			}
			result := ParseChildren(
				parent,
				[]manifest.DocOccurrence{{Document: document}},
				manifest.ParseDocOptions{DefaultNamespace: "default"},
				nil,
			)
			assert.Equal(t, len(result.Failures), 0)
			assert.Equal(t, len(result.Children), 1)
			assert.Equal(t, result.Children[0].Obj.Named().Namespace, "default")
			assert.Equal(t, manifest.KindOf(parent).Parent, false)
		})
	}
	assert.Equal(
		t,
		len(ParseChildren(nil, nil, manifest.ParseDocOptions{DefaultNamespace: "default"}, nil).Children),
		0,
	)
	for _, phase := range []string{"initial", "runtime", "unselected", "declaration", "file_indexed"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			testFilterAcceptedChildrenCase(t, &phase)
		},
		)
	}
}

func TestFilter_LateProducerIndexedDeclarations(t *testing.T) {
	t.Parallel()
	t.Run(
		"oci_provider_credentials",
		func(t *testing.T) { t.Parallel(); testFilterLateProducerIndexedDeclarationsOciProviderCredentials(t) },
	)
	for _, tc := range []struct {
		name        string
		prepared    bool
		otherOwner  bool
		changed     bool
		transformed bool
		new         bool
		want        bool
	}{
		{name: "file_indexed"},
		{name: "prepared", prepared: true},
		{name: "different_prepared_owner", prepared: true, otherOwner: true, want: true},
		{name: "changed_input", changed: true, want: true},
		{name: "transformed", transformed: true, want: true},
		{name: "new", new: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFilterLateProducerIndexedDeclarationsCase(
				t,
				&tc,
			)
		},
		)
	}
}

func testFilterAcceptedChildrenCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	st := store.New()
	parent := &manifest.ResourceSet{Name: "parent", Namespace: "ns"}
	child := &manifest.ResourceSet{Name: "child", Namespace: "ns"}
	leaf := &manifest.RawObject{APIVersion: "v1", Kind: manifest.KindConfigMap, Name: "leaf", Namespace: "ns"}
	sibling := &manifest.HelmRelease{Name: "sibling", Namespace: "ns"}
	declaration := &manifest.HelmRelease{Name: "transformed", Namespace: "ns"}
	transformed := declaration.Clone()
	transformed.Suspend = true
	emitter := &manifest.Kustomization{Name: "emitter", Namespace: "ns"}
	st.AddObjects([]manifest.BaseManifest{parent, emitter})
	st.RecordEmitted(
		parent.Named(),
		&store.Emission{
			Children: []store.Child{{Obj: child}, {Obj: sibling}, {Obj: transformed}},
			Complete: true,
		},
		nil,
	)
	st.RecordEmitted(child.Named(), &store.Emission{Children: []store.Child{{Obj: leaf}}, Complete: true}, nil)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		parent.Named():  {File: "parent.yaml"},
		emitter.Named(): {File: "emitter.yaml"},
		sibling.Named(): {
			File:         "sibling.yaml",
			Builder:      "parent.yaml",
			BuilderOwner: parent.Named(),
			Declaration:  sibling,
			Productions: []manifest.Production{
				{Owner: parent.Named(), Authored: sibling.Named(), File: "sibling.yaml"},
			},
		},
		transformed.Named(): {
			File:         "transformed.yaml",
			Builder:      "parent.yaml",
			BuilderOwner: parent.Named(),
			Declaration:  declaration,
			Productions: []manifest.Production{
				{Owner: parent.Named(), Authored: transformed.Named(), File: "transformed.yaml"},
			},
		},
	}}
	if (*testCaseInput) == "file_indexed" {
		for _, obj := range []manifest.BaseManifest{sibling, transformed} {
			record := index.Records[obj.Named()]
			record.Builder, record.BuilderOwner = "", manifest.NamedResource{}
			index.Records[obj.Named()] = record
		}
	}
	file := "emitter.yaml"
	switch *testCaseInput {
	case "initial":
		file = "parent.yaml"
	case "declaration":
		file = "sibling.yaml"
	}
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{file}), index: index, inventory: st, root: ""},
	)
	switch *testCaseInput {
	case "runtime", "file_indexed":
		f.AddEmitted(emitter.Named(), new(store.Child{Obj: parent}), f.objs)
	case "unselected":
		f.AddEmitted(parent.Named(), new(store.Child{Obj: child}), f.objs)
	}
	for _, obj := range []manifest.BaseManifest{child, leaf} {
		assert.Equal(
			t,
			f.Reconcile(obj.Named()), (*testCaseInput) ==
				"initial" || (*testCaseInput) == "runtime" || (*testCaseInput) == "file_indexed",
		)
	}
	assert.Equal(t, f.Reconcile(sibling.Named()), (*testCaseInput) == "declaration")
	assert.Equal(
		t,
		f.Reconcile(transformed.Named()), (*testCaseInput) ==
			"initial" || (*testCaseInput) == "runtime" || (*testCaseInput) == "file_indexed",
	)
}

func testFilterLateProducerIndexedDeclarationsOciProviderCredentials(t *testing.T) {
	t.Helper()

	for _, route := range []string{"named", "selector"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			provider := &manifest.ResourceSetInputProvider{
				Name: "tags", Namespace: "effective", Labels: map[string]string{"role": "images"},
				Type: fluxopv1.InputProviderOCIArtifactTag, SecretRef: &manifest.LocalObjectReference{Name: "auth"},
			}
			secret := &manifest.Secret{Name: "auth", Namespace: "effective"}
			producer := &manifest.Kustomization{Name: "credentials", Namespace: "effective"}
			consumer := &manifest.ResourceSet{
				Name:       "set",
				Namespace:  "effective",
				InputsFrom: []fluxopv1.InputProviderReference{{Name: "tags"}},
			}
			if route == "selector" {
				consumer.InputsFrom = []fluxopv1.InputProviderReference{
					{Selector: &metav1.LabelSelector{MatchLabels: provider.Labels}},
				}
			}
			sibling := &manifest.Kustomization{Name: "sibling", Namespace: "effective"}
			st := store.New()
			st.AddObjects([]manifest.BaseManifest{consumer, sibling})
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				consumer.Named(): {File: "consumer.yaml", Declaration: consumer},
				provider.Named(): {File: "provider.yaml", Declaration: provider},
				secret.Named(): {
					File: "credentials/secret.yaml", Declaration: secret,
					Productions: []manifest.Production{{Owner: producer.Named(), File: "credentials/secret.yaml"}},
				},
				producer.Named(): {File: "producer.yaml", Declaration: producer},
				sibling.Named():  {File: "sibling.yaml", Declaration: sibling},
			}}
			filter := newAdmission(
				t,
				admissionConfig{
					changes:   change.NewSet([]string{"credentials/secret.yaml"}),
					index:     index,
					inventory: st,
				},
			)
			filter.loader.locations = map[manifest.NamedResource]authoredOccurrence{
				provider.Named(): compactOccurrence(provider, projectedProducerForTest(provider)),
			}
			filter.prepare(selection.PrepareRequest{Invocation: true}, false)
			var preparation *selection.Preparation
			defer func() { preparation.Retire() }()
			for range 2 {
				proposal, err := prepareSelection(t.Context(), t, filter.loader, selection.PrepareRequest{
					Changes:     filter.loader.Options.Changes,
					Invocation:  true,
					Preparation: &preparation,
				})
				assert.NoError(t, err)
				for _, id := range []manifest.NamedResource{secret.Named(), provider.Named(), producer.Named(), consumer.Named()} {
					assert.Equal(t, slices.Contains(proposal.Needed, id), true)
				}
				assert.Equal(t, slices.Contains(proposal.Needed, sibling.Named()), false)
				proposal.Retire()
			}

			for _, id := range []manifest.NamedResource{secret.Named(), provider.Named(), producer.Named(), consumer.Named()} {
				assert.Equal(t, filter.Reconcile(id), true)
			}
			assert.Equal(t, filter.Reconcile(sibling.Named()), false)
			assert.Equal(t, st.GetObject(provider.Named()) == nil, true)
			assert.Equal(t, st.GetObject(secret.Named()) == nil, true)
			assert.Equal(t, st.GetObject(producer.Named()) == nil, true)
		})
	}
}

func testFilterLateProducerIndexedDeclarationsCase(
	t *testing.T,
	testCaseInput *struct {
		name        string
		prepared    bool
		otherOwner  bool
		changed     bool
		transformed bool
		new         bool
		want        bool
	},
) {
	t.Helper()

	st := store.New()
	emitter := &manifest.Kustomization{Name: "emitter", Namespace: "apps"}
	parent := &manifest.Kustomization{Name: "late", Namespace: "apps"}
	declaration := &manifest.HelmRelease{Name: "sibling", Namespace: "apps"}
	child := declaration.Clone()
	child.Suspend = testCaseInput.transformed
	st.AddObjects([]manifest.BaseManifest{emitter, parent, declaration})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		emitter.Named(): {File: "emitter.yaml"},
	}}
	if !testCaseInput.new {
		record := manifest.DiscoveryRecord{File: "sibling.yaml", Declaration: declaration}
		if testCaseInput.prepared {
			record.Builder, record.BuilderOwner = "kustomization.yaml", parent.Named()
			if testCaseInput.otherOwner {
				record.BuilderOwner.Name = "other"
			}
		}
		if testCaseInput.changed {
			record.Inputs = []string{"config.yaml"}
		}
		index.Records[declaration.Named()] = record
	}
	f := newAdmission(
		t,
		admissionConfig{
			changes:   change.NewSet([]string{"emitter.yaml", "config.yaml"}),
			index:     index,
			inventory: st,
		},
	)
	f.AddEmitted(emitter.Named(), new(store.Child{Obj: parent}), st)
	assert.Equal(t, f.Reconcile(parent.Named()), true)
	f.AddEmitted(
		parent.Named(),
		new(store.Child{
			Obj:         child,
			Productions: []manifest.Production{{Owner: parent.Named(), File: "sibling.yaml"}},
		}),
		st,
	)
	assert.Equal(t, f.Reconcile(child.Named()), testCaseInput.want)
}
