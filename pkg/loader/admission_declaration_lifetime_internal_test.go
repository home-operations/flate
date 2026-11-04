package loader

import (
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_ReadonlyDeclarationsAfterConstruction(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"consumer.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec: {path: ./app}\n",
			),
		},
		"readonly.yaml": {
			Data: []byte(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\nmetadata: {name: readonly, namespace: apps}\nspec: {url: https://fixture.invalid/charts}\n",
			),
		},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Changes = change.NewSet([]string{"consumer.yaml"})
	_, err := l.Load(t.Context(), root, nil)
	assert.NoError(t, err)
	readonly := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmRepository,
		Namespace: "apps",
		Name:      "readonly",
	}
	for _, freeze := range []bool{false, true} {
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, freeze))
		proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
		assert.NoError(t, err)
		present := slices.ContainsFunc(
			proposal.Objects,
			func(child store.Child) bool { return child.Obj.Named() == readonly },
		)
		assert.Equal(t, present, false)
		assert.Equal(t, l.CatalogObject(readonly).Named(), readonly)
		assert.Equal(t, st.GetObject(readonly) == nil, true)
		assert.Equal(t, slices.Contains(proposal.Needed, readonly), false)
	}
}

func TestAdmission_PreparedDeclarationUsesFullDocument(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		changed, absent, stored bool
	}{{name: "same document"}, {name: "changed unknown field", changed: true}, {name: "absent document", absent: true}, {name: "stored document", absent: true, stored: true}, {name: "changed stored document", changed: true, absent: true, stored: true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAdmissionPreparedDeclarationUsesFullDocumentCase(
				t,
				&tc,
			)
		},
		)
	}
}

func TestAdmission_UnmaterializedOwner(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                           string
		independent, knownOwner, knownBuilder, rawEdit bool
		want                                           bool
	}{
		{name: "missing_owner", want: true},
		{name: "independent", independent: true},
		{name: "independent declaration edit", independent: true, rawEdit: true, want: true},
		{name: "built declaration edit", independent: true, rawEdit: true, knownBuilder: true},
		{name: "known_owner", knownOwner: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAdmissionUnmaterializedOwnerCase(t, &tc)
		},
		)
	}
}

func testAdmissionPreparedDeclarationUsesFullDocumentCase(
	t *testing.T,
	testCaseInput *struct {
		name    string
		changed bool
		absent  bool
		stored  bool
	},
) {
	t.Helper()

	id := (&manifest.Kustomization{Name: "child", Namespace: "apps"}).Named()
	owner := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	stores, inputs, prepared := preparedDeclarationPair(t, id, owner.Named())
	changes, err := change.Qualify(
		t.Context(),
		change.NewSet([]string{"declaration.yaml"}), [2]*store.Store{
			stores[0],
			stores[1],
		}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	changes, err = changes.WithDeclarations(
		t.Context(),
		change.DeclarationInputs{Stores: [2]*store.Store{stores[0], stores[1]}, Prepared: prepared},
	)
	assert.NoError(t, err)
	l := New(stores[0])
	l.SourceRoot = inputs[0].LocalPath
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Changes, l.Options.Filter = changes, change.NewFilter()
	l.frozen, l.sideKnown, l.side = true, true, manifest.TreeBase
	l.bindings.Store(
		&map[manifest.NamedResource]admissionSource{manifest.BootstrapSourceID: {initial: inputs[0]}},
	)
	l.candidates = prepared[0]
	l.Index.Records[id] = manifest.DiscoveryRecord{
		Builder:     "kustomization.yaml",
		Declaration: prepared[0][id].Obj,
		Inputs:      []string{"declaration.yaml"},
		Productions: []manifest.Production{{Owner: owner.Named(), Source: manifest.BootstrapSourceID}},
	}
	_, retry := l.Options.Filter.AddEmitted([]manifest.NamedResource{id}, []manifest.NamedResource{id}, nil, 0)
	assert.Equal(t, retry, false)
	child := prepared[0][id]
	child.Obj = child.Obj.(*manifest.Kustomization).Clone()
	child.Doc = manifest.DeepCopyMap(child.Doc)
	if testCaseInput.changed {
		child.Doc["spec"].(map[string]any)["unknown"] = "changed"
	}
	if testCaseInput.stored {
		stores[0].AddObject(owner)
		stores[0].RecordEmitted(
			owner.Named(),
			&store.Emission{Children: []store.Child{child}, Complete: true},
			nil,
		)
		assert.Equal(t, stores[0].GetDeclaration(id) == child.Obj, true)
	}
	if testCaseInput.absent {
		child.Doc = nil
	}
	proposal, err := l.Prepare(t.Context(), child, nil, nil)
	assert.NoError(t, err)
	assert.Equal(
		t,
		slices.Contains(proposal.Dirty, id),
		testCaseInput.changed || testCaseInput.absent && !testCaseInput.stored,
	)
}

func testAdmissionUnmaterializedOwnerCase(t *testing.T, testCaseInput *struct {
	name         string
	independent  bool
	knownOwner   bool
	knownBuilder bool
	rawEdit      bool
	want         bool
},
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"child.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: child, namespace: apps}\nspec: {path: ./workload}\n",
			),
		},
		"owner.yaml": {
			Data: []byte(
				"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: owner, namespace: apps}\nspec:\n  resourcesTemplate: |\n    apiVersion: kustomize.toolkit.fluxcd.io/v1\n    kind: Kustomization\n    metadata: {name: child, namespace: apps}\n    spec: {path: ./workload}\n",
			),
		},
		"workload/kustomization.yaml": {Data: []byte("resources: []\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Selection = manifest.Selection{Changed: true}
	changed := "workload/kustomization.yaml"
	if testCaseInput.rawEdit {
		changed = "child.yaml"
	}
	l.Options.Changes, l.Options.Filter = change.NewSet([]string{changed}), change.NewFilter()
	_, err := l.Load(t.Context(), root, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	child := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "child",
	}
	producer := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "owner",
	}
	l.roots[child] = testCaseInput.independent
	if testCaseInput.knownOwner {
		record := l.Index.Records[child]
		record.Parent = manifest.NamedResource{
			Group:     manifest.FluxKustomizeDomain,
			Kind:      manifest.KindKustomization,
			Namespace: "apps",
			Name:      "known",
		}
		l.Index.Records[child] = record
	}
	if testCaseInput.knownBuilder {
		record := l.Index.Records[child]
		record.BuilderOwner = manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).
			Named()
		l.Index.Records[child] = record
	}
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, producer), testCaseInput.want)
	assert.Equal(t, slices.Contains(proposal.Selected, producer), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, producer), false)
	assert.Equal(t, st.GetObject(child) == nil, true)
	if testCaseInput.independent && testCaseInput.want {
		assert.Equal(
			t,
			slices.ContainsFunc(proposal.Objects, func(o store.Child) bool { return o.Obj.Named() == child }),
			false,
		)
		st.AddObject(&manifest.ResourceSet{Name: producer.Name, Namespace: producer.Namespace})
		st.RecordEmitted(producer, &store.Emission{Complete: true}, nil)
		st.UpdateStatus(producer, store.StatusInfo{Status: store.StatusReady})
		proposal, err = l.Prepare(t.Context(), store.Child{}, nil, nil)
		assert.NoError(t, err)
		assert.Equal(
			t,
			slices.ContainsFunc(proposal.Objects, func(o store.Child) bool { return o.Obj.Named() == child }),
			true,
		)
	}
}
