package loader

import (
	"fmt"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdmission_NamedDeclarationAndInputs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := fstest.MapFS{"objects.yaml": {Data: []byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: app, namespace: apps}
spec:
  interval: 1m
  chart: {spec: {chart: demo, sourceRef: {kind: HelmRepository, name: charts}}}
  valuesFrom: [{kind: ConfigMap, name: values}]
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: unrelated, namespace: apps}
spec:
  interval: 1m
  chart: {spec: {chart: other, sourceRef: {kind: HelmRepository, name: unrelated}}}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: charts, namespace: apps}
spec: {url: https://fixture.invalid/charts}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: values, namespace: apps}
data: {values.yaml: '{}'}
`)}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
	l := New(st)
	app := manifest.NamedResource{
		Group:     manifest.HelmReleaseDomain,
		Kind:      manifest.KindHelmRelease,
		Namespace: "apps",
		Name:      "app",
	}
	l.SourceRoot = root
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
		assert.Equal(t, child.Obj.Named().Namespace, "apps")
	}
	slices.Sort(names)
	assert.Diff(t, names, []string{"app", "charts", "values"})
	assert.Diff(t, proposal.Selected, []manifest.NamedResource{app})
	assert.Equal(t, len(st.Objects()), 0)
	assert.Equal(t, l.Options.Session.Snapshot().Reconcile(app), false)
	added := slices.Clone(proposal.Needed)
	objectsBeforeCommit := slices.Clone(proposal.Objects)
	_, committed, err := l.commitSelection(t.Context(), t, proposal, selection.Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, len(added), 3)
	assert.Equal(t, l.Options.Session.Snapshot().Reconcile(app), true)
	unrelated := app
	unrelated.Name = "unrelated"
	assert.Equal(t, l.Options.Session.Snapshot().Reconcile(unrelated), false)
	for _, child := range objectsBeforeCommit {
		st.DeleteObject(child.Obj.Named())
	}
	retried, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	names = make([]string, 0, len(retried.Objects))
	for _, child := range retried.Objects {
		names = append(names, child.Obj.Named().Name)
	}
	slices.Sort(names)
	assert.Diff(t, names, []string{"app", "charts", "values"})
}

func TestAdmission_EmptyDelta(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{
			LocalPath: root,
			FS: fstest.MapFS{
				"object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data}\n")},
			},
		},
	)
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	l.Options.Changes = change.NewSet(nil)

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
	assert.Equal(t, len(proposal.Needed), 0)
	assert.Equal(t, len(proposal.Objects), 0)
}

func TestAdmission_UncertainProducerKeepsExactOwner(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"app.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\n" + "kind: Kustomization\n" +
			"metadata: {name: app, namespace: apps}\n" +
			"spec:\n" +
			"  interval: 1m\n" +
			"  path: ./consumer\n" +
			"  postBuild:\n" +
			"    substituteFrom: [{kind: Secret, name: vars}]\n")},
		"owner.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\n" + "kind: Kustomization\n" +
			"metadata: {name: owner, namespace: apps}\n" +
			"spec: {interval: 1m, path: ./producer, targetNamespace: apps}\n")},
		"unrelated.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\n" + "kind: Kustomization\n" +
			"metadata: {name: unrelated, namespace: apps}\n" +
			"spec: {interval: 1m, path: ./other, targetNamespace: apps}\n")},
		"producer/kustomization.yaml": {Data: []byte("resources: [external.yaml]\n")},
		"producer/external.yaml": {Data: []byte("apiVersion: external-secrets.io/v1\n" + "kind: ExternalSecret\n" +
			"metadata: {name: '${CLUSTER_NAME:=biohazard}-vars', namespace: apps}\n" +
			"spec: {target: {name: vars}}\n")},
		"consumer/kustomization.yaml": {Data: []byte("resources: []\n")},
		"other/kustomization.yaml":    {Data: []byte("resources: []\n")},
	}})
	l := New(st)
	l.SourceRoot = root

	configureSelection(t, l, selection.Request{
		Roots: []manifest.NamedResource{
			{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "app"},
		},
	})
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
	owner := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "owner",
	}
	assert.Equal(t, slices.Contains(proposal.Needed, owner), true)
	owner.Name = "unrelated"
	assert.Equal(t, slices.Contains(proposal.Needed, owner), false)
	for id := range l.Index.Records {
		if manifest.HasEnvsubstReference(id.Name) || id.Name == "" {
			t.Fatalf("uncertain producer entered concrete index: %s", id)
		}
	}
}

func TestAdmission_AncestorRetainsSupportRole(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"parent.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\n" + "kind: Kustomization\n" +
			"metadata: {name: parent, namespace: apps}\n" +
			"spec: {interval: 1m, path: ./apps}\n")},
		"apps/kustomization.yaml": {Data: []byte("resources: [app.yaml]\n")},
		"apps/app.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\n" + "kind: Kustomization\n" +
			"metadata: {name: app, namespace: apps}\n" +
			"spec: {interval: 1m, path: ./apps/child}\n")},
		"apps/child/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"apps/child/cm.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {key: changed}\n",
			),
		},
	}})
	l := New(st)
	l.SourceRoot = root

	l.Options.Changes = change.NewSet([]string{"apps/child/cm.yaml"})
	configureSelection(t, l, selection.Request{Changed: true})
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
	parent := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "parent",
	}
	app := parent
	app.Name = "app"
	assert.Equal(t, slices.Contains(proposal.Needed, parent), true)
	assert.Equal(t, slices.Contains(proposal.Selected, parent), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, parent), false)
	assert.Equal(t, slices.Contains(proposal.Selected, app), true)
	assert.Equal(t, slices.Contains(proposal.Dirty, app), true)
}

func TestAdmission_MultipleReferences(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		keys    []string
		changed bool
		direct  bool
	}{
		{name: "unchanged then changed", keys: []string{"stable", "changed"}, changed: true},
		{name: "changed then unchanged", keys: []string{"changed", "stable"}, changed: true},
		{name: "unchanged then unknown", keys: []string{"stable", "unknown"}, changed: true},
		{name: "unknown then unchanged", keys: []string{"unknown", "stable"}, changed: true},
		{name: "all unchanged", keys: []string{"stable", "also-stable"}},
		{name: "another selecting cause", keys: []string{"stable", "also-stable"}, direct: true, changed: true},
		{name: "ownership without reference", changed: true},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				t.Parallel()
				stores, loaders := admissionReferencePair(t, tc.keys)
				input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"}
				consumer := testutil.HelmReleaseID("apps", "app")
				if tc.keys == nil {
					input = testutil.KustomizationID("apps", "parent")
				}
				paths := []string{"input.yaml"}
				if tc.direct {
					paths = append(paths, "consumer.yaml")
					obj := stores[1].GetObject(consumer).(*manifest.HelmRelease).Clone()
					obj.HelmReleaseSpec.ReleaseName = "another-cause"
					stores[1].AddObject(obj)
				}
				changes, err := change.Qualify(
					t.Context(),
					change.NewSet(paths), [2]*store.Store{stores[0], stores[1]},
					manifest.WorkScope{},
				)
				assert.NoError(t, err)
				changes, err = changes.WithDeclarations(
					t.Context(),
					change.DeclarationInputs{
						Stores:   [2]*store.Store{stores[0], stores[1]},
						Prepared: [2]map[manifest.NamedResource]store.Child{},
					},
				)
				assert.NoError(t, err)
				for side, l := range loaders {
					l.Options.Changes = changes
					obj := stores[side].GetObject(consumer)
					assert.Equal(t, stores[side].GetObject(input) != nil, true)
					for ref := range manifest.References(obj, manifest.ReferenceRelations) {
						outputChangedResult := changes.OutputChanged(
							manifest.TreeSide(side),
							manifest.BootstrapSourceID,
							ref,
							obj,
						)
						changed, known := outputChangedResult.Changed, outputChangedResult.Known
						assert.Equal(t, known, ref.Key != "unknown")
						if known {
							assert.Equal(t, changed, ref.Key == "changed")
						}
					}
					proposal, err := prepareSelection(
						t.Context(),
						t,
						l,
						selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
					)
					assert.NoError(t, err)
					t.Run(fmt.Sprintf("side-%d", side), func(t *testing.T) {
						t.Parallel()
						assert.Equal(t, slices.Contains(proposal.Selected, consumer), tc.changed)
						assert.Equal(t, slices.Contains(proposal.Dirty, consumer), tc.changed)
					})
				}
			},
		)
	}
}

func TestAdmission_UnchangedInputDoesNotCaptureConsumers(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"input.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\ndata: {VALUE: stable}\n",
			),
		},
		"consumer.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\n" + "kind: Kustomization\n" +
			"metadata: {name: consumer, namespace: apps}\n" +
			"spec:\n" +
			"  path: ./app\n" +
			"  postBuild:\n" +
			"    substituteFrom: [{kind: ConfigMap, name: input}]\n")},
	}})
	l := New(st)
	l.SourceRoot = root

	configureSelection(t, l, selection.Request{
		Roots: []manifest.NamedResource{
			{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Name: "consumer"},
		},
	})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	for _, child := range proposal.Objects {
		st.AddObject(child.Obj)
	}
	input := manifest.NamedResource{Kind: "ConfigMap", Namespace: "apps", Name: "input"}
	var reads store.ReadSet
	reads.Init(st, input, nil)
	defer reads.Retire()
	_, err = prepareSelection(t.Context(), t, l, selection.PrepareRequest{
		Changes:    l.Options.Changes,
		Generation: []store.Child{{Obj: st.GetObject(input)}},
		Reads:      &reads,
	})
	assert.NoError(t, err)
	assert.Equal(t, reads.Valid(), true)
	consumer := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "consumer",
	}
	st.DeleteObject(consumer)
	if !reads.Valid() {
		t.Fatal("changing a consumer invalidated its unchanged input")
	}
}

func TestAdmission_IncomingChildPublicationPreservesProof(t *testing.T) {
	t.Parallel()
	st := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	st.AddObject(parent)
	l := New(st)

	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{child.Named()}})
	var reads store.ReadSet
	reads.Init(st, parent.Named(), nil)
	defer reads.Retire()
	_, _, next := reads.GetEmission(parent.Named())
	defer next.Finish()
	incoming := store.Child{Obj: child, Productions: []manifest.Production{{Owner: parent.Named()}}}
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Generation: []store.Child{incoming}, Reads: &reads},
	)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Selected, child.Named()), true)
	assert.Equal(t, reads.Valid(), true)
	next.Children = []store.Child{incoming}
	next.Complete = true
	reads.Bind(&next)
	st.RecordEmitted(parent.Named(), &next, nil)

	assert.FatalIf(
		t,
		st.GetObject(child.Named()) == nil || !reads.Valid(),
		"%s",
		"the selected incoming child invalidated its own publication",
	)
}

//nolint:gocognit // The same assertions cover unrestricted and scoped session lifecycles.
func TestAdmission_EmittedNamespaceChildren(t *testing.T) {
	t.Parallel()
	for _, scope := range []string{"namespace", "named", "all"} {
		t.Run(scope, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			selected := &manifest.Kustomization{Name: "selected", Namespace: "apps"}
			selected.SourceRef.Kind = manifest.KindOCIRepository
			selected.SourceRef.Name, selected.SourceRef.Namespace = "required", "monitoring"
			excluded := &manifest.Kustomization{Name: "excluded", Namespace: "monitoring"}
			excluded.Path, excluded.TargetNamespace = "excluded", "monitoring"
			required := &manifest.OCIRepository{Name: "required", Namespace: "monitoring"}
			st.AddObjects([]manifest.BaseManifest{parent, selected})
			l := New(st)

			root := parent.Named()
			switch scope {
			case "namespace":
				root.Name = ""
			case "all":
				root = manifest.NamedResource{}
			}
			if scope != "all" {
				configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{root}})
			}
			allChildren := scope != "namespace"
			if l.Options.Session != nil {
				initial, err := prepareSelection(
					t.Context(),
					t,
					l,
					selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
				)
				assert.NoError(t, err)
				_, committed, err := l.commitSelection(t.Context(), t, initial, selection.Execution)
				assert.NoError(t, err)
				assert.Equal(t, committed, true)
			}
			children := make([]store.Child, 0, 3)
			for _, obj := range []manifest.BaseManifest{selected, excluded, required} {
				incoming := store.Child{Obj: obj, Productions: []manifest.Production{{Owner: parent.Named()}}}
				decision := l.Options.Session.Snapshot()
				output, needed := decision.Output(obj.Named()), decision.Reconcile(obj.Named())
				if l.Options.Session != nil {
					proposal, prepareErr := prepareSelection(
						t.Context(),
						t,
						l,
						selection.PrepareRequest{Changes: l.Options.Changes, Generation: []store.Child{incoming}},
					)
					assert.NoError(t, prepareErr)
					output, needed = slices.Contains(
						proposal.Selected,
						obj.Named(),
					), slices.Contains(
						proposal.Needed,
						obj.Named(),
					)
				}
				assert.Equal(t, output, obj == selected || allChildren)
				assert.Equal(t, needed, obj != excluded || allChildren)
				children = append(children, incoming)
			}
			st.RecordEmitted(parent.Named(), &store.Emission{Children: children, Complete: true}, nil)

			decision := l.Options.Session.Snapshot()
			outputExcluded, neededExcluded := decision.Output(excluded.Named()), decision.Reconcile(excluded.Named())
			outputRequired, neededRequired := decision.Output(required.Named()), decision.Reconcile(required.Named())
			if l.Options.Session != nil {
				proposal, err := prepareSelection(
					t.Context(),
					t,
					l,
					selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
				)
				assert.NoError(t, err)
				outputExcluded, neededExcluded = slices.Contains(
					proposal.Selected,
					excluded.Named(),
				), slices.Contains(
					proposal.Needed,
					excluded.Named(),
				)
				outputRequired, neededRequired = slices.Contains(
					proposal.Selected,
					required.Named(),
				), slices.Contains(
					proposal.Needed,
					required.Named(),
				)
			}
			assert.Equal(t, outputExcluded, allChildren)
			assert.Equal(t, neededExcluded, allChildren)
			assert.Equal(t, outputRequired, allChildren)
			assert.Equal(t, neededRequired, true)
			_, _, batch := st.GetEmission(parent.Named())
			defer batch.Finish()
			assert.Equal(t, len(batch.Children), 3)
		})
	}
}

func admissionReferencePair(t *testing.T, keys []string) ([2]*store.Store, [2]*Loader) {
	t.Helper()
	stores := [2]*store.Store{store.New(), store.New()}
	loaders := [2]*Loader{}
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"}
	consumer := testutil.HelmReleaseID("apps", "app")
	if keys == nil {
		input = testutil.KustomizationID("apps", "parent")
	}
	for side, st := range stores {
		root := t.TempDir()
		artifact := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
		st.SetArtifact(manifest.BootstrapSourceID, artifact)
		obj := &manifest.HelmRelease{Name: consumer.Name, Namespace: consumer.Namespace}
		for _, key := range keys {
			obj.ValuesFrom = append(
				obj.ValuesFrom,
				manifest.ValuesReference{Kind: input.Kind, Name: input.Name, ValuesKey: key},
			)
		}
		st.AddObject(obj)
		if keys == nil {
			st.AddObject(
				&manifest.Kustomization{
					Name:      input.Name,
					Namespace: input.Namespace,
					Path:      fmt.Sprintf("payload-%d", side),
				},
			)
		} else {
			st.AddObject(
				&manifest.ConfigMap{
					Name:      input.Name,
					Namespace: input.Namespace,
					Data:      map[string]any{"stable": "same", "also-stable": "same", "changed": strconv.Itoa(side)},
				},
			)
		}
		l := New(st)
		l.SourceRoot = root
		configureSelection(t, l, selection.Request{Changed: true})

		l.publishSource(manifest.BootstrapSourceID, &admissionSource{initial: artifact})
		l.frozen, l.sideKnown, l.side = true, true, manifest.TreeSide(side)
		l.Index.Records[input] = manifest.DiscoveryRecord{File: "input.yaml"}
		l.Index.Records[consumer] = manifest.DiscoveryRecord{File: "consumer.yaml"}
		if keys == nil {
			l.Index.Records[consumer] = manifest.DiscoveryRecord{
				Productions: []manifest.Production{{Owner: input, Source: manifest.BootstrapSourceID}},
			}
		}
		st.SetDiscoveryIndex(l.Index)
		if keys == nil {
			var next store.Emission
			reads := new(store.ReadSet)
			reads.Init(st, input, nil)
			reads.GetDeclaration(input)
			reads.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
			reads.Bind(&next)
			next.Children, next.Complete = []store.Child{{Obj: obj}}, true
			st.RecordEmitted(input, &next, nil)
		}
		loaders[side] = l
	}
	return stores, loaders
}
