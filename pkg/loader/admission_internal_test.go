package loader

import (
	"fmt"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestAdmission_HistoryProductionAllocations(t *testing.T) {
	for _, matching := range []bool{false, true} {
		t.Run(strconv.FormatBool(matching), func(t *testing.T) {
			l := New(store.New())
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
			owner := testutil.KustomizationID("apps", "owner")
			other := testutil.KustomizationID("apps", "other")
			want := []manifest.Production{{Owner: owner, Authored: id, File: "apps/input.yaml"}}
			l.Index.Records[id] = manifest.DiscoveryRecord{Productions: want}
			c := admissionClosure{
				loader: l, objects: l.Store,
				accepted: map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{other: {}},
				replace:  map[manifest.NamedResource]bool{other: true},
			}
			if matching {
				c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{
					owner: {id: {Productions: want}},
				}
				c.replace = map[manifest.NamedResource]bool{owner: true}
			}
			assert.Diff(t, c.productions(id), want)
			{
				allocations := testing.AllocsPerRun(100, func() { c.productions(id) })
				assert.FatalIf(
					t,
					allocations != 0,
					"accepted history allocates %g times per repeated production lookup",
					allocations,
				)
			}
			if !matching {
				{
					allocations := testing.AllocsPerRun(100, func() {
						c.historyProductions = nil
						c.productions(id)
					})
					assert.FatalIf(
						t,
						allocations != 0,
						"unrelated history allocates %g times per cold production lookup",
						allocations,
					)
				}
			}
			assert.Diff(t, l.Index.Records[id].Productions, want)
		})
	}
}

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
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{app}}
	l.Options.Filter = change.NewFilter()
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
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
	assert.Equal(t, l.Options.Filter.ShouldReconcile(app), false)
	added, retry := l.Options.Filter.AddEmitted(proposal.Needed, proposal.Selected, proposal.Dirty, proposal.Revision)
	assert.Equal(t, retry, false)
	assert.Equal(t, len(added), 3)
	assert.Equal(t, l.Options.Filter.ShouldReconcile(app), true)
	unrelated := app
	unrelated.Name = "unrelated"
	assert.Equal(t, l.Options.Filter.ShouldReconcile(unrelated), false)
	retried, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	names = nil
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
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Changes = change.NewSet(nil)
	l.Options.Filter = change.NewFilter()
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(proposal.Needed), 0)
	assert.Equal(t, len(proposal.Objects), 0)
}

func TestAdmission_OwnWipedInput(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"owner.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: app, namespace: apps}\nspec:\n  interval: 1m\n  path: ./app\n  postBuild:\n    substituteFrom: [{kind: Secret, name: vars}]\n",
			),
		},
		"app/kustomization.yaml": {Data: []byte("resources: [secret.yaml]\n")},
		"app/secret.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: Secret\nmetadata: {name: vars, namespace: apps}\nstringData: {KEY: 'ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]'}\nsops: {mac: encrypted}\n",
			),
		},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{
		Roots: []manifest.NamedResource{
			{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "app"},
		},
	}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	found := false
	for _, child := range proposal.Objects {
		if secret, ok := child.Obj.(*manifest.Secret); ok {
			found = true
			assert.Equal(t, secret.HasPlaceholder(), true)
		}
	}
	assert.Equal(t, found, true)
	assert.Equal(t, len(st.Objects()), 0)
}

func TestAdmission_UncertainProducerKeepsExactOwner(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"app.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: app, namespace: apps}\nspec:\n  interval: 1m\n  path: ./consumer\n  postBuild:\n    substituteFrom: [{kind: Secret, name: vars}]\n",
			),
		},
		"owner.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner, namespace: apps}\nspec: {interval: 1m, path: ./producer, targetNamespace: apps}\n",
			),
		},
		"unrelated.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: unrelated, namespace: apps}\nspec: {interval: 1m, path: ./other, targetNamespace: apps}\n",
			),
		},
		"producer/kustomization.yaml": {Data: []byte("resources: [external.yaml]\n")},
		"producer/external.yaml": {
			Data: []byte(
				"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: '${CLUSTER_NAME:=biohazard}-vars', namespace: apps}\nspec: {target: {name: vars}}\n",
			),
		},
		"consumer/kustomization.yaml": {Data: []byte("resources: []\n")},
		"other/kustomization.yaml":    {Data: []byte("resources: []\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{
		Roots: []manifest.NamedResource{
			{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "app"},
		},
	}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
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
		assert.FatalIf(
			t,
			manifest.HasEnvsubstReference(id.Name) || id.Name == "",
			"uncertain producer entered concrete index: %s",
			id,
		)
	}
}

func TestAdmission_AncestorRetainsSupportRole(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"parent.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: parent, namespace: apps}\nspec: {interval: 1m, path: ./apps}\n",
			),
		},
		"apps/kustomization.yaml": {Data: []byte("resources: [app.yaml]\n")},
		"apps/app.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: app, namespace: apps}\nspec: {interval: 1m, path: ./apps/child}\n",
			),
		},
		"apps/child/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"apps/child/cm.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {key: changed}\n",
			),
		},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet([]string{"apps/child/cm.yaml"})
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
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

func TestAdmission_UnrelatedSourcePublicationPreservesProof(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"consumer.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec:\n  path: ./app\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: absent, optional: true}]\n",
			),
		},
		"open.yaml": {
			Data: []byte(
				"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: open, namespace: apps}\nspec: {resources: []}\n",
			),
		},
		"index.yaml": {
			Data: []byte(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\nmetadata: {name: index, namespace: apps}\nspec: {url: https://fixture.invalid/charts}\n",
			),
		},
	}})
	l := New(st)
	l.SourceRoot = root
	consumer := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "consumer",
	}
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{consumer}}
	l.Options.Filter = change.NewFilter()
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	requester := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmRepository,
		Namespace: "apps",
		Name:      "index",
	}
	var reads store.ReadSet
	reads.Init(st, requester, nil)
	defer reads.Retire()
	reads.RequireData(consumer)
	proposal, err := l.Prepare(t.Context(), store.Child{}, &reads, nil)
	assert.NoError(t, err)
	producer := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      "ResourceSet",
		Namespace: "apps",
		Name:      "open",
	}
	assert.Equal(t, slices.Contains(proposal.Needed, producer), false)
	assert.Equal(t, reads.Valid(), true)
	st.SetArtifact(requester, &store.SourceArtifact{LocalPath: root + "/acquired", FS: fstest.MapFS{}})
	{
		assert.FatalIf(
			t,
			!reads.Valid(),
			"%s",
			"an unrelated source publication invalidated the consumer's materializer proof",
		)
	}
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
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAdmissionMultipleReferencesCase(t, &tc)
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
		"consumer.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec:\n  path: ./app\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: input}]\n",
			),
		},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{
		Roots: []manifest.NamedResource{
			{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Name: "consumer"},
		},
	}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	for _, child := range proposal.Objects {
		st.AddObject(child.Obj)
	}
	input := manifest.NamedResource{Kind: "ConfigMap", Namespace: "apps", Name: "input"}
	var reads store.ReadSet
	reads.Init(st, input, nil)
	defer reads.Retire()
	_, err = l.Prepare(t.Context(), store.Child{Obj: st.GetObject(input)}, &reads, nil)
	assert.NoError(t, err)
	assert.Equal(t, reads.Valid(), true)
	consumer := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "consumer",
	}
	st.DeleteObject(consumer)
	{
		assert.FatalIf(t, !reads.Valid(), "%s", "changing a consumer invalidated its unchanged input")
	}
}

func TestAdmission_IncomingChildPublicationPreservesProof(t *testing.T) {
	t.Parallel()
	st := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	st.AddObject(parent)
	l := New(st)
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{child.Named()}}
	var reads store.ReadSet
	reads.Init(st, parent.Named(), nil)
	defer reads.Retire()
	_, _, next := reads.GetEmission(parent.Named())
	defer next.Finish()
	incoming := store.Child{Obj: child, Productions: []manifest.Production{{Owner: parent.Named()}}}
	proposal, err := l.Prepare(t.Context(), incoming, &reads, nil)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Selected, child.Named()), true)
	assert.Equal(t, reads.Valid(), true)
	next.Children = []store.Child{incoming}
	next.Complete = true
	reads.Bind(&next)
	st.RecordEmitted(parent.Named(), &next, nil)
	{
		assert.FatalIf(
			t,
			st.GetObject(child.Named()) == nil || !reads.Valid(),
			"%s",
			"the selected incoming child invalidated its own publication",
		)
	}
}

func TestAdmission_EmittedNamespaceChildren(t *testing.T) {
	t.Parallel()
	for _, scope := range []string{"namespace", "named", "all"} {
		t.Run(scope, func(t *testing.T) {
			t.Parallel()
			testAdmissionEmittedNamespaceChildrenCase(t, &scope)
		},
		)
	}
	for _, tc := range []struct {
		name      string
		inherited bool
		stored    bool
	}{
		{name: "incoming inherited", inherited: true},
		{name: "incoming changed production"},
		{name: "stored inherited", inherited: true, stored: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAdmissionEmittedNamespaceChildrenCase2(t, &tc)
		},
		)
	}
}

func testAdmissionMultipleReferencesCase(t *testing.T, testCaseInput *struct {
	name    string
	keys    []string
	changed bool
	direct  bool
},
) {
	t.Helper()

	stores := [2]*store.Store{store.New(), store.New()}
	loaders := [2]*Loader{}
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"}
	consumer := testutil.HelmReleaseID("apps", "app")
	if testCaseInput.keys == nil {
		input = testutil.KustomizationID("apps", "parent")
	}
	for side, st := range stores {
		root := t.TempDir()
		artifact := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
		st.SetArtifact(manifest.BootstrapSourceID, artifact)
		obj := &manifest.HelmRelease{Name: consumer.Name, Namespace: consumer.Namespace}
		for _, key := range testCaseInput.keys {
			obj.ValuesFrom = append(
				obj.ValuesFrom,
				manifest.ValuesReference{Kind: input.Kind, Name: input.Name, ValuesKey: key},
			)
		}
		st.AddObject(obj)
		if testCaseInput.keys == nil {
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
					Data: map[string]any{
						"stable":      "same",
						"also-stable": "same",
						"changed":     strconv.Itoa(side),
					},
				},
			)
		}
		l := New(st)
		l.SourceRoot = root
		l.Options.Selection = manifest.Selection{Changed: true}
		l.Options.Filter = change.NewFilter()
		l.frozen, l.sideKnown, l.side = true, true, manifest.TreeSide(side)
		l.bindings.Store(
			&map[manifest.NamedResource]admissionSource{manifest.BootstrapSourceID: {initial: artifact}},
		)
		l.Index.Records[input] = manifest.DiscoveryRecord{File: "input.yaml"}
		l.Index.Records[consumer] = manifest.DiscoveryRecord{File: "consumer.yaml"}
		if testCaseInput.keys == nil {
			l.Index.Records[consumer] = manifest.DiscoveryRecord{
				Productions: []manifest.Production{{Owner: input, Source: manifest.BootstrapSourceID}},
			}
		}
		st.SetDiscoveryIndex(l.Index)
		loaders[side] = l
	}
	paths := []string{"input.yaml"}
	if testCaseInput.direct {
		paths = append(paths, "consumer.yaml")
		obj := stores[1].GetObject(consumer).(*manifest.HelmRelease).Clone()
		obj.HelmReleaseSpec.ReleaseName = "another-cause"
		stores[1].AddObject(obj)
	}
	changes, err := change.Qualify(t.Context(), change.NewSet(paths), stores[0], stores[1])
	assert.NoError(t, err)
	changes, err = changes.WithDeclarations(
		t.Context(),
		stores[0],
		stores[1],
		[2]map[manifest.NamedResource]store.Child{},
	)
	assert.NoError(t, err)
	for side, l := range loaders {
		l.Options.Changes = changes
		c := admissionClosure{loader: l, objects: stores[side]}
		obj := stores[side].GetObject(consumer)
		assert.Equal(t, c.object(input) != nil, true)
		for ref := range manifest.References(obj, manifest.ReferenceRelations) {
			changed, known := c.outputChanged(consumer, new(ref), obj)
			assert.Equal(t, known, ref.Key != "unknown")
			if known {
				assert.Equal(t, changed, ref.Key == "changed")
			}
		}
		proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
		assert.NoError(t, err)
		t.Run(fmt.Sprintf("side-%d", side), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, slices.Contains(proposal.Selected, consumer), testCaseInput.changed)
			assert.Equal(t, slices.Contains(proposal.Dirty, consumer), testCaseInput.changed)
		})
	}
}

func testAdmissionEmittedNamespaceChildrenCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	st := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	selected := &manifest.Kustomization{Name: "selected", Namespace: "apps"}
	selected.SourceRef.Kind, selected.SourceRef.Name, selected.SourceRef.Namespace = manifest.KindOCIRepository, "required", "monitoring"
	excluded := &manifest.Kustomization{Name: "excluded", Namespace: "monitoring"}
	excluded.Path, excluded.TargetNamespace = "excluded", "monitoring"
	required := &manifest.OCIRepository{Name: "required", Namespace: "monitoring"}
	st.AddObjects([]manifest.BaseManifest{parent, selected})
	l := New(st)
	l.Options.Filter = change.NewFilter()
	root := parent.Named()
	switch *testCaseInput {
	case "namespace":
		root.Name = ""
	case "all":
		root = manifest.NamedResource{}
	}
	if (*testCaseInput) != "all" {
		l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{root}}
	}
	allChildren := (*testCaseInput) != "namespace"
	initial, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	_, retry := l.Options.Filter.AddEmitted(initial.Needed, initial.Selected, initial.Dirty, initial.Revision)
	assert.Equal(t, retry, false)
	children := make([]store.Child, 0, 3)
	for _, obj := range []manifest.BaseManifest{selected, excluded, required} {
		incoming := store.Child{Obj: obj, Productions: []manifest.Production{{Owner: parent.Named()}}}
		proposal, prepareErr := l.Prepare(t.Context(), incoming, nil, nil)
		assert.NoError(t, prepareErr)
		assert.Equal(t, slices.Contains(proposal.Selected, obj.Named()), obj == selected || allChildren)
		assert.Equal(t, slices.Contains(proposal.Needed, obj.Named()), obj != excluded || allChildren)
		children = append(children, incoming)
	}
	st.RecordEmitted(parent.Named(), &store.Emission{Children: children, Complete: true}, nil)
	l.Options.Filter = change.NewFilter()
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Selected, excluded.Named()), allChildren)
	assert.Equal(t, slices.Contains(proposal.Needed, excluded.Named()), allChildren)
	assert.Equal(t, slices.Contains(proposal.Selected, required.Named()), allChildren)
	assert.Equal(t, slices.Contains(proposal.Needed, required.Named()), true)
	_, _, batch := st.GetEmission(parent.Named())
	defer batch.Finish()
	assert.Equal(t, len(batch.Children), 3)
}

func testAdmissionEmittedNamespaceChildrenCase2(
	t *testing.T,
	testCaseInput *struct {
		name      string
		inherited bool
		stored    bool
	},
) {
	t.Helper()

	st := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	stable := &manifest.Kustomization{Name: "stable", Namespace: "apps"}
	changed := &manifest.Kustomization{Name: "changed", Namespace: "apps", Path: "changed"}
	st.AddObject(parent)
	l := New(st)
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet([]string{"production.yaml", "changed.yaml"})
	root := parent.Named()
	root.Name = ""
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{root}, Changed: true}
	var selected []manifest.NamedResource
	if testCaseInput.inherited {
		selected = append(selected, parent.Named())
	}
	_, retry := l.Options.Filter.AddEmitted([]manifest.NamedResource{parent.Named()}, selected, selected, 0)
	assert.Equal(t, retry, false)
	children := make([]store.Child, 0, 2)
	for _, obj := range []*manifest.Kustomization{stable, changed} {
		declaration := obj.Clone()
		declaration.Path = ""
		production := manifest.Production{
			Owner:       parent.Named(),
			Declaration: obj.Named(),
			Source:      manifest.BootstrapSourceID,
			File:        "production.yaml",
		}
		l.Index.Records[obj.Named()] = manifest.DiscoveryRecord{
			File: obj.Name + ".yaml", Builder: "kustomization.yaml", Declaration: declaration,
			Productions: []manifest.Production{production},
		}
		children = append(children, store.Child{Obj: obj, Productions: []manifest.Production{production}})
	}
	if testCaseInput.stored {
		st.RecordEmitted(parent.Named(), &store.Emission{Children: children, Complete: true}, nil)
		proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, slices.Contains(proposal.Selected, stable.Named()), false)
		assert.Equal(t, slices.Contains(proposal.Dirty, stable.Named()), false)
		assert.Equal(t, slices.Contains(proposal.Selected, changed.Named()), true)
		assert.Equal(t, slices.Contains(proposal.Dirty, changed.Named()), true)
		return
	}
	for _, child := range children {
		proposal, err := l.Prepare(t.Context(), child, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, slices.Contains(proposal.Selected, child.Obj.Named()), child.Obj == changed)
		assert.Equal(t, slices.Contains(proposal.Dirty, child.Obj.Named()), child.Obj == changed)
	}
}

func TestAdmission_HistoryProductionRefresh(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{
			LocalPath: root,
			FS: fstest.MapFS{
				"owner.yaml": {Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./payload\n"))},
			},
		},
	)
	l := New(st)
	l.SourceRoot = root
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Changes = change.NewSet([]string{"owner.yaml"})
	l.Options.Filter = change.NewFilter()
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	owner := testutil.KustomizationID("apps", "owner")
	current := manifest.Production{Owner: owner, Authored: id, File: "current.yaml"}
	accepted := manifest.Production{Owner: owner, Authored: id, File: "accepted.yaml"}
	l.Index.Records[id] = manifest.DiscoveryRecord{Productions: []manifest.Production{current}}
	c := admissionClosure{
		loader:   l,
		objects:  st,
		proposal: &Admission{},
		accepted: map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{
			owner: {id: {Productions: []manifest.Production{accepted}}},
		},
		replace: map[manifest.NamedResource]bool{owner: true},
	}
	assert.Diff(t, c.productions(id), []manifest.Production{accepted})
	assert.NoError(t, c.start(t.Context(), func(materializerDemand, manifest.NamedResource) bool { return false }))
	assert.Equal(t, c.states[owner].generation > 1, true)
	assert.Equal(t, c.replace[owner], false)
	assert.Diff(t, c.productions(id), []manifest.Production{current, accepted})
}
