package loader

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
	app := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "app"}
	l.SourceRoot = root
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{app}}
	l.Options.Filter = change.NewFilter()
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	var names []string
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
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{"object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data}\n")}}})
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
		"owner.yaml":             {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: app, namespace: apps}\nspec:\n  interval: 1m\n  path: ./app\n  postBuild:\n    substituteFrom: [{kind: Secret, name: vars}]\n")},
		"app/kustomization.yaml": {Data: []byte("resources: [secret.yaml]\n")},
		"app/secret.yaml":        {Data: []byte("apiVersion: v1\nkind: Secret\nmetadata: {name: vars, namespace: apps}\nstringData: {KEY: 'ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]'}\nsops: {mac: encrypted}\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "app"}}}
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
		"app.yaml":                    {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: app, namespace: apps}\nspec:\n  interval: 1m\n  path: ./consumer\n  postBuild:\n    substituteFrom: [{kind: Secret, name: vars}]\n")},
		"owner.yaml":                  {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner, namespace: apps}\nspec: {interval: 1m, path: ./producer, targetNamespace: apps}\n")},
		"unrelated.yaml":              {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: unrelated, namespace: apps}\nspec: {interval: 1m, path: ./other, targetNamespace: apps}\n")},
		"producer/kustomization.yaml": {Data: []byte("resources: [external.yaml]\n")},
		"producer/external.yaml":      {Data: []byte("apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: '${CLUSTER_NAME:=biohazard}-vars', namespace: apps}\nspec: {target: {name: vars}}\n")},
		"consumer/kustomization.yaml": {Data: []byte("resources: []\n")},
		"other/kustomization.yaml":    {Data: []byte("resources: []\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "app"}}}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	owner := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "owner"}
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
		"parent.yaml":                   {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: parent, namespace: apps}\nspec: {interval: 1m, path: ./apps}\n")},
		"apps/kustomization.yaml":       {Data: []byte("resources: [app.yaml]\n")},
		"apps/app.yaml":                 {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: app, namespace: apps}\nspec: {interval: 1m, path: ./apps/child}\n")},
		"apps/child/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"apps/child/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {key: changed}\n")},
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
	parent := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "parent"}
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
		"consumer.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec:\n  path: ./app\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: absent, optional: true}]\n")},
		"open.yaml":     {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: open, namespace: apps}\nspec: {resources: []}\n")},
		"index.yaml":    {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\nmetadata: {name: index, namespace: apps}\nspec: {url: https://fixture.invalid/charts}\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	consumer := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{consumer}}
	l.Options.Filter = change.NewFilter()
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	requester := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "apps", Name: "index"}
	var reads store.ReadSet
	reads.Init(st, requester, nil)
	defer reads.Retire()
	reads.RequireData(consumer)
	proposal, err := l.Prepare(t.Context(), store.Child{}, &reads, nil)
	assert.NoError(t, err)
	producer := manifest.NamedResource{Group: "fluxcd.controlplane.io", Kind: "ResourceSet", Namespace: "apps", Name: "open"}
	assert.Equal(t, slices.Contains(proposal.Needed, producer), false)
	assert.Equal(t, reads.Valid(), true)
	st.SetArtifact(requester, &store.SourceArtifact{LocalPath: root + "/acquired", FS: fstest.MapFS{}})
	if !reads.Valid() {
		t.Fatal("an unrelated source publication invalidated the consumer's materializer proof")
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
			stores := [2]*store.Store{store.New(), store.New()}
			loaders := [2]*Loader{}
			input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"}
			consumer := testutil.HelmReleaseID("apps", "app")
			if tc.keys == nil {
				input = testutil.KustomizationID("apps", "parent")
			}
			for side, st := range stores {
				root := t.TempDir()
				artifact := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
				st.SetArtifact(manifest.BootstrapSourceID, artifact)
				obj := &manifest.HelmRelease{Name: consumer.Name, Namespace: consumer.Namespace}
				for _, key := range tc.keys {
					obj.ValuesFrom = append(obj.ValuesFrom, manifest.ValuesReference{Kind: input.Kind, Name: input.Name, ValuesKey: key})
				}
				st.AddObject(obj)
				if tc.keys == nil {
					st.AddObject(&manifest.Kustomization{Name: input.Name, Namespace: input.Namespace, Path: fmt.Sprintf("payload-%d", side)})
				} else {
					st.AddObject(&manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace, Data: map[string]any{"stable": "same", "also-stable": "same", "changed": fmt.Sprint(side)}})
				}
				l := New(st)
				l.SourceRoot = root
				l.Options.Selection = manifest.Selection{Changed: true}
				l.Options.Filter = change.NewFilter()
				l.frozen, l.sideKnown, l.side = true, true, manifest.TreeSide(side)
				l.bindings = map[manifest.NamedResource]*store.SourceArtifact{manifest.BootstrapSourceID: artifact}
				l.Index.Records[input] = manifest.DiscoveryRecord{File: "input.yaml"}
				l.Index.Records[consumer] = manifest.DiscoveryRecord{File: "consumer.yaml"}
				if tc.keys == nil {
					l.Index.Records[consumer] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: input, Source: manifest.BootstrapSourceID}}}
				}
				st.SetDiscoveryIndex(l.Index)
				loaders[side] = l
			}
			paths := []string{"input.yaml"}
			if tc.direct {
				paths = append(paths, "consumer.yaml")
				obj := stores[1].GetObject(consumer).(*manifest.HelmRelease).Clone()
				obj.HelmReleaseSpec.ReleaseName = "another-cause"
				stores[1].AddObject(obj)
			}
			changes, err := change.Qualify(t.Context(), change.NewSet(paths), stores[0], stores[1])
			assert.NoError(t, err)
			changes, err = changes.WithDeclarations(t.Context(), stores[0], stores[1], [2]map[manifest.NamedResource]store.Child{})
			assert.NoError(t, err)
			for side, l := range loaders {
				l.Options.Changes = changes
				c := admissionClosure{loader: l, objects: stores[side]}
				obj := stores[side].GetObject(consumer)
				assert.Equal(t, c.object(input) != nil, true)
				for ref := range manifest.References(obj, manifest.ReferenceRelations) {
					changed, known := c.outputChanged(consumer, ref, obj)
					assert.Equal(t, known, ref.Key != "unknown")
					if known {
						assert.Equal(t, changed, ref.Key == "changed")
					}
				}
				proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
				assert.NoError(t, err)
				t.Run(fmt.Sprintf("side-%d", side), func(t *testing.T) {
					t.Parallel()
					assert.Equal(t, slices.Contains(proposal.Selected, consumer), tc.changed)
					assert.Equal(t, slices.Contains(proposal.Dirty, consumer), tc.changed)
				})
			}
		})
	}
}

func TestAdmission_UnchangedInputDoesNotCaptureConsumers(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"input.yaml":    {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\ndata: {VALUE: stable}\n")},
		"consumer.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec:\n  path: ./app\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: input}]\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Name: "consumer"}}}
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
	consumer := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
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
	if st.GetObject(child.Named()) == nil || !reads.Valid() {
		t.Fatal("the selected incoming child invalidated its own publication")
	}
}

func TestAdmission_ReadonlyDeclarationsAfterConstruction(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"consumer.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec: {path: ./app}\n")},
		"readonly.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\nmetadata: {name: readonly, namespace: apps}\nspec: {url: https://fixture.invalid/charts}\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Changes = change.NewSet([]string{"consumer.yaml"})
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	readonly := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "apps", Name: "readonly"}
	for _, freeze := range []bool{false, true} {
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, freeze))
		proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
		assert.NoError(t, err)
		present := slices.ContainsFunc(proposal.Objects, func(child store.Child) bool { return child.Obj.Named() == readonly })
		assert.Equal(t, present, false)
		assert.Equal(t, l.CatalogObject(readonly).Named(), readonly)
		assert.Equal(t, st.GetObject(readonly) == nil, true)
		assert.Equal(t, slices.Contains(proposal.Needed, readonly), false)
	}
}

func TestAdmission_OrderingEmittedPathProof(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                                       string
		foreign, incompatible, independent, visited, completeEmpty bool
		want                                                       bool
	}{
		{name: "compatible_namespace"},
		{name: "foreign_source", foreign: true},
		{name: "incompatible_namespace", incompatible: true},
		{name: "independent_parent", independent: true},
		{name: "visited_path", visited: true},
		{name: "complete_empty", completeEmpty: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"entry/consumer.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec: {path: ./consumer, dependsOn: [{name: target, namespace: infra}]}\n")},
			}}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			source := &manifest.GitRepository{Name: "cluster", Namespace: "sources"}
			st.AddObject(source)
			st.SetArtifact(source.Named(), input)
			l := New(st)
			l.SourceRoot = root
			consumer := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
			l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{consumer}}
			l.Options.Filter = change.NewFilter()
			_, err := l.Load(t.Context(), filepath.Join(root, "entry"))
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			parent := &manifest.Kustomization{Name: "infra-parent", Namespace: "flux-system"}
			parent.Path, parent.TargetNamespace = "unvisited", "infra"
			parent.SourceRef.Kind, parent.SourceRef.Name, parent.SourceRef.Namespace = source.Named().Kind, source.Name, source.Namespace
			if tc.foreign {
				st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
			}
			if tc.incompatible {
				parent.TargetNamespace = "other"
			}
			if tc.visited {
				l.documents.cache[filepath.Join(root, parent.Path)] = document{traversed: true}
			}
			producer := &manifest.ResourceSet{Name: "namespaces", Namespace: "flux-system"}
			st.AddObject(producer)
			var reads store.ReadSet
			reads.Init(st, producer.Named(), nil)
			defer reads.Close()
			if tc.independent {
				st.AddObject(parent)
			} else {
				_, _, next := reads.GetEmission(producer.Named())
				defer next.Finish()
				next.Children, next.Complete = []store.Child{{Obj: parent}}, true
				reads.Bind(&next)
				st.RecordEmitted(producer.Named(), &next, nil)
				assert.Equal(t, st.GetObject(parent.Named()), manifest.BaseManifest(parent))
			}
			if tc.completeEmpty {
				var emptyReads store.ReadSet
				emptyReads.Init(st, parent.Named(), nil)
				defer emptyReads.Close()
				_, _, empty := emptyReads.GetEmission(parent.Named())
				defer empty.Finish()
				empty.Complete = true
				emptyReads.Bind(&empty)
				st.RecordEmitted(parent.Named(), &empty, nil)
				st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady})
			}
			proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, parent.Named()), tc.want)
			assert.Equal(t, slices.Contains(proposal.Selected, parent.Named()), false)
			assert.Equal(t, slices.Contains(proposal.Dirty, parent.Named()), false)
		})
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
			stores := [2]*store.Store{store.New(), store.New()}
			inputs := [2]*store.SourceArtifact{}
			var prepared [2]map[manifest.NamedResource]store.Child
			id := (&manifest.Kustomization{Name: "child", Namespace: "apps"}).Named()
			owner := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			for side, st := range stores {
				inputs[side] = &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}}
				st.SetArtifact(manifest.BootstrapSourceID, inputs[side])
				st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Productions: []manifest.Production{{Owner: owner.Named(), Source: manifest.BootstrapSourceID}}}}})
				obj := &manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "./payload"}
				doc := map[string]any{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": id.Kind, "metadata": map[string]any{"name": id.Name, "namespace": id.Namespace}, "spec": map[string]any{"path": obj.Path, "unknown": "stable"}}
				prepared[side] = map[manifest.NamedResource]store.Child{id: {Obj: obj, Doc: doc}}
			}
			changes, err := change.Qualify(t.Context(), change.NewSet([]string{"declaration.yaml"}), stores[0], stores[1])
			assert.NoError(t, err)
			changes, err = changes.WithDeclarations(t.Context(), stores[0], stores[1], prepared)
			assert.NoError(t, err)
			l := New(stores[0])
			l.SourceRoot = inputs[0].LocalPath
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Changes, l.Options.Filter = changes, change.NewFilter()
			l.frozen, l.sideKnown, l.side = true, true, manifest.TreeBase
			l.bindings = map[manifest.NamedResource]*store.SourceArtifact{manifest.BootstrapSourceID: inputs[0]}
			l.candidates = prepared[0]
			l.Index.Records[id] = manifest.DiscoveryRecord{Builder: "kustomization.yaml", Declaration: prepared[0][id].Obj, Inputs: []string{"declaration.yaml"}, Productions: []manifest.Production{{Owner: owner.Named(), Source: manifest.BootstrapSourceID}}}
			_, retry := l.Options.Filter.AddEmitted([]manifest.NamedResource{id}, []manifest.NamedResource{id}, nil, 0)
			assert.Equal(t, retry, false)
			child := prepared[0][id]
			child.Obj = child.Obj.(*manifest.Kustomization).Clone()
			child.Doc = manifest.DeepCopyMap(child.Doc)
			if tc.changed {
				child.Doc["spec"].(map[string]any)["unknown"] = "changed"
			}
			if tc.stored {
				stores[0].AddObject(owner)
				stores[0].RecordEmitted(owner.Named(), &store.Emission{Children: []store.Child{child}, Complete: true}, nil)
				assert.Equal(t, stores[0].GetDeclaration(id) == child.Obj, true)
			}
			if tc.absent {
				child.Doc = nil
			}
			proposal, err := l.Prepare(t.Context(), child, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Dirty, id), tc.changed || tc.absent && !tc.stored)
		})
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
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"child.yaml":                  {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: child, namespace: apps}\nspec: {path: ./workload}\n")},
				"owner.yaml":                  {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: owner, namespace: apps}\nspec:\n  resourcesTemplate: |\n    apiVersion: kustomize.toolkit.fluxcd.io/v1\n    kind: Kustomization\n    metadata: {name: child, namespace: apps}\n    spec: {path: ./workload}\n")},
				"workload/kustomization.yaml": {Data: []byte("resources: []\n")},
			}})
			l := New(st)
			l.SourceRoot = root
			l.Options.Selection = manifest.Selection{Changed: true}
			changed := "workload/kustomization.yaml"
			if tc.rawEdit {
				changed = "child.yaml"
			}
			l.Options.Changes, l.Options.Filter = change.NewSet([]string{changed}), change.NewFilter()
			_, err := l.Load(t.Context(), root)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			child := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "child"}
			producer := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "owner"}
			l.roots[child] = tc.independent
			if tc.knownOwner {
				record := l.Index.Records[child]
				record.Parent = manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "known"}
				l.Index.Records[child] = record
			}
			if tc.knownBuilder {
				record := l.Index.Records[child]
				record.BuilderOwner = manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
				l.Index.Records[child] = record
			}
			proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, producer), tc.want)
			assert.Equal(t, slices.Contains(proposal.Selected, producer), false)
			assert.Equal(t, slices.Contains(proposal.Dirty, producer), false)
			assert.Equal(t, st.GetObject(child) == nil, true)
			if tc.independent && tc.want {
				assert.Equal(t, slices.ContainsFunc(proposal.Objects, func(o store.Child) bool { return o.Obj.Named() == child }), false)
				st.AddObject(&manifest.ResourceSet{Name: producer.Name, Namespace: producer.Namespace})
				st.RecordEmitted(producer, &store.Emission{Complete: true}, nil)
				st.UpdateStatus(producer, store.StatusInfo{Status: store.StatusReady})
				proposal, err = l.Prepare(t.Context(), store.Child{}, nil, nil)
				assert.NoError(t, err)
				assert.Equal(t, slices.ContainsFunc(proposal.Objects, func(o store.Child) bool { return o.Obj.Named() == child }), true)
			}
		})
	}
}

func TestAdmission_GeneratedInputRetainsUnchangedConsumers(t *testing.T) {
	t.Parallel()
	for _, file := range []string{"apps/changed.yaml", "apps/kustomization.yaml", "apps/vars.yaml"} {
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"parent.yaml":                {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: parent, namespace: apps}\nspec:\n  path: ./apps\n  postBuild: {substituteFrom: [{kind: Secret, name: vars}]}\n")},
				"apps/kustomization.yaml":    {Data: []byte("resources: [changed.yaml, stable.yaml, vars.yaml]\n")},
				"apps/changed.yaml":          {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: changed, namespace: apps}\nspec: {path: ./payload}\n")},
				"apps/stable.yaml":           {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: stable, namespace: apps}\nspec:\n  path: ./payload\n  postBuild: {substituteFrom: [{kind: Secret, name: vars}]}\n")},
				"apps/vars.yaml":             {Data: []byte("apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: declaration, namespace: apps}\nspec:\n  target: {name: vars}\n  data: [{secretKey: VALUE, remoteRef: {key: fixture}}]\n")},
				"payload/kustomization.yaml": {Data: []byte("resources: []\n")},
			}})
			l := New(st)
			l.SourceRoot = root
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Changes, l.Options.Filter = change.NewSet([]string{file}), change.NewFilter()
			_, err := l.Load(t.Context(), root)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
			assert.NoError(t, err)
			_, retry := l.Options.Filter.AddEmitted(proposal.Needed, proposal.Selected, proposal.Dirty, proposal.Revision)
			assert.Equal(t, retry, false)
			parent := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Name: "parent", Namespace: "apps"}
			input := manifest.NamedResource{Kind: manifest.KindSecret, Name: "vars", Namespace: "apps"}
			declaration := manifest.NamedResource{Group: "external-secrets.io", Kind: "ExternalSecret", Name: "declaration", Namespace: "apps"}
			proposal, err = l.Prepare(t.Context(), store.Child{Obj: manifest.NewSecret(input, nil, map[string]any{"VALUE": "current"}), Productions: []manifest.Production{{Owner: parent, Declaration: declaration, Authored: declaration, Source: manifest.BootstrapSourceID, File: "apps/vars.yaml"}}}, nil, nil)
			assert.NoError(t, err)
			want := file != "apps/changed.yaml"
			assert.Equal(t, slices.Contains(proposal.Dirty, input), want)
			stable := parent
			stable.Name = "stable"
			assert.Equal(t, slices.Contains(proposal.Selected, stable), want)
			assert.Equal(t, slices.Contains(proposal.Dirty, stable), want)
		})
	}
}

func TestAdmission_ClosedRootFrontier(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"published", "unpublished", "incoming"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"provider.yaml":               {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: provider, namespace: apps}\nspec:\n  path: ./provider\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n")},
				"consumer.yaml":               {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec:\n  path: ./consumer\n  dependsOn: [{name: provider}]\n")},
				"settings.yaml":               {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: old}\n")},
				"new-settings.yaml":           {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: new-settings, namespace: apps}\ndata: {VALUE: current}\n")},
				"provider/kustomization.yaml": {Data: []byte("resources: []\n")},
				"consumer/kustomization.yaml": {Data: []byte("resources: []\n")},
			}})
			l := New(st)
			l.SourceRoot = root
			_, err := l.Load(t.Context(), root)
			assert.NoError(t, err)
			provider := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Name: "provider", Namespace: "apps"}
			consumer := provider
			consumer.Name = "consumer"
			settings := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "apps"}
			providerChild := l.IndexedObject(provider, l.Index.Records[provider])
			consumerChild := l.IndexedObject(consumer, l.Index.Records[consumer])
			if providerChild.Obj == nil || consumerChild.Obj == nil {
				t.Fatal("fixture declarations are unavailable")
			}
			l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{consumer}}
			l.Options.Filter = change.NewFilter()
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			l.roots = map[manifest.NamedResource]bool{provider: true}
			st.AddObjects([]manifest.BaseManifest{providerChild.Obj, consumerChild.Obj, manifest.NewConfigMap(settings, map[string]any{"VALUE": "old"})})
			fresh := settings
			fresh.Name = "new-settings"
			st.AddObject(manifest.NewConfigMap(fresh, map[string]any{"VALUE": "current"}))
			_, _, _, revision := l.Options.Filter.Membership(provider)
			_, retry := l.Options.Filter.AddEmitted([]manifest.NamedResource{provider, settings, manifest.BootstrapSourceID}, nil, nil, revision)
			assert.Equal(t, retry, false)
			incoming := store.Child{Obj: st.GetObject(consumer)}
			switch mode {
			case "unpublished":
				st.DeleteObject(provider)
			case "incoming":
				updated := st.GetObject(provider).(*manifest.Kustomization).Clone()
				updated.PostBuild.SubstituteFrom[0].Name = "new-settings"
				incoming.Obj = updated
			}
			var reads store.ReadSet
			reads.Init(st, consumer, nil)
			defer reads.Close()
			proposal, err := l.Prepare(t.Context(), incoming, &reads, nil)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, provider), mode != "published")
			if mode == "incoming" {
				settings.Name = "new-settings"
				assert.Equal(t, slices.Contains(proposal.Needed, settings), true)
			}
		})
	}
}

func TestAdmission_KnownInputDoesNotOpenUnrelatedRenderer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"input.yaml":     {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: changed}\n")},
		"unrelated.yaml": {Data: []byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: unrelated, namespace: apps}\nspec:\n  chartRef: {kind: OCIRepository, name: chart}\n  interval: 1m\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet([]string{"input.yaml"})
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "apps"}
	renderer := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Name: "unrelated", Namespace: "apps"}
	assert.Equal(t, slices.Contains(proposal.Selected, input), true)
	assert.Equal(t, slices.Contains(proposal.Needed, renderer), false)
}

func TestAdmission_UnclaimedInputDoesNotOpenUnrelatedRenderer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"input.yaml":     {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: changed}\n")},
		"unrelated.yaml": {Data: []byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: unrelated, namespace: apps}\nspec:\n  chartRef: {kind: OCIRepository, name: chart}\n  interval: 1m\n")},
		"templated.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: templated, namespace: apps}\nspec: {path: './payload/${APP}'}\n")},
		"producer.yaml":  {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: producer, namespace: apps}\nspec: {resourcesTemplate: unexecuted}\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet([]string{"unclaimed/notes.txt"})
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	templated := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Name: "templated", Namespace: "apps"}
	producer := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Name: "producer", Namespace: "apps"}
	record := l.Index.Records[templated]
	record.Productions = []manifest.Production{{Owner: producer}}
	l.Index.Records[templated] = record
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "apps"}
	renderer := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Name: "unrelated", Namespace: "apps"}
	assert.Equal(t, slices.Contains(proposal.Selected, input), false)
	assert.Equal(t, slices.Contains(proposal.Needed, renderer), false)
	assert.Equal(t, slices.Contains(proposal.Selected, renderer), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, renderer), false)
	assert.Equal(t, slices.Contains(proposal.Needed, templated), false)
	assert.Equal(t, slices.Contains(proposal.Selected, templated), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, templated), false)
	assert.Equal(t, slices.Contains(proposal.Needed, producer), false)
	assert.Equal(t, slices.Contains(proposal.Selected, producer), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, producer), false)
	l.Options.HistoryOwners = []manifest.NamedResource{producer}
	proposal, err = l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, producer), true)
	assert.Equal(t, slices.Contains(proposal.Selected, producer), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, producer), false)
}

func TestAdmission_OrderingEmittedTemplateProof(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"emitted", "known_target", "foreign", "unowned", "complete_empty"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"entry/consumer.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec: {path: ./consumer, dependsOn: [{name: target}]}\n")},
			}})
			l := New(st)
			l.SourceRoot = root
			l.Options.Filter = change.NewFilter()
			consumer := (&manifest.Kustomization{Name: "consumer", Namespace: "apps"}).Named()
			l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{consumer}}
			_, err := l.Load(t.Context(), filepath.Join(root, "entry"))
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			producer := &manifest.ResourceSet{Name: "templates", Namespace: "apps"}
			if mode == "known_target" {
				producer.ResourcesTemplate = "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: target, namespace: apps}\nspec: {path: ./target}\n"
			}
			st.AddObject(parent)
			source := manifest.BootstrapSourceID
			if mode == "foreign" {
				source.Name = "foreign"
				st.SetArtifact(source, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
			}
			if mode == "unowned" {
				st.AddObject(producer)
			} else {
				var reads store.ReadSet
				reads.Init(st, parent.Named(), nil)
				defer reads.Retire()
				_, _, output := reads.GetEmission(parent.Named())
				output.Children = []store.Child{{Obj: producer, Productions: []manifest.Production{{Source: source}}}}
				output.Complete = true
				reads.Bind(&output)
				st.RecordEmitted(parent.Named(), &output, nil)
				output.Finish()
			}
			if mode == "complete_empty" {
				var reads store.ReadSet
				reads.Init(st, producer.Named(), nil)
				defer reads.Retire()
				_, _, output := reads.GetEmission(producer.Named())
				output.Complete = true
				reads.Bind(&output)
				st.RecordEmitted(producer.Named(), &output, nil)
				output.Finish()
				st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
			}
			proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, producer.Named()), mode == "known_target")
			assert.Equal(t, slices.Contains(proposal.Selected, producer.Named()), false)
			assert.Equal(t, slices.Contains(proposal.Dirty, producer.Named()), false)
		})
	}
}

func TestAdmission_InputGate(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"consumer.yaml": {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: consumer, namespace: apps}\nspec: {dependsOn: [{apiVersion: kustomize.toolkit.fluxcd.io/v1, kind: Kustomization, name: gate}]}\n")},
		"gate.yaml":     {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: gate, namespace: apps}\nspec: {path: ./gate}\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{(&manifest.ResourceSet{Name: "consumer", Namespace: "apps"}).Named()}}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	gate := (&manifest.Kustomization{Name: "gate", Namespace: "apps"}).Named()
	assert.Equal(t, slices.Contains(proposal.Needed, gate), true)
	assert.Equal(t, slices.Contains(proposal.Selected, gate), false)
}

func TestAdmission_ChangedInputRequiresReaderHistory(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"input.yaml":    {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: apps}\ndata: {replicas: '2'}\n")},
		"producer.yaml": {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: templates, namespace: apps}\nspec:\n  resourcesTemplate: |\n    apiVersion: helm.toolkit.fluxcd.io/v2\n    kind: HelmRelease\n    metadata: {name: child}\n    spec: {interval: 1m, valuesFrom: [{kind: ConfigMap, name: values}]}\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet([]string{"input.yaml"})
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	producer := (&manifest.ResourceSet{Name: "templates", Namespace: "apps"}).Named()
	assert.Equal(t, slices.Contains(proposal.Needed, producer), false)
	assert.Equal(t, slices.Contains(proposal.Selected, producer), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, producer), false)
	l.Options.HistoryOwners = []manifest.NamedResource{producer}
	proposal, err = l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, producer), true)
	assert.Equal(t, slices.Contains(proposal.Selected, producer), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, producer), false)
}

type admissionArtifactReads struct {
	admissionReader
	calls int
	lists int
}

func (r *admissionArtifactReads) GetArtifact(id manifest.NamedResource, read store.ArtifactRead) store.Artifact {
	r.calls++
	return r.admissionReader.GetArtifact(id, read)
}

func (r *admissionArtifactReads) ListObjects(group, kind string) []manifest.BaseManifest {
	r.lists++
	return r.admissionReader.ListObjects(group, kind)
}

func TestAdmission_InputChangesQualifiedOnce(t *testing.T) {
	t.Parallel()
	for _, changed := range []string{"selected.yaml", "unrelated.yaml"} {
		t.Run(changed, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			l := New(st)
			l.SourceRoot = root
			l.Options.Changes = change.NewSet([]string{changed})
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
			inputs := make([]string, 100)
			for i := range inputs {
				inputs[i] = "other-" + strings.Repeat("x", i) + ".yaml"
			}
			inputs[len(inputs)-1] = "selected.yaml"
			l.Index.Records[id] = manifest.DiscoveryRecord{File: "settings.yaml", Inputs: inputs}
			reads := &admissionArtifactReads{admissionReader: st}
			c := admissionClosure{loader: l, objects: reads}
			assert.Equal(t, c.affected(id, nil), changed == "selected.yaml")
			if reads.calls > 3 {
				t.Fatalf("qualified one changed path %d times for %d recorded inputs; want at most 3 artifact reads", reads.calls, len(inputs))
			}
		})
	}
}

func TestAdmission_ChangedDeclarationsPrepareLinearly(t *testing.T) {
	t.Parallel()
	for _, count := range []int{32, 128} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := fstest.MapFS{}
			paths := make([]string, count)
			for i := range paths {
				name := fmt.Sprintf("item-%05d", i)
				paths[i] = name + ".yaml"
				files[paths[i]] = &fstest.MapFile{Data: []byte(testutil.KustomizationYAML(name, "apps", "  suspend: true\n"))}
			}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
			l := New(st)
			l.SourceRoot = root
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Changes = change.NewSet(paths)
			l.Options.Filter = change.NewFilter()
			_, err := l.Load(t.Context(), root)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			for _, published := range []bool{false, true} {
				reads := &admissionArtifactReads{admissionReader: st}
				c := admissionClosure{loader: l, objects: reads}
				c.proposal = &c.result
				proposal, err := c.evaluate(t.Context(), nil)
				assert.NoError(t, err)
				assert.Equal(t, len(proposal.Selected), count)
				wantObjects := count
				if published {
					wantObjects = 0
				}
				assert.Equal(t, len(proposal.Objects), wantObjects)
				t.Logf("%d declarations, published %v: %d artifact reads, %d inventories", count, published, reads.calls, reads.lists)
				if reads.calls > 32*count+32 {
					t.Errorf("%d changed declarations caused %d artifact reads; want at most %d", count, reads.calls, 32*count+32)
				}
				assert.Equal(t, reads.lists, 1)
				for _, child := range proposal.Objects {
					st.AddObject(child.Obj, child.Doc)
				}
			}
			var required store.ReadSet
			required.Init(st, manifest.NamedResource{}, nil)
			defer required.Close()
			ids := make([]manifest.NamedResource, 0, count)
			for _, object := range st.Objects() {
				ids = append(ids, object.Named())
				required.RequireData(object.Named())
			}
			_, _, _, revision := l.Options.Filter.Membership(manifest.NamedResource{})
			_, retry := l.Options.Filter.AddEmitted(ids, ids, ids, revision)
			assert.Equal(t, retry, false)
			reads := &admissionArtifactReads{admissionReader: st}
			c := admissionClosure{loader: l, objects: reads}
			c.proposal = &c.result
			proposal, err := c.evaluate(t.Context(), &required)
			assert.NoError(t, err)
			assert.Equal(t, len(proposal.Needed), 0)
			assert.Equal(t, len(proposal.Selected), 0)
			assert.Equal(t, len(proposal.Objects), 0)
			assert.Equal(t, reads.calls, 0)
			assert.Equal(t, reads.lists, 0)
			calls := 0
			for _, object := range st.Objects() {
				var proof store.ReadSet
				proof.Init(st, object.Named(), nil)
				reads := &admissionArtifactReads{admissionReader: &proof}
				c := admissionClosure{loader: l, objects: reads, incoming: store.Child{Obj: object, Productions: st.Productions(object.Named(), l.Index)}}
				c.proposal = &c.result
				proposal, err := c.evaluate(t.Context(), &proof)
				proof.Close()
				assert.NoError(t, err)
				assert.Equal(t, len(proposal.Objects), 0)
				assert.Equal(t, reads.lists, 1)
				calls += reads.calls
			}
			t.Logf("%d dispatches: %d artifact reads", count, calls)
			if calls > 32*count+32 {
				t.Errorf("%d declarations reopened %d artifact reads during dispatch; want at most %d", count, calls, 32*count+32)
			}
			child := store.Child{Obj: &manifest.ConfigMap{Name: "emitted", Namespace: "apps"}, Productions: []manifest.Production{{Owner: ids[0]}}}
			reads = &admissionArtifactReads{admissionReader: st}
			c = admissionClosure{loader: l, objects: reads, incoming: child}
			c.proposal = &c.result
			proposal, err = c.evaluate(t.Context(), nil)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, child.Obj.Named()), true)
			assert.Equal(t, slices.Contains(proposal.Selected, child.Obj.Named()), true)
			t.Logf("emitted data child with %d unrelated declarations: %d inventories", count, reads.lists)
			assert.Equal(t, reads.lists, 0)
		})
	}
}

func TestAdmission_UnreferencedInputCollision(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"component_collision", "unreferenced_file"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var stores [2]*store.Store
			var loaders [2]*Loader
			for side := range stores {
				root := t.TempDir()
				files := fstest.MapFS{
					"flux/parent.yaml":         {Data: []byte(testutil.KustomizationYAML("parent", "flux-system", "  path: ./decls\n"))},
					"decls/kustomization.yaml": {Data: []byte("resources: [child.yaml]\n")},
					"decls/child.yaml":         {Data: []byte(testutil.KustomizationYAML("child", "flux-system", "  path: ./app\n"))},
					"app/kustomization.yaml":   {Data: []byte("resources: [cm.yaml]\n")},
					"app/cm.yaml":              {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {value: original}\n")},
				}
				if side == 1 {
					if name == "component_collision" {
						files["unused/kustomization.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [cm.yaml]\n")}
						files["unused/cm.yaml"] = files["app/cm.yaml"]
					} else {
						files["app/unreferenced.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unreferenced, namespace: apps}\n")}
					}
				}
				st := store.New()
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: files})
				l := New(st)
				l.SourceRoot = root
				l.Options.Selection = manifest.Selection{Changed: true}
				l.Options.Filter = change.NewFilter()
				for _, dir := range []string{"flux", "decls", "app"} {
					_, err := l.Load(t.Context(), filepath.Join(root, dir))
					assert.NoError(t, err)
				}
				assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
				stores[side], loaders[side] = st, l
			}
			changes, err := change.Qualify(t.Context(), nil, stores[0], stores[1])
			assert.NoError(t, err)
			for side, l := range loaders {
				l.Options.Changes = changes
				proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
				assert.NoError(t, err)
				if len(proposal.Needed) != 0 {
					t.Errorf("side %d admitted unrelated objects: %v", side, proposal.Needed)
				}
			}
		})
	}
}

func TestAdmission_ParentPatchRemoval(t *testing.T) {
	t.Parallel()
	var stores [2]*store.Store
	var loaders [2]*Loader
	for side := range stores {
		root := t.TempDir()
		files := fstest.MapFS{"flux/consumer.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: flux-system}\nspec:\n interval: 10m\n path: ./app\n sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n postBuild:\n  substituteFrom: [{kind: ConfigMap, name: vars}]\n patches:\n - target: {kind: HelmRelease, name: a}\n   patch: |\n    - op: add\n      path: /metadata/annotations\n      value: {kustomize.toolkit.fluxcd.io/substitute: disabled}\n")},
			"flux/vars.yaml":          {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: flux-system}\ndata:\n FLAG: old\n")},
			"app/kustomization.yaml":  {Data: []byte("resources: [hr.yaml]\n")},
			"chart/Chart.yaml":        {Data: []byte("apiVersion: v2\nname: demo\nversion: 1.0.0\n")},
			"chart/values.yaml":       {Data: []byte("value: original\n")},
			"chart/payload.txt":       {Data: []byte("original")},
			"chart/templates/cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: \"{{ .Release.Name }}\", namespace: apps}\ndata:\n value: {{ .Values.value | quote }}\n version: {{ .Chart.Version | quote }}\n file: {{ .Files.Get \"payload.txt\" | quote }}\n")},
			"app/hr.yaml":             {Data: []byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: a, namespace: apps}\nspec:\n interval: 10m\n releaseName: a\n chart:\n  spec:\n   chart: ./chart\n   sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n values: {value: \"${FLAG}\"}\n")}}
		if side == 1 {
			files["flux/vars.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: flux-system}\ndata:\n FLAG: new\n")}
			files["flux/consumer.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: flux-system}\nspec:\n interval: 10m\n path: ./app\n sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n postBuild:\n  substituteFrom: [{kind: ConfigMap, name: vars}]\n")}
		}
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: files})
		l := New(st)
		l.SourceRoot = root
		l.Options.Selection = manifest.Selection{Changed: true}
		l.Options.Filter = change.NewFilter()
		_, err := l.Load(t.Context(), filepath.Join(root, "flux"))
		assert.NoError(t, err)
		_, err = l.Load(t.Context(), filepath.Join(root, "app"))
		assert.NoError(t, err)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
		stores[side], loaders[side] = st, l
	}
	changes, err := change.Qualify(t.Context(), nil, stores[0], stores[1])
	assert.NoError(t, err)
	for side, l := range loaders {
		l.Options.Changes = changes
		proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
		assert.NoError(t, err)
		id := (&manifest.Kustomization{Name: "consumer", Namespace: "flux-system"}).Named()
		if !slices.ContainsFunc(proposal.Objects, func(child store.Child) bool { return child.Obj.Named() == id }) {
			t.Errorf("side %d failed to hydrate changed parent", side)
		}
	}
}

func TestAdmission_HistoryOwnerDeclarationAndInputs(t *testing.T) {
	t.Parallel()
	for _, seeded := range []bool{false, true} {
		t.Run(fmt.Sprint(seeded), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := fstest.MapFS{
				"release.yaml": {Data: []byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: emitter, namespace: apps}\nspec:\n  interval: 1m\n  values: {marker: keep}\n  valuesFrom: [{kind: ConfigMap, name: supplied}]\n  chart: {spec: {chart: ./charts/demo, sourceRef: {kind: GitRepository, name: charts}}}\n")},
				"values.yaml":  {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: supplied, namespace: apps}\ndata: {values.yaml: '{}'}\n")},
			}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
			l := New(st)
			l.SourceRoot = root
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Changes = change.NewSet([]string{"removed/child.yaml"})
			l.Options.Filter = change.NewFilter()
			owner := testutil.HelmReleaseID("apps", "emitter")
			if seeded {
				l.Options.HistoryOwners = []manifest.NamedResource{owner}
			}
			_, err := l.Load(t.Context(), root)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			for range 2 {
				proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
				assert.NoError(t, err)
				assert.Equal(t, slices.Contains(proposal.Needed, owner), seeded)
				assert.Equal(t, slices.Contains(proposal.Selected, owner), false)
				assert.Equal(t, slices.Contains(proposal.Dirty, owner), false)
				inputID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "supplied"}
				assert.Equal(t, slices.Contains(proposal.Needed, inputID), seeded)
				found := false
				for _, child := range proposal.Objects {
					if child.Obj.Named() == owner {
						declaration := child.Obj.(*manifest.HelmRelease)
						assert.Equal(t, declaration.Values["marker"], any("keep"))
						assert.Equal(t, len(declaration.ValuesFrom), 1)
						found = true
					}
				}
				assert.Equal(t, found, seeded)
			}
		})
	}
}

func TestAdmission_StableAncestorsPrepareLinearly(t *testing.T) {
	t.Parallel()
	for _, count := range []int{32, 128} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			l := New(st)
			l.SourceRoot = root
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Changes = change.NewSet([]string{"unrelated.yaml"})
			l.Options.Filter = change.NewFilter()
			l.locations = map[manifest.NamedResource]Occurrence{}
			var parent manifest.NamedResource
			for i := range count {
				object := &manifest.Kustomization{Name: fmt.Sprintf("item-%05d", i), Namespace: "apps"}
				object.Path = "./apps"
				id := object.Named()
				file := fmt.Sprintf("declarations/%05d.yaml", i)
				l.Index.Records[id] = manifest.DiscoveryRecord{File: file, Parent: parent, Declaration: object}
				occurrence := compactOccurrence(object, manifest.ProjectProducer(object))
				occurrence.Declaration = OccurrenceID{Source: root, File: file}
				l.locations[id] = occurrence
				parent = id
			}
			reads := &admissionArtifactReads{admissionReader: st}
			c := admissionClosure{loader: l, objects: reads}
			c.proposal = &c.result
			proposal, err := c.evaluate(t.Context(), nil)
			assert.NoError(t, err)
			assert.Equal(t, len(proposal.Needed), 0)
			t.Logf("%d stable ancestors: %d artifact reads", count, reads.calls)
			if reads.calls > 128*count+128 {
				t.Errorf("%d stable ancestors caused %d artifact reads; want at most %d", count, reads.calls, 128*count+128)
			}
		})
	}
}
