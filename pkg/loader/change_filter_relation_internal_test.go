package loader

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestAddEmitted_CrossSourceOptionalInputEvidence(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"valid", "artifact_replaced", "declaration_replaced", "nonmatch"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testAddEmittedCrossSourceOptionalInputEvidenceCase(
				t,
				&mode,
			)
		},
		)
	}
}

func TestFilter_ProducedTargetsWithoutObjects(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ kind, edge string }{
		{manifest.KindConfigMap, "owner"},
		{manifest.KindConfigMap, "declaration"},
		{manifest.KindOCIRepository, "owner"},
		{manifest.KindOCIRepository, "declaration"},
		{manifest.KindOCIRepository, "file"},
		{manifest.KindOCIRepository, "file_index_only"},
		{manifest.KindOCIRepository, "file_present"},
		{manifest.KindOCIRepository, "file_provisional"},
		{manifest.KindOCIRepository, "file_external"},
	} {
		t.Run(tc.kind+"/"+tc.edge, func(t *testing.T) {
			t.Parallel()
			testFilterProducedTargetsWithoutObjectsCase(t, &tc)
		},
		)
	}
}

func TestAddEmitted_EffectiveSourceRoots(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"bootstrap", "alias", "pinned", "paired_pinned", "paired_alias", "external", "parent_segments", "resolved_component", "changed_component"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testAddEmittedEffectiveSourceRootsCase(t, &mode)
		},
		)
	}
}

func TestAddEmitted_IncomingEvidence(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"source", "contribution", "unrelated"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			selected := &manifest.GitRepository{Name: "new-source", Namespace: "apps"}
			st.AddObject(selected)
			index := &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{selected.Named(): {File: "source.yaml"}},
			}
			f := newAdmission(
				t,
				admissionConfig{
					changes:   change.NewSet([]string{"source.yaml", "incoming.yaml"}),
					index:     index,
					inventory: st,
					root:      "/repo",
				},
			)
			stale := &manifest.Kustomization{
				Name:      "child",
				Namespace: "apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindGitRepository,
					Name: "old-source",
				},
			}
			st.AddObject(stale)
			incoming := *stale
			child := store.Child{Obj: &incoming}
			if mode == "source" {
				incoming.SourceRef.Name = selected.Name
			}
			if mode == "contribution" {
				child.Productions = []manifest.Production{
					{
						Owner:  testutil.KustomizationID("apps", "dependency"),
						Source: manifest.BootstrapSourceID,
						File:   "incoming.yaml",
					},
				}
			}
			f.AddEmitted(testutil.KustomizationID("apps", "dependency"), child, f.objs)
			assert.Equal(t, f.ShouldReconcile(incoming.Named()), mode != "unrelated")
			assert.Equal(t, f.ShouldReconcile(stale.SourceID()), mode == "contribution")
			assert.Equal(t, st.GetObject(incoming.Named()), manifest.BaseManifest(stale))
		})
	}
}

func TestIntegration_ResourceEscapeSelection(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/homelab/kustomization.yaml", "resources: [../base/postgres]\n")
	testutil.WriteFile(t, root, "apps/base/postgres/kustomization.yaml", "resources: [cluster.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/base/postgres/cluster.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cluster, namespace: apps}\ndata: {value: changed}\n",
	)
	st := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "apps/homelab"}
	st.AddObject(owner)
	l := New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.FinalizeIndex(t.Context(), root, true)
	f := newAdmission(
		t,
		admissionConfig{
			changes:   change.NewSet([]string{"apps/base/postgres/cluster.yaml"}),
			index:     l.Index,
			inventory: st,
			root:      root,
		},
	)
	assert.Equal(t, f.ShouldReconcile(owner.Named()), true)
}

type pausedIncoming struct {
	*store.Store

	id            manifest.NamedResource
	read, release chan struct{}
	once          sync.Once
}

type countedProductions struct {
	*store.Store

	id     manifest.NamedResource
	reads  int
	stored int
}

func (c *countedProductions) Productions(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,
) []manifest.Production {
	if id == c.id {
		c.stored++
	}
	return c.Store.Productions(id, index)
}

func (c *countedProductions) IncomingProductions(
	child store.Child,
	index *manifest.DiscoveryIndex,
) []manifest.Production {
	if child.Obj.Named() == c.id {
		c.reads++
	}
	return c.Store.IncomingProductions(child, index)
}

func TestFilter_SharedInputRelationLookupBounded(t *testing.T) {
	t.Parallel()
	st, files := seedKSStore(100)
	index := new(filterFixture{files: files, root: "/repo", objects: st}).index()
	counted := &countedProductions{Store: st, id: manifest.BootstrapSourceID}
	f := newAdmission(
		t,
		admissionConfig{
			changes:   change.NewSet([]string{"apps/app-7/ks.yaml"}),
			index:     index,
			inventory: counted,
			root:      "/repo",
		},
	)
	assert.Equal(t, f.ShouldReconcile(testutil.KustomizationID("flux-system", "app-7")), true)
	assert.Equal(t, f.ShouldReconcile(testutil.KustomizationID("flux-system", "app-8")), false)
	assert.Equal(t, counted.stored, 1)
}

func TestAddEmitted_PrimaryAdmissionReadsRelationOnce(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"primary_parent", "primary_child", "primary_input"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			parent := testutil.KustomizationID("apps", "parent")
			child := &manifest.Kustomization{
				Name:      "child",
				Namespace: "apps",
				Path:      "unrelated",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindGitRepository,
					Name: "incoming",
				},
			}
			st := &countedProductions{Store: store.New(), id: child.Named()}
			f := newAdmission(
				t,
				admissionConfig{
					changes: change.NewSet([]string{"parent.yaml"}),
					index: &manifest.DiscoveryIndex{
						Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent: {File: "parent.yaml"}},
					},
					inventory: st,
					root:      "/repo",
				},
			)
			switch mode {
			case "primary_child":
				f.keep[child.Named()] = primary
				parent = testutil.KustomizationID("apps", "dependency")
			case "primary_input":
				f.keep[child.SourceID()] = primary
				parent = testutil.KustomizationID("apps", "dependency")
			}
			f.AddEmitted(parent, store.Child{Obj: child}, st)
			assert.Equal(t, f.ShouldReconcile(child.Named()), true)
			assert.Equal(t, f.ShouldReconcile(child.SourceID()), true)
			assert.Equal(t, st.reads, 1)
		})
	}
}

func (p *pausedIncoming) IncomingProductions(child store.Child, index *manifest.DiscoveryIndex) []manifest.Production {
	if child.Obj.Named() == p.id {
		p.once.Do(func() { close(p.read) })
		<-p.release
	}
	return p.Store.IncomingProductions(child, index)
}

func TestAddEmitted_PromotionDuringEvaluation(t *testing.T) {
	t.Parallel()
	st := store.New()
	target := &manifest.ConfigMap{Name: "target", Namespace: "apps"}
	primary := &manifest.Kustomization{
		Name:      "primary",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "settings"},
				{Kind: manifest.KindConfigMap, Name: target.Name},
			},
		},
	}
	dependency := testutil.KustomizationID("apps", "dependency")
	settings := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	child := &manifest.Kustomization{
		Name:      "late",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: target.Name}},
		},
	}
	st.AddObject(primary)
	objects := &pausedIncoming{Store: st, id: child.Named(), read: make(chan struct{}), release: make(chan struct{})}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		primary.Named(): {File: "primary.yaml"}, settings: {Productions: []manifest.Production{{Owner: dependency}}},
	}}
	f := newAdmission(
		t,
		admissionConfig{
			changes:   change.NewSet([]string{"primary.yaml"}),
			index:     index,
			inventory: objects,
			root:      "/repo",
		},
	)
	assert.Equal(t, f.ShouldReconcile(dependency), true)
	assert.Equal(t, f.keep[target.Named()], kept)
	slots := task.NewBounded(2)
	slots.Go(t.Context(), "admit", func(context.Context) { f.AddEmitted(dependency, store.Child{Obj: child}, f.objs) })
	slots.Go(t.Context(), "promote", func(context.Context) {
		<-objects.read
		f.AddEmitted(primary.Named(), store.Child{Obj: target}, f.objs)
		close(objects.release)
	})
	slots.BlockTillDone()
	assert.Equal(t, slots.Failures(), 0)
	assert.Equal(t, f.ShouldReconcile(child.Named()), true)
	assert.Equal(t, f.keep[target.Named()], f.keep[primary.Named()])
	assert.Equal(t, f.ShouldReconcile(target.Named()), true)
}

func TestFilter_ProductionReadersSourceCoordinates(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"alias", "pinned", "pinned_reader", "external", "index_only_pinned", "index_only_external"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testFilterProductionReadersSourceCoordinatesCase(
				t,
				&mode,
			)
		},
		)
	}
}

type relationReader struct {
	fs.FS

	opens atomic.Int64
}

func (r *relationReader) Open(name string) (fs.File, error) { r.opens.Add(1); return r.FS.Open(name) }

func TestQualify_SharedNonComparableArtifact(t *testing.T) {
	t.Parallel()
	stores := [2]*store.Store{store.New(), store.New()}
	first := &manifest.GitRepository{Name: "first", Namespace: "apps"}
	second := &manifest.GitRepository{Name: "second", Namespace: "apps"}
	var readers [2]*relationReader
	var artifacts [2]*store.SourceArtifact
	for side, st := range stores {
		readers[side] = &relationReader{FS: fstest.MapFS{"file": {Data: []byte([]string{"old", "new"}[side])}}}
		input := struct {
			*relationReader

			noncomparable []byte
		}{relationReader: readers[side]}
		artifacts[side] = &store.SourceArtifact{FS: input, LocalPath: []string{"/before", "/after"}[side]}
		st.AddObject(first)
		st.SetArtifact(first.Named(), artifacts[side])
	}
	_, err := change.Qualify(t.Context(), change.NewSet(nil), stores[0], stores[1])
	assert.NoError(t, err)
	var single [2]int64
	for side, st := range stores {
		single[side] = readers[side].opens.Load()
		{
			assert.FatalIf(t, single[side] == 0, "%s", "source pair was not compared")
		}
		readers[side].opens.Store(0)
		st.AddObject(second)
		st.SetArtifact(second.Named(), artifacts[side])
	}
	changes, err := change.Qualify(t.Context(), change.NewSet(nil), stores[0], stores[1])
	assert.NoError(t, err)
	for side := range stores {
		assert.Equal(t, readers[side].opens.Load(), single[side])
	}
	for _, source := range []*manifest.GitRepository{first, second} {
		assert.Diff(t, sourceChanges(changes, source.Named()).Paths(), []string{"file"})
	}
}

func testAddEmittedCrossSourceOptionalInputEvidenceCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	changes, stores, roots, source := optionalSupplierPair(t, "apps")
	st := stores[1]
	child := store.Child{
		Obj: &manifest.Kustomization{
			Name:      "late",
			Namespace: "apps",
			Path:      "apps",
			PostBuild: &kustomizev1.PostBuild{
				SubstituteFrom: []manifest.SubstituteReference{
					{Kind: manifest.KindConfigMap, Name: "optional", Optional: true},
				},
			},
		},
	}
	if (*testCaseInput) == "nonmatch" {
		child.Obj.(*manifest.Kustomization).PostBuild.SubstituteFrom[0].Name = "unrelated"
	}
	emitter := &manifest.Kustomization{Name: "emitter", Namespace: "apps", Path: "unchanged"}
	st.AddObject(emitter)
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			emitter.Named(): {File: "flux/emitter.yaml"},
		},
	}
	filter := newAdmission(t, admissionConfig{changes: changes, index: index, inventory: st, root: roots[1]})
	assert.Equal(t, st.GetObject(child.Obj.Named()) == nil, true)
	_, indexed := index.Lookup(child.Obj.Named())
	assert.Equal(t, indexed, false)
	assert.Equal(t, filter.ShouldReconcile(child.Obj.Named()), false)
	assert.Equal(t, filter.ShouldReconcile(emitter.Named()), false)
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "optional", Namespace: "apps"}
	assert.Equal(t, filter.ShouldReconcile(input), false)
	assert.Equal(t, changes.Len(), 0)
	assert.Equal(t, len(inputMap(changes)), 0)
	setup := admissionClosure{loader: filter.loader, objects: st, incoming: child}
	assert.Equal(t, setup.affected(child.Obj.Named(), child.Obj), false)
	productions := st.IncomingProductions(child, index)
	for i := range productions {
		production := productions[i]
		assert.Equal(t, setup.fileChanged(production.Source, production.File), false)
	}
	var reads store.ReadSet
	reads.Init(st, child.Obj.Named(), nil)
	defer reads.Close()
	filter.AddEmitted(emitter.Named(), child, &reads)
	assert.Equal(t, filter.ShouldReconcile(child.Obj.Named()), (*testCaseInput) != "nonmatch")
	assert.Equal(t, st.GetObject(child.Obj.Named()) == nil, true)
	assert.Equal(t, reads.Valid(), true)
	if (*testCaseInput) == "valid" || (*testCaseInput) == "nonmatch" {
		return
	}
	if (*testCaseInput) == "artifact_replaced" {
		replacement := *st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
		replacement.Content, replacement.Revision = "replacement", "replacement"
		st.SetArtifact(source.Named(), &replacement)
	} else {
		replacement := *source
		replacement.URL = "https://example.test/replacement"
		st.AddObject(&replacement)
	}
	assert.Equal(t, reads.Valid(), false)
	fresh := newAdmission(t, admissionConfig{changes: changes, index: index, inventory: st, root: roots[1]})
	var current store.ReadSet
	current.Init(st, child.Obj.Named(), nil)
	defer current.Close()
	fresh.AddEmitted(emitter.Named(), child, &current)
	assert.Equal(t, fresh.ShouldReconcile(child.Obj.Named()), false)
	assert.Equal(t, current.Valid(), true)
}

func testFilterProducedTargetsWithoutObjectsCase(
	t *testing.T,
	testCaseInput *struct {
		kind string
		edge string
	},
) {
	t.Helper()

	producer := testutil.KustomizationID("apps", "producer")
	target := manifest.NamedResource{Kind: testCaseInput.kind, Namespace: "apps", Name: "target"}
	var obj manifest.BaseManifest = &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: target.Name}}}}
	if testCaseInput.kind == manifest.KindOCIRepository {
		target.Group = manifest.SourceDomain
		obj = &manifest.HelmRelease{
			Name:      "consumer",
			Namespace: "apps",
			Chart:     manifest.HelmChart{Source: target},
			ChartRef: &helmv2.CrossNamespaceSourceReference{
				Kind:      target.Kind,
				Name:      target.Name,
				Namespace: target.Namespace,
			},
		}
	}
	consumer := obj.Named()
	proof := manifest.Production{Owner: producer, Source: manifest.BootstrapSourceID}
	if testCaseInput.edge == "declaration" {
		proof.Owner, proof.Declaration = manifest.NamedResource{}, producer
	}
	changes := []string{"generator.env"}
	if testCaseInput.edge == "file" || testCaseInput.edge == "file_index_only" ||
		testCaseInput.edge == "file_present" ||
		testCaseInput.edge == "file_provisional" ||
		testCaseInput.edge == "file_external" {
		proof.File, proof.Path, proof.Authored = "sources/target.yaml", "/repo/sources/target.yaml", target
		changes = []string{"flux/consumer.yaml"}
		if testCaseInput.edge == "file_external" {
			proof.Declaration = testutil.KustomizationID("apps", "external-declaration")
			proof.File = "../external/target.yaml"
		}
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		producer: {File: "flux/producer.yaml"},
		target:   {Productions: []manifest.Production{proof}},
		consumer: {File: "flux/consumer.yaml", Refs: []manifest.NamedResource{target}},
	}, Readers: map[string][]manifest.FileReader{"generator.env": {{Reader: producer, Source: manifest.BootstrapSourceID}}}}
	st := store.New()
	st.AddObject(obj)
	if testCaseInput.edge == "file_present" || testCaseInput.edge == "file_provisional" {
		st.AddObject(&manifest.OCIRepository{Name: target.Name, Namespace: target.Namespace})
	}
	if testCaseInput.edge == "file_provisional" {
		st.SetDiscoveryIndex(index)
		assert.Equal(t, st.GetDeclaration(target), nil)
	}
	var inventory objects = st
	if testCaseInput.edge == "file_index_only" {
		inventory = nil
	}
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet(changes), index: index, inventory: inventory, root: "/repo"},
	)
	want := []string{producer.String(), target.String(), consumer.String()}
	if testCaseInput.edge == "file_present" {
		want = []string{target.String(), consumer.String()}
	}
	if testCaseInput.edge == "file_external" {
		want = []string{target.String(), consumer.String(), proof.Declaration.String()}
	}
	slices.Sort(want)
	assert.Diff(t, f.KeepNames(), want)
	assert.Equal(
		t,
		st.GetObject(target) == nil,
		testCaseInput.edge != "file_present" && testCaseInput.edge != "file_provisional",
	)
	if testCaseInput.edge == "file" || testCaseInput.edge == "file_provisional" ||
		testCaseInput.edge == "file_index_only" {
		assert.Diff(t, f.keep, map[manifest.NamedResource]role{consumer: primary, target: kept, producer: kept})
	}
}

func testAddEmittedEffectiveSourceRootsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	source := manifest.BootstrapSourceID
	path := "apps"
	changed := "apps/config.yaml"
	if (*testCaseInput) == "parent_segments" {
		path = "apps/../../base"
		changed = "base/config.yaml"
	}
	if (*testCaseInput) != "bootstrap" && (*testCaseInput) != "parent_segments" {
		src := &manifest.GitRepository{Name: "input", Namespace: "apps"}
		st.AddObject(src)
		source = src.Named()
		art := &store.SourceArtifact{
			FS:         os.DirFS(root),
			LocalPath:  filepath.Join(root, ".inputs/source"),
			LocalRoot:  filepath.Join(root, "subtree"),
			InputAlias: true,
			Key:        "derived-alias",
		}
		changed = "subtree/apps/config.yaml"
		if (*testCaseInput) == "pinned" || (*testCaseInput) == "paired_pinned" {
			art.InputAlias = false
			art.Key, art.Content = "pin", "pin"
			art.LocalRoot = root
			changed = ".inputs/source/apps/config.yaml"
		}
		if (*testCaseInput) == "external" {
			art.LocalRoot = filepath.Dir(root)
			art.InputAlias = false
			changed = "apps/config.yaml"
		}
		st.SetArtifact(source, art)
	}
	child := &manifest.Kustomization{
		Name:      "late",
		Namespace: "apps",
		Path:      path,
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      source.Kind,
			Name:      source.Name,
			Namespace: source.Namespace,
		},
	}
	if (*testCaseInput) == "resolved_component" || (*testCaseInput) == "changed_component" {
		child.Path = "apps/app"
		child.Components = []string{"../../components/common"}
		changed = "other/config.yaml"
		if (*testCaseInput) == "changed_component" {
			changed = "components/common/config.yaml"
		}
		testutil.WriteFile(t, root, "apps/app/kustomization.yaml", "resources: []\n")
		testutil.WriteFile(t, root, "components/common/kustomization.yaml", "kind: Component\n")
		st.SetArtifact(source, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: os.DirFS(root)})
	}
	changes := change.NewSet([]string{changed})
	if (*testCaseInput) == "paired_pinned" || (*testCaseInput) == "paired_alias" {
		current := *st.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
		before := current
		before.FS = fstest.MapFS{"apps/config.yaml": {Data: []byte("before")}}
		current.FS = fstest.MapFS{"apps/config.yaml": {Data: []byte("after")}}
		current.Revision = "after"
		st.SetArtifact(source, &current)
		peer := store.New()
		peer.AddObject(st.GetObject(source))
		peer.SetArtifact(source, &before)
		var err error
		changes, err = change.Qualify(t.Context(), change.NewSet(nil), peer, st)
		assert.NoError(t, err)
	}
	f := newAdmission(t, admissionConfig{changes: changes, index: nil, inventory: st, root: root})
	f.AddEmitted(testutil.KustomizationID("apps", "dependency"), store.Child{Obj: child}, f.objs)
	assert.Equal(
		t,
		f.ShouldReconcile(child.Named()), (*testCaseInput) ==
			"bootstrap" || (*testCaseInput) == "alias" || (*testCaseInput) == "parent_segments" || (*testCaseInput) == "paired_pinned" || (*testCaseInput) == "paired_alias" || (*testCaseInput) ==
			"changed_component",
	)
	sibling := *child
	sibling.Name, sibling.Path = "unrelated", "elsewhere"
	sibling.Components = nil
	f.AddEmitted(testutil.KustomizationID("apps", "dependency"), store.Child{Obj: &sibling}, f.objs)
	assert.Equal(t, f.ShouldReconcile(sibling.Named()), false)
}

func testFilterProductionReadersSourceCoordinatesCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	st.AddObject(source)
	art := &store.SourceArtifact{
		FS:         os.DirFS(root),
		LocalPath:  filepath.Join(root, ".mount"),
		LocalRoot:  filepath.Join(root, "subtree"),
		InputAlias: true,
	}
	if (*testCaseInput) == "pinned" || (*testCaseInput) == "pinned_reader" ||
		(*testCaseInput) == "index_only_pinned" {
		art.LocalRoot, art.InputAlias, art.Key, art.Content = root, false, "pin", "pin"
	}
	if (*testCaseInput) == "external" || (*testCaseInput) == "index_only_external" {
		art.LocalRoot, art.InputAlias = filepath.Dir(root), false
		if (*testCaseInput) == "index_only_external" {
			art.Key, art.Content = "external", "external"
		}
	}
	st.SetArtifact(source.Named(), art)
	target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	consumer := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: target.Kind, Name: target.Name}},
		},
	}
	st.AddObject(consumer)
	owner := testutil.KustomizationID("apps", "producer")
	reader := testutil.KustomizationID("apps", "reader")
	unrelated := testutil.KustomizationID("apps", "unrelated")
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			consumer.Named(): {File: "consumer.yaml"},
			target: {
				Productions: []manifest.Production{
					{
						Owner:  owner,
						Source: source.Named(),
						File:   "wrong/declaration.yaml",
						Path:   filepath.Join(art.LocalPath, "declaration.yaml"),
					},
				},
			},
		},
		Readers: map[string][]manifest.FileReader{
			"subtree/declaration.yaml": {{Reader: reader, Source: manifest.BootstrapSourceID}},
			"wrong/declaration.yaml":   {{Reader: unrelated, Source: manifest.BootstrapSourceID}},
			".mount/declaration.yaml":  {{Reader: unrelated, Source: manifest.BootstrapSourceID}},
		},
	}
	if (*testCaseInput) == "pinned_reader" {
		st.AddObject(
			&manifest.Kustomization{
				Name:      reader.Name,
				Namespace: reader.Namespace,
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: source.Named().Kind,
					Name: source.Name,
				},
			},
		)
		index.Readers[".mount/declaration.yaml"] = []manifest.FileReader{
			{Reader: reader, Source: source.Named()},
		}
	}
	foreign := &manifest.Kustomization{Name: "foreign", Namespace: "apps", TargetNamespace: "other"}
	if (*testCaseInput) == "index_only_pinned" || (*testCaseInput) == "index_only_external" {
		index.Records[reader] = manifest.DiscoveryRecord{File: "reader.yaml"}
		index.Readers[".mount/declaration.yaml"] = []manifest.FileReader{
			{Reader: reader, Source: source.Named()},
			{Reader: unrelated, Source: manifest.BootstrapSourceID},
			{Reader: foreign.Named(), Source: source.Named()},
		}
		st.AddObject(foreign)
		assert.Equal(t, st.GetObject(reader) == nil, true)
	}
	filter := newAdmission(
		t,
		admissionConfig{
			changes:   change.NewSet([]string{"consumer.yaml"}),
			index:     index,
			inventory: st,
			root:      root,
		},
	)
	assert.Equal(t, filter.ShouldReconcile(target), true)
	assert.Equal(t, filter.ShouldReconcile(owner), true)
	assert.Equal(
		t,
		filter.ShouldReconcile(reader), (*testCaseInput) ==
			"alias" || (*testCaseInput) == "pinned_reader" || (*testCaseInput) == "index_only_pinned" || (*testCaseInput) ==
			"index_only_external",
	)
	assert.Equal(t, filter.ShouldReconcile(unrelated), false)
	assert.Equal(t, filter.ShouldReconcile(foreign.Named()), false)
}
