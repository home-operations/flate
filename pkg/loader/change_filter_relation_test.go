package loader

import (
	"context"
	"errors"
	"io/fs"
	"maps"
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
			changes, stores, roots, source := optionalSupplierPair(t, "apps")
			st := stores[1]
			child := store.Child{Obj: &manifest.Kustomization{Name: "late", Namespace: "apps", Path: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "optional", Optional: true}}}}}
			if mode == "nonmatch" {
				child.Obj.(*manifest.Kustomization).PostBuild.SubstituteFrom[0].Name = "unrelated"
			}
			emitter := &manifest.Kustomization{Name: "emitter", Namespace: "apps", Path: "unchanged"}
			st.AddObject(emitter)
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{emitter.Named(): {File: "flux/emitter.yaml"}}}
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
			for _, production := range st.IncomingProductions(child, index) {
				assert.Equal(t, setup.fileChanged(production.Source, production.File), false)
			}
			var reads store.ReadSet
			reads.Init(st, child.Obj.Named(), nil)
			defer reads.Close()
			filter.AddEmitted(emitter.Named(), child, &reads)
			assert.Equal(t, filter.ShouldReconcile(child.Obj.Named()), mode != "nonmatch")
			assert.Equal(t, st.GetObject(child.Obj.Named()) == nil, true)
			assert.Equal(t, reads.Valid(), true)
			if mode == "valid" || mode == "nonmatch" {
				return
			}
			if mode == "artifact_replaced" {
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
		})
	}
}

func TestFilter_ProducedTargetsWithoutObjects(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ kind, edge string }{
		{manifest.KindConfigMap, "owner"}, {manifest.KindConfigMap, "declaration"},
		{manifest.KindOCIRepository, "owner"}, {manifest.KindOCIRepository, "declaration"},
		{manifest.KindOCIRepository, "file"}, {manifest.KindOCIRepository, "file_index_only"}, {manifest.KindOCIRepository, "file_present"}, {manifest.KindOCIRepository, "file_provisional"}, {manifest.KindOCIRepository, "file_external"},
	} {
		t.Run(tc.kind+"/"+tc.edge, func(t *testing.T) {
			t.Parallel()
			producer := testutil.KustomizationID("apps", "producer")
			target := manifest.NamedResource{Kind: tc.kind, Namespace: "apps", Name: "target"}
			var obj manifest.BaseManifest = &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: target.Name}}}}
			if tc.kind == manifest.KindOCIRepository {
				target.Group = manifest.SourceDomain
				obj = &manifest.HelmRelease{Name: "consumer", Namespace: "apps", Chart: manifest.HelmChart{Source: target}, ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: target.Kind, Name: target.Name, Namespace: target.Namespace}}
			}
			consumer := obj.Named()
			proof := manifest.Production{Owner: producer, Source: manifest.BootstrapSourceID}
			if tc.edge == "declaration" {
				proof.Owner, proof.Declaration = manifest.NamedResource{}, producer
			}
			changes := []string{"generator.env"}
			if tc.edge == "file" || tc.edge == "file_index_only" || tc.edge == "file_present" || tc.edge == "file_provisional" || tc.edge == "file_external" {
				proof.File, proof.Path, proof.Authored = "sources/target.yaml", "/repo/sources/target.yaml", target
				changes = []string{"flux/consumer.yaml"}
				if tc.edge == "file_external" {
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
			if tc.edge == "file_present" || tc.edge == "file_provisional" {
				st.AddObject(&manifest.OCIRepository{Name: target.Name, Namespace: target.Namespace})
			}
			if tc.edge == "file_provisional" {
				st.SetDiscoveryIndex(index)
				assert.Equal(t, st.GetDeclaration(target), nil)
			}
			var inventory objects = st
			if tc.edge == "file_index_only" {
				inventory = nil
			}
			f := newAdmission(t, admissionConfig{changes: change.NewSet(changes), index: index, inventory: inventory, root: "/repo"})
			want := []string{producer.String(), target.String(), consumer.String()}
			if tc.edge == "file_present" {
				want = []string{target.String(), consumer.String()}
			}
			if tc.edge == "file_external" {
				want = []string{target.String(), consumer.String(), proof.Declaration.String()}
			}
			slices.Sort(want)
			assert.Diff(t, f.KeepNames(), want)
			assert.Equal(t, st.GetObject(target) == nil, tc.edge != "file_present" && tc.edge != "file_provisional")
			if tc.edge == "file" || tc.edge == "file_provisional" || tc.edge == "file_index_only" {
				assert.Diff(t, f.keep, map[manifest.NamedResource]role{consumer: primary, target: kept, producer: kept})
			}
		})
	}
}

func TestAddEmitted_EffectiveSourceRoots(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"bootstrap", "alias", "pinned", "paired_pinned", "paired_alias", "external", "parent_segments", "resolved_component", "changed_component"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			source := manifest.BootstrapSourceID
			path := "apps"
			changed := "apps/config.yaml"
			if mode == "parent_segments" {
				path = "apps/../../base"
				changed = "base/config.yaml"
			}
			if mode != "bootstrap" && mode != "parent_segments" {
				src := &manifest.GitRepository{Name: "input", Namespace: "apps"}
				st.AddObject(src)
				source = src.Named()
				art := &store.SourceArtifact{FS: os.DirFS(root), LocalPath: filepath.Join(root, ".inputs/source"), LocalRoot: filepath.Join(root, "subtree"), InputAlias: true, Key: "derived-alias"}
				changed = "subtree/apps/config.yaml"
				if mode == "pinned" || mode == "paired_pinned" {
					art.InputAlias = false
					art.Key, art.Content = "pin", "pin"
					art.LocalRoot = root
					changed = ".inputs/source/apps/config.yaml"
				}
				if mode == "external" {
					art.LocalRoot = filepath.Dir(root)
					art.InputAlias = false
					changed = "apps/config.yaml"
				}
				st.SetArtifact(source, art)
			}
			child := &manifest.Kustomization{Name: "late", Namespace: "apps", Path: path, SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Kind, Name: source.Name, Namespace: source.Namespace}}
			if mode == "resolved_component" || mode == "changed_component" {
				child.Path = "apps/app"
				child.Components = []string{"../../components/common"}
				changed = "other/config.yaml"
				if mode == "changed_component" {
					changed = "components/common/config.yaml"
				}
				testutil.WriteFile(t, root, "apps/app/kustomization.yaml", "resources: []\n")
				testutil.WriteFile(t, root, "components/common/kustomization.yaml", "kind: Component\n")
				st.SetArtifact(source, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: os.DirFS(root)})
			}
			changes := change.NewSet([]string{changed})
			if mode == "paired_pinned" || mode == "paired_alias" {
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
			assert.Equal(t, f.ShouldReconcile(child.Named()), mode == "bootstrap" || mode == "alias" || mode == "parent_segments" || mode == "paired_pinned" || mode == "paired_alias" || mode == "changed_component")
			sibling := *child
			sibling.Name, sibling.Path = "unrelated", "elsewhere"
			sibling.Components = nil
			f.AddEmitted(testutil.KustomizationID("apps", "dependency"), store.Child{Obj: &sibling}, f.objs)
			assert.Equal(t, f.ShouldReconcile(sibling.Named()), false)
		})
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
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{selected.Named(): {File: "source.yaml"}}}
			f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"source.yaml", "incoming.yaml"}), index: index, inventory: st, root: "/repo"})
			stale := &manifest.Kustomization{Name: "child", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "old-source"}}
			st.AddObject(stale)
			incoming := *stale
			child := store.Child{Obj: &incoming}
			if mode == "source" {
				incoming.SourceRef.Name = selected.Name
			}
			if mode == "contribution" {
				child.Productions = []manifest.Production{{Owner: testutil.KustomizationID("apps", "dependency"), Source: manifest.BootstrapSourceID, File: "incoming.yaml"}}
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
	testutil.WriteFile(t, root, "apps/base/postgres/cluster.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cluster, namespace: apps}\ndata: {value: changed}\n")
	st := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "apps/homelab"}
	st.AddObject(owner)
	l := New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.FinalizeIndex(t.Context(), root, true)
	f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"apps/base/postgres/cluster.yaml"}), index: l.Index, inventory: st, root: root})
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

func (c *countedProductions) Productions(id manifest.NamedResource, index *manifest.DiscoveryIndex) []manifest.Production {
	if id == c.id {
		c.stored++
	}
	return c.Store.Productions(id, index)
}

func (c *countedProductions) IncomingProductions(child store.Child, index *manifest.DiscoveryIndex) []manifest.Production {
	if child.Obj.Named() == c.id {
		c.reads++
	}
	return c.Store.IncomingProductions(child, index)
}

func TestFilter_SharedInputRelationLookupBounded(t *testing.T) {
	t.Parallel()
	st, files := seedKSStore(100)
	index := filterFixture{files: files, root: "/repo", objects: st}.index()
	counted := &countedProductions{Store: st, id: manifest.BootstrapSourceID}
	f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"apps/app-7/ks.yaml"}), index: index, inventory: counted, root: "/repo"})
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
			child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "unrelated", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "incoming"}}
			st := &countedProductions{Store: store.New(), id: child.Named()}
			f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"parent.yaml"}), index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent: {File: "parent.yaml"}}}, inventory: st, root: "/repo"})
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
	primary := &manifest.Kustomization{Name: "primary", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "settings"}, {Kind: manifest.KindConfigMap, Name: target.Name}}}}
	dependency := testutil.KustomizationID("apps", "dependency")
	settings := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	child := &manifest.Kustomization{Name: "late", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: target.Name}}}}
	st.AddObject(primary)
	objects := &pausedIncoming{Store: st, id: child.Named(), read: make(chan struct{}), release: make(chan struct{})}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		primary.Named(): {File: "primary.yaml"}, settings: {Productions: []manifest.Production{{Owner: dependency}}},
	}}
	f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"primary.yaml"}), index: index, inventory: objects, root: "/repo"})
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
			root := t.TempDir()
			st := store.New()
			source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
			st.AddObject(source)
			art := &store.SourceArtifact{FS: os.DirFS(root), LocalPath: filepath.Join(root, ".mount"), LocalRoot: filepath.Join(root, "subtree"), InputAlias: true}
			if mode == "pinned" || mode == "pinned_reader" || mode == "index_only_pinned" {
				art.LocalRoot, art.InputAlias, art.Key, art.Content = root, false, "pin", "pin"
			}
			if mode == "external" || mode == "index_only_external" {
				art.LocalRoot, art.InputAlias = filepath.Dir(root), false
				if mode == "index_only_external" {
					art.Key, art.Content = "external", "external"
				}
			}
			st.SetArtifact(source.Named(), art)
			target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: target.Kind, Name: target.Name}}}}
			st.AddObject(consumer)
			owner := testutil.KustomizationID("apps", "producer")
			reader := testutil.KustomizationID("apps", "reader")
			unrelated := testutil.KustomizationID("apps", "unrelated")
			index := &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					consumer.Named(): {File: "consumer.yaml"},
					target:           {Productions: []manifest.Production{{Owner: owner, Source: source.Named(), File: "wrong/declaration.yaml", Path: filepath.Join(art.LocalPath, "declaration.yaml")}}},
				},
				Readers: map[string][]manifest.FileReader{"subtree/declaration.yaml": {{Reader: reader, Source: manifest.BootstrapSourceID}}, "wrong/declaration.yaml": {{Reader: unrelated, Source: manifest.BootstrapSourceID}}, ".mount/declaration.yaml": {{Reader: unrelated, Source: manifest.BootstrapSourceID}}},
			}
			if mode == "pinned_reader" {
				st.AddObject(&manifest.Kustomization{Name: reader.Name, Namespace: reader.Namespace, SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name}})
				index.Readers[".mount/declaration.yaml"] = []manifest.FileReader{{Reader: reader, Source: source.Named()}}
			}
			foreign := &manifest.Kustomization{Name: "foreign", Namespace: "apps", TargetNamespace: "other"}
			if mode == "index_only_pinned" || mode == "index_only_external" {
				index.Records[reader] = manifest.DiscoveryRecord{File: "reader.yaml"}
				index.Readers[".mount/declaration.yaml"] = []manifest.FileReader{
					{Reader: reader, Source: source.Named()},
					{Reader: unrelated, Source: manifest.BootstrapSourceID},
					{Reader: foreign.Named(), Source: source.Named()},
				}
				st.AddObject(foreign)
				assert.Equal(t, st.GetObject(reader) == nil, true)
			}
			filter := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"consumer.yaml"}), index: index, inventory: st, root: root})
			assert.Equal(t, filter.ShouldReconcile(target), true)
			assert.Equal(t, filter.ShouldReconcile(owner), true)
			assert.Equal(t, filter.ShouldReconcile(reader), mode == "alias" || mode == "pinned_reader" || mode == "index_only_pinned" || mode == "index_only_external")
			assert.Equal(t, filter.ShouldReconcile(unrelated), false)
			assert.Equal(t, filter.ShouldReconcile(foreign.Named()), false)
		})
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
		if single[side] == 0 {
			t.Fatal("source pair was not compared")
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

func TestQualify_ContentFingerprint(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"immutable", "revision", "digest", "shaping", "content", "family_only", "mutable", "empty", "unknown", "changed", "error", "canceled", "callback_canceled", "callback_deadline", "bootstrap"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			st := [2]*store.Store{store.New(), store.New()}
			source := &manifest.GitRepository{Name: "selected", Namespace: "apps"}
			id := source.Named()
			if mode == "bootstrap" {
				id = manifest.BootstrapSourceID
			}
			var readers [2]*relationReader
			for i := range st {
				readers[i] = &relationReader{FS: fstest.MapFS{"file": {Data: []byte{byte(i)}}}}
				art := &store.SourceArtifact{FS: readers[i], LocalPath: []string{"/a", "/b"}[i], Key: "family", Content: "generation", Revision: "commit", Digest: "digest"}
				if mode == "content" {
					art.Content += string(rune('a' + i))
				}
				if mode == "family_only" {
					art.Content = ""
				}
				if mode == "revision" {
					art.Revision += string(rune('a' + i))
				}
				if mode == "digest" {
					art.Digest += string(rune('a' + i))
				}
				if mode == "shaping" {
					art.Key += string(rune('a' + i))
				}
				if mode == "mutable" || mode == "empty" || mode == "unknown" || mode == "changed" || mode == "error" || mode == "callback_canceled" || mode == "callback_deadline" {
					art.Key, art.Content = "", ""
					if mode != "unknown" {
						art.ContentFingerprint = func(context.Context, fs.FS) (string, error) {
							if mode == "callback_canceled" {
								return "", context.Canceled
							}
							if mode == "callback_deadline" {
								return "", context.DeadlineExceeded
							}
							if mode == "error" {
								return "", fs.ErrPermission
							}
							if mode == "empty" {
								return "", nil
							}
							if mode == "changed" {
								return string(rune('a' + i)), nil
							}
							return "selected bytes", nil
						}
					}
				}
				st[i].AddObject(source)
				st[i].SetArtifact(id, art)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			changes := change.NewSet(nil)
			if mode == "bootstrap" {
				changes = nil
			}
			qualified, err := change.Qualify(ctx, changes, st[0], st[1])
			if mode == "error" || mode == "canceled" || mode == "callback_canceled" || mode == "callback_deadline" {
				want := fs.ErrPermission
				if mode == "canceled" || mode == "callback_canceled" {
					want = context.Canceled
				}
				if mode == "callback_deadline" {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, want) {
					t.Fatalf("change.Qualify error = %v, want %v", err, want)
				}
				assert.Equal(t, errors.Is(err, manifest.ErrInput), mode == "error")
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), mode == "error")
				return
			}
			assert.NoError(t, err)
			var paths []string
			if mode != "immutable" && mode != "mutable" {
				paths = []string{"file"}
			}
			assert.Diff(t, sourceChanges(qualified, id).Paths(), paths)
			for _, reader := range readers {
				assert.Equal(t, reader.opens.Load() == 0, paths == nil)
			}
		})
	}
}

func TestFilter_EqualSelectedSources(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"equal", "authored_file", "reader_reuse", "comparison_policy", "missing_bootstrap", "ownership_only", "payload_edit", "declaration_edit", "artifact_replaced", "declaration_replaced", "equal_labels", "unavailable", "unpaired_pinned"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
			owner := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name}}
			selected := []*store.SourceArtifact{
				{LocalPath: "/head", LocalRoot: "/head", InputAlias: true, FS: fstest.MapFS{
					"apps/cm.yaml": {Data: []byte("same\n")}, "other.txt": {Data: []byte("also same\n")},
				}},
				{LocalPath: "/selected", LocalRoot: "/base", Key: "committed", Content: "committed", FS: fstest.MapFS{
					"apps/cm.yaml": {Data: []byte("same\n")}, "other.txt": {Data: []byte("also same\n")},
				}},
			}
			if mode == "authored_file" {
				for _, input := range selected {
					input.FS.(fstest.MapFS)["flux/source.yaml"] = &fstest.MapFile{Data: []byte("selected declaration\n")}
				}
			}
			if mode == "payload_edit" || mode == "ownership_only" {
				selected[1].FS.(fstest.MapFS)["apps/cm.yaml"].Data = []byte("changed\n")
			}
			if mode == "equal_labels" {
				selected[1].LocalPath = selected[0].LocalPath
			}
			if mode == "comparison_policy" {
				for _, input := range selected {
					input.FS.(fstest.MapFS)["nested/extra.txt"] = &fstest.MapFile{Data: []byte("same\n")}
					input.FS = &relationReader{FS: input.FS}
				}
			}
			if mode == "reader_reuse" {
				for _, input := range selected {
					input.FS = &relationReader{FS: input.FS}
				}
			}
			delta, err := change.Detect(t.Context(), selected[0], selected[1])
			assert.NoError(t, err)
			var wantDelta []string
			if mode == "payload_edit" || mode == "ownership_only" {
				wantDelta = []string{"apps/cm.yaml"}
			}
			assert.Diff(t, delta.Paths(), wantDelta)
			t.Logf("complete selected filesystem comparison: apps/cm.yaml and other.txt; delta=%v", delta.Paths())
			var expectedOpens [2]int64
			if mode == "reader_reuse" {
				for i, input := range selected {
					expectedOpens[i] = input.FS.(*relationReader).opens.Swap(0)
				}
			}
			stores := []*store.Store{store.New(), store.New()}
			roots := []string{"/head", "/base"}
			if mode == "comparison_policy" {
				roots[1] = "/head/nested"
			}
			indexes := make([]*manifest.DiscoveryIndex, 2)
			for i, st := range stores {
				st.AddObjects([]manifest.BaseManifest{source, owner})
				st.SetArtifact(source.Named(), selected[i])
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: roots[i], FS: fstest.MapFS{
					"apps/cm.yaml": {Data: []byte(roots[i])},
				}})
				indexes[i] = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					source.Named(): {File: "flux/source.yaml", Productions: []manifest.Production{{Source: manifest.BootstrapSourceID, File: "flux/source.yaml", Path: roots[i] + "/flux/source.yaml"}}},
					owner.Named(): {File: "flux/ks.yaml", Pinned: i == 1, Refs: []manifest.NamedResource{source.Named()}, Productions: []manifest.Production{
						{Owner: owner.Named(), Source: source.Named(), File: "apps/cm.yaml", Path: selected[i].LocalPath + "/apps/cm.yaml"},
					}},
				}, Claims: map[string][]manifest.NamedResource{}, Readers: map[string][]manifest.FileReader{}}
				if mode == "authored_file" {
					record := indexes[i].Records[source.Named()]
					record.Path, record.File = roots[i]+"/bootstrap/source.yaml", "bootstrap/source.yaml"
					record.Productions = []manifest.Production{{Owner: owner.Named(), Authored: source.Named(), Source: source.Named(), File: "flux/source.yaml", Path: selected[i].LocalPath + "/flux/source.yaml"}}
					indexes[i].Records[source.Named()] = record
				}
				coordinate := "apps/cm.yaml"
				if i == 1 {
					coordinate = "../selected/apps/cm.yaml"
				}
				indexes[i].Claims[filepath.ToSlash(filepath.Dir(coordinate))+"/"] = []manifest.NamedResource{owner.Named()}
				indexes[i].Readers[coordinate] = []manifest.FileReader{{Reader: owner.Named(), Source: source.Named()}}
				if mode == "payload_edit" {
					indexes[i].Claims = nil
					indexes[i].Readers = nil
				}
				if mode == "ownership_only" {
					record := indexes[i].Records[owner.Named()]
					record.Productions = []manifest.Production{{Source: manifest.BootstrapSourceID, File: "flux/ks.yaml", Path: roots[i] + "/flux/ks.yaml"}}
					indexes[i].Records[owner.Named()] = record
				}
			}
			if mode == "comparison_policy" || mode == "reader_reuse" {
				for i, st := range stores {
					st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: roots[i], FS: selected[i].FS})
				}
			}
			if mode == "reader_reuse" {
				other := *source
				other.Name = "other"
				for i, st := range stores {
					st.AddObject(&other)
					art := *selected[i]
					art.LocalPath = "/equal"
					st.SetArtifact(other.Named(), &art)
				}
			}
			paths := []string{"apps/cm.yaml"}
			if mode == "payload_edit" || mode == "ownership_only" {
				paths = nil
			}
			if mode == "authored_file" {
				paths = []string{"bootstrap/source.yaml"}
			}
			if mode == "declaration_edit" {
				paths = []string{"flux/source.yaml"}
			}
			if mode == "unpaired_pinned" {
				selected[0].FS = nil
				paths = []string{"../selected/apps/cm.yaml"}
			}
			if mode == "unavailable" {
				selected[1].FS = nil
			}
			supplied := change.NewSet(paths)
			if mode == "missing_bootstrap" {
				stores[0].SetArtifact(manifest.BootstrapSourceID, nil)
				_, err := change.Qualify(t.Context(), nil, stores[0], stores[1])
				assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
				return
			}
			compareInput := supplied
			if mode == "comparison_policy" || mode == "reader_reuse" {
				compareInput = nil
			}
			changes, err := change.Qualify(t.Context(), compareInput, stores[0], stores[1])
			assert.NoError(t, err)
			assert.Diff(t, supplied.Paths(), change.NewSet(paths).Paths())
			assert.Equal(t, sourceChanges(supplied, source.Named()) == nil, true)
			if mode == "reader_reuse" {
				for i, input := range selected {
					assert.Equal(t, input.FS.(*relationReader).opens.Load(), expectedOpens[i])
				}
			}
			if mode == "comparison_policy" {
				assert.Equal(t, sourceChanges(changes, source.Named()).Len(), 0)
				assert.Equal(t, changes.Contains("nested/extra.txt"), true)
			}
			if mode == "artifact_replaced" {
				for i, st := range stores {
					replacement := *selected[i]
					replacement.Content = "replacement"
					replacement.FS = fstest.MapFS{"apps/cm.yaml": {Data: []byte("replacement\n")}}
					st.SetArtifact(source.Named(), &replacement)
				}
			}
			if mode == "declaration_replaced" {
				replacement := *source
				replacement.URL = "git://fixture.invalid/replaced"
				for _, st := range stores {
					st.AddObject(&replacement)
				}
			}
			var kept [2][]string
			for i := range stores {
				kept[i] = newAdmission(t, admissionConfig{changes: changes, index: indexes[i], inventory: stores[i], root: roots[i]}).KeepNames()
				t.Logf("side %d exact kept identities: %v", i, kept[i])
				want := []string{}
				if mode == "payload_edit" || mode == "ownership_only" || mode == "declaration_edit" || mode == "authored_file" || i == 0 && (mode == "artifact_replaced" || mode == "declaration_replaced" || mode == "unavailable") {
					want = []string{source.Named().String(), owner.Named().String()}
					slices.Sort(want)
				}
				assert.Diff(t, kept[i], want)
			}
			if mode == "equal" || mode == "equal_labels" || mode == "payload_edit" || mode == "declaration_edit" || mode == "authored_file" {
				assert.Diff(t, kept[0], kept[1])
			}
		})
	}
}

func TestFilter_CurrentSideEvidenceAmbiguity(t *testing.T) {
	t.Parallel()
	source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	owner := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name}}
	head := store.New()
	head.AddObjects([]manifest.BaseManifest{source, owner})
	current := &store.SourceArtifact{LocalPath: "/head", LocalRoot: "/head", InputAlias: true, FS: fstest.MapFS{"apps/cm.yaml": {Data: []byte("B\n")}}}
	head.SetArtifact(source.Named(), current)
	head.SetArtifact(manifest.BootstrapSourceID, current)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		source.Named(): {File: "flux/source.yaml", Productions: []manifest.Production{{Source: manifest.BootstrapSourceID, File: "flux/source.yaml", Path: "/head/flux/source.yaml"}}},
		owner.Named():  {File: "flux/ks.yaml", Refs: []manifest.NamedResource{source.Named()}, Productions: []manifest.Production{{Owner: owner.Named(), Source: source.Named(), File: "apps/cm.yaml", Path: "/head/apps/cm.yaml"}}},
	}, Claims: map[string][]manifest.NamedResource{"apps/": {owner.Named()}}, Readers: map[string][]manifest.FileReader{"apps/cm.yaml": {{Reader: owner.Named(), Source: source.Named()}}}}
	head.SetDiscoveryIndex(index)
	selectedNames := []string{source.Named().String(), owner.Named().String()}
	slices.Sort(selectedNames)
	var unqualified [2][]string
	for i, payload := range []string{"B\n", "A\n"} {
		before := store.New()
		before.AddObjects([]manifest.BaseManifest{source, owner})
		input := &store.SourceArtifact{LocalPath: "/base", LocalRoot: "/base", FS: fstest.MapFS{"apps/cm.yaml": {Data: []byte("A\n")}}}
		before.SetArtifact(manifest.BootstrapSourceID, input)
		before.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: "/selected", LocalRoot: "/base", Key: "committed", Content: "committed", FS: fstest.MapFS{"apps/cm.yaml": {Data: []byte(payload)}}})
		paths, err := change.Detect(t.Context(), input, current)
		assert.NoError(t, err)
		assert.Diff(t, paths.Paths(), []string{"apps/cm.yaml"})
		unqualified[i] = newAdmission(t, admissionConfig{changes: paths, index: index, inventory: head, root: ""}).KeepNames()
		assert.Diff(t, unqualified[i], selectedNames)
		evidence, err := change.Qualify(t.Context(), paths, before, head)
		assert.NoError(t, err)
		got := newAdmission(t, admissionConfig{changes: evidence, index: index, inventory: head, root: ""}).KeepNames()
		want := []string{}
		if i == 1 {
			want = selectedNames
		}
		assert.Diff(t, got, want)
		t.Logf("same current Store=%p/index=%p/current artifact=%p; bootstrap delta=%v; peer selected payload=%q; unqualified keep=%v; required paired keep=%v", head, index, current, paths.Paths(), payload, unqualified[i], got)
	}
	assert.Diff(t, unqualified[0], unqualified[1])
}

func TestFilter_PrimaryRolesAndCycles(t *testing.T) {
	t.Parallel()
	st := store.New()
	source := &manifest.GitRepository{Name: "shared", Namespace: "apps"}
	a := &manifest.Kustomization{Name: "a", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name}, PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "from-b", Optional: true}}}}
	b := a.Clone()
	b.Name = "b"
	b.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "from-a", Optional: true}}}
	peer := a.Clone()
	peer.Name, peer.PostBuild = "unrelated", nil
	st.AddObjects([]manifest.BaseManifest{source, a, b, peer})
	fromA := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "from-a"}
	fromB := fromA
	fromB.Name = "from-b"
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		a.Named(): {File: "a.yaml"}, b.Named(): {File: "b.yaml"}, peer.Named(): {File: "peer.yaml"},
		fromA: {Productions: []manifest.Production{{Owner: a.Named(), Source: manifest.BootstrapSourceID}}},
		fromB: {Productions: []manifest.Production{{Owner: b.Named(), Source: manifest.BootstrapSourceID}}},
	}}
	filter := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"a.yaml"}), index: index, inventory: st, root: "/repo"})
	assert.Diff(t, filter.keep, map[manifest.NamedResource]role{a.Named(): primary, b.Named(): primary, fromA: primary, fromB: primary, source.Named(): kept})
	assert.Equal(t, filter.ShouldReconcile(peer.Named()), false)

	c := a.Clone()
	c.Name = "c"
	c.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: fromA.Name, Optional: true}}}
	fromC := fromA
	fromC.Name = "from-c"
	b = b.Clone()
	b.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: fromC.Name, Optional: true}}}
	st.AddObjects([]manifest.BaseManifest{b, c})
	next := *index
	next.Records = maps.Clone(index.Records)
	next.Records[c.Named()] = manifest.DiscoveryRecord{File: "c.yaml"}
	next.Records[fromC] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: c.Named(), Source: manifest.BootstrapSourceID}}}
	filter = newAdmission(t, admissionConfig{changes: change.NewSet([]string{"a.yaml"}), index: &next, inventory: st, root: "/repo"})
	assert.Diff(t, filter.keep, map[manifest.NamedResource]role{a.Named(): primary, b.Named(): primary, c.Named(): primary, fromA: primary, fromB: primary, fromC: primary, source.Named(): kept})
	assert.Equal(t, filter.ShouldReconcile(peer.Named()), false)
}

func TestAddEmitted_RetainedInputsRemainUnconsumed(t *testing.T) {
	t.Parallel()
	st := store.New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	target := &manifest.ConfigMap{Name: "values", Namespace: "apps"}
	consumer := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: target.Name}}}
	st.AddObjects([]manifest.BaseManifest{producer, consumer})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		consumer.Named(): {File: "consumer.yaml"},
		target.Named():   {Productions: []manifest.Production{{Owner: producer.Named()}}},
	}}
	f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"consumer.yaml"}), index: index, inventory: st, root: "/repo"})
	assert.Equal(t, f.ShouldReconcile(producer.Named()), true)
	var reads store.ReadSet
	reads.Init(st, consumer.Named(), nil)
	defer reads.Close()
	f.AddEmitted(manifest.NamedResource{}, store.Child{Obj: consumer}, &reads)
	_, _, next := st.GetEmission(producer.Named())
	defer next.Finish()
	next.Children = []store.Child{{Obj: target}}
	next.Complete = true
	st.RecordEmitted(producer.Named(), &next, nil)
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, reads.GetObject(target.Named()), manifest.BaseManifest(target))
}

func TestFilter_ProducedOrderingTarget(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"kustomization", "helmrelease", "emitted"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			ancestor := testutil.KustomizationID("apps", "ancestor")
			declaration := testutil.KustomizationID("apps", "raw")
			target := testutil.KustomizationID("apps", "generated-raw")
			var consumer manifest.BaseManifest = &manifest.Kustomization{Name: "consumer", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}}}
			if mode == "helmrelease" {
				target = testutil.HelmReleaseID("apps", "generated-raw")
				consumer = &manifest.HelmRelease{Name: "consumer", Namespace: "apps", DependsOn: []helmv2.DependencyReference{{Name: target.Name}}}
			}
			sibling := testutil.KustomizationID("apps", "sibling")
			emitter := testutil.KustomizationID("apps", "emitter")
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				consumer.Named(): {File: "consumer.yaml"},
				emitter:          {File: "emitter.yaml"},
				producer.Named(): {File: "producer.yaml", Parent: ancestor},
				target:           {Productions: []manifest.Production{{Owner: producer.Named(), Declaration: declaration, Authored: declaration}}},
				sibling:          {Productions: []manifest.Production{{Owner: producer.Named()}}},
			}}
			st := store.New()
			st.AddObject(producer)
			changed := "consumer.yaml"
			if mode == "emitted" {
				changed = "emitter.yaml"
			} else {
				st.AddObject(consumer)
			}
			f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{changed}), index: index, inventory: st, root: "/repo"})
			want := map[manifest.NamedResource]role{consumer.Named(): primary, target: kept, producer.Named(): kept, declaration: kept, ancestor: kept}
			if mode == "emitted" {
				assert.Equal(t, f.ShouldReconcile(consumer.Named()), false)
				var reads store.ReadSet
				reads.Init(st, emitter, nil)
				defer reads.Close()
				added := f.AddEmitted(emitter, store.Child{Obj: consumer}, &reads)
				assert.Equal(t, slices.IsSortedFunc(added, manifest.NamedResource.Compare), true)
				assert.Equal(t, len(added), len(want))
				assert.Equal(t, reads.Valid(), true)
				want[emitter] = primary
			}
			assert.Diff(t, f.keep, want)
			assert.Equal(t, f.ShouldReconcile(sibling), false)
			assert.Equal(t, st.GetObject(target), nil)
		})
	}
}

func TestAddEmitted_CapturedAbsentOrderingTarget(t *testing.T) {
	t.Parallel()
	emitter := testutil.KustomizationID("apps", "emitter")
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	target := &manifest.Kustomization{Name: "target", Namespace: "apps"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}}}
	st := store.New()
	st.AddObject(producer)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		emitter:        {File: "emitter.yaml"},
		target.Named(): {Productions: []manifest.Production{{Owner: producer.Named()}}},
	}}
	f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"emitter.yaml"}), index: index, inventory: st, root: "/repo"})
	var reads store.ReadSet
	reads.Init(st, emitter, nil)
	defer reads.Close()
	assert.Equal(t, reads.GetObject(target.Named()), nil)
	assert.Equal(t, reads.Valid(), true)
	st.AddObject(target)
	assert.Equal(t, reads.GetObject(target.Named()), nil)
	f.AddEmitted(emitter, store.Child{Obj: child}, &reads)
	assert.Diff(t, f.keep, map[manifest.NamedResource]role{emitter: primary, child.Named(): primary, target.Named(): kept, producer.Named(): kept})
	assert.Equal(t, reads.Valid(), false)

	fresh := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"emitter.yaml"}), index: index, inventory: st, root: "/repo"})
	var current store.ReadSet
	current.Init(st, emitter, nil)
	defer current.Close()
	fresh.AddEmitted(emitter, store.Child{Obj: child}, &current)
	assert.Diff(t, fresh.keep, map[manifest.NamedResource]role{emitter: primary, child.Named(): primary})
	assert.Equal(t, current.Valid(), true)
}

func TestAddEmitted_CapturedUnpublishedDeclaration(t *testing.T) {
	t.Parallel()
	emitter := testutil.KustomizationID("apps", "emitter")
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	target := &manifest.GitRepository{Name: "input", Namespace: "apps"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: target.Name}}
	proof := manifest.Production{Owner: producer.Named(), Authored: target.Named(), Path: "/repo/input.yaml", File: "input.yaml", Source: manifest.BootstrapSourceID}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		emitter:        {File: "emitter.yaml"},
		target.Named(): {Productions: []manifest.Production{proof}},
	}}
	st := store.New()
	st.AddObjects([]manifest.BaseManifest{producer, target})
	st.SetDiscoveryIndex(index)
	f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"emitter.yaml"}), index: index, inventory: st, root: "/repo"})
	var reads store.ReadSet
	reads.Init(st, emitter, nil)
	defer reads.Close()
	assert.Equal(t, reads.GetObject(target.Named()), manifest.BaseManifest(target))
	assert.Equal(t, reads.GetDeclaration(target.Named()), nil)
	reads.Productions(target.Named(), index)
	assert.Equal(t, reads.Valid(), true)
	st.RecordEmitted(producer.Named(), &store.Emission{Children: []store.Child{{Obj: target, Productions: []manifest.Production{proof}}}, Complete: true}, nil)
	assert.Equal(t, st.GetDeclaration(target.Named()), manifest.BaseManifest(target))
	f.AddEmitted(emitter, store.Child{Obj: child}, &reads)
	assert.Diff(t, f.keep, map[manifest.NamedResource]role{emitter: primary, child.Named(): primary, target.Named(): kept, producer.Named(): kept})
	assert.Equal(t, reads.Valid(), false)

	fresh := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"emitter.yaml"}), index: index, inventory: st, root: "/repo"})
	fresh.AddEmitted(emitter, store.Child{Obj: child}, st)
	assert.Diff(t, fresh.keep, map[manifest.NamedResource]role{emitter: primary, child.Named(): primary, target.Named(): kept})
}

func TestAddEmitted_OrderingDoesNotSelectConsumer(t *testing.T) {
	t.Parallel()
	target := &manifest.Kustomization{Name: "target", Namespace: "apps"}
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}}}
	st := store.New()
	st.AddObject(target)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{target.Named(): {File: "target.yaml"}}}
	f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"target.yaml"}), index: index, inventory: st, root: "/repo"})
	f.AddEmitted(testutil.KustomizationID("apps", "unchanged"), store.Child{Obj: consumer}, st)
	assert.Diff(t, f.keep, map[manifest.NamedResource]role{target.Named(): primary})
}

func TestAddEmitted_DoesNotReadIncomingDeclaration(t *testing.T) {
	t.Parallel()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	target := &manifest.GitRepository{Name: "input", Namespace: "apps"}
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: target.Name}}
	proof := manifest.Production{Owner: producer.Named(), Authored: target.Named(), Path: "/repo/input.yaml", File: "input.yaml", Source: manifest.BootstrapSourceID}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		consumer.Named(): {File: "consumer.yaml"},
		target.Named():   {Productions: []manifest.Production{proof}},
	}}
	st := store.New()
	st.AddObjects([]manifest.BaseManifest{producer, consumer})
	f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"consumer.yaml"}), index: index, inventory: st, root: "/repo"})
	assert.Equal(t, f.keep[target.Named()], kept)
	var reads store.ReadSet
	reads.Init(st, producer.Named(), nil)
	defer reads.Close()
	child := store.Child{Obj: target, Productions: []manifest.Production{proof}}
	f.AddEmitted(producer.Named(), child, &reads)
	assert.Equal(t, reads.Valid(), true)
	st.RecordEmitted(producer.Named(), &store.Emission{Children: []store.Child{child}, Complete: true}, nil)
	assert.Equal(t, reads.Valid(), true)
}

func (c *countedProductions) fixtureStore() *store.Store { return c.Store }
func (p *pausedIncoming) fixtureStore() *store.Store     { return p.Store }
