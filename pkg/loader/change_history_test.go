package loader

import (
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRefineHistory_ContributorReplacement(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"empty", "unknown", "positive", "peer", "static", "primary", "exposed_primary", "parent_inventory", "parent_unknown"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := testutil.KustomizationID("apps", "owner")
			peer := testutil.KustomizationID("apps", "peer")
			target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: target.Kind, Name: target.Name}}}}
			st.AddObject(consumer)
			proofs := []manifest.Production{{Owner: owner}}
			if mode == "peer" {
				proofs = append(proofs, manifest.Production{Owner: peer})
			}
			if mode == "static" {
				proofs = append(proofs, manifest.Production{Source: manifest.BootstrapSourceID, File: "static.yaml"})
			}
			extra := testutil.KustomizationID("apps", "extra")
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				owner: {File: "owner.yaml"}, consumer.Named(): {File: "consumer.yaml"}, target: {Productions: proofs},
				extra: {Productions: []manifest.Production{{Owner: owner}}},
			}}
			consumerFile := "consumer.yaml"
			if mode == "parent_inventory" || mode == "parent_unknown" {
				consumerFile = "apps/consumer.yaml"
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: "/repo", FS: fstest.MapFS{consumerFile: {Data: []byte("changed")}}})
				record := index.Records[consumer.Named()]
				record.File = consumerFile
				index.Records[consumer.Named()] = record
			}
			changes := []string{consumerFile}
			if mode == "primary" {
				changes = append(changes, "owner.yaml")
			}
			f := newAdmission(t, admissionConfig{changes: change.NewSet(changes), index: index, inventory: st, root: "/repo"})
			assert.Equal(t, f.ShouldReconcile(owner), true)
			calls := 0
			records := func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
				calls++
				if mode == "exposed_primary" {
					yield(owner, manifest.DiscoveryRecord{Refs: []manifest.NamedResource{consumer.Named()}, Productions: []manifest.Production{{Owner: owner}}})
				}
			}
			history := map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{owner: records}
			if mode == "unknown" || mode == "parent_unknown" {
				history[owner] = nil
			}
			precise := map[manifest.NamedResource]bool{owner: mode != "positive"}
			if mode == "parent_inventory" || mode == "parent_unknown" {
				parent := testutil.KustomizationID("apps", "parent")
				history[parent] = func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
					yield(owner, manifest.DiscoveryRecord{Refs: []manifest.NamedResource{manifest.BootstrapSourceID}, Productions: []manifest.Production{{Owner: parent, Source: manifest.BootstrapSourceID}}, ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"apps"}}})
				}
				precise[parent] = true
			}
			f.RefineHistory(history, precise)
			assert.Equal(t, f.ShouldReconcile(owner), mode == "unknown" || mode == "positive" || mode == "primary" || mode == "exposed_primary" || mode == "parent_unknown")
			assert.Equal(t, f.ShouldReconcile(peer), mode == "peer")
			assert.Equal(t, f.ShouldReconcile(extra), mode == "primary" || mode == "exposed_primary" || mode == "parent_unknown")
			assert.Equal(t, f.ShouldReconcile(consumer.Named()), true)
			assert.Equal(t, f.ShouldReconcile(target), true)
			assert.Equal(t, len(index.Records[target].Productions), len(proofs))
			if mode == "static" {
				assert.Equal(t, len(f.last.productions(target)), 1)
			}
			if mode == "unknown" || mode == "parent_unknown" {
				assert.Equal(t, calls, 0)
			} else {
				assert.Equal(t, calls, 1)
			}
		})
	}
}

func TestRefineHistory_RestoredSourceQualification(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := testutil.KustomizationID("apps", "owner")
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	source := &manifest.GitRepository{Name: "pinned", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{source, child})
	st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: "/pinned", LocalRoot: "/repo", Key: "commit", Content: "commit", FS: fstest.MapFS{"child.yaml": {Data: []byte("unchanged")}}})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		owner:         {File: "owner.yaml"},
		child.Named(): {File: "child.yaml", Builder: "builder.yaml", Declaration: child, Productions: []manifest.Production{{Owner: owner, Source: source.Named(), File: "child.yaml"}}},
	}}
	f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"owner.yaml", "child.yaml"}), index: index, inventory: st, root: "/repo"})
	f.RefineHistory(map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{owner: func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}}, map[manifest.NamedResource]bool{owner: true})
	assert.Equal(t, f.last.replace[owner], false)
	assert.Equal(t, f.ShouldReconcile(owner), true)
	assert.Equal(t, f.last.states[child.Named()].changed, false)
	assert.Equal(t, f.ShouldReconcile(child.Named()), false)
}

func TestRefineHistory_SourceQualifiedRoots(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"mutable", "alias", "pinned", "paired_pinned", "external", "unavailable", "component", "symlink", "unresolved", "remote", "escaped", "source_root", "symlink_escape"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			owner := testutil.KustomizationID("apps", "emitter")
			child := testutil.KustomizationID("apps", "child")
			target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "old"}
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: target.Kind, Name: target.Name}}}}
			source := &manifest.GitRepository{Name: "content", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{source, consumer})
			art := &store.SourceArtifact{FS: fstest.MapFS{"apps/red/config.yaml": {Data: []byte("changed")}, "parts/shared/file": {Data: []byte("component")}}, LocalPath: root}
			changed := "apps/red/config.yaml"
			roots := []string{"apps/red"}
			if mode == "alias" {
				art.LocalPath, art.LocalRoot, art.InputAlias = "/mounted/content", filepath.Join(root, "subtree"), true
				changed = "subtree/" + changed
			}
			if mode == "pinned" || mode == "paired_pinned" {
				art.LocalPath, art.LocalRoot, art.Key, art.Content = "/mounted/content", root, "commit", "commit"
				changed = "../mounted/content/apps/red/config.yaml"
			}
			if mode == "external" {
				art.LocalPath, art.LocalRoot = "/external", "/external"
			}
			if mode == "unavailable" {
				art.FS = nil
			}
			if mode == "component" {
				roots, changed = []string{"apps/red", "apps/red/../../parts/shared"}, "parts/shared/file"
			}
			if mode == "symlink" {
				testutil.WriteFile(t, root, "apps/red/file", "same")
				testutil.WriteFile(t, root, "parts/deep/placeholder", "same")
				testutil.WriteFile(t, root, "parts/shared/file", "changed")
				assert.NoError(t, os.Symlink("../../parts/deep", filepath.Join(root, "apps/red/link")))
				art.FS = os.DirFS(root)
				roots, changed = []string{"apps/red/link/../shared"}, "parts/shared/file"
			}
			if mode == "symlink_escape" {
				assert.NoError(t, os.Symlink("..", filepath.Join(root, "escape")))
				art.FS = os.DirFS(root)
				roots = []string{"escape"}
			}
			if mode == "unresolved" {
				roots = []string{"absent/../apps/red"}
			}
			if mode == "remote" {
				roots = []string{"https://example.invalid/component"}
			}
			if mode == "escaped" {
				roots = []string{"../../outside"}
			}
			if mode == "source_root" {
				roots = []string{"."}
			}
			st.SetArtifact(source.Named(), art)
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				owner: {File: "owner.yaml"}, consumer.Named(): {File: "consumer.yaml"}, target: {Productions: []manifest.Production{{Owner: owner}}},
			}}
			changes := change.NewSet([]string{changed, "consumer.yaml"})
			if mode == "paired_pinned" {
				previous := *art
				previous.Revision = "before"
				previous.FS = fstest.MapFS{"apps/red/config.yaml": {Data: []byte("before")}}
				art.Revision = "after"
				peer := store.New()
				peer.AddObject(source)
				peer.SetArtifact(source.Named(), &previous)
				var err error
				changes, err = change.Qualify(t.Context(), change.NewSet([]string{"consumer.yaml"}), peer, st)
				assert.NoError(t, err)
			}
			f := newAdmission(t, admissionConfig{changes: changes, index: index, inventory: st, root: root})
			record := manifest.DiscoveryRecord{Refs: []manifest.NamedResource{source.Named()}, ContentRoots: map[manifest.NamedResource][]string{source.Named(): roots}, Productions: []manifest.Production{{Owner: owner, Source: manifest.BootstrapSourceID}}}
			f.RefineHistory(map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{owner: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) { yield(child, record) }}, map[manifest.NamedResource]bool{owner: true})
			selected := mode == "mutable" || mode == "paired_pinned" || mode == "alias" || mode == "component" || mode == "symlink" || mode == "source_root" || mode == "unresolved" || mode == "escaped"
			assert.Equal(t, f.ShouldReconcile(child), selected)
			assert.Equal(t, f.ShouldReconcile(source.Named()), selected)
			assert.Equal(t, st.GetObject(child) == nil, true)
			assert.Equal(t, len(index.Records), 3)
			remaining := f.last.productions(target)
			assert.Equal(t, slices.ContainsFunc(remaining, func(p manifest.Production) bool { return p.Owner == owner }), mode == "unavailable" || mode == "unresolved" || mode == "remote" || mode == "escaped" || mode == "symlink_escape")
		})
	}
}

func TestRefineHistory_PeerContentSources(t *testing.T) {
	t.Parallel()
	t.Run("peers", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		st := store.New()
		child := testutil.KustomizationID("apps", "child")
		owners := []manifest.NamedResource{testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "z")}
		sources := []*manifest.GitRepository{{Name: "a", Namespace: "apps"}, {Name: "z", Namespace: "apps"}}
		history := map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{}
		for i, source := range sources {
			st.AddObject(source)
			st.SetArtifact(source.Named(), &store.SourceArtifact{FS: fstest.MapFS{"apps/red/file": {Data: []byte("value")}}, LocalPath: "/mounted/" + source.Name, LocalRoot: filepath.Join(root, source.Name), InputAlias: true})
			history[owners[i]] = func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
				yield(child, manifest.DiscoveryRecord{Refs: []manifest.NamedResource{source.Named()}, ContentRoots: map[manifest.NamedResource][]string{source.Named(): {"apps/red"}}, Productions: []manifest.Production{{Owner: owners[i], Source: manifest.BootstrapSourceID}}})
			}
		}
		f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"z/apps/red/file"}), index: nil, inventory: st, root: root})
		f.RefineHistory(history, map[manifest.NamedResource]bool{owners[0]: true, owners[1]: true})
		assert.Equal(t, f.ShouldReconcile(child), true)
		for _, owner := range owners {
			assert.Equal(t, f.ShouldReconcile(owner), true)
		}
		for _, source := range sources {
			assert.Equal(t, f.ShouldReconcile(source.Named()), true)
		}
	})
	t.Run("reader_sources", func(t *testing.T) {
		t.Parallel()
		var stores [2]*store.Store
		var roots [2]string
		sources := []*manifest.GitRepository{{Name: "changed", Namespace: "apps"}, {Name: "unchanged", Namespace: "apps"}}
		owner := testutil.KustomizationID("apps", "emitter")
		retained := []manifest.NamedResource{testutil.HelmReleaseID("apps", "retained-changed"), testutil.HelmReleaseID("apps", "retained-unchanged")}
		projected := []manifest.NamedResource{testutil.KustomizationID("apps", "projected-changed"), testutil.KustomizationID("apps", "projected-unchanged")}
		for side := range stores {
			root := t.TempDir()
			roots[side] = root
			st := store.New()
			stores[side] = st
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{".selected/apps/red/removed.yaml": {Data: []byte("unchanged bootstrap collision")}}})
			for i, source := range sources {
				st.AddObject(source)
				files := fstest.MapFS{"apps/red/stable.yaml": {Data: []byte("stable")}}
				if i == 1 || side == 0 {
					files["apps/red/removed.yaml"] = &fstest.MapFile{Data: []byte("removed only in changed source")}
				}
				st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: filepath.Join(root, ".selected"), LocalRoot: root, Key: fmt.Sprintf("%s-%d", source.Name, side), Content: fmt.Sprintf("%s-%d", source.Name, side), FS: files})
			}
		}
		changes, err := change.Qualify(t.Context(), nil, stores[0], stores[1])
		assert.NoError(t, err)
		assert.Diff(t, sourceChanges(changes, sources[0].Named()).Paths(), []string{"apps/red/removed.yaml"})
		assert.Equal(t, sourceChanges(changes, sources[1].Named()).Len(), 0)
		var indexes [2]*manifest.DiscoveryIndex
		for side, st := range stores {
			index := &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{owner: {File: "flux/emitter.yaml"}, retained[0]: {File: "flux/retained-changed.yaml"}, retained[1]: {File: "flux/retained-unchanged.yaml"}},
				Readers: map[string][]manifest.FileReader{".selected/apps/red/removed.yaml": {{Reader: retained[0], Source: sources[0].Named()}, {Reader: retained[1], Source: sources[1].Named()}}},
			}
			filter := newAdmission(t, admissionConfig{changes: changes, index: index, inventory: st, root: roots[side]})
			filter.RefineHistory(map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{owner: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
				for i, source := range sources {
					if !yield(projected[i], manifest.DiscoveryRecord{Refs: []manifest.NamedResource{source.Named()}, ContentRoots: map[manifest.NamedResource][]string{source.Named(): {"apps/red"}}, Productions: []manifest.Production{{Owner: owner, Source: manifest.BootstrapSourceID}}}) {
						return
					}
				}
			}}, map[manifest.NamedResource]bool{owner: true})
			indexes[side] = index

			for i, source := range sources {
				assert.Diff(t, filter.last.record(projected[i]).ContentRoots[source.Named()], []string{"apps/red"})
				assert.Equal(t, filter.ShouldReconcile(retained[i]), i == 0)
				assert.Equal(t, filter.ShouldReconcile(projected[i]), i == 0)
				assert.Equal(t, st.GetObject(projected[i]), nil)
			}
		}
		for side, st := range stores {
			filter := newAdmission(t, admissionConfig{changes: changes, index: indexes[side], inventory: st, root: roots[side]})
			assert.Equal(t, filter.ShouldReconcile(retained[0]), true)
			assert.Equal(t, filter.ShouldReconcile(retained[1]), false)
			assert.Equal(t, len(indexes[side].Records), 3)
		}

	})
}

func TestRefineHistory_PreservesDeclarationEvidence(t *testing.T) {
	t.Parallel()
	for _, changed := range []string{"owner.yaml", "source.yaml"} {
		t.Run(changed, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name}}
			target := testutil.KustomizationID("apps", "predicted")
			st.AddObjects([]manifest.BaseManifest{source, owner})
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				source.Named(): {File: "source.yaml", Productions: []manifest.Production{{Owner: owner.Named()}}},
				owner.Named():  {File: "owner.yaml", Refs: []manifest.NamedResource{source.Named()}, Productions: []manifest.Production{{Owner: owner.Named()}}},
				target:         {Productions: []manifest.Production{{Owner: owner.Named()}}},
			}}
			filter := newAdmission(t, admissionConfig{changes: change.NewSet([]string{changed}), index: index, inventory: st, root: "/repo"})
			assert.Equal(t, filter.keep[owner.Named()], primary)
			filter.RefineHistory(map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{owner.Named(): func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}}, map[manifest.NamedResource]bool{owner.Named(): true})
			assert.Equal(t, filter.keep[owner.Named()], primary)
			assert.Equal(t, filter.ShouldReconcile(target), true)
		})
	}
}

func TestRefineHistory_OrderingClosure(t *testing.T) {
	t.Parallel()
	st := store.New()
	source := manifest.BootstrapSourceID
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	target := testutil.KustomizationID("apps", "target")
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}}}
	st.AddObjects([]manifest.BaseManifest{producer, consumer})
	st.SetArtifact(source, &store.SourceArtifact{LocalPath: "/repo", FS: fstest.MapFS{"app/cm.yaml": {Data: []byte("unchanged")}}})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		consumer.Named(): {File: "consumer.yaml"},
		target:           {Productions: []manifest.Production{{Owner: producer.Named()}}},
	}}
	f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"consumer.yaml"}), index: index, inventory: st, root: "/repo"})
	historyOwner := testutil.KustomizationID("apps", "history-owner")
	f.RefineHistory(map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{historyOwner: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		yield(consumer.Named(), manifest.DiscoveryRecord{Refs: []manifest.NamedResource{source}, ContentRoots: map[manifest.NamedResource][]string{source: {"app"}}})
	}}, nil)
	assert.Equal(t, f.last.record(consumer.Named()).ContentRoots != nil, true)
	assert.Diff(t, f.last.record(consumer.Named()).Refs, []manifest.NamedResource{source})
	assert.Diff(t, f.keep, map[manifest.NamedResource]role{consumer.Named(): primary, target: kept, producer.Named(): kept, source: kept})
}

func TestRefineHistory_OrderingReferences(t *testing.T) {
	t.Parallel()
	for _, changed := range []string{"consumer.yaml", "target.yaml"} {
		t.Run(changed, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			source := manifest.BootstrapSourceID
			target := &manifest.Kustomization{Name: "target", Namespace: "apps"}
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}}}
			st.AddObjects([]manifest.BaseManifest{target, consumer})
			st.SetArtifact(source, &store.SourceArtifact{LocalPath: "/repo", FS: fstest.MapFS{"app/cm.yaml": {Data: []byte("unchanged")}}})
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				consumer.Named(): {File: "consumer.yaml"},
				target.Named():   {File: "target.yaml"},
			}}
			f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{changed}), index: index, inventory: st, root: "/repo"})
			historyOwner := testutil.KustomizationID("apps", "history-owner")
			f.RefineHistory(map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{historyOwner: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
				yield(consumer.Named(), manifest.DiscoveryRecord{Refs: []manifest.NamedResource{source, target.Named()}, ContentRoots: map[manifest.NamedResource][]string{source: {"app"}}})
			}}, nil)
			assert.Equal(t, f.last.record(consumer.Named()).ContentRoots != nil, true)
			assert.Diff(t, f.last.record(consumer.Named()).Refs, []manifest.NamedResource{source, target.Named()})
			want := map[manifest.NamedResource]role{}
			if changed == "consumer.yaml" {
				want[consumer.Named()], want[source] = primary, kept
			} else {
				want[target.Named()] = primary
			}
			assert.Diff(t, f.keep, want)
		})
	}
}

func TestAddEmitted_ReplacesHistoricalOrdering(t *testing.T) {
	t.Parallel()
	st := store.New()
	emitter := testutil.KustomizationID("apps", "emitter")
	historyOwner := testutil.KustomizationID("apps", "history-owner")
	oldTarget := testutil.KustomizationID("apps", "old-target")
	target := testutil.KustomizationID("apps", "target")
	oldProducer := testutil.KustomizationID("apps", "old-producer")
	producer := testutil.KustomizationID("apps", "producer")
	stale := &manifest.Kustomization{Name: "child", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: oldTarget.Name}}}
	st.AddObject(stale)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		emitter:   {File: "emitter.yaml"},
		target:    {Productions: []manifest.Production{{Owner: producer}}},
		oldTarget: {Productions: []manifest.Production{{Owner: oldProducer}}},
	}}
	f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"emitter.yaml"}), index: index, inventory: st, root: "/repo"})
	f.RefineHistory(map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{historyOwner: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		yield(stale.Named(), manifest.DiscoveryRecord{Refs: []manifest.NamedResource{oldTarget}, Productions: []manifest.Production{{Owner: historyOwner}}})
	}}, nil)
	assert.Equal(t, f.ShouldReconcile(stale.Named()), false)
	incoming := stale.Clone()
	incoming.DependsOn = []kustomizev1.DependencyReference{{Name: target.Name}}
	var reads store.ReadSet
	reads.Init(st, emitter, nil)
	defer reads.Close()
	f.AddEmitted(emitter, store.Child{Obj: incoming, Productions: []manifest.Production{{Owner: emitter}}}, &reads)
	assert.Diff(t, f.keep, map[manifest.NamedResource]role{emitter: primary, incoming.Named(): primary, target: kept, producer: kept})
	assert.Equal(t, st.GetObject(incoming.Named()), manifest.BaseManifest(stale))
	assert.Equal(t, reads.Valid(), true)
}
