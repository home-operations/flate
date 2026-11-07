package loader

import (
	"fmt"
	"iter"
	"os"
	"path/filepath"
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
	for _, mode := range []string{
		"empty", "unknown", "positive", "peer", "static", "primary", "exposed_primary", "parent_inventory", "parent_unknown",
	} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testContributorReplacement(t, mode)
		})
	}
}

func TestRefineHistory_SourceQualifiedRoots(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{
		"mutable",
		"alias",
		"pinned",
		"paired_pinned",
		"external",
		"unavailable",
		"component",
		"symlink",
		"unresolved",
		"remote",
		"escaped",
		"source_root",
		"symlink_escape",
	} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			owner := testutil.KustomizationID("apps", "emitter")
			child := testutil.KustomizationID("apps", "child")
			target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "old"}
			consumer := &manifest.Kustomization{
				Name:      "consumer",
				Namespace: "apps",
				PostBuild: &kustomizev1.PostBuild{
					SubstituteFrom: []manifest.SubstituteReference{{Kind: target.Kind, Name: target.Name}},
				},
			}
			source := &manifest.GitRepository{Name: "content", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{source, consumer})
			historySourceInputResult := historySourceInput(t, mode, root)
			art, changed, roots := historySourceInputResult.artifact, historySourceInputResult.changed, historySourceInputResult.roots
			st.SetArtifact(source.Named(), art)
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				owner: {
					File: "owner.yaml",
				}, consumer.Named(): {
					File: "consumer.yaml",
				}, target: {
					Productions: []manifest.Production{{Owner: owner}},
				},
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
				changes, err = change.Qualify(
					t.Context(),
					change.NewSet([]string{"consumer.yaml"}), [2]*store.Store{peer, st},
					manifest.WorkScope{},
				)
				assert.NoError(t, err)
			}
			f := newAdmission(t, admissionConfig{changes: changes, index: index, inventory: st, root: root})
			record := manifest.DiscoveryRecord{
				Refs:         []manifest.NamedResource{source.Named()},
				ContentRoots: map[manifest.NamedResource][]string{source.Named(): roots},
				Productions:  []manifest.Production{{Owner: owner, Source: manifest.BootstrapSourceID}},
			}
			f.RefineHistory(
				map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{
					owner: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
						yield(child, record)
					},
				},
				map[manifest.NamedResource]bool{owner: true},
			)
			selected := mode == "mutable" || mode == "paired_pinned" || mode == "alias" || mode == "component" ||
				mode == "symlink" ||
				mode == "source_root" ||
				mode == "unresolved" ||
				mode == "escaped"
			assert.Equal(t, f.Reconcile(child), selected)
			assert.Equal(t, f.Reconcile(source.Named()), selected)
			assert.Equal(t, st.GetObject(child) == nil, true)
			assert.Equal(t, len(index.Records), 3)
		},
		)
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
			st.SetArtifact(
				source.Named(),
				&store.SourceArtifact{
					FS:         fstest.MapFS{"apps/red/file": {Data: []byte("value")}},
					LocalPath:  "/mounted/" + source.Name,
					LocalRoot:  filepath.Join(root, source.Name),
					InputAlias: true,
				},
			)
			history[owners[i]] = func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
				yield(
					child,
					manifest.DiscoveryRecord{
						Refs:         []manifest.NamedResource{source.Named()},
						ContentRoots: map[manifest.NamedResource][]string{source.Named(): {"apps/red"}},
						Productions:  []manifest.Production{{Owner: owners[i], Source: manifest.BootstrapSourceID}},
					},
				)
			}
		}
		f := newAdmission(
			t,
			admissionConfig{changes: change.NewSet([]string{"z/apps/red/file"}), index: nil, inventory: st, root: root},
		)
		f.RefineHistory(history, map[manifest.NamedResource]bool{owners[0]: true, owners[1]: true})
		assert.Equal(t, f.Reconcile(child), true)
		for _, owner := range owners {
			assert.Equal(t, f.Reconcile(owner), true)
		}
		for _, source := range sources {
			assert.Equal(t, f.Reconcile(source.Named()), true)
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
			owner := &manifest.Kustomization{
				Name:      "owner",
				Namespace: "apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name},
			}
			target := testutil.KustomizationID("apps", "predicted")
			st.AddObjects([]manifest.BaseManifest{source, owner})
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				source.Named(): {File: "source.yaml", Productions: []manifest.Production{{Owner: owner.Named()}}},
				owner.Named(): {
					File:        "owner.yaml",
					Refs:        []manifest.NamedResource{source.Named()},
					Productions: []manifest.Production{{Owner: owner.Named()}},
				},
				target: {Productions: []manifest.Production{{Owner: owner.Named()}}},
			}}
			filter := newAdmission(
				t,
				admissionConfig{changes: change.NewSet([]string{changed}), index: index, inventory: st, root: "/repo"},
			)
			assert.Equal(t, filter.role(owner.Named()), primary)
			filter.RefineHistory(
				map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{
					owner.Named(): func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {},
				},
				map[manifest.NamedResource]bool{owner.Named(): true},
			)
			assert.Equal(t, filter.role(owner.Named()), primary)
			assert.Equal(t, filter.Reconcile(target), true)
		})
	}
}

func TestAddEmitted_ReplacesHistoricalOrdering(t *testing.T) {
	t.Parallel()
	st := store.New()
	emitter := testutil.KustomizationID("apps", "emitter")
	st.AddObject(&manifest.Kustomization{Name: emitter.Name, Namespace: emitter.Namespace})
	historyOwner := testutil.KustomizationID("apps", "history-owner")
	oldTarget := testutil.KustomizationID("apps", "old-target")
	target := testutil.KustomizationID("apps", "target")
	oldProducer := testutil.KustomizationID("apps", "old-producer")
	producer := testutil.KustomizationID("apps", "producer")
	stale := &manifest.Kustomization{
		Name:      "child",
		Namespace: "apps",
		DependsOn: []kustomizev1.DependencyReference{{Name: oldTarget.Name}},
	}
	st.AddObject(stale)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		emitter:   {File: "emitter.yaml"},
		target:    {Productions: []manifest.Production{{Owner: producer}}},
		oldTarget: {Productions: []manifest.Production{{Owner: oldProducer}}},
	}}
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"emitter.yaml"}), index: index, inventory: st, root: "/repo"},
	)
	f.RefineHistory(
		map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{
			historyOwner: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
				yield(
					stale.Named(),
					manifest.DiscoveryRecord{
						Refs:        []manifest.NamedResource{oldTarget},
						Productions: []manifest.Production{{Owner: historyOwner}},
					},
				)
			},
		},
		nil,
	)
	assert.Equal(t, f.Reconcile(stale.Named()), false)
	incoming := stale.Clone()
	incoming.DependsOn = []kustomizev1.DependencyReference{{Name: target.Name}}
	var reads store.ReadSet
	reads.Init(st, emitter, nil)
	defer reads.Close()
	f.AddEmitted(emitter, new(store.Child{Obj: incoming, Productions: []manifest.Production{{Owner: emitter}}}), &reads)
	assert.Diff(
		t,
		f.roles(),
		map[manifest.NamedResource]role{emitter: primary, incoming.Named(): primary, target: kept, producer: kept},
	)
	assert.Equal(t, st.GetObject(incoming.Named()), manifest.BaseManifest(stale))
	assert.Equal(t, reads.Valid(), true)
}

type historyInput struct {
	artifact *store.SourceArtifact
	changed  string
	roots    []string
}

func historySourceInput(t *testing.T, mode, root string) historyInput {
	t.Helper()
	art := &store.SourceArtifact{
		FS: fstest.MapFS{
			"apps/red/config.yaml": {Data: []byte("changed")},
			"parts/shared/file":    {Data: []byte("component")},
		},
		LocalPath: root,
	}
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
		assert.NoError(t, os.Symlink("../../parts/deep", filepath.Join(root, "apps", "red", "link")))
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
	return historyInput{artifact: art, changed: changed, roots: roots}
}

func TestRefineHistory_PeerReaderContentSources(t *testing.T) {
	t.Parallel()
	t.Run("reader_sources", func(t *testing.T) {
		t.Parallel()
		testPeerReaderContentSources(t)
	})
}

func testContributorReplacement(t *testing.T, mode string) {
	t.Helper()
	st := store.New()
	owner := testutil.KustomizationID("apps", "owner")
	peer := testutil.KustomizationID("apps", "peer")
	target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	consumer := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: target.Kind, Name: target.Name}},
		},
	}
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
		st.SetArtifact(
			manifest.BootstrapSourceID,
			&store.SourceArtifact{LocalPath: "/repo", FS: fstest.MapFS{consumerFile: {Data: []byte("changed")}}},
		)
		record := index.Records[consumer.Named()]
		record.File = consumerFile
		index.Records[consumer.Named()] = record
	}
	changes := []string{consumerFile}
	if mode == "primary" {
		changes = append(changes, "owner.yaml")
	}
	f := newAdmission(t, admissionConfig{changes: change.NewSet(changes), index: index, inventory: st, root: "/repo"})
	assert.Equal(t, f.Reconcile(owner), true)
	calls := 0
	records := func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		calls++
		if mode == "exposed_primary" {
			yield(
				owner,
				manifest.DiscoveryRecord{
					Refs:        []manifest.NamedResource{consumer.Named()},
					Productions: []manifest.Production{{Owner: owner}},
				},
			)
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
			yield(
				owner,
				manifest.DiscoveryRecord{
					Refs:         []manifest.NamedResource{manifest.BootstrapSourceID},
					Productions:  []manifest.Production{{Owner: parent, Source: manifest.BootstrapSourceID}},
					ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"apps"}},
				},
			)
		}
		precise[parent] = true
	}
	f.RefineHistory(history, precise)
	assert.Equal(
		t,
		f.Reconcile(owner),
		mode == "unknown" || mode == "positive" || mode == "primary" || mode == "exposed_primary" ||
			mode == "parent_unknown",
	)
	assert.Equal(t, f.Reconcile(peer), mode == "peer")
	assert.Equal(
		t,
		f.Reconcile(extra),
		mode == "primary" || mode == "exposed_primary" || mode == "parent_unknown",
	)
	assert.Equal(t, f.Reconcile(consumer.Named()), true)
	assert.Equal(t, f.Reconcile(target), true)
	assert.Equal(t, len(index.Records[target].Productions), len(proofs))
	if mode == "unknown" || mode == "parent_unknown" {
		assert.Equal(t, calls, 0)
	} else {
		assert.Equal(t, calls, 1)
	}
}

func testPeerReaderContentSources(t *testing.T) {
	t.Helper()
	var stores [2]*store.Store
	var roots [2]string
	sources := []*manifest.GitRepository{
		{Name: "changed", Namespace: "apps"},
		{Name: "unchanged", Namespace: "apps"},
	}
	owner := testutil.KustomizationID("apps", "emitter")
	retained := []manifest.NamedResource{
		testutil.HelmReleaseID("apps", "retained-changed"),
		testutil.HelmReleaseID("apps", "retained-unchanged"),
	}
	projected := []manifest.NamedResource{
		testutil.KustomizationID("apps", "projected-changed"),
		testutil.KustomizationID("apps", "projected-unchanged"),
	}
	for side := range stores {
		root := t.TempDir()
		roots[side] = root
		st := store.New()
		stores[side] = st
		st.SetArtifact(
			manifest.BootstrapSourceID,
			&store.SourceArtifact{
				LocalPath: root,
				FS: fstest.MapFS{
					".selected/apps/red/removed.yaml": {Data: []byte("unchanged bootstrap collision")},
				},
			},
		)
		for i, source := range sources {
			st.AddObject(source)
			files := fstest.MapFS{"apps/red/stable.yaml": {Data: []byte("stable")}}
			if i == 1 || side == 0 {
				files["apps/red/removed.yaml"] = &fstest.MapFile{Data: []byte("removed only in changed source")}
			}
			st.SetArtifact(
				source.Named(),
				&store.SourceArtifact{
					LocalPath: filepath.Join(root, ".selected"),
					LocalRoot: root,
					Key:       fmt.Sprintf("%s-%d", source.Name, side),
					Content:   fmt.Sprintf("%s-%d", source.Name, side),
					FS:        files,
				},
			)
		}
	}
	changes, err := change.Qualify(t.Context(), nil, [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	assert.Diff(t, sourceChanges(changes, sources[0].Named()).Paths(), []string{"apps/red/removed.yaml"})
	assert.Equal(t, sourceChanges(changes, sources[1].Named()).Len(), 0)
	var indexes [2]*manifest.DiscoveryIndex
	for side, st := range stores {
		index := &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				owner:       {File: "flux/emitter.yaml"},
				retained[0]: {File: "flux/retained-changed.yaml"},
				retained[1]: {File: "flux/retained-unchanged.yaml"},
			},
			Readers: map[string][]manifest.FileReader{
				".selected/apps/red/removed.yaml": {
					{Reader: retained[0], Source: sources[0].Named()},
					{Reader: retained[1], Source: sources[1].Named()},
				},
			},
		}
		filter := newAdmission(t, admissionConfig{changes: changes, index: index, inventory: st, root: roots[side]})
		filter.RefineHistory(
			map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{
				owner: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
					for i, source := range sources {
						if !yield(
							projected[i],
							manifest.DiscoveryRecord{
								Refs:         []manifest.NamedResource{source.Named()},
								ContentRoots: map[manifest.NamedResource][]string{source.Named(): {"apps/red"}},
								Productions: []manifest.Production{
									{Owner: owner, Source: manifest.BootstrapSourceID},
								},
							},
						) {
							return
						}
					}
				},
			},
			map[manifest.NamedResource]bool{owner: true},
		)
		indexes[side] = index
		for i := range sources {
			assert.Equal(t, filter.Reconcile(retained[i]), i == 0)
			assert.Equal(t, filter.Reconcile(projected[i]), i == 0)
			assert.Equal(t, st.GetObject(projected[i]), nil)
		}
	}
	for side, st := range stores {
		filter := newAdmission(
			t,
			admissionConfig{changes: changes, index: indexes[side], inventory: st, root: roots[side]},
		)
		assert.Equal(t, filter.Reconcile(retained[0]), true)
		assert.Equal(t, filter.Reconcile(retained[1]), false)
		assert.Equal(t, len(indexes[side].Records), 3)
	}
}
