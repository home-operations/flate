package loader

import (
	"reflect"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_OrderingEmittedPathProof_LiveOwnership(t *testing.T) {
	t.Parallel()
	l, source, factory, owner, child := admissionMembershipFixture(t)
	record := l.Index.Records[child]
	object := l.CatalogObject(child)
	if !l.AdmitsRoot(object, record.Path) {
		t.Fatal("ownerless indexed declaration has no static root scope")
	}
	static := l.roots.get(child)
	proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	assert.Equal(
		t,
		slices.ContainsFunc(proposal.Objects, func(c store.Child) bool { return c.Obj.Named() == child }),
		true,
	)
	_, _, next := l.Store.GetEmission(factory.Named())
	next.Children, next.Complete = []store.Child{{Obj: owner}}, true
	l.Store.RecordEmitted(factory.Named(), &next, nil)
	next.Finish()
	if l.AdmitsRoot(object, record.Path) {
		t.Error("fresh root query ignores the arriving emitted covering owner")
	}
	proposal, err = l.Prepare(t.Context(), PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), true)
	assert.Equal(
		t,
		slices.ContainsFunc(proposal.Objects, func(c store.Child) bool { return c.Obj.Named() == child }),
		false,
	)
	_, _, empty := l.Store.GetEmission(factory.Named())
	empty.Children, empty.Manifests, empty.Complete = nil, nil, true
	l.Store.RecordEmitted(factory.Named(), &empty, nil)
	empty.Finish()
	assert.Equal(t, l.Store.GetObject(owner.Named()) == nil, true)
	if !l.AdmitsRoot(object, record.Path) {
		t.Error("fresh root query retains the withdrawn covering owner")
	}
	proposal, err = l.Prepare(t.Context(), PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), false)
	assert.Equal(
		t,
		slices.ContainsFunc(proposal.Objects, func(c store.Child) bool { return c.Obj.Named() == child }),
		true,
	)
	assert.Equal(t, reflect.DeepEqual(l.roots.get(child), static), true)
	assert.Equal(t, l.Store.GetArtifact(source.Named(), store.ArtifactContent) != nil, true)
}

func TestAdmission_OwnershipReadCapture(t *testing.T) {
	t.Parallel()
	l, source, factory, owner, child := admissionMembershipFixture(t)
	unrelated := owner.Clone()
	unrelated.Path = "./elsewhere"
	_, _, outside := l.Store.GetEmission(factory.Named())
	outside.Children, outside.Complete = []store.Child{{Obj: unrelated}}, true
	l.Store.RecordEmitted(factory.Named(), &outside, nil)
	outside.Finish()
	var uncovered store.ReadSet
	uncovered.Init(l.Store, factory.Named(), nil)
	defer uncovered.Retire()
	uncovered.RequireData(child)
	unowned, err := l.Prepare(t.Context(), PrepareRequest{Reads: &uncovered})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(unowned.Needed, owner.Named()), false)
	assert.Equal(
		t,
		slices.ContainsFunc(unowned.Objects, func(c store.Child) bool { return c.Obj.Named() == child }),
		true,
	)
	assert.Equal(t, unowned.reads.Valid(), true)
	_, _, next := l.Store.GetEmission(factory.Named())
	next.Children, next.Complete = []store.Child{{Obj: owner}}, true
	l.Store.RecordEmitted(factory.Named(), &next, nil)
	next.Finish()
	assert.Equal(t, unowned.reads.Valid(), false)
	var reads store.ReadSet
	reads.Init(l.Store, factory.Named(), nil)
	defer reads.Retire()
	reads.RequireData(child)
	proposal, err := l.Prepare(t.Context(), PrepareRequest{Reads: &reads})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), true)
	assert.Equal(
		t,
		slices.ContainsFunc(proposal.Objects, func(c store.Child) bool { return c.Obj.Named() == child }),
		false,
	)
	assert.Equal(t, proposal.reads.Valid(), true)
	l.Store.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	if proposal.reads.Valid() {
		t.Error("ownership source replacement did not invalidate the captured proposal")
	}
	var fresh store.ReadSet
	fresh.Init(l.Store, factory.Named(), nil)
	defer fresh.Retire()
	fresh.RequireData(child)
	proposal, err = l.Prepare(t.Context(), PrepareRequest{Reads: &fresh})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), false)
	assert.Equal(
		t,
		slices.ContainsFunc(proposal.Objects, func(c store.Child) bool { return c.Obj.Named() == child }),
		true,
	)
	assert.Equal(t, proposal.reads.Valid(), true)
}

func admissionMembershipFixture(
	t *testing.T,
) (*Loader, *manifest.GitRepository, *manifest.ResourceSet, *manifest.Kustomization, manifest.NamedResource) {
	t.Helper()
	root := t.TempDir()
	st := store.New()
	st.AddObject(
		&manifest.GitRepository{Name: manifest.BootstrapSourceID.Name, Namespace: manifest.BootstrapSourceID.Namespace},
	)
	input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{
		"declarations/child.yaml": {
			Data: []byte(
				testutil.KustomizationYAML(
					"child",
					"apps",
					"  path: ./payload\n  sourceRef: {kind: GitRepository, name: local, namespace: sources}\n",
				),
			),
		},
		"payload/kustomization.yaml": {Data: []byte("resources: []\n")},
	}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	source := &manifest.GitRepository{Name: "local", Namespace: "sources"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), input)
	l := New(st)
	l.SourceRoot = root
	child := testutil.KustomizationID("apps", "child")
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{child}}
	l.Options.Filter = change.NewFilter()
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	factory := &manifest.ResourceSet{Name: "factory", Namespace: "apps"}
	st.AddObject(factory)
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./declarations", TargetNamespace: "apps"}
	owner.SourceRef.Kind, owner.SourceRef.Name, owner.SourceRef.Namespace = source.Named().Kind, source.Name, source.Namespace
	return l, source, factory, owner, child
}

func TestAdmission_SourceQualifiedMembership(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"local", "local alias", "foreign", "pinned"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			l, source, factory, owner, child := admissionMembershipFixture(t)
			local := l.Store.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
			if mode != "local" {
				other := *local
				other.LocalPath = t.TempDir()
				other.LocalRoot = other.LocalPath
				other.FS = fstest.MapFS{
					"declarations/child.yaml": {
						Data: []byte(testutil.KustomizationYAML("child", "apps", "  path: ./foreign-payload\n")),
					},
				}
				if mode == "local alias" {
					other.InputAlias, other.LocalRoot, other.FS = true, local.LocalRoot, local.FS
				}
				if mode == "pinned" {
					other.Content, other.Key, other.LocalRoot = "pinned-content", "pinned-key", local.LocalRoot
				}
				l.Store.SetArtifact(source.Named(), &other)
			}
			_, _, next := l.Store.GetEmission(factory.Named())
			next.Children, next.Complete = []store.Child{{Obj: owner}}, true
			l.Store.RecordEmitted(factory.Named(), &next, nil)
			next.Finish()
			proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
			assert.NoError(t, err)
			owned := mode == "local" || mode == "local alias"
			assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), owned)
			assert.Equal(
				t,
				slices.ContainsFunc(proposal.Objects, func(c store.Child) bool { return c.Obj.Named() == child }),
				!owned,
			)
			assert.Diff(
				t,
				local.FS.(fstest.MapFS)["declarations/child.yaml"].Data,
				[]byte(
					testutil.KustomizationYAML(
						"child",
						"apps",
						"  path: ./payload\n  sourceRef: {kind: GitRepository, name: local, namespace: sources}\n",
					),
				),
			)
		})
	}
}

func TestRootQuery_ReusesOwnership(t *testing.T) {
	t.Parallel()
	l, _, factory, owner, child := admissionMembershipFixture(t)
	l.Options.Changes = change.NewSet([]string{"declarations/child.yaml"})
	record := l.Index.Records[child]
	object := l.CatalogObject(child)
	normal := rootQuery{loader: l}
	normal.live = &admissionClosure{loader: l, objects: l.Store}
	normal.live.productions(child)
	if normal.live.comparisons == nil || len(normal.live.comparisons.bindings) == 0 {
		t.Fatal("production lookup did not borrow source comparisons")
	}
	assert.Equal(t, normal.admitsRoot(object, record.Path), true)
	assert.Equal(t, normal.live.comparisons == nil, true)
	unrelated := owner.Clone()
	unrelated.Name, unrelated.Path = "a-unrelated", "./elsewhere"
	_, _, next := l.Store.GetEmission(factory.Named())
	next.Children, next.Complete = []store.Child{{Obj: unrelated}, {Obj: owner}}, true
	l.Store.RecordEmitted(factory.Named(), &next, nil)
	next.Finish()
	reads := &admissionArtifactReads{admissionReader: l.Store}
	query := rootQuery{loader: l}
	query.live = &admissionClosure{loader: l, objects: reads}
	query.live.productions(child)
	if query.live.comparisons == nil || len(query.live.comparisons.bindings) == 0 {
		t.Fatal("production lookup did not borrow source comparisons before rejection")
	}
	assert.Equal(t, query.admitsRoot(object, record.Path), false)
	assert.Equal(t, query.live.comparisons == nil, true)
	lists := reads.lists
	if lists == 0 {
		t.Fatal("root query did not collect current ownership")
	}
	assert.Equal(t, query.admitsRoot(object, record.Path), false)
	assert.Equal(t, reads.lists, lists)
	assert.Equal(t, slices.Contains(reads.productions, unrelated.Named()), false)
	_, _, empty := l.Store.GetEmission(factory.Named())
	empty.Children, empty.Manifests, empty.Complete = nil, nil, true
	l.Store.RecordEmitted(factory.Named(), &empty, nil)
	empty.Finish()
	fresh := rootQuery{loader: l}
	assert.Equal(t, fresh.admitsRoot(object, record.Path), true)
	assert.Equal(t, fresh.live.comparisons == nil, true)
	l.Store.AddObject(owner)
	pendingReads := &admissionArtifactReads{admissionReader: l.Store}
	pending := rootQuery{loader: l}
	pending.live = &admissionClosure{
		loader: l, objects: pendingReads,
		accepted: map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{
			factory.Named(): {owner.Named(): {Productions: []manifest.Production{{Owner: factory.Named()}}}},
		},
	}
	assert.Equal(t, pending.admitsRoot(object, record.Path), false)
	assert.Equal(t, pendingReads.wideLists, 0)
}

func TestAdmitsRoot_PreparedParent(t *testing.T) {
	t.Parallel()
	hidden := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).Named()
	ordinary := testutil.KustomizationID("apps", "owner")
	for _, tc := range []struct {
		name      string
		parent    manifest.NamedResource
		compact   bool
		authored  bool
		candidate bool
		want      bool
	}{
		{name: "parentless", candidate: true, want: true},
		{name: "hidden native root", parent: hidden, compact: true, candidate: true, want: true},
		{name: "ordinary owner", parent: ordinary, compact: true, candidate: true},
		{name: "unprepared hidden owner", parent: hidden, candidate: true},
		{name: "authored without indexed file", authored: true, candidate: true},
		{name: "no prepared document"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			obj := &manifest.ConfigMap{Name: "prepared", Namespace: "apps", Data: map[string]any{"VALUE": "retained"}}
			id := obj.Named()
			doc := obj.Snapshot()
			doc["metadata"].(map[string]any)["uid"] = "prepared-origin"
			record := manifest.DiscoveryRecord{Parent: tc.parent, BuilderOwner: tc.parent}
			if tc.authored {
				record.Authored = id
			}
			l.Index.Records[id] = record
			l.documents.compact = tc.compact
			if tc.candidate {
				l.candidates = map[manifest.NamedResource]store.Child{id: {Obj: obj, Doc: doc}}
			}
			assert.Equal(t, l.AdmitsRoot(obj, ""), tc.want)
			if tc.candidate {
				retained := l.IndexedObject(id, record)
				assert.Equal(t, retained.Obj, manifest.BaseManifest(obj))
				assert.Diff(t, retained.Doc, doc)
				assert.Diff(t, l.Index.Records[id], record)
			}
		})
	}
}

func TestEntryRoots_CurrentOwnership(t *testing.T) {
	t.Parallel()
	l, _, factory, owner, child := admissionMembershipFixture(t)
	object := l.CatalogObject(child)
	l.Store.AddObject(object, testutil.ObjectDocument(object))
	var reads store.ReadSet
	reads.Init(l.Store, manifest.NamedResource{}, nil)
	defer reads.Retire()
	contains := func(roots []Occurrence) bool {
		return slices.ContainsFunc(roots, func(root Occurrence) bool { return root.Identity == child })
	}
	assert.Equal(t, contains(l.EntryRoots(nil, &reads)), true)
	l.Store.RecordEmitted(factory.Named(), &store.Emission{Children: []store.Child{{Obj: owner}}, Complete: true}, nil)
	assert.Equal(t, reads.Valid(), false)
	assert.Equal(t, contains(l.EntryRoots(nil, nil)), false)
	l.Store.RecordEmitted(factory.Named(), &store.Emission{Complete: true}, nil)
	assert.Equal(t, contains(l.EntryRoots(nil, nil)), true)
}

func TestAdmission_HistoryProductionPass(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	owner := testutil.KustomizationID("apps", "owner")
	child := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "child"}
	want := []manifest.Production{{Owner: owner, Authored: child, File: "child.yaml"}}
	l.Index.Records[owner] = manifest.DiscoveryRecord{File: "owner.yaml"}
	l.Index.Records[child] = manifest.DiscoveryRecord{File: "child.yaml", Productions: want}
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Changes = change.NewSet([]string{"owner.yaml"})
	c := admissionClosure{
		loader: l, objects: l.Store,
		accepted: map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{owner: {}},
		replace:  map[manifest.NamedResource]bool{owner: true},
	}
	c.proposal = &c.result
	assert.Equal(t, len(c.productions(child)), 0)
	assert.NoError(t, c.start(t.Context(), func(materializerDemand, manifest.NamedResource) bool { return false }))
	assert.Equal(t, c.states[owner].changed, true)
	assert.Equal(t, len(c.historyProductions), 0)
	assert.Equal(t, c.replace[owner], false)
	assert.Diff(t, c.productions(child), want)
}

func TestEntryRoots_AcceptedSupport(t *testing.T) {
	t.Parallel()
	l, _, factory, _, child := admissionMembershipFixture(t)
	object := l.CatalogObject(child)
	record := l.Index.Records[child]
	l.Store.SetDiscoveryIndex(l.Index)
	var admission store.ReadSet
	admission.Init(l.Store, manifest.NamedResource{}, nil)
	admission.Admit(
		store.Child{Obj: object, SourceSupport: manifest.BootstrapSourceID},
		manifest.BootstrapSourceID,
		manifest.NamedResource{},
	)
	admission.Retire()
	l.Store.RecordEmitted(factory.Named(), &store.Emission{
		Children: []store.Child{{Obj: object}}, Complete: true,
	}, nil)
	assert.Equal(t, l.Store.SourceAdmission(child, manifest.BootstrapSourceID), false)
	assert.Equal(t, l.AdmitsRoot(object, record.Path), false)
	var reads store.ReadSet
	reads.Init(l.Store, manifest.NamedResource{}, nil)
	defer reads.Retire()
	roots := l.EntryRoots(nil, &reads)
	assert.Equal(t, slices.ContainsFunc(roots, func(root Occurrence) bool { return root.Identity == child }), true)
	l.Store.RecordEmitted(factory.Named(), &store.Emission{Complete: true}, nil)
	assert.Equal(t, reads.Valid(), false)
	assert.Equal(t, l.Store.SourceAdmission(child, manifest.BootstrapSourceID), false)
	assert.Equal(t, l.AdmitsRoot(object, record.Path), false)
	roots = l.EntryRoots(nil, nil)
	assert.Equal(t, slices.ContainsFunc(roots, func(root Occurrence) bool { return root.Identity == child }), false)
}

func TestRootQuery_EmittedOwnership(t *testing.T) {
	t.Parallel()
	l, _, factory, _, child := admissionMembershipFixture(t)
	object := l.CatalogObject(child)
	record := l.Index.Records[child]
	l.Store.RecordEmitted(factory.Named(), &store.Emission{
		Children: []store.Child{{Obj: object}}, Complete: true,
	}, nil)
	assert.Equal(t, l.Store.SourceAdmission(child, manifest.BootstrapSourceID), true)
	assert.Equal(t, l.AdmitsRoot(object, record.Path), false)
}
