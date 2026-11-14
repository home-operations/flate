package loader

import (
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_OrderingEmittedPathProof_LiveOwnership(t *testing.T) {
	t.Parallel()
	l, source, factory, owner, child := admissionMembershipFixture(t)
	record := l.Index.Records[child]
	object := l.CatalogObject(child)
	if !selection.Root(rootAtForTest(l, object, record.Path)).Admitted {
		t.Fatal("ownerless indexed declaration has no static root scope")
	}
	static := *l.roots.get(child)
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
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
	if selection.Root(rootAtForTest(l, object, record.Path)).Admitted {
		t.Error("fresh root query ignores the arriving emitted covering owner")
	}
	proposal, err = prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
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
	if !selection.Root(rootAtForTest(l, object, record.Path)).Admitted {
		t.Error("fresh root query retains the withdrawn covering owner")
	}
	proposal, err = prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), false)
	assert.Equal(
		t,
		slices.ContainsFunc(proposal.Objects, func(c store.Child) bool { return c.Obj.Named() == child }),
		true,
	)
	assert.Equal(
		t,
		//nolint:govet // Snapshot equality must include immutable unexported facts.
		reflect.DeepEqual(*l.roots.get(child), static),
		true,
	)
	assert.Equal(t, l.Store.GetArtifact(source.Named(), store.ArtifactContent) != nil, true)
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
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{child}})

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
			otherOwner := owner.Clone()
			otherOwner.Name, otherOwner.Namespace = "second", "other"
			_, _, next := l.Store.GetEmission(factory.Named())
			next.Children, next.Complete = []store.Child{{Obj: owner}, {Obj: otherOwner}}, true
			l.Store.RecordEmitted(factory.Named(), &next, nil)
			next.Finish()
			proposal, err := prepareSelection(
				t.Context(),
				t,
				l,
				selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
			)
			assert.NoError(t, err)
			owned := mode == "local" || mode == "local alias"
			assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), owned)
			assert.Equal(t, slices.Contains(proposal.Needed, otherOwner.Named()), owned)
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
	l.SourceFacts(manifest.BootstrapSourceID, nil)
	bindings := l.sourceBucket(manifest.BootstrapSourceID).Load()
	if bindings == nil {
		t.Fatal("source lookup did not retain comparisons in the source owner")
	}
	assert.Equal(t, normal.admitsRoot(object, record.Path), true)
	assert.Equal(t, l.sourceBucket(manifest.BootstrapSourceID).Load() == bindings, true)
	unrelated := owner.Clone()
	unrelated.Name, unrelated.Path = "a-unrelated", "./elsewhere"
	_, _, next := l.Store.GetEmission(factory.Named())
	next.Children, next.Complete = []store.Child{{Obj: unrelated}, {Obj: owner}}, true
	l.Store.RecordEmitted(factory.Named(), &next, nil)
	next.Finish()
	reads := &rootOwnershipReads{admissionReader: l.Store}
	query := rootQuery{loader: l, objects: reads}
	l.SourceFacts(manifest.BootstrapSourceID, nil)
	bindings = l.sourceBucket(manifest.BootstrapSourceID).Load()
	if bindings == nil {
		t.Fatal("source lookup did not retain comparisons before rejection")
	}
	assert.Equal(t, query.admitsRoot(object, record.Path), false)
	assert.Equal(t, l.sourceBucket(manifest.BootstrapSourceID).Load() == bindings, true)
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
	assert.Equal(t, l.sourceBucket(manifest.BootstrapSourceID).Load() == bindings, true)
	l.Store.AddObject(owner)
	pendingReads := &rootOwnershipReads{admissionReader: l.Store}
	l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{
		Productions: []manifest.Production{{Owner: factory.Named()}},
	}
	pending := rootQuery{loader: l, objects: pendingReads}
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
			assert.Equal(t, selection.Root(rootAtForTest(l, obj, "")).Admitted, tc.want)
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
	contains := func(roots []selection.OccurrenceFacts) bool {
		return slices.ContainsFunc(roots, func(root selection.OccurrenceFacts) bool { return root.Identity == child })
	}
	assert.Equal(t, contains(l.EntryRoots(nil, &reads)), true)
	l.Store.RecordEmitted(factory.Named(), &store.Emission{Children: []store.Child{{Obj: owner}}, Complete: true}, nil)
	assert.Equal(t, reads.Valid(), false)
	assert.Equal(t, len(l.EntryRoots(nil, &reads)), 0)
	assert.Equal(t, contains(l.EntryRoots(nil, nil)), false)
	l.Store.RecordEmitted(factory.Named(), &store.Emission{Complete: true}, nil)
	assert.Equal(t, contains(l.EntryRoots(nil, nil)), true)
}

func TestEntryRoots_RejectsChangedQuery(t *testing.T) {
	t.Parallel()
	l, _, factory, owner, child := admissionMembershipFixture(t)
	object := l.CatalogObject(child)
	l.Store.AddObject(object, testutil.ObjectDocument(object))
	var reads store.ReadSet
	reads.Init(l.Store, manifest.NamedResource{}, nil)
	defer reads.Retire()
	roots := l.EntryRoots(func(yield func(manifest.BaseManifest) bool) {
		if !yield(object) {
			return
		}
		l.Store.RecordEmitted(
			factory.Named(),
			&store.Emission{Children: []store.Child{{Obj: owner}}, Complete: true},
			nil,
		)
	}, &reads)
	assert.Equal(t, reads.Valid(), false)
	assert.Equal(t, len(roots), 0)
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
	assert.Equal(t, selection.Root(rootAtForTest(l, object, record.Path)).Admitted, false)
	var reads store.ReadSet
	reads.Init(l.Store, manifest.NamedResource{}, nil)
	defer reads.Retire()
	roots := l.EntryRoots(nil, &reads)
	assert.Equal(
		t,
		slices.ContainsFunc(roots, func(root selection.OccurrenceFacts) bool { return root.Identity == child }),
		true,
	)
	l.Store.RecordEmitted(factory.Named(), &store.Emission{Complete: true}, nil)
	assert.Equal(t, reads.Valid(), false)
	assert.Equal(t, l.Store.SourceAdmission(child, manifest.BootstrapSourceID), false)
	assert.Equal(t, selection.Root(rootAtForTest(l, object, record.Path)).Admitted, false)
	roots = l.EntryRoots(nil, nil)
	assert.Equal(
		t,
		slices.ContainsFunc(roots, func(root selection.OccurrenceFacts) bool { return root.Identity == child }),
		false,
	)
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
	assert.Equal(t, selection.Root(rootAtForTest(l, object, record.Path)).Admitted, false)
}

type rootOwnershipReads struct {
	admissionReader

	lists, wideLists int
	productions      []manifest.NamedResource
}

func (r *rootOwnershipReads) ListProductionCandidates(
	group, kind string,
	index *manifest.DiscoveryIndex,
	prospective map[manifest.NamedResource]bool,
) []manifest.BaseManifest {
	r.lists++
	if group == "" && kind == "" {
		r.wideLists++
	}
	return r.admissionReader.ListProductionCandidates(group, kind, index, prospective)
}

func (r *rootOwnershipReads) Productions(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,
) []manifest.Production {
	r.productions = append(r.productions, id)
	return r.admissionReader.Productions(id, index)
}

func TestEntryRoots_NativeOperand(t *testing.T) {
	t.Parallel()
	for _, native := range []bool{false, true} {
		t.Run(strconv.FormatBool(native), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"one/object.yaml",
				"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: one, namespace: apps}\nspec: {resources: []}\n",
			)
			if native {
				testutil.WriteFile(t, root, "one/kustomization.yaml", "resources: [object.yaml]\n")
			}
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{
					Kind:      manifest.KindGitRepository,
					LocalPath: root,
					LocalRoot: root,
					FS:        os.DirFS(root),
				},
			)
			l := New(st)
			l.SourceRoot = root

			configureSelection(t, l, selection.Request{Changed: true})
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.Admit(t.Context(), root))
			id := manifest.NamedResource{
				Group:     manifest.FluxOperatorDomain,
				Kind:      manifest.KindResourceSet,
				Namespace: "apps",
				Name:      "one",
			}
			assert.Equal(
				t,
				selection.Root(l.roots.get(id).view()).Independent &&
					l.roots.get(id).occurrence.Declaration != (occurrenceID{}),
				!native,
			)
			assert.Equal(
				t,
				selection.Root(rootAtForTest(l, l.CatalogObject(id), l.Index.Records[id].Path)).Admitted,
				true,
			)
		})
	}
}

func TestEntryRoots_UnavailableOutputCapturesAvailability(t *testing.T) {
	t.Parallel()
	st := store.New()
	root := t.TempDir()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, Content: "first"})
	object := &manifest.ConfigMap{Name: "static", Namespace: "apps"}
	st.AddObject(object)
	id := object.Named()
	l := New(st)
	l.SourceRoot = root
	l.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: root + "/object.yaml", Declaration: object}
	l.roots.set(id, &rootEvidence{
		identity: id,
		occurrence: authoredOccurrence{
			Identity: id, Authored: id, object: object,
			Declaration: occurrenceID{Source: root, File: "object.yaml"},
		},
		record: selection.RootRecord{Authored: id, Path: root + "/object.yaml"},
		scope:  selection.RootScopeFacts{NamespaceEligible: true, Initial: true, Eligibility: selection.RootEligible},
	})
	var unavailable store.ReadSet
	unavailable.Init(st, manifest.NamedResource{}, nil)
	defer unavailable.Retire()
	assert.Equal(t, len(l.EntryRoots(nil, &unavailable)), 0)
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, Content: "second"})
	assert.Equal(t, unavailable.Valid(), true)
	st.RecordEmitted(id, &store.Emission{Complete: true}, nil)
	assert.Equal(t, unavailable.Valid(), false)
	var available store.ReadSet
	available.Init(st, manifest.NamedResource{}, nil)
	defer available.Retire()
	roots := l.EntryRoots(nil, &available)
	assert.Equal(t, len(roots), 1)
	assert.Equal(t, roots[0].Identity, id)
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, Content: "third"})
	assert.Equal(t, available.Valid(), false)
}
