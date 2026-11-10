package loader

import (
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestCommit_ConstructionGuard(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, name := range []string{"first", "second"} {
		testutil.WriteFile(
			t,
			root,
			name+".yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {value: present}\n",
		)
	}
	st := store.New()
	admitForTest(st, root)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	st.AddObject(parent)
	l := New(st)
	l.SourceRoot = root

	configureSelection(t, l, selection.Request{Changed: true})
	l.Options.Changes = change.NewSet([]string{"first.yaml", "second.yaml"})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	incoming := store.Child{Obj: &manifest.Kustomization{
		Name: "entry", Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "first"},
			{Kind: manifest.KindConfigMap, Name: "second"},
		}},
	}, Entry: &manifest.EntryApplication{}}
	for _, name := range []string{"first", "second"} {
		assert.Equal(
			t,
			st.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name}),
			nil,
		)
	}
	committed := false
	for round := range 4 {
		guard := new(store.ReadSet)
		guard.Init(st, parent.Named(), nil)
		guard.GetDeclaration(parent.Named())
		guard.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
		proposal, prepareErr := prepareSelection(t.Context(), t, l, selection.PrepareRequest{
			Changes:    l.Options.Changes,
			Generation: []store.Child{incoming},
			Reads:      guard,
			Invocation: true,
		})
		assert.NoError(t, prepareErr)
		_, committed, err = l.Options.Session.Commit(t.Context(), proposal, selection.Construction)
		assert.NoError(t, err)
		proposal.Retire()
		if committed {
			assert.FatalIf(t, !guard.Valid(), "round %d accepted a stale binding guard", round)
			st.Retire(guard)
			break
		}
		assert.FatalIf(t, guard.Valid(), "round %d rejected a current binding guard", round)
		st.Retire(guard)
	}
	assert.FatalIf(t, !committed, "fresh construction proposals did not converge")
	for _, name := range []string{"first", "second"} {
		assert.FatalIf(
			t,
			st.GetDeclaration(
				manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name},
			) == nil,
			"construction omitted dependency %s",
			name,
		)
	}
	assert.Equal(t, l.Options.Session.Snapshot().Output(incoming.Obj.Named()), false)
	assert.Equal(t, len(proposalSelected(l.Options.Session.Snapshot(), l.Index)), 0)

	guard := new(store.ReadSet)
	guard.Init(st, parent.Named(), nil)
	guard.GetDeclaration(parent.Named())
	source := guard.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	defer st.Retire(guard)
	proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{
		Changes:    l.Options.Changes,
		Generation: []store.Child{incoming},
		Reads:      guard,
		Invocation: true,
	})
	assert.NoError(t, err)
	replaced := *source
	replaced.Content = "replacement"
	st.SetArtifact(manifest.BootstrapSourceID, &replaced)
	_, committed, err = l.Options.Session.Commit(t.Context(), proposal, selection.Construction)
	assert.NoError(t, err)
	assert.FatalIf(t, committed || guard.Valid(), "construction accepted a replaced source")
	proposal.Retire()
}

func TestCommit_RetiredSourceSupport(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"input.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\n",
	)
	st := store.New()
	admitForTest(st, root)
	l := New(st)
	l.SourceRoot = root

	configureSelection(t, l, selection.Request{Changed: true})
	l.Options.Changes = change.NewSet([]string{"input.yaml"})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{
		Changes:    l.Options.Changes,
		Invocation: true,
		Generation: []store.Child{{Obj: &manifest.Kustomization{
			Name: "entry", Namespace: "apps",
			PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "input"},
			}},
		}, Entry: &manifest.EntryApplication{}}},
	})
	assert.NoError(t, err)
	_, committed, err := l.commitSelection(t.Context(), t, proposal, selection.Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	assert.FatalIf(t, st.GetDeclaration(id) == nil, "commit omitted the initial declaration")
	proposal.Retire()
	l.Retire()
	for range st.Sweep() {
	}
	input := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	replacement := *input
	replacement.Content = "replacement"
	st.SetArtifact(manifest.BootstrapSourceID, &replacement)
	assert.Equal(t, st.GetDeclaration(id), nil)
}

func TestCommit_RequiredClosureRetries(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, name := range []string{"first", "second"} {
		testutil.WriteFile(
			t,
			root,
			name+".yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {value: present}\n",
		)
	}
	st := store.New()
	admitForTest(st, root)
	parent := &manifest.Kustomization{
		Name:      "entry",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "first"},
			{Kind: manifest.KindConfigMap, Name: "second"},
		}},
	}
	st.AddObject(parent)
	l := New(st)
	l.SourceRoot = root

	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{parent.Named()}})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	initial, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	_, initialCommitted, err := l.commitSelection(t.Context(), t, initial, selection.Execution)
	assert.NoError(t, err)
	assert.Equal(t, initialCommitted, true)
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	_, committed, err := l.commitSelection(t.Context(), t, proposal, selection.Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	for _, name := range []string{"first", "second"} {
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name}
		assert.FatalIf(t, st.GetDeclaration(id) == nil, "retry omitted required declaration %s", id)
		assert.Equal(t, l.Options.Session.Snapshot().Reconcile(id), true)
	}
}

func TestCommit_OrdinaryNativeRoots(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"explicit", "implicit"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testCommitOrdinaryNativeRoots(t, name == "implicit")
		})
	}
}

func TestFinalizeIndex_AuthoredSourceScan(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, content string
		alias         bool
	}{
		{name: "empty"},
		{name: "ordinary", content: "ordinary-source"},
		{name: "alias", content: "alias-source", alias: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for _, name := range []string{"explicit", "implicit"} {
				sourceRef := ""
				if name == "explicit" {
					sourceRef = "  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
				}
				testutil.WriteFile(t, root, "shared/"+name+".yaml",
					testutil.KustomizationYAML(name, "apps", "  path: ./shared\n"+sourceRef))
			}
			testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [explicit.yaml, implicit.yaml]\n")
			st := store.New()
			admitForTest(st, root)
			source := *st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			source.Content, source.InputAlias = tc.content, tc.alias
			st.SetArtifact(manifest.BootstrapSourceID, &source)
			l := New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: true})
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			explicit := testutil.KustomizationID("apps", "explicit")
			implicit := testutil.KustomizationID("apps", "implicit")
			assert.Diff(t, l.Index.Claims, map[string][]manifest.NamedResource{"shared/": {implicit}})
			assert.Equal(t, slices.ContainsFunc(l.scopes.contributions, func(c scopeContribution) bool {
				return c.owner == explicit
			}), false)
			assert.Equal(t, slices.ContainsFunc(l.scopes.contributions, func(c scopeContribution) bool {
				return c.owner == implicit
			}), true)

			_, err = l.Load(t.Context(), root+"/shared", explicit, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			assert.Diff(t, l.Index.Claims, map[string][]manifest.NamedResource{"shared/": {explicit, implicit}})
			assert.Equal(t, slices.ContainsFunc(l.scopes.contributions, func(c scopeContribution) bool {
				return c.owner == explicit
			}), true)
		})
	}
}

func testCommitOrdinaryNativeRoots(t *testing.T, implicit bool) {
	t.Helper()
	root := t.TempDir()
	path := "./shared"
	sourceRef := "  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
	if implicit {
		sourceRef = ""
		path = "./workload"
		testutil.WriteFile(t, root, "workload/kustomization.yaml", "resources: [../shared/cm.yaml]\n")
	}
	for _, name := range []string{"a", "b"} {
		testutil.WriteFile(
			t,
			root,
			"shared/"+name+".yaml",
			testutil.KustomizationYAML(
				name,
				"apps",
				"  path: "+path+"\n"+sourceRef,
			),
		)
	}
	testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [a.yaml, b.yaml, cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"shared/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\ndata: {value: changed}\n",
	)
	st := store.New()
	admitForTest(st, root)
	source := *st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	source.Content = "ordinary-source"
	st.SetArtifact(manifest.BootstrapSourceID, &source)
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})

	l.Options.Changes = change.NewSet([]string{"shared/cm.yaml"})
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
	for _, name := range []string{"a", "b"} {
		id := testutil.KustomizationID("apps", name)
		assert.Equal(
			t,
			selection.Root(l.RootEvidence(id, nil)).Independent &&
				selection.Root(l.RootEvidence(id, nil)).Occurrence.Declaration != (selection.OccurrenceID{}),
			false,
		)
		at := slices.IndexFunc(proposal.Objects, func(child store.Child) bool { return child.Obj.Named() == id })
		assert.FatalIf(t, at < 0, "ordinary native root omitted %s", id)
		assert.Equal(t, proposal.Objects[at].SourceSupport, manifest.BootstrapSourceID)
	}
	_, committed, err := l.commitSelection(t.Context(), t, proposal, selection.Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	assert.FatalIf(
		t,
		st.GetDeclaration(a) == nil || st.GetDeclaration(b) == nil,
		"ordinary roots lack current source authority",
	)
	l.Retire()
	for range st.Sweep() {
	}
	replacement := source
	replacement.Content = "replacement-source"
	st.SetArtifact(manifest.BootstrapSourceID, &replacement)
	assert.Equal(t, st.GetDeclaration(a), nil)
	assert.Equal(t, st.GetDeclaration(b), nil)
	retry, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	defer retry.Retire()
	assert.Equal(
		t,
		slices.ContainsFunc(retry.Objects, func(child store.Child) bool { return child.Obj.Named() == b }),
		false,
	)
}

func TestCommit_AuthoritativeEntry(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	admitForTest(st, root)
	source := *st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	source.Content = "source"
	st.SetArtifact(manifest.BootstrapSourceID, &source)
	parent := &manifest.Kustomization{
		Name: "parent", Namespace: "apps", Path: "./parent",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.BootstrapSourceID.Kind, Name: manifest.BootstrapSourceID.Name,
			Namespace: manifest.BootstrapSourceID.Namespace,
		},
	}
	st.AddObject(parent)
	child := store.Child{Obj: &manifest.Kustomization{Name: "entry", Namespace: "apps", Path: "./entry"}}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		child.Obj.Named(): {
			Productions: []manifest.Production{{Owner: parent.Named(), Source: manifest.BootstrapSourceID}},
		},
	}})
	var output store.ReadSet
	output.Init(st, parent.Named(), nil)
	defer st.Retire(&output)
	emission := &store.Emission{Children: []store.Child{child}, Complete: true}
	output.Bind(emission)
	st.RecordEmitted(parent.Named(), emission, nil)
	assert.Equal(t, st.GetDeclaration(child.Obj.Named()), child.Obj)
	l := New(st)
	l.SourceRoot = root

	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{child.Obj.Named()}})
	var guard store.ReadSet
	guard.Init(st, manifest.BootstrapSourceID, nil)
	defer st.Retire(&guard)
	guard.GetDeclaration(child.Obj.Named())
	child.Entry = &manifest.EntryApplication{}
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Generation: []store.Child{child}, Reads: &guard},
	)
	assert.NoError(t, err)
	assert.Equal(t, len(proposal.Objects), 0)
	_, _, err = l.Options.Session.Commit(t.Context(), proposal, selection.Construction)
	assert.NoError(t, err)
	assert.Equal(t, output.Valid(), true)
	assert.Equal(t, st.GetDeclaration(child.Obj.Named()), child.Obj)
	assert.Equal(t, st.EmissionOwner(child.Obj.Named()), parent.Named())
}
