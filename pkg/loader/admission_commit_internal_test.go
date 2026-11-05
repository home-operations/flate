package loader

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"testing"
	"weak"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
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
	l.Options.Filter = change.NewFilter()
	l.Options.Selection.Changed = true
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
		proposal, prepareErr := l.Prepare(
			t.Context(),
			PrepareRequest{Generation: []store.Child{incoming}, Reads: guard, Invocation: true},
		)
		assert.NoError(t, prepareErr)
		committed, err = l.Commit(t.Context(), proposal, Construction)
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
	assert.Equal(t, l.Options.Filter.Membership(incoming.Obj.Named()).Selected, false)
	assert.Equal(t, len(l.Options.Filter.KeepNames()), 0)

	guard := new(store.ReadSet)
	guard.Init(st, parent.Named(), nil)
	guard.GetDeclaration(parent.Named())
	source := guard.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	defer st.Retire(guard)
	proposal, err := l.Prepare(
		t.Context(),
		PrepareRequest{Generation: []store.Child{incoming}, Reads: guard, Invocation: true},
	)
	assert.NoError(t, err)
	replaced := *source
	replaced.Content = "replacement"
	st.SetArtifact(manifest.BootstrapSourceID, &replaced)
	committed, err = l.Commit(t.Context(), proposal, Construction)
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
	l.Options.Filter = change.NewFilter()
	l.Options.Selection.Changed = true
	l.Options.Changes = change.NewSet([]string{"input.yaml"})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	proposal, err := l.Prepare(
		t.Context(),
		PrepareRequest{Invocation: true, Generation: []store.Child{{Obj: &manifest.Kustomization{
			Name: "entry", Namespace: "apps",
			PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "input"},
			}},
		}, Entry: &manifest.EntryApplication{}}}},
	)
	assert.NoError(t, err)
	committed, err := l.Commit(t.Context(), proposal, Execution)
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
	l.Options.Filter = change.NewFilter()
	l.Options.Selection.Roots = []manifest.NamedResource{parent.Named()}
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	_, retry := l.Options.Filter.AddEmitted(
		change.MembershipUpdate{
			Selected: []manifest.NamedResource{parent.Named()},
			Dirty:    []manifest.NamedResource{parent.Named()},
		},
		0,
	)
	assert.Equal(t, retry, false)
	proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	committed, err := l.Commit(t.Context(), proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	for _, name := range []string{"first", "second"} {
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name}
		assert.FatalIf(t, st.GetDeclaration(id) == nil, "retry omitted required declaration %s", id)
		assert.Equal(t, l.Options.Filter.Membership(id).Needed, true)
	}
}

func TestCommit_OrdinaryNativeRoots(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, name := range []string{"a", "b"} {
		testutil.WriteFile(
			t,
			root,
			"shared/"+name+".yaml",
			testutil.KustomizationYAML(
				name,
				"apps",
				"  path: ./shared\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
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
	l.Options.Selection.Changed = true
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet([]string{"shared/cm.yaml"})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	for _, name := range []string{"a", "b"} {
		id := testutil.KustomizationID("apps", name)
		assert.Equal(
			t,
			l.roots.get(id).independent && l.roots.get(id).occurrence.Declaration != (OccurrenceID{}),
			false,
		)
		at := slices.IndexFunc(proposal.Objects, func(child store.Child) bool { return child.Obj.Named() == id })
		assert.FatalIf(t, at < 0, "ordinary native root omitted %s", id)
		assert.Equal(t, proposal.Objects[at].SourceSupport, manifest.BootstrapSourceID)
	}
	committed, err := l.Commit(t.Context(), proposal, Execution)
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
	retry, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
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
	l.Options.Filter = change.NewFilter()
	var guard store.ReadSet
	guard.Init(st, manifest.BootstrapSourceID, nil)
	defer st.Retire(&guard)
	guard.GetDeclaration(child.Obj.Named())
	child.Entry = &manifest.EntryApplication{}
	proposal, err := l.Prepare(t.Context(), PrepareRequest{Generation: []store.Child{child}, Reads: &guard})
	assert.NoError(t, err)
	assert.Equal(t, len(proposal.Objects), 0)
	_, err = l.Commit(t.Context(), proposal, Construction)
	assert.NoError(t, err)
	assert.Equal(t, output.Valid(), true)
	assert.Equal(t, st.GetDeclaration(child.Obj.Named()), child.Obj)
	assert.Equal(t, st.EmissionOwner(child.Obj.Named()), parent.Named())
}

func TestCommit_ProposalLifetime(t *testing.T) {
	t.Parallel()
	for _, outcome := range []string{"success", "retry", "cancel", "abandon", "stale"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			admitForTest(st, t.TempDir())
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			st.AddObject(parent)
			l := New(st)
			l.Options.Filter = change.NewFilter()
			var guard store.ReadSet
			guard.Init(st, parent.Named(), nil)
			t.Cleanup(guard.Retire)
			guard.GetDeclaration(parent.Named())
			guard.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
			initial, err := l.Prepare(t.Context(), PrepareRequest{Generation: []store.Child{{Obj: parent}}})
			assert.NoError(t, err)
			committed, err := l.Commit(t.Context(), initial, Construction)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			before := l.constructionPurpose.Load()
			assert.Equal(t, before != nil, true)
			assert.Diff(t, *before, initial.Ordinary)
			var declaration weak.Pointer[manifest.Kustomization]
			proposal := func() *Admission {
				child := &manifest.Kustomization{Name: "entry", Namespace: "apps"}
				declaration = weak.Make(child)
				proposal, err := l.Prepare(t.Context(), PrepareRequest{
					Generation: []store.Child{{Obj: child, Entry: &manifest.EntryApplication{}}}, Reads: &guard,
				})
				assert.NoError(t, err)
				return proposal
			}()
			t.Cleanup(proposal.Retire)
			needed := slices.Clone(proposal.Needed)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			switch outcome {
			case "cancel":
				cancel()
			case "stale":
				replacement := parent.Clone()
				replacement.Path = "./replacement"
				st.AddObject(replacement)
			case "retry":
				_, retry := l.Options.Filter.AddEmitted(change.MembershipUpdate{
					Needed: []manifest.NamedResource{testutil.KustomizationID("apps", "other")},
				}, proposal.Revision)
				assert.Equal(t, retry, false)
			}
			if outcome == "abandon" {
				proposal.Retire()
			} else {
				committed, err := l.Commit(ctx, proposal, Construction)
				assert.Equal(t, errors.Is(err, context.Canceled), outcome == "cancel")
				assert.Equal(t, committed, outcome == "success" || outcome == "retry")
			}
			assert.Equal(t, proposal.reads, nil)
			assert.Equal(t, len(proposal.Objects)+len(proposal.sources), 0)
			assert.Equal(t, len(proposal.Request.Generation), 0)
			assert.Equal(t, proposal.Request.Reads, nil)
			assert.Diff(t, proposal.Needed, needed)
			assert.Equal(t, guard.Valid(), outcome != "stale")
			if outcome == "success" || outcome == "retry" {
				assert.Equal(t, l.constructionPurpose.Load() != before, true)
				assert.Diff(t, *l.constructionPurpose.Load(), proposal.Ordinary)
				empty, err := l.Prepare(t.Context(), PrepareRequest{})
				assert.NoError(t, err)
				committed, err := l.Commit(t.Context(), empty, Construction)
				assert.NoError(t, err)
				assert.Equal(t, committed, true)
				assert.Equal(t, len(*l.constructionPurpose.Load()), 0)
			} else {
				assert.Equal(t, l.constructionPurpose.Load(), before)
			}
			l.Retire()
			assert.Equal(t, l.constructionPurpose.Load(), nil)
			proposal.Retire()
			st.DeleteObject(testutil.KustomizationID("apps", "entry"))
			for range st.Sweep() {
			}
			runtime.GC()
			assert.Equal(t, declaration.Value(), nil)
			runtime.KeepAlive(proposal)
			runtime.KeepAlive(&guard)
		})
	}
}
