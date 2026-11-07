package loader

import (
	"path/filepath"
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestBuiltChildren_ConstructionDeclarations(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		compact bool
	}{
		{name: "full"},
		{name: "compact", compact: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [child.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"parent/child.yaml",
				testutil.KustomizationYAML("child", "apps", "  path: ./unpatched\n"),
			)
			st := store.New()
			admitForTest(st, root)
			owner := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./parent"}
			st.AddObject(owner)
			l := New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: tc.compact})

			packageRoot := filepath.Join(root, "parent")
			_, err := l.Load(t.Context(), packageRoot, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			id := testutil.KustomizationID("apps", "child")
			raw := candidateForTest(t, l, id)
			children := builtObjectsForTest(t, testutil.KustomizationYAML("child", "apps", "  path: ./entry\n"))
			support := new(store.ReadSet)
			support.Init(st, owner.Named(), nil)
			support.GetDeclaration(owner.Named())
			support.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
			l.RegisterBuilt(
				BuiltInput{
					Root:    packageRoot,
					Owner:   owner.Named(),
					Source:  manifest.BootstrapSourceID,
					Support: support,
				},
				children,
			)
			record, ok := l.Index.Lookup(id)
			assert.FatalIf(t, !ok || record.Declaration != children[0].Obj, "effective declaration = %+v", record)
			assert.Equal(t, record.Authored, id)
			if tc.compact {
				assert.Equal(t, l.candidates[id].Obj, raw)
			}
			binding := new(store.ReadSet)
			binding.Init(st, testutil.KustomizationID("apps", "binding"), nil)
			defer st.Retire(binding)
			for range 2 {
				got, complete := l.BuiltChildren(packageRoot, owner.Named(), binding)
				assert.FatalIf(t, !complete || len(got) != 1 || got[0].Obj != record.Declaration,
					"construction children = %+v, complete = %v", got, complete)
				assert.Equal(t, got[0].Obj.(*manifest.Kustomization).Path, "./entry")
				assert.Equal(t, record.BuilderOwner, owner.Named())
				assert.Diff(t, got[0].Productions, record.Productions)
				assert.FatalIf(t, !slices.ContainsFunc(record.Productions, func(p manifest.Production) bool {
					return p.Owner == owner.Named() && p.Source == manifest.BootstrapSourceID
				}), "source production missing: %+v", record.Productions)
			}
			assert.Equal(t, st.GetArtifact(owner.Named(), store.ArtifactContent), nil)
			_, _, emission := st.GetEmission(owner.Named())
			assert.FatalIf(
				t,
				emission.Complete || emission.Footprint() != nil,
				"construction published runtime output: %+v",
				emission,
			)
			emission.Finish()
			status, _ := st.GetStatus(owner.Named())
			assert.FatalIf(t, status.Status == store.StatusReady, "construction marked the owner Ready")
			configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{id}})
			commit := func(child store.Child, reads *store.ReadSet) bool {
				proposal, prepareErr := prepareSelection(t.Context(), t, l, selection.PrepareRequest{
					Changes:    l.Options.Changes,
					Generation: []store.Child{child},
					Reads:      reads,
					Invocation: true,
				})
				assert.NoError(t, prepareErr)
				_, committed, commitErr := l.Options.Session.Commit(t.Context(), proposal, selection.Construction)
				assert.NoError(t, commitErr)
				return committed
			}

			current, valid := l.BuiltChildren(packageRoot, owner.Named(), binding)
			assert.FatalIf(t, !valid || len(current) != 1, "supported construction batch disappeared")
			current[0].Entry = &manifest.EntryApplication{}
			committed := commit(current[0], binding)
			assert.Equal(t, committed, false)
			assert.Equal(t, binding.Valid(), false)
			st.Retire(binding)
			binding = new(store.ReadSet)
			binding.Init(st, testutil.KustomizationID("apps", "binding"), nil)
			defer st.Retire(binding)
			current, valid = l.BuiltChildren(packageRoot, owner.Named(), binding)
			assert.FatalIf(t, !valid || len(current) != 1, "supported construction batch disappeared")
			current[0].Entry = &manifest.EntryApplication{}
			committed = commit(current[0], binding)
			assert.Equal(t, committed, true)
			assert.Equal(t, st.GetDeclaration(id), children[0].Obj)

			replacement := new(store.ReadSet)
			replacement.Init(st, owner.Named(), nil)
			replacement.GetDeclaration(owner.Named())
			replacement.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
			l.RegisterBuilt(
				BuiltInput{
					Root:    packageRoot,
					Owner:   owner.Named(),
					Source:  manifest.BootstrapSourceID,
					Support: replacement,
				},
				nil,
			)
			l.Admit(root)
			got, complete := l.BuiltChildren(packageRoot, owner.Named(), binding)
			assert.FatalIf(t, !complete || len(got) != 0, "replacement = %+v, complete = %v", got, complete)
			_, ok = l.Index.Lookup(id)
			assert.FatalIf(
				t,
				ok || l.candidates[id].Obj != nil,
				"omitted child retained construction or authored authority",
			)

			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			_, complete = l.BuiltChildren(packageRoot, owner.Named(), binding)
			assert.FatalIf(t, complete, "frozen loader retained construction authority")
			l.Retire()
			assert.Equal(t, l.documents.cache[packageRoot].builtInput != nil, true)
			l.Close()
			assert.FatalIf(
				t,
				l.documents.cache[packageRoot].builtInput != nil,
				"retired loader retained preparation capture",
			)
		})
	}
}

func TestBuiltChildren_InvalidatedPreparation(t *testing.T) {
	t.Parallel()
	for _, invalidation := range []string{"owner", "source", "native_source"} {
		t.Run(invalidation, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: []\n")
			st := store.New()
			admitForTest(st, root)
			owner := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./parent"}
			if invalidation == "native_source" {
				owner = manifest.NewEntryKustomization(manifest.BootstrapSourceID, "parent", manifest.NamedResource{})
			}
			st.AddObject(owner)
			l := New(st)
			l.SourceRoot = root
			if invalidation == "native_source" {
				configureSelection(t, l, selection.Request{Changed: true})
			}
			packageRoot := filepath.Join(root, "parent")
			_, err := l.Load(t.Context(), packageRoot, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			children := builtObjectsForTest(t, testutil.KustomizationYAML("child", "apps", "  path: ./entry\n"))
			support := new(store.ReadSet)
			support.Init(st, owner.Named(), nil)
			support.GetDeclaration(owner.Named())
			source := support.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			input := BuiltInput{
				Root:    packageRoot,
				Owner:   owner.Named(),
				Source:  manifest.BootstrapSourceID,
				Support: support,
			}
			l.RegisterBuilt(input, children)
			if invalidation == "native_source" {
				var before store.ReadSet
				before.Init(st, manifest.NamedResource{}, nil)
				facts := l.RootEvidence(children[0].Obj.Named(), &before)
				assert.Equal(t, facts.PreparedRoot, packageRoot)
				assert.Equal(t, facts.Scope.Prepared, true)
				assert.Equal(t, selection.Root(facts).Independent, true)
				before.Retire()
			}
			binding := new(store.ReadSet)
			binding.Init(st, testutil.KustomizationID("apps", "binding"), nil)
			defer st.Retire(binding)
			_, complete := l.BuiltChildren(packageRoot, owner.Named(), binding)
			assert.FatalIf(t, !complete, "supported preparation unavailable")
			if invalidation == "owner" {
				replaced := *owner
				replaced.Path = "./replacement"
				st.AddObject(&replaced)
			} else {
				replaced := *source
				replaced.Content = "replacement"
				st.SetArtifact(manifest.BootstrapSourceID, &replaced)
			}
			if invalidation == "native_source" {
				var after store.ReadSet
				after.Init(st, manifest.NamedResource{}, nil)
				facts := l.RootEvidence(children[0].Obj.Named(), &after)
				assert.Equal(t, facts.Record.Builder != "", true)
				assert.Equal(t, facts.PreparedRoot, "")
				assert.Equal(t, facts.Scope.Prepared, false)
				assert.Equal(t, facts.BuiltCurrent, false)
				result := selection.Root(facts)
				assert.Equal(t, result.Owner, owner.Named())
				assert.Equal(t, result.Admitted, false)
				assert.Equal(t, result.Independent, false)
				after.Retire()
			}
			assert.FatalIf(
				t,
				support.Valid() || binding.Valid(),
				"replaced input retained preparation or proposal authority",
			)
			_, complete = l.BuiltChildren(packageRoot, owner.Named(), binding)
			assert.FatalIf(t, complete, "invalidated batch proved child presence or absence")
			l.RegisterBuilt(input, nil)
			fresh := new(store.ReadSet)
			fresh.Init(st, owner.Named(), nil)
			defer st.Retire(fresh)
			_, complete = l.BuiltChildren(packageRoot, owner.Named(), fresh)
			assert.FatalIf(t, complete, "rejected preparation installed an empty batch")
			l.Retire()
		})
	}
}

func TestBuiltChildren_RetiredSourceAuthority(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: []\n")
	st := store.New()
	admitForTest(st, root)
	source := *st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	source.Content = "source"
	st.SetArtifact(manifest.BootstrapSourceID, &source)
	owner := &manifest.Kustomization{
		Name: "parent", Namespace: "apps", Path: "./parent",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.BootstrapSourceID.Kind,
			Name:      manifest.BootstrapSourceID.Name,
			Namespace: manifest.BootstrapSourceID.Namespace,
		},
	}
	st.AddObject(owner)
	l := New(st)
	l.SourceRoot = root
	packageRoot := filepath.Join(root, "parent")
	_, err := l.Load(t.Context(), packageRoot, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	children := builtObjectsForTest(t, testutil.KustomizationYAML("child", "apps", "  path: ./entry\n"))
	register := func() {
		support := new(store.ReadSet)
		support.Init(st, owner.Named(), nil)
		support.GetDeclaration(owner.Named())
		support.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
		l.RegisterBuilt(
			BuiltInput{Root: packageRoot, Owner: owner.Named(), Source: manifest.BootstrapSourceID, Support: support},
			children,
		)
	}
	register()
	st.SetDiscoveryIndex(l.Index)
	var output store.ReadSet
	output.Init(st, owner.Named(), nil)
	emission := &store.Emission{Complete: true}
	output.Bind(emission)
	st.RecordEmitted(owner.Named(), emission, nil)
	defer st.Retire(&output)
	register()
	var reads store.ReadSet
	reads.Init(st, testutil.KustomizationID("apps", "binding"), nil)
	defer st.Retire(&reads)
	_, current := l.BuiltChildren(packageRoot, owner.Named(), &reads)
	assert.Equal(t, current, false)
	assert.Equal(t, st.GetDeclaration(children[0].Obj.Named()), nil)
	l.Retire()
}

func TestBuiltChildren_SuppliedPrecedence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: []\n")
	st := store.New()
	admitForTest(st, root)
	owner := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./parent"}
	st.AddObject(owner)
	l := New(st)
	l.SourceRoot = root
	packageRoot := filepath.Join(root, "parent")
	_, err := l.Load(t.Context(), packageRoot, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	children := builtObjectsForTest(t, testutil.KustomizationYAML("child", "apps", "  path: ./entry\n"))
	support := new(store.ReadSet)
	support.Init(st, owner.Named(), nil)
	support.GetDeclaration(owner.Named())
	support.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	l.RegisterBuilt(
		BuiltInput{Root: packageRoot, Owner: owner.Named(), Source: manifest.BootstrapSourceID, Support: support},
		children,
	)
	supplied := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "./supplied"}
	st.AddDeclaration(store.Declaration{Obj: supplied, Origin: manifest.SuppliedDeclaration})
	reads := new(store.ReadSet)
	reads.Init(st, testutil.KustomizationID("apps", "binding"), nil)
	defer st.Retire(reads)
	got, current := l.BuiltChildren(packageRoot, owner.Named(), reads)
	assert.Equal(t, current, true)
	assert.Equal(t, len(got), 0)
	assert.Equal(t, reads.GetDeclaration(supplied.Named()), manifest.BaseManifest(supplied))
}
