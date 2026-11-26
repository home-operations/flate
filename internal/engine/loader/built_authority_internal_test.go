package loader

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRegisterBuilt_KeepsCheckoutScope(t *testing.T) {
	t.Parallel()
	root, mount := t.TempDir(), t.TempDir()
	const entry = "clusters/dev"
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "flux-system",
		Name:      "fleet",
	}
	owner := testutil.KustomizationID("flux-system", "flux-system")
	sync := testutil.KustomizationYAML(owner.Name, owner.Namespace,
		"  path: ./clusters/dev\n  sourceRef: {kind: GitRepository, name: fleet}\n")
	testutil.WriteFile(t, root, entry+"/flux-system/kustomization.yaml", "resources: [sync.yaml]\n")
	testutil.WriteFile(t, root, entry+"/flux-system/sync.yaml", sync)
	testutil.WriteFile(
		t,
		root,
		entry+"/apps.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: fleet}\n",
		),
	)

	st := store.New()
	admitForTest(st, root)
	alias := &store.SourceArtifact{LocalPath: mount, LocalRoot: root, Provider: store.ProviderAlias, FS: os.DirFS(root)}
	st.SetArtifact(source, alias)
	l := New(st)
	t.Cleanup(l.Close)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), filepath.Join(root, entry), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.PreferExisting = true
	_, err = l.Load(t.Context(), filepath.Join(mount, entry), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.Admit(t.Context(), root))
	declaration := st.GetDeclaration(owner)
	assert.FatalIf(t, declaration == nil, "sync declaration missing before registration")
	input := supportedBuiltInputForTest(l, &BuiltInput{Root: filepath.Join(mount, entry), Owner: owner, Source: source})
	before, found := l.Index.Lookup(owner)
	assert.FatalIf(t, !found || before.File == "", "authored file missing for %s", owner)
	_, registrationErr := l.RegisterBuilt(t.Context(), input, builtObjectsForTest(t, sync))
	assert.NoError(t, registrationErr)
	assert.Equal(t, st.GetDeclaration(owner), declaration)
	assert.Equal(t, input.Support.Valid(), true)
	const stamp = "  labels:\n    kustomize.toolkit.fluxcd.io/name: flux-system\n" +
		"    kustomize.toolkit.fluxcd.io/namespace: flux-system\n"
	stampedSync := strings.Replace(sync, "spec:\n", stamp+"spec:\n", 1)
	stamped := builtObjectsForTest(t, stampedSync)
	assert.Equal(t, manifest.Equal(stamped[0].Obj, declaration), false)
	next := input
	next = supportedBuiltInputForTest(l, &next)
	_, registrationErr2 := l.RegisterBuilt(t.Context(), next, stamped)
	assert.NoError(t, registrationErr2)
	after, found := l.Index.Lookup(owner)
	assert.Equal(t, found, true)
	assert.Equal(t, after.File, before.File)
	assert.Equal(t, after.Path, before.Path)
	assert.Equal(t, after.Authored, owner)
	assert.Equal(t, declaration.GetLabels()["kustomize.toolkit.fluxcd.io/name"], "")
	assert.Equal(t, next.Support.Valid(), true)
	assert.Equal(t, l.scopesMatch(l.kustomizations(), root), true)
	scopes := slices.Clone(l.scopes.inputs)
	assert.NoError(t, l.Admit(t.Context(), root))
	assert.Equal(t, st.GetDeclaration(owner), declaration)
	assert.Equal(t, st.GetArtifact(source, store.ArtifactContent), store.Artifact(alias))
	assert.Equal(t, slices.EqualFunc(l.scopes.inputs, scopes, func(a, b scopeInput) bool {
		return a.owner == b.owner && a.source == b.source && a.published == b.published
	}), true)
}

func TestAdmitBuiltRoot_MountScopeWithoutOwnerPath(t *testing.T) {
	t.Parallel()
	root, mount := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: []\n")
	st := store.New()
	admitForTest(st, root)
	repo := &manifest.GitRepository{Name: "fleet", Namespace: "flux-system"}
	st.AddObject(repo)
	alias := &store.SourceArtifact{LocalPath: mount, LocalRoot: root, Provider: store.ProviderAlias, FS: os.DirFS(root)}
	st.SetArtifact(repo.Named(), alias)
	l := New(st)
	t.Cleanup(l.Close)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), mount, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, l.documents.artifacts[mount], alias)

	input := &BuiltInput{
		Root: mount, Owner: testutil.KustomizationID("flux-system", "flux-system"), Source: repo.Named(),
	}
	scope, admitted, err := l.admitBuiltRoot(t.Context(), input, mount)
	assert.NoError(t, err)
	assert.Equal(t, admitted, true)
	assert.Equal(t, scope, mount)
}

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
			_, registrationErr := l.RegisterBuilt(t.Context(),
				BuiltInput{
					Root:    packageRoot,
					Owner:   owner.Named(),
					Source:  manifest.BootstrapSourceID,
					Support: support,
				},
				children,
			)
			assert.NoError(t, registrationErr)
			record, ok := l.Index.Lookup(id)
			assert.FatalIf(t, !ok || record.Declaration != children[0].Obj, "effective declaration = %+v", record)
			assert.Equal(t, record.Authored, id)
			assert.Equal(t, l.registeredRoot(id, &record), packageRoot)
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
				_, committed, commitErr := l.commitSelection(t.Context(), t, proposal, selection.Construction)
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
			_, registrationErr2 := l.RegisterBuilt(t.Context(),
				BuiltInput{
					Root:    packageRoot,
					Owner:   owner.Named(),
					Source:  manifest.BootstrapSourceID,
					Support: replacement,
				},
				nil,
			)
			assert.NoError(t, registrationErr2)
			assert.NoError(t, l.Admit(t.Context(), root))
			got, complete := l.BuiltChildren(packageRoot, owner.Named(), binding)
			assert.Equal(t, l.registeredRoot(id, &record), "")
			assert.Equal(t, len(l.builtRoots), 0)
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
			assert.Equal(t, l.builtBatches[owner.Named()] != nil, true)
			l.Close()
			assert.FatalIf(
				t,
				l.builtBatches[owner.Named()] != nil,
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
			_, registrationErr := l.RegisterBuilt(t.Context(), input, children)
			assert.NoError(t, registrationErr)
			if invalidation == "native_source" {
				var before store.ReadSet
				before.Init(st, manifest.NamedResource{}, nil)
				facts := rootEvidenceForTest(l, children[0].Obj.Named(), &before)
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
			var documentReads store.ReadSet
			documentReads.Init(st, owner.Named(), nil)
			defer documentReads.Close()
			prepared, current := l.PreparedObject(children[0].Obj.Named(), &documentReads)
			assert.Equal(t, current, true)
			assert.Equal(t, prepared.Obj, children[0].Obj)
			assert.Diff(t, prepared.Doc, children[0].Doc)
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
				facts := rootEvidenceForTest(l, children[0].Obj.Named(), &after)
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
			assert.Equal(t, documentReads.Valid(), false)
			_, current = l.PreparedObject(children[0].Obj.Named(), &documentReads)
			assert.Equal(t, current, false)
			_, complete = l.BuiltChildren(packageRoot, owner.Named(), binding)
			assert.FatalIf(t, complete, "invalidated batch proved child presence or absence")
			publication, registrationErr2 := l.RegisterBuilt(t.Context(), input, nil)
			assert.NoError(t, registrationErr2)
			assert.Equal(t, publication, BuiltRejected)
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
		_, registrationErr := l.RegisterBuilt(t.Context(),
			BuiltInput{Root: packageRoot, Owner: owner.Named(), Source: manifest.BootstrapSourceID, Support: support},
			children,
		)
		assert.NoError(t, registrationErr)
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
	_, registrationErr := l.RegisterBuilt(t.Context(),
		BuiltInput{Root: packageRoot, Owner: owner.Named(), Source: manifest.BootstrapSourceID, Support: support},
		children,
	)
	assert.NoError(t, registrationErr)
	supplied := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "./supplied"}
	st.AddDeclaration(store.Declaration{Obj: supplied, Role: store.SuppliedDeclaration})
	reads := new(store.ReadSet)
	reads.Init(st, testutil.KustomizationID("apps", "binding"), nil)
	defer st.Retire(reads)
	got, current := l.BuiltChildren(packageRoot, owner.Named(), reads)
	assert.Equal(t, current, true)
	assert.Equal(t, len(got), 0)
	assert.Equal(t, reads.GetDeclaration(supplied.Named()), manifest.BaseManifest(supplied))
}

func TestRootEvidence_CapturesRequestedBuiltChild(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [child.yaml]\n")
	testutil.WriteFile(t, root, "parent/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./raw\n"))
	st := store.New()
	admitForTest(st, root)
	owner := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./parent"}
	st.AddObject(owner)
	l := New(st)
	defer l.Close()
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	packageRoot := filepath.Join(root, "parent")
	_, err := l.Load(t.Context(), packageRoot, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	documents := make([]string, 0, 32)
	for i := range 32 {
		documents = append(
			documents,
			testutil.KustomizationYAML(fmt.Sprintf("child-%d", i), "apps", "  path: ./entry\n"),
		)
	}
	children := builtObjectsForTest(t, strings.Join(documents, "---\n"))
	support := new(store.ReadSet)
	support.Init(st, owner.Named(), nil)
	support.GetDeclaration(owner.Named())
	support.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	_, registrationErr := l.RegisterBuilt(t.Context(),
		BuiltInput{Root: packageRoot, Owner: owner.Named(), Source: manifest.BootstrapSourceID, Support: support},
		children,
	)
	assert.NoError(t, registrationErr)
	var reads store.ReadSet
	reads.Init(st, testutil.KustomizationID("apps", "binding"), nil)
	defer reads.Retire()
	facts := rootEvidenceForTest(l, children[0].Obj.Named(), &reads)
	assert.Equal(t, facts.BuiltCurrent, true)
	assert.Equal(t, facts.BuiltChild.Obj, children[0].Obj)
	captured := slices.Collect(reads.Inputs())
	for _, child := range children[1:] {
		assert.Equal(t, slices.Contains(captured, child.Obj.Named()), false)
	}
	replacement := owner.Clone()
	replacement.Path = "./other"
	st.AddObject(replacement)
	assert.Equal(t, reads.Valid(), false)
	var fresh store.ReadSet
	fresh.Init(st, testutil.KustomizationID("apps", "binding"), nil)
	defer fresh.Retire()
	assert.Equal(t, rootEvidenceForTest(l, children[0].Obj.Named(), &fresh).BuiltCurrent, false)
}

// A prepared producer's directory probes must not claim unrelated descendant content.
func TestRegisterBuilt_DirectoryReadsDoNotOwnDescendants(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "workload/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t, root, "workload/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\n",
	)

	st := store.New()
	admitForTest(st, root)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./parent"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "./workload"}
	st.AddObject(parent)
	st.AddObject(child)

	l := New(st)
	defer l.Close()
	l.SourceRoot = root
	packageRoot := filepath.Join(root, "parent")
	_, err := l.Load(t.Context(), packageRoot, manifest.NamedResource{}, nil)
	assert.NoError(t, err)

	_, registrationErr := l.RegisterBuilt(t.Context(), supportedBuiltInputForTest(l, &BuiltInput{
		Root: packageRoot, Owner: parent.Named(), Source: manifest.BootstrapSourceID,
		Reads: map[string]bool{"workload": false, "missing.yaml": false, "workload/settings.txt": true},
	}), []store.Child{{Obj: child}})
	assert.NoError(t, registrationErr)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))

	owners := l.Index.OwnersOfFile("workload/cm.yaml").Owners
	assert.Equal(t, slices.Contains(owners, parent.Named()), false)
	assert.Equal(t, slices.Contains(owners, child.Named()), true)

	for _, file := range []string{"workload/cm.yaml", "workload/added.yaml", "missing.yaml"} {
		readers := slices.Collect(l.Index.ReadersOfFile(file))
		assert.Equal(t, slices.ContainsFunc(readers, func(reader manifest.FileReader) bool {
			return reader.Reader == parent.Named()
		}), false)
	}

	readers := slices.Collect(l.Index.ReadersOfFile("workload/settings.txt"))
	assert.Equal(t, slices.Contains(readers, manifest.FileReader{
		Reader: parent.Named(), Source: manifest.BootstrapSourceID, Exact: true,
	}), true)
}

func TestRootEvidence_PreparedNativeOwnerAfterClose(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		authored, compact bool
	}{
		{"authored false", false, true},
		{"authored true", true, false},
		{"authored compact", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			authored := tc.authored
			independent := !authored || !tc.compact
			root := t.TempDir()
			testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: []\n")
			if authored {
				testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [child.yaml]\n")
				testutil.WriteFile(
					t,
					root,
					"parent/child.yaml",
					testutil.KustomizationYAML("child", "apps", "  path: ./before\n"),
				)
			}
			st := store.New()
			admitForTest(st, root)
			owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "parent", manifest.NamedResource{})
			st.AddObject(owner)
			l := New(st)
			defer l.Close()
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: tc.compact})
			packageRoot := filepath.Join(root, "parent")
			_, err := l.Load(t.Context(), packageRoot, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			children := builtObjectsForTest(t, testutil.KustomizationYAML("child", "apps", "  path: ./entry\n"))
			_, registrationErr := l.RegisterBuilt(t.Context(), supportedBuiltInputForTest(l, &BuiltInput{
				Root: packageRoot, Owner: owner.Named(), Source: manifest.BootstrapSourceID,
			}), children)
			assert.NoError(t, registrationErr)
			id := children[0].Obj.Named()
			construction := rootEvidenceForTest(l, id, nil)
			assert.Equal(t, construction.Captured, false)
			assert.Equal(t, construction.Scope.Prepared, true)
			assert.Equal(t, construction.BuiltChild.Obj, children[0].Obj)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			st.SetDiscoveryIndex(l.Index)
			var before store.ReadSet
			before.Init(st, manifest.NamedResource{}, nil)
			defer before.Retire()
			evidence := rootEvidenceForTest(l, id, &before)
			assert.Equal(t, evidence.Scope.Prepared, true)
			assert.Equal(t, evidence.PreparedRoot, packageRoot)
			assert.Equal(t, evidence.BuiltChild.Obj, children[0].Obj)
			assert.Equal(t, selection.Root(evidence).Independent, independent)
			assert.Equal(t, selection.Root(evidence).InitialAdmitted, authored && tc.compact)
			assert.Equal(t, selection.Root(evidence).InitialIndependent, false)
			if authored && !tc.compact {
				assert.Equal(t, selection.Root(evidence).Admitted, false)
			}
			if authored && tc.compact {
				needed := evidence
				needed.Needed = true
				assert.Equal(t, selection.Root(needed).Admitted, true)
				assert.Equal(t, selection.Root(needed).Independent, independent)
				assert.Equal(t, selection.Root(needed).Entry, false)
			}
			input := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			replacement := *input
			replacement.Content = "replacement"
			st.SetArtifact(manifest.BootstrapSourceID, &replacement)
			var replacedReads store.ReadSet
			replacedReads.Init(st, manifest.NamedResource{}, nil)
			defer replacedReads.Retire()
			replaced := rootEvidenceForTest(l, id, &replacedReads)
			assert.Equal(t, replaced.Scope.Prepared, true)
			assert.Equal(t, replaced.PreparedRoot, "")
			assert.Equal(t, replaced.BuiltCurrent, false)
			catalog := l.DetachCatalog()
			defer catalog.Close()
			var replacedCatalogReads store.ReadSet
			replacedCatalogReads.Init(st, manifest.NamedResource{}, nil)
			defer replacedCatalogReads.Retire()
			replacedCatalog := rootCatalogEvidenceForTest(catalog, id, &replacedCatalogReads)
			assert.Equal(t, replacedCatalog.Scope.Prepared, true)
			assert.Equal(t,
				cmp.Diff(
					selection.Root(replaced), selection.Root(replacedCatalog),
					cmp.Exporter(func(reflect.Type) bool { return true }),
				),
				"",
			)
			st.SetArtifact(manifest.BootstrapSourceID, input)
			l.Close()
			var after store.ReadSet
			after.Init(st, manifest.NamedResource{}, nil)
			defer after.Retire()
			detached := rootCatalogEvidenceForTest(catalog, id, &after)
			assert.Equal(t, detached.Scope.Prepared, true)
			assert.Equal(t, selection.Root(detached).Independent, independent)
			assert.Equal(t,
				cmp.Diff(
					selection.Root(evidence), selection.Root(detached),
					cmp.Exporter(func(reflect.Type) bool { return true }),
				),
				"",
			)
			// Detached reporting cannot borrow retired build captures or prepare new objects.
			assert.Equal(t, detached.PreparedRoot, "")
			assert.Equal(t, detached.BuiltChild.Obj, manifest.BaseManifest(nil))
			assert.Equal(t, detached.BuiltCurrent, false)
			evidence.PreparedRoot, evidence.BuiltChild, evidence.BuiltCurrent = "", store.Child{}, false
			assert.Equal(t,
				cmp.Diff(
					evidence, detached,
					cmp.Exporter(func(reflect.Type) bool { return true }),
				),
				"",
			)
			assert.Equal(t, after.Valid(), true)
		})
	}
}

func TestPreparedObject_SharedDirectoryProducerLifetimes(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [parent.yaml, child.yaml]\n")
	parentYAML := testutil.KustomizationYAML("parent", "apps", "  path: ./parent\n")
	childYAML := testutil.KustomizationYAML("child", "apps", "  path: ./workload\n")
	testutil.WriteFile(t, root, "parent/parent.yaml", parentYAML)
	testutil.WriteFile(t, root, "parent/child.yaml", childYAML)
	st := store.New()
	admitForTest(st, root)
	native := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "parent", manifest.NamedResource{})
	st.AddObject(native)
	l := New(st)
	defer l.Close()
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{})
	packageRoot := filepath.Join(root, "parent")
	_, err := l.Load(t.Context(), packageRoot, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	initial := builtObjectsForTest(t, parentYAML+"---\n"+childYAML)
	nativeInput := supportedBuiltInputForTest(l, &BuiltInput{
		Root: packageRoot, Owner: native.Named(), Source: manifest.BootstrapSourceID,
	})
	_, registrationErr := l.RegisterBuilt(t.Context(), nativeInput, initial)
	assert.NoError(t, registrationErr)
	parent, child := initial[0].Obj.Named(), initial[1].Obj.Named()
	current := builtObjectsForTest(t, parentYAML+"---\n"+childYAML)
	parentInput := supportedBuiltInputForTest(l, &BuiltInput{
		Root: packageRoot, Owner: parent, Source: manifest.BootstrapSourceID,
	})
	_, registrationErr2 := l.RegisterBuilt(t.Context(), parentInput, current)
	assert.NoError(t, registrationErr2)
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{}, nil)
	defer st.Retire(&reads)
	retained, valid := l.PreparedObject(parent, &reads)
	assert.Equal(t, valid, true)
	assert.Equal(t, retained.Obj, initial[0].Obj)
	prepared, valid := l.BuiltChildren(packageRoot, parent, &reads)
	assert.Equal(t, valid, true)
	assert.Equal(t, len(prepared), 1)
	assert.Equal(t, prepared[0].Obj, current[1].Obj)
	replacement := supportedBuiltInputForTest(l, &BuiltInput{
		Root: packageRoot, Owner: parent, Source: manifest.BootstrapSourceID,
	})
	validationInput := testutil.KustomizationID("apps", "validation-input")
	replacement.Support.GetDeclaration(validationInput)
	_, registrationErr3 := l.RegisterBuilt(t.Context(), replacement, nil)
	assert.NoError(t, registrationErr3)
	assert.Equal(t, l.builtBatches[parent].input.Support, replacement.Support)
	record, _ := l.Index.Lookup(parent)
	assert.Equal(t, l.registeredRoot(parent, &record), packageRoot)
	_, known := l.Index.Lookup(child)
	assert.Equal(t, known, false)
	assert.Equal(t, nativeInput.Support.Valid(), true)
	l.scopes.roots = []string{"parent"}
	assert.NoError(t, l.captureNativeRoots(root))
	st.AddObject(&manifest.Kustomization{Name: validationInput.Name, Namespace: validationInput.Namespace})
	assert.Equal(t, replacement.Support.Valid(), false)
	l.SweepPreparations()
	assert.Equal(t, l.builtBatches[parent] == nil, true)
	assert.Equal(t, nativeInput.Support.Valid(), true)
	var survivingReads store.ReadSet
	survivingReads.Init(st, manifest.NamedResource{}, nil)
	defer st.Retire(&survivingReads)
	retained, valid = l.PreparedObject(parent, &survivingReads)
	assert.Equal(t, valid, true)
	assert.Equal(t, retained.Obj, initial[0].Obj)
	assert.Equal(t, l.SourceFacts(manifest.BootstrapSourceID, nil).Context.Roots[0].Built, true)
	assert.NoError(t, l.captureNativeRoots(root))
	roots := l.SourceFacts(manifest.BootstrapSourceID, nil).Context.Roots
	assert.FatalIf(t, len(roots) != 1, "native roots = %v", roots)
	assert.Equal(t, roots[0].Path, "parent")
	assert.Equal(t, roots[0].Built, true)
	st.DeleteObject(native.Named())
	l.SweepPreparations()
	assert.Equal(t, l.SourceFacts(manifest.BootstrapSourceID, nil).Context.Roots[0].Built, false)
	assert.Equal(t, roots[0].Built, true)
	l.Close()
	assert.Equal(t, len(l.builtBatches), 0)
	assert.Equal(t, l.registeredRoot(parent, &record), "")
}
