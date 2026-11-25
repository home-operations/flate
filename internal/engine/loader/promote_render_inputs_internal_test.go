package loader

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRegisterBuilt_RenderInputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, before, after, file string
	}{
		{name: "prune", before: "prune: true", after: "prune: false", file: "child.yaml"},
		{name: "path", before: "path: ./app", after: "path: ./other", file: "kustomization.yaml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [child.yaml]\n")
			child := testutil.KustomizationYAML("child", "apps", "  path: ./app\n  prune: true\n")
			testutil.WriteFile(t, root, "child.yaml", child)
			st := store.New()
			admitForTest(st, root)
			owner := &manifest.Kustomization{Name: "entry", Namespace: "apps", Path: "."}
			st.AddObject(owner)
			l := New(st)
			l.SourceRoot = root
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			docs := builtObjectsForTest(t, strings.Replace(child, tc.before, tc.after, 1))
			_, registrationErr := l.RegisterBuilt(
				t.Context(),
				supportedBuiltInputForTest(
					l,
					&BuiltInput{Root: root, Owner: owner.Named(), Source: manifest.BootstrapSourceID},
				),
				docs,
			)
			assert.NoError(t, registrationErr)
			record, ok := l.Index.Lookup(docs[0].Obj.Named())
			assert.Equal(t, ok, true)
			assert.Equal(t, record.File, tc.file)
			assert.Equal(t, record.Builder, "kustomization.yaml")
		})
	}
}

func TestRegisterBuilt_CompleteEmptyReplacementRetiresOnlyItsOwner(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "previous/kustomization.yaml", "resources: [release.yaml]\n")
	st := store.New()
	admitForTest(st, root)
	l := New(st)
	l.SourceRoot = root
	entry := filepath.Join(root, "entry")
	_, err := l.Load(t.Context(), entry, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	owner := &manifest.Kustomization{Name: "current", Namespace: "apps", Path: "./previous"}
	st.AddObject(owner)
	first := (manifest.Production{Owner: owner.Named(), Source: manifest.BootstrapSourceID, File: "previous/release.yaml"})
	second := (manifest.Production{
		Owner: testutil.KustomizationID("apps", "second"), Source: manifest.BootstrapSourceID, File: "second/release.yaml",
	})
	pending := &manifest.HelmRelease{Name: "demo"}
	l.Index.Records[pending.Named()] = manifest.DiscoveryRecord{
		Path: filepath.Join(root, first.File), File: first.File, Productions: []manifest.Production{first, second},
	}
	input := BuiltInput{Root: entry, Source: manifest.BootstrapSourceID, Owner: owner.Named()}

	oldInput := input
	oldInput.Root = filepath.Join(root, "previous")
	incomplete := supportedBuiltInputForTest(l, &oldInput)
	replacement := owner.Clone()
	replacement.Path = "./entry"
	st.AddObject(replacement)
	assert.Equal(t, incomplete.Support.Valid(), false)
	_, registrationErr := l.RegisterBuilt(t.Context(), incomplete, nil)
	assert.NoError(t, registrationErr)
	assert.Diff(t, l.Index.Records[pending.Named()].Productions, []manifest.Production{first, second})

	complete := supportedBuiltInputForTest(l, &input)
	_, registrationErr2 := l.RegisterBuilt(t.Context(), complete, nil)
	assert.NoError(t, registrationErr2)
	assert.Diff(t, l.Index.Records[pending.Named()].Productions, []manifest.Production{second})
}

func TestRegisterBuilt_EntryScopeSurvivesOrdinaryRegistration(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: []\n")
	st := store.New()
	admitForTest(st, root)
	l := New(st)
	t.Cleanup(l.Close)
	l.SourceRoot = root
	entry := filepath.Join(root, "leaf")
	_, err := l.Load(t.Context(), entry, manifest.NamedResource{}, nil)
	assert.NoError(t, err)

	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./leaf"}
	child := &manifest.HelmRelease{Name: "outside", Namespace: "apps"}
	st.AddObject(owner)
	input := BuiltInput{Root: entry, Source: manifest.BootstrapSourceID, Owner: owner.Named()}
	full := []store.Child{{Obj: child}}
	_, registrationErr := l.RegisterBuilt(t.Context(), supportedBuiltInputForTest(l, &input), full)
	assert.NoError(t, registrationErr)
	_, ok := l.Index.Lookup(child.Named())
	assert.Equal(t, ok, true)

	prepared := supportedBuiltInputForTest(l, &input)
	scope := &selection.EntryScope{Preparation: prepared.Support}
	l.SetEntryScope(owner.Named(), scope)
	l.Store.Retire(prepared.Support)
	assert.Equal(t, l.RootEvidence(owner.Named(), nil).EntryScope, scope)
	_, registrationErr2 := l.RegisterBuilt(t.Context(), supportedBuiltInputForTest(l, &input), full)
	assert.NoError(t, registrationErr2)
	_, ok = l.Index.Lookup(child.Named())
	assert.Equal(t, ok, false)
	assert.Equal(t, l.RootEvidence(owner.Named(), nil).EntryScope, scope)
	assert.Equal(t, scope.Current(), true)

	catalog := l.DetachCatalog()
	t.Cleanup(catalog.Close)
	l.Close()
	assert.Equal(t, catalog.RootEvidence(owner.Named(), nil).EntryScope, scope)
	assert.Equal(t, scope.Current(), true)
	before, _ := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	replacement := *before
	replacement.Content = "replacement"
	st.SetArtifact(manifest.BootstrapSourceID, &replacement)
	assert.Equal(t, scope.Current(), false)
}
