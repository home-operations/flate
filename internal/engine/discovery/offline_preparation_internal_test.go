package discovery

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type cancelOnInventoryReturn struct {
	context.Context //nolint:containedctx // The fixture overrides Err at the cancellation boundary.

	cancel  context.CancelFunc
	checked bool
}

func (c *cancelOnInventoryReturn) Err() error {
	if c.checked {
		c.cancel()
	}
	c.checked = true
	return c.Context.Err()
}

func TestPrepareLookupPackage_CanceledAfterInventoryReuse(t *testing.T) {
	t.Parallel()
	input := &store.SourceArtifact{LocalPath: t.TempDir(), Provider: store.ProviderInput}
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{})
	session := newDiscoverer(&Config{Store: store.New(), Input: input, Path: input.LocalPath})
	t.Cleanup(session.Close)
	prepared := &declarationPreparation{owner: owner, source: input, complete: true}
	session.prepared = map[manifest.NamedResource]*declarationPreparation{owner.Named(): prepared}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	boundary := &cancelOnInventoryReturn{Context: ctx, cancel: cancel}
	err := session.prepareLookupPackage(boundary, owner, input, nil)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, prepared.lookup, noLookup)
}

func TestFinishLookup_ChangedExploratorySupport(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "entry")
	id := manifest.BootstrapSourceID
	owner := manifest.NewEntryKustomization(id, "candidate", manifest.NamedResource{})
	input := &store.SourceArtifact{
		LocalPath: root, Provider: store.ProviderInput,
		FS: fstest.MapFS{
			"candidate/kustomization.yaml": {Data: []byte("namespace: other\nresources: [missing.yaml]\n")},
		},
	}
	st := store.New()
	st.SetArtifact(id, input)
	session := newDiscoverer(&Config{
		Store: st, Input: input, Path: path, Trees: testTrees(t), PrepareDocuments: base.PrepareDocuments,
	})
	t.Cleanup(session.Close)
	frame, err := session.lookupInventory(t.Context(), owner, input, exploratoryLookup)
	assert.NoError(t, err)
	assert.FatalIf(t, frame.inventoryError() == nil, "expected an inventory error")
	result := &Result{}
	session.finishLookup(result)
	assert.Equal(t, len(result.LookupEvidence), 1)
	failure := result.LookupEvidence[0]
	assert.Equal(t, failure.Package, manifest.DiagnosticFor(owner.Named(), owner))
	assert.Equal(t, failure.Path, path)
	assert.Equal(t, failure.Cause, frame.inventoryError())
	assert.Equal(t, len(result.LookupFailures), 0)
	_, failed := st.GetStatus(owner.Named())
	assert.Equal(t, failed, false)

	replacement := &store.SourceArtifact{
		LocalPath: root, Provider: store.ProviderInput,
		FS: fstest.MapFS{
			"candidate/kustomization.yaml": {Data: []byte("namespace: other\nresources: []\n")},
		},
	}
	st.SetArtifact(id, replacement)
	result = &Result{}
	session.finishLookup(result)
	assert.Equal(t, len(result.LookupEvidence), 0)
	replaced, err := session.lookupInventory(t.Context(), owner, replacement, exploratoryLookup)
	assert.NoError(t, err)
	assert.Equal(t, replaced.inventoryReady(), true)
	session.finishLookup(result)
	assert.Equal(t, len(result.LookupEvidence), 0)
}

type lookupReadChange struct {
	fstest.MapFS

	path      string
	validated bool
}

func (f *lookupReadChange) ReadFile(name string) ([]byte, error) {
	data, err := f.MapFS.ReadFile(name)
	if err != nil || name != f.path {
		return data, err
	}
	if !f.validated {
		f.validated = true
		return data, nil
	}
	return []byte(string(data) + "# changed read support\n"), nil
}

func TestPrepareLookupPackage_ChangedReadSupport(t *testing.T) {
	t.Parallel()
	const control = "package/kustomization.yaml"
	const outside = "package/outside.yaml"
	for _, tc := range []struct{ name, file, sort string }{
		{name: "preflight support", file: control, sort: "sortOptions: {order: fifo}\n"},
		{name: "observation support", file: outside},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := &lookupReadChange{MapFS: fstest.MapFS{
				control: {Data: []byte("resources: [outside.yaml, leaf/inside.yaml]\n" + tc.sort)},
				outside: {
					Data: []byte(testutil.KustomizationYAML("outside", "apps", "  path: ./outside\n")),
				},
				"package/leaf/inside.yaml": {
					Data: []byte(testutil.KustomizationYAML("inside", "apps", "  path: ./inside\n")),
				},
			}}
			entry := filepath.Join(root, "package", "leaf")
			input := &store.SourceArtifact{LocalPath: root, FS: files, Provider: store.ProviderInput}
			owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "package", manifest.NamedResource{})
			match := testutil.KustomizationID("apps", "outside")
			session := newDiscoverer(&Config{
				Store:            store.New(),
				Input:            input,
				Path:             entry,
				Trees:            testTrees(t),
				PrepareDocuments: base.PrepareDocuments,
				Selection:        selection.Request{Roots: []manifest.NamedResource{match}},
			})
			t.Cleanup(session.Close)
			assert.NoError(t, session.prepareLookupPackage(t.Context(), owner, input, nil))
			frame := session.prepared[owner.Named()]
			assert.NoError(t, frame.lookupErr)
			assert.Equal(t, len(frame.children), 2)

			files.path = tc.file
			assert.NoError(t, session.prepareLookupPackage(t.Context(), owner, input, nil))
			assert.Equal(t, session.prepared[owner.Named()], frame)
			assert.FatalIf(t, frame.lookupErr == nil, "the outside match must remain unresolved")
			assert.Equal(
				t,
				frame.lookupErr.Error(),
				selection.EntryMembershipFailure(entry, owner.Named(), match).Error(),
			)
			assert.Equal(t, frame.registered, false)
			_, indexed := session.loader.Index.Lookup(match)
			assert.Equal(t, indexed, false)
		})
	}
}

func TestAdvance_UnreadablePreparationDefers(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"unreadable", "readable failure", "malformed readable", "canceled"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/parent.yaml", testutil.KustomizationYAML(
				"parent",
				"apps",
				"  path: ./parent\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n  postBuild: {substituteFrom: [{kind: Secret, name: settings}]}\n",
			))
			testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [healthy.yaml, child.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"parent/healthy.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"parent/child.yaml",
				testutil.KustomizationYAML("child", "apps", "  path: ${TOKEN}\n"),
			)
			st := store.New()
			value := "ENC[AES256_GCM,data:secret,iv:x,tag:y,type:str]"
			if state == "readable failure" {
				value = "["
			}
			secret := &manifest.Secret{Name: "settings", Namespace: "apps", StringData: map[string]any{"TOKEN": value}}
			if state == "malformed readable" {
				secret.StringData = nil
				secret.Data = map[string]any{"TOKEN": "!!!"}
			}
			st.AddObject(secret)
			l := loader.New(st)
			configureSelection(t, l, selection.Request{Changed: true})
			session, err := beginTest(t.Context(), Config{
				Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st, Loader: l,
				Trees: testTrees(t), Selection: selection.Request{Changed: true},
			})
			assert.NoError(t, err)
			t.Cleanup(session.Close)
			parent := testutil.KustomizationID("apps", "parent")
			child := testutil.KustomizationID("apps", "child")
			record, _ := l.Index.Lookup(parent)
			declaration := l.IndexedObject(parent, record)
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "flux", manifest.NamedResource{})
			st.RecordEmitted(entry.Named(), &store.Emission{Children: []store.Child{declaration}, Complete: true}, nil)
			st.AddObject(declaration.Obj, declaration.Doc)
			ctx := t.Context()
			if state == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			_, err = session.Advance(ctx, &selection.AdmissionResult{Needed: []manifest.NamedResource{child}})
			switch state {
			case "unreadable":
				assert.NoError(t, err)
				assert.Equal(t, st.GetDeclaration(child) == nil, true)
				frame, prepareErr := session.prepareDocuments(
					ctx,
					declaration.Obj.(*manifest.Kustomization),
					session.cfg.Input,
					nil,
				)
				assert.NoError(t, prepareErr)
				assert.Equal(t, frame.complete, false)
				assert.Equal(t, frame.registered, false)
				assert.Equal(t, len(frame.children), 0)
				assert.Equal(t, frame.receipt == nil, true)
				assert.NoError(
					t,
					session.prepareProducer(ctx, declaration.Obj.(*manifest.Kustomization), session.cfg.Input, nil),
				)
				assert.Equal(t, frame.registered, false)
			case "readable failure", "malformed readable":
				if err == nil {
					err = session.prepareProducer(
						ctx,
						declaration.Obj.(*manifest.Kustomization),
						session.cfg.Input,
						nil,
					)
				}
				assert.FatalIf(t, err == nil, "readable render failure disappeared")
			case "canceled":
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			}
			assert.Equal(
				t,
				st.GetObject(
					manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "healthy"},
				) == nil,
				true,
			)
		})
	}
}

func TestAdvance_RestrictedLookupWaitsForSubstitution(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	entry := filepath.Join(root, "package", "leaf")
	testutil.WriteFile(t, root, "package/kustomization.yaml", "resources: [leaf/inside.yaml, outside.yaml]\n")
	testutil.WriteFile(t, root, "package/leaf/inside.yaml", testutil.KustomizationYAML(
		"${CHILD}", "apps", "  path: ./inside\n"))
	testutil.WriteFile(t, root, "package/outside.yaml", testutil.KustomizationYAML(
		"outside", "apps", "  path: ./outside\n"))
	st := store.New()
	l := loader.New(st)
	child := testutil.KustomizationID("apps", "rendered")
	request := selection.Request{Roots: []manifest.NamedResource{child}}
	configureSelection(t, l, request)
	session, err := beginTest(t.Context(), Config{
		Path: entry, RepoRoot: root, Store: st, Loader: l, Trees: testTrees(t), Selection: request,
	})
	assert.NoError(t, err)
	t.Cleanup(session.Close)

	owner := &manifest.Kustomization{
		Name: "enclosing", Namespace: "apps", Path: "./package",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "settings"},
		}},
	}
	st.AddObject(owner)
	assert.NoError(t, session.prepareLookupPackage(t.Context(), owner, session.cfg.Input, nil))
	scope := l.RootEvidence(owner.Named(), nil).EntryScope
	assert.FatalIf(t, scope == nil || scope.Current(), "scope prepared before substitution input")
	assert.Equal(t, scope.Allows(child), false)
	_, indexed := l.Index.Lookup(child)
	assert.Equal(t, indexed, false)

	st.AddObject(&manifest.ConfigMap{Name: "settings", Namespace: "apps", Data: map[string]any{"CHILD": "rendered"}})
	progressed, err := session.Advance(t.Context(), &selection.AdmissionResult{})
	assert.NoError(t, err)
	assert.Equal(t, progressed, true)
	scope = l.RootEvidence(owner.Named(), nil).EntryScope
	assert.Equal(t, scope.Allows(child), true)
	assert.Equal(t, scope.Allows(testutil.KustomizationID("apps", "outside")), false)
	_, indexed = l.Index.Lookup(child)
	assert.Equal(t, indexed, true)
	assert.Equal(t, len(session.prepared[owner.Named()].children), 2)
}
