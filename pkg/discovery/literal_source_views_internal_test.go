package discovery

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestBindEntry_LiteralSourceViews(t *testing.T) {
	t.Parallel()
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed=%t", changed), func(t *testing.T) {
			t.Parallel()
			var previous []manifest.Warning
			for _, order := range [][2]int{{0, 1}, {1, 0}} {
				warnings := literalSourceViewsOrder(t, changed, order)
				if !changed && previous != nil {
					assert.Diff(t, warnings, previous)
				}
				previous = warnings
			}
		})
	}
	t.Run("source replaced before commit", func(t *testing.T) {
		t.Parallel()
		f := newLiteralSourceViewsFixture(t, true)
		f.literal(t, 0)
		f.literal(t, 1)
		f.publish(t, 0)
		root, binding, err := f.admission.BindEntry(t.Context(), f.store)
		assert.NoError(t, err)
		assert.FatalIf(t, root == nil, "source replacement control did not propose an entry")
		application, ok := manifest.ApplicationFor(root)
		assert.Equal(t, ok, true)
		proposal, err := f.loader.Prepare(
			t.Context(),
			loader.PrepareRequest{Generation: []store.Child{{Obj: root, Entry: &application}}, Reads: binding.Reads},
		)
		assert.NoError(t, err)
		replacement := *f.views[0]
		replacement.Content = manifest.Fingerprint("replacement")
		f.views[0] = &replacement
		f.store.SetArtifact(f.sources[0], &replacement)
		assert.Equal(t, binding.Reads.Valid(), false)
		committed, err := f.loader.Commit(t.Context(), proposal, loader.Execution)
		assert.NoError(t, err)
		assert.Equal(t, committed, false)
		assert.Equal(t, acceptEntryForTest(t, f.admission, &binding), Binding{})
		assert.Equal(t, f.admission.entryBound.Load(), false)
		assert.Equal(t, len(f.store.Warnings()), 0)
		fresh, freshBinding, err := f.admission.BindEntry(t.Context(), f.store)
		assert.NoError(t, err)
		assert.Equal(t, fresh, (*manifest.Kustomization)(nil))
		assert.Equal(t, freshBinding, Binding{})
		f.publish(t, 0)
		fresh, freshBinding, err = f.admission.BindEntry(t.Context(), f.store)
		assert.NoError(t, err)
		assert.FatalIf(t, fresh == nil, "fresh source output did not restore qualification")
		assert.Equal(t, fresh.Named(), testutil.KustomizationID("apps", "z"))
		assert.Equal(t, acceptEntryForTest(t, f.admission, &freshBinding).Producer, fresh.Named())
		f.assertRawAuthority(t)
	})
}

type literalSourceViewsFixture struct {
	store     *store.Store
	loader    *loader.Loader
	admission *Admission
	views     [2]*store.SourceArtifact
	sources   [2]manifest.NamedResource
	raw       [2]manifest.BaseManifest
	literals  [2]*manifest.Kustomization
	trees     *kustomize.TreeCache
	fetches   atomic.Int32
}

func newLiteralSourceViewsFixture(t *testing.T, changed bool) *literalSourceViewsFixture {
	t.Helper()
	root := t.TempDir()
	st := store.New()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	f := &literalSourceViewsFixture{store: st, loader: loader.New(st), trees: testTrees(t)}
	f.loader.SourceRoot = root
	f.loader.Options.Selection = manifest.Selection{Changed: changed}
	t.Cleanup(f.loader.Retire)
	f.admission = &Admission{
		store:       st,
		input:       input,
		path:        filepath.Join(root, "entry"),
		loader:      f.loader,
		trees:       f.trees,
		entryDemand: true,
	}
	for i, name := range []string{"first", "later"} {
		f.sources[i] = manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindGitRepository,
			Namespace: "apps",
			Name:      name,
		}
		source, _ := f.sources[i].BindLocal(filepath.Join(root, name))
		st.AddObject(source)
		applier := []string{"z", "a"}[i]
		f.views[i] = &store.SourceArtifact{
			LocalPath:  filepath.Join(root, name),
			LocalRoot:  root,
			InputAlias: true,
			FS: fstest.MapFS{
				"outer/kustomization.yaml": {Data: []byte("resources: [applier.yaml]\n")},
				"outer/applier.yaml": {
					Data: []byte(
						testutil.KustomizationYAML(
							applier,
							"apps",
							"  path: ./unpatched\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
						),
					),
				},
			},
		}
		f.views[i].FS.(fstest.MapFS)["outer/kustomization.yaml"].Data = []byte(
			"resources: [applier.yaml]\npatches:\n  - target: {kind: Kustomization, name: " + applier + "}\n    patch: |-\n      - op: replace\n        path: /spec/path\n        value: ./entry\n",
		)
		st.SetArtifact(f.sources[i], f.views[i])
		document := map[string]any{
			"apiVersion": manifest.FluxOperatorDomain + "/v1",
			"kind":       "FluxInstance",
			"metadata":   map[string]any{"name": name, "namespace": "apps"},
			"spec": map[string]any{
				"sync": map[string]any{
					"kind": manifest.KindGitRepository,
					"name": name,
					"url":  "https://fixture.invalid/" + name,
					"ref":  "main",
					"path": "./outer",
				},
			},
		}
		parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
		assert.NoError(t, err)
		f.raw[i] = parsed.Object
		st.AddObject(parsed.Object, parsed.Document)
	}
	f.trees.SetGitBaseFetcher(
		func(_ context.Context, _ kustomize.GitBaseRequest, _ cache.RetryConfig) (*store.SourceArtifact, error) {
			f.fetches.Add(1)
			return nil, manifest.NewError(manifest.ErrInput, nil, "unexpected source-view acquisition")
		},
	)
	return f
}

func (f *literalSourceViewsFixture) literal(t *testing.T, index int) *manifest.Kustomization {
	t.Helper()
	if f.literals[index] != nil {
		return f.literals[index]
	}
	application, ok := manifest.ApplicationFor(f.raw[index])
	assert.Equal(t, ok, true)
	literal := manifest.NewEntryKustomization(application.Source, application.Path, f.raw[index].Named())
	var reads store.ReadSet
	reads.Init(f.store, literal.Named(), nil)
	reads.GetDeclaration(f.raw[index].Named())
	reads.GetArtifact(application.Source, store.ArtifactContent)
	proposal, err := f.loader.Prepare(
		t.Context(),
		loader.PrepareRequest{Generation: []store.Child{{Obj: literal, Entry: &application}}, Reads: &reads},
	)
	assert.NoError(t, err)
	committed, err := f.loader.Commit(t.Context(), proposal, loader.Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	reads.Retire()
	f.literals[index] = literal
	return literal
}

func (f *literalSourceViewsFixture) publish(t *testing.T, index int) {
	t.Helper()
	literal := f.literal(t, index)
	spec, err := kustomize.Prepare(literal, nil)
	assert.NoError(t, err)
	docs, receipt, err := kustomize.Render(
		t.Context(),
		f.trees,
		kustomize.RenderRequest{Source: f.views[index], SubPath: spec.Path, Spec: spec},
		cache.RetryConfig{},
	)
	assert.NoError(t, err)
	assert.NoError(t, kustomize.Shape(t.Context(), spec, docs, kustomize.ShapeOptions{}))
	parsed := loader.ParseChildren(spec, docs, manifest.ParseDocOptions{}, nil)
	assert.NoError(t, manifest.DocumentFailure(parsed.Failures))
	var reads store.ReadSet
	reads.Init(f.store, literal.Named(), nil)
	reads.GetDeclaration(f.raw[index].Named())
	reads.GetDeclaration(literal.Named())
	assert.Equal(t, reads.GetArtifact(f.sources[index], store.ArtifactContent), store.Artifact(f.views[index]))
	output := &store.Emission{Children: parsed.Children, Complete: true, Render: receipt}
	reads.Bind(output)
	f.store.RecordEmitted(literal.Named(), output, nil)
	reads.Accept(store.AssessmentReconciled)
	reads.Retire()
	accepted, ok := f.store.GetArtifact(literal.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(
		t,
		!ok || !accepted.Complete || !accepted.OutputReads.Valid(),
		"literal source output was not accepted",
	)
	assert.Equal(t, f.store.EmissionOwner(parsed.Children[0].Obj.Named()), literal.Named())
}

func (f *literalSourceViewsFixture) assertRawAuthority(t *testing.T) {
	t.Helper()
	for i := range f.raw {
		assert.Equal(t, f.store.GetArtifact(f.raw[i].Named(), store.ArtifactContent), store.Artifact(nil))
		_, scheduled := f.store.GetStatus(f.raw[i].Named())
		assert.Equal(t, scheduled, false)
		assert.Equal(t, slices.Contains(f.admission.EntryProducers(), f.raw[i].Named()), false)
		assert.Equal(t, f.store.EmissionOwner(f.sources[i]), manifest.NamedResource{})
	}
	assert.Equal(t, f.fetches.Load(), int32(0))
}

func literalSourceViewsOrder(t *testing.T, changed bool, order [2]int) []manifest.Warning {
	t.Helper()
	f := newLiteralSourceViewsFixture(t, changed)
	root, binding, err := f.admission.BindEntry(t.Context(), f.store)
	assert.NoError(t, err)
	assert.Equal(t, root, (*manifest.Kustomization)(nil))
	assert.Equal(t, binding, Binding{})
	first := f.literal(t, 0)
	f.literal(t, 1)
	assert.ErrorIf(
		t,
		first.Named() == f.literal(t, 1).Named(),
		"source-qualified literals share an identity",
	)
	for _, index := range order {
		f.publish(t, index)
	}
	root, binding, err = f.admission.BindEntry(t.Context(), f.store)
	assert.NoError(t, err)
	assert.FatalIf(t, root == nil, "independently supported source views did not bind")
	want := "a"
	if changed {
		want = "z"
	}
	assert.Equal(t, root.Named(), testutil.KustomizationID("apps", want))
	assert.Equal(t, binding.Source, manifest.BootstrapSourceID)
	assert.Equal(t, len(f.store.Warnings()), 0)
	binding = acceptEntryForTest(t, f.admission, &binding)
	assert.Equal(t, binding.Producer, root.Named())
	assert.Equal(t, len(f.admission.EntryProducers()), 0)
	warnings := f.store.Warnings()
	wantWarnings := 1
	if changed {
		wantWarnings = 0
	}
	assert.Equal(t, len(warnings), wantWarnings)
	if !changed {
		assert.Equal(t, warnings[0].Category, manifest.WarnConflictingProducers)
		assert.Equal(t, warnings[0].Count, 1)
	}
	f.assertRawAuthority(t)
	return warnings
}
