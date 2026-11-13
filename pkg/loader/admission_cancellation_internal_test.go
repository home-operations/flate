package loader

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmit_CanceledBoundaries(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"entry", "empty root", "cached completion"} {
		t.Run(boundary, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			l := New(store.New())
			defer l.Close()
			l.documents.compact = true
			if boundary == "empty root" {
				root = ""
			}
			if boundary == "cached completion" {
				assert.NoError(t, l.Admit(t.Context(), root))
			}
			before, scopes := l.construction, l.scopes
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			assert.Equal(t, l.Admit(ctx, root), ctx.Err())
			assert.Equal(t, l.construction, before)
			assert.Equal(t, l.scopes, scopes)
		})
	}
}

func TestAdmit_CancellationDuringRound(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := &contextualCancellationFS{FS: fstest.MapFS{
		"kustomization.yaml":      {Data: []byte("namespace: apps\nresources: [seed/child.yaml]\n")},
		"seed/child.yaml":         {Data: []byte(testutil.KustomizationYAML("child", "", "  path: ./next\n"))},
		"next/kustomization.yaml": {Data: []byte("resources: []\n")},
	}, target: "kustomization.yaml"}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
	l := New(st)
	defer l.Close()
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{})
	_, err := l.Load(t.Context(), filepath.Join(root, "seed"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.entryPath, l.entry = root, entryKustomize
	l.documents.compact = true
	before := l.construction
	input.cancel = cancel
	input.armed = func() bool { return l.construction > before }
	assert.Equal(t, errors.Is(l.Admit(ctx, root), context.Canceled), true)
	assert.Equal(t, input.cancel == nil, true)
	assert.Equal(t, l.scopes.complete, false)
	assert.FatalIf(t, len(l.scopes.inputs) != 1, "scope inputs = %d", len(l.scopes.inputs))
	assert.Equal(t, l.scopes.inputs[0].owner.Namespace, "")
	assert.NoError(t, l.Admit(t.Context(), root))
	assert.FatalIf(t, len(l.scopes.inputs) != 1, "scope inputs = %d", len(l.scopes.inputs))
	assert.Equal(t, l.scopes.inputs[0].owner.Namespace, "apps")
}

func TestAdmit_CancellationDuringProjection(t *testing.T) {
	t.Parallel()
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "changed"}[changed], func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			input := &contextualCancellationFS{FS: fstest.MapFS{
				"overlay/transformer/kustomization.yaml": {Data: []byte("resources: []\n")},
			}, target: "overlay/transformer/kustomization.yaml"}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
			l := New(st)
			defer l.Close()
			l.SourceRoot = root
			l.mountSources()
			l.ensureScopes()
			file := filepath.Join(root, "overlay", "child.yaml")
			child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "./next"}
			l.Index.Records[child.Named()] = manifest.DiscoveryRecord{
				Authored: child.Named(),
				Path:     file,
				File:     "overlay/child.yaml",
			}
			l.documents.cache[file] = document{objects: []parsedObject{{Obj: child}}}
			doc := l.documents.cache[filepath.Join(root, "overlay")]
			doc.body = &types.Kustomization{Transformers: []string{"transformer"}}
			l.documents.cache[filepath.Join(root, "overlay")] = doc
			assert.NoError(t, l.Admit(t.Context(), root))
			assert.Equal(t, l.projectAuthored(root, nil), false)
			if changed {
				record := l.Index.Records[child.Named()]
				record.Refs = append(record.Refs, manifest.NamedResource{Name: "stale"})
				l.Index.Records[child.Named()] = record
			}
			delete(l.documents.cache, filepath.Join(root, "overlay", "transformer"))
			l.scopes.complete = false
			input.cancel = cancel
			assert.Equal(t, l.Admit(ctx, root), ctx.Err())
			assert.Equal(t, input.cancel == nil, true)
			assert.Equal(t, errors.Is(ctx.Err(), context.Canceled), true)
			assert.Equal(t, l.scopes.complete, false)
		})
	}
}

func TestRegisterBuilt_CancellationDuringRound(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"entry", "before replacement", "after replacement"} {
		t.Run(boundary, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			input := &contextualCancellationFS{FS: fstest.MapFS{
				"kustomization.yaml":      {Data: []byte("resources: []\n")},
				"next/kustomization.yaml": {Data: []byte("resources: []\n")},
			}}
			st := store.New()
			source := &store.SourceArtifact{LocalPath: root, FS: input}
			st.SetArtifact(manifest.BootstrapSourceID, source)
			other := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "other",
			}
			st.SetArtifact(other, source)
			l := New(st)
			defer l.Close()
			l.SourceRoot = root
			support := func(name string) *store.ReadSet {
				r := new(store.ReadSet)
				r.Init(st, testutil.KustomizationID("apps", name), nil)
				r.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
				r.GetArtifact(other, store.ArtifactContent)
				return r
			}
			old, incoming := support("old"), support("incoming")
			assert.NoError(
				t,
				l.RegisterBuilt(
					t.Context(),
					BuiltInput{Root: root, Source: manifest.BootstrapSourceID, Support: old},
					nil,
				),
			)
			child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "./next"}
			if boundary == "entry" {
				cancel()
			} else {
				input.target, input.cancel = "next/kustomization.yaml", cancel
				if boundary == "before replacement" {
					st.AddObject(child)
				}
			}
			err := l.RegisterBuilt(
				ctx,
				BuiltInput{Root: root, Source: manifest.BootstrapSourceID, Support: incoming},
				[]store.Child{{Obj: child}},
			)
			assert.Equal(t, errors.Is(err, context.Canceled), true)
			assert.Equal(t, ctx.Err(), context.Canceled)
			retained := old
			want := testutil.KustomizationID("apps", "old")
			if boundary == "after replacement" {
				retained, want = incoming, testutil.KustomizationID("apps", "incoming")
				assert.Equal(t, l.scopes.complete, false)
			}
			assert.Equal(t, l.documents.cache[root].builtInput.Support, retained)
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: root, FS: input, Content: "changed"},
			)
			assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{want})
			l.Close()
			st.SetArtifact(other, &store.SourceArtifact{LocalPath: root, FS: input, Content: "changed"})
			assert.Equal(t, len(slices.Collect(st.Sweep())), 0)
		})
	}
}

func TestFinalizeIndex_AdmissionCancellation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := &contextualCancellationFS{FS: fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: []\n")},
	}, target: "kustomization.yaml", cancel: cancel}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
	l := New(st)
	defer l.Close()
	configureSelection(t, l, selection.Request{})
	l.SourceRoot, l.entryPath, l.entry = root, root, entryKustomize
	input.armed = func() bool { return l.construction > 0 }
	assert.Equal(t, errors.Is(l.FinalizeIndex(ctx, root, true), context.Canceled), true)
	assert.Equal(t, input.cancel == nil, true)
	assert.Equal(t, l.scopes.complete, false)
	assert.Equal(t, l.frozen, false)
	assert.Equal(t, l.indexDerived, false)
}
