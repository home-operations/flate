package discovery_test

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSession_OnlyRequiredStandaloneConstruction(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := fstest.MapFS{
		"one/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"two/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"one/cm.yaml": {
			Data: []byte(
				"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: one, namespace: apps}\nspec: {resources: []}\n",
			),
		},
		"two/cm.yaml": {
			Data: []byte(
				"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: two, namespace: apps}\nspec: {resources: []}\n",
			),
		},
	}
	st := store.New()
	l := loader.New(st)
	session, err := discovery.Begin(
		t.Context(),
		discovery.Config{
			Input:     &store.SourceArtifact{LocalPath: root, FS: input},
			Path:      root,
			Store:     st,
			Loader:    l,
			Trees:     testTrees(t),
			Selection: manifest.Selection{Changed: true},
		},
	)
	assert.NoError(t, err)
	for _, object := range st.Objects() {
		if !manifest.KindOf(object).SourceArtifact {
			t.Fatalf("unexpected construction payload: %s", object.Named())
		}
	}
	advanced, err := session.Advance(t.Context(), &loader.Admission{Blocked: []string{"one"}})
	assert.NoError(t, err)
	assert.Equal(t, advanced, true)
	assert.FatalIf(t, st.GetObject(
		manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      manifest.KindResourceSet,
			Namespace: "apps",
			Name:      "two",
		},
	) != nil, "%v", "unrelated package was hydrated")
	one := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "one",
	}
	built := l.PromotionDocuments(one)
	assert.Equal(t, len(built), 1)
	advanced, err = session.Advance(t.Context(), &loader.Admission{Objects: built})
	assert.NoError(t, err)
	assert.Equal(t, advanced, false)
	result, err := session.Freeze(t.Context())
	assert.NoError(t, err)
	again, err := session.Freeze(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, result, again)
	assert.Equal(t, result.Index, l.Index)
	assert.Equal(t, st.GetObject(one) != nil, true)
	assert.FatalIf(t, st.GetObject(
		manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      manifest.KindResourceSet,
			Namespace: "apps",
			Name:      "two",
		},
	) != nil, "%v", "freeze hydrated an unrelated package")
}

func TestRun_RestrictedUsesSession(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := fstest.MapFS{
		"one/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"two/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"one/cm.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: one, namespace: apps}\ndata: {value: selected}\n",
			),
		},
		"two/cm.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: two, namespace: apps}\ndata: {value: unrelated}\n",
			),
		},
	}
	st := store.New()
	l := loader.New(st)
	one := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "one"}
	two := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "two"}
	result, err := discovery.Run(t.Context(), discovery.Config{
		Input: &store.SourceArtifact{LocalPath: root, FS: input},
		Path:  root, Store: st, Loader: l, Trees: testTrees(t),
		Selection: manifest.Selection{Roots: []manifest.NamedResource{one}},
	})
	assert.NoError(t, err)
	assert.Equal(t, result.Index, l.Index)
	assert.Equal(t, len(l.PromotionDocuments(one)), 0)
	assert.Equal(t, len(l.PromotionDocuments(two)), 0)
	assert.Equal(t, st.GetObject(one) != nil, true)
	assert.Equal(t, st.GetObject(two), nil)
	assert.Equal(t, l.Options.Filter.ShouldReconcile(one), true)
	assert.Equal(t, l.Options.Filter.ShouldReconcile(two), false)
}

func testSessionUnchangedEntryConstruction(t *testing.T) {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	for side, name := range []string{"base", "head"} {
		t.Run(name, func(t *testing.T) {
			logs.Reset()
			root := t.TempDir()
			input := fstest.MapFS{
				"entry/kustomization.yaml": {Data: []byte("resources: [child.yaml]\n")},
				"entry/child.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: apps}
spec: {path: ./workload}
`)},
				"workload/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
				"workload/cm.yaml": {
					Data: []byte(
						"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\ndata: {value: changed}\n",
					),
				},
			}
			st := store.New()
			l := loader.New(st)
			l.Options.Filter = change.NewFilter()
			source := &store.SourceArtifact{LocalPath: root, FS: input}
			st.SetArtifact(manifest.BootstrapSourceID, source)
			peer := store.New()
			peer.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
			stores := [2]*store.Store{st, peer}
			if side == 1 {
				stores[0], stores[1] = stores[1], stores[0]
			}
			var err error
			l.Options.Changes, err = change.Qualify(
				t.Context(),
				change.NewSet([]string{"workload/cm.yaml"}),
				stores[0],
				stores[1],
			)
			assert.NoError(t, err)
			session, err := discovery.Begin(
				t.Context(),
				discovery.Config{
					Input:     source,
					Path:      filepath.Join(root, "entry"),
					Store:     st,
					Loader:    l,
					Trees:     testTrees(t),
					Selection: manifest.Selection{Changed: true},
				},
			)
			assert.NoError(t, err)
			requests := 0
			for {
				proposal, prepareErr := l.Prepare(t.Context(), store.Child{}, nil, nil)
				assert.NoError(t, prepareErr)
				for _, path := range proposal.Blocked {
					if path == "entry" {
						requests++
					}
				}
				advanced, advanceErr := session.Advance(t.Context(), proposal)
				assert.NoError(t, advanceErr)
				if !advanced {
					break
				}
			}
			_, err = session.Freeze(t.Context())
			assert.NoError(t, err)
			builds := strings.Count(logs.String(), `"msg":"kustomize: build"`)
			t.Logf("root construction requests=%d actual builds=%d", requests, builds)
			assert.Equal(t, requests, 0)
			assert.Equal(t, builds, 0)
		})
	}
}
