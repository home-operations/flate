package loader

import (
	"errors"
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestInputCache_CurrentConstructionMatchesCold(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"unchanged", "builder", "precedence", "removed", "added", "list", "declaration forward", "declaration reverse", "discovery only"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := fstest.MapFS{
				"kustomization.yml": {Data: []byte("namespace: old\nresources: [object.yaml]\n")},
				"object.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: object}\ndata: {value: before}\n"),
				},
			}
			declaration := func(path string) []byte {
				return []byte(
					"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: child, namespace: flux-system}\nspec:\n  path: ./" + path + "\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				)
			}
			switch name {
			case "declaration forward":
				files["object.yaml"].Data = declaration("before")
			case "declaration reverse":
				files["object.yaml"].Data = declaration("after")
			case "discovery only":
				delete(files, "kustomization.yml")
				files["unclaimed.yaml"] = &fstest.MapFile{Data: declaration("unclaimed")}
			}
			owner, err := cache.New(t.Context(), t.TempDir())
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			persistent := cache.RenderPersistenceDisabledReason() == ""
			assert.Equal(t, owner.CanPersistRenders(), persistent)
			first := cachedFixtureLoader(t, root, files, owner)
			assert.NoError(t, first.PersistInputs(t.Context()))
			first.Close()
			switch name {
			case "declaration forward":
				files["object.yaml"].Data = declaration("after")
			case "declaration reverse":
				files["object.yaml"].Data = declaration("before")
			case "builder":
				files["kustomization.yml"] = &fstest.MapFile{Data: []byte("namespace: new\nresources: [object.yaml]\n")}
			case "precedence":
				files["kustomization.yaml"] = &fstest.MapFile{
					Data: []byte("namespace: first\nresources: [object.yaml]\n"),
				}
			case "removed":
				delete(files, "object.yaml")
				files["kustomization.yml"] = &fstest.MapFile{Data: []byte("resources: []\n")}
			case "added":
				files["added.yaml"] = &fstest.MapFile{
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: added}\n"),
				}
			case "list":
				files["object.yaml"] = &fstest.MapFile{
					Data: []byte(
						"apiVersion: v1\nkind: List\nitems:\n- apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: first}\n- apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: second}\n",
					),
				}
			}
			repaired := cachedFixtureLoader(t, root, files, owner)
			defer repaired.Close()
			cold := cachedFixtureLoader(t, root, files, nil)
			defer cold.Close()
			assert.Diff(t, repaired.Index, cold.Index)
			if name == "discovery only" {
				id := manifest.NamedResource{
					Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization,
					Namespace: "flux-system", Name: "child",
				}
				_, indexed := cold.Index.Records[id]
				assert.Equal(t, indexed, true)
				assert.Equal(t, cold.Store.GetObject(id), nil)
				assert.Equal(t, repaired.Store.GetObject(id), nil)
			}
			assert.NoError(t, repaired.PersistInputs(t.Context()))
			restored := cachedFixtureLoader(t, root, files, owner)
			defer restored.Close()
			assert.Diff(t, restored.Index, cold.Index)
			for id := range cold.Index.Records {
				want, ok := cold.Occurrence(id)
				if !ok {
					continue
				}
				got, ok := restored.Occurrence(id)
				assert.Equal(t, ok, true)
				assert.Equal(t, got.RequiresBuild, want.RequiresBuild)
				wantOccurrence, wantKnown := cold.catalogOccurrence(id)
				gotOccurrence, gotKnown := restored.catalogOccurrence(id)
				assert.Equal(t, wantKnown, true)
				assert.Equal(t, gotKnown, true)
				assert.Equal(t, gotOccurrence.facts.collection, wantOccurrence.facts.collection)
				wantChild, wantErr := cold.OccurrenceObject(t.Context(), &want, nil)
				gotChild, gotErr := restored.OccurrenceObject(t.Context(), &got, nil)
				assert.NoError(t, wantErr)
				assert.NoError(t, gotErr)
				assert.Diff(t, gotChild.Doc, wantChild.Doc)
			}
		})
	}
}

func TestInputCache_IncompleteAcceptanceMatchesCold(t *testing.T) {
	t.Parallel()
	persistent := cache.RenderPersistenceDisabledReason() == ""
	root := t.TempDir()
	files := fstest.MapFS{
		"object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: object}\n")},
		"owner.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner, namespace: apps}\nspec: {path: ./children, postBuild: {substitute: {NAME: resolved, NS: work}}}\n",
			),
		},
		"children/child.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: '${NAME}', namespace: '${NS}'}\nspec: {path: ./payload}\n",
			),
		},
		"payload/kustomization.yaml": {Data: []byte("resources: []\n")},
	}
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	first := cachedFixtureLoader(t, root, files, owner)
	assert.NoError(t, first.PersistInputs(t.Context()))
	input := first.documents.artifacts[root]
	lookup, _ := cache.InputFrameIdentity(input.OriginPath(root), input.URL, nil)
	generation, _, _, readErr := owner.Blob(t.Context(), lookup, &cache.Policy{MaxAge: cache.Forever}, nil)
	if persistent {
		assert.NoError(t, readErr)
		frame, frameErr := readInputFrame(t.Context(), generation)
		assert.NoError(t, frameErr)
		content, prepareErr := frame.prepare(t.Context())
		assert.NoError(t, prepareErr)
		publication, prepareErr := content.PreparePublication(t.Context(), cache.FramePublicationInput{})
		assert.NoError(t, prepareErr)
		assert.NoError(t, cache.RecordInventory(t.Context(), owner, cache.AcceptedInventory[*cache.FramePublication]{
			Key: lookup.Content.(string), Acceptance: publication.Identity(), Content: publication,
		}))
	} else {
		assert.Equal(t, errors.Is(readErr, errdef.ErrNotFound), true)
		assert.Equal(t, generation == nil, true)
	}
	first.Close()
	restored := cachedFixtureLoader(t, root, files, owner)
	defer restored.Close()
	cold := cachedFixtureLoader(t, root, files, nil)
	defer cold.Close()
	assert.Diff(t, restored.Index, cold.Index)
	assert.NoError(t, restored.FinalizeIndex(t.Context(), root, true))
	assert.NoError(t, cold.FinalizeIndex(t.Context(), root, true))
	assert.Diff(t, restored.Index, cold.Index)
	id := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "work",
		Name:      "resolved",
	}
	got, found := restored.Occurrence(id)
	assert.Equal(t, found, true)
	want, found := cold.Occurrence(id)
	assert.Equal(t, found, true)
	if diff := cmp.Diff(want, got, cmp.Exporter(func(reflect.Type) bool { return true })); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
	assert.Equal(t, got.Authored.Name, "${NAME}")
	assert.Equal(t, got.Authored.Namespace, "${NS}")
	assert.Equal(t, got.Declaration.File, "children/child.yaml")
	assert.NoError(t, restored.PersistInputs(t.Context()))
}
