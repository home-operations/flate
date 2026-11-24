package loader

import (
	"errors"
	"io/fs"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestInputCache_CurrentConstructionMatchesCold(t *testing.T) {
	t.Parallel()
	const discoveryFile = "unclaimed.yaml"
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
				files[discoveryFile] = &fstest.MapFile{Data: declaration("unclaimed")}
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
			if name != "discovery only" {
				return
			}
			id := manifest.NamedResource{
				Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization,
				Namespace: "flux-system", Name: "child",
			}
			_, indexed := cold.Index.Records[id]
			assert.Equal(t, indexed, true)
			assert.Equal(t, cold.Store.GetObject(id), nil)
			assert.Equal(t, repaired.Store.GetObject(id), nil)
			assert.Equal(t, restored.Store.GetObject(id), nil)
			if persistent {
				_, fromFrame := restored.inputCaches[root].files[inputCoordinate{name: discoveryFile}]
				assert.Equal(t, fromFrame, true)
			}
			want, found := cold.catalogOccurrence(id)
			assert.FatalIf(t, !found, "cold discovery omitted %v", id)
			got, found := restored.catalogOccurrence(id)
			assert.FatalIf(t, !found, "restored discovery omitted %v", id)
			wantRefs, gotRefs := want.facts.relations(), got.facts.relations()
			assert.FatalIf(t, len(wantRefs) != 1, "cold discovery has %d references, want 1", len(wantRefs))
			assert.FatalIf(t, len(gotRefs) != 1, "restored discovery has %d references, want 1", len(gotRefs))
			wantRef, gotRef := wantRefs[0], gotRefs[0]
			assert.Equal(t, gotRef.NamedResource, wantRef.NamedResource)
			assert.Equal(t, wantRef.BindsSource(), true)
			assert.Equal(t, wantRef.ReadsSourceTree(), true)
			assert.Equal(t, gotRef.BindsSource(), wantRef.BindsSource())
			assert.Equal(t, gotRef.ReadsSourceTree(), wantRef.ReadsSourceTree())
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
	lookup := cache.InputFrameLookup(input.OriginPath(root), input.URL)
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

func TestInputCache_ArtifactGeneratorProduction(t *testing.T) {
	t.Parallel()
	const generatorFile = "shared/generator.yaml"
	root := t.TempDir()
	files := fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [entry.yaml]\n")},
		"entry.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: entry, namespace: apps}
spec: {path: ./apps}
`)},
		"apps/kustomization.yaml": {
			Data: []byte("namespace: apps\nresources: [consumer.yaml]\ncomponents: [../shared]\n"),
		},
		"apps/consumer.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: apps}
spec:
  path: ./leaf
  targetNamespace: delivery-api
  sourceRef: {kind: ExternalArtifact, namespace: delivery, name: bundle-api}
`)},
		"shared/kustomization.yaml": {Data: []byte(`apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
resources: [generator.yaml]
`)},
		generatorFile: {Data: []byte(`apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: generator, namespace: authored}
spec:
  sources: [{alias: repo, kind: GitRepository, namespace: flux-system, name: flux-system}]
  pathPattern: '@repo/apps/{app}'
  artifacts:
  - {name: local, copy: [{from: '@repo/copied/', to: '@artifact/'}]}
  - {name: 'bundle-{app}', namespace: delivery, copy: [{from: '@repo/copied/', to: '@artifact/'}]}
  - {name: 'branch-{app}', namespace: 'delivery-{app}', copy: [{from: '@repo/copied/', to: '@artifact/'}]}
  managedNamespaces:
    allowedNames: ['true']
    metadata: {fromSource: {path: '@repo/metadata/{app}.yaml'}}
`)},
	}
	const metadataFile = "metadata/api.yaml"
	withMetadata := maps.Clone(files)
	withMetadata[metadataFile] = &fstest.MapFile{Data: []byte("labels: {team: api}\n")}
	const unrelatedFile = "unrelated/file.yaml"
	withUnrelated := maps.Clone(files)
	withUnrelated[unrelatedFile] = &fstest.MapFile{Data: []byte("labels: {team: other}\n")}
	c, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, c.Close()) })
	generator := manifest.NamedResource{
		Group:     manifest.SourceExtensionsDomain,
		Kind:      manifest.KindArtifactGenerator,
		Namespace: "apps",
		Name:      "generator",
	}
	cold := cachedFixtureLoader(t, root, files, c)
	defer cold.Close()
	_, err = cold.Load(t.Context(), filepath.Join(root, "apps"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, cold.FinalizeIndex(t.Context(), root, false))
	assert.NoError(t, cold.PersistInputs(t.Context()))
	warm := cachedFixtureLoader(t, root, files, c)
	defer warm.Close()
	if c.CanPersistRenders() {
		_, restored := warm.inputCaches[root].files[inputCoordinate{name: generatorFile}]
		assert.Equal(t, restored, true)
		t.Log("generator restored from the persistent input frame")
	} else {
		restoreGeneratorInputFrame(t, cold, warm, generatorFile)
		t.Log("generator restored from an in-process input frame")
	}
	_, err = warm.Load(t.Context(), filepath.Join(root, "apps"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, warm.FinalizeIndex(t.Context(), root, false))

	authored := generator
	authored.Namespace = "authored"
	want := []manifest.Production{
		{
			Owner: manifest.NamedResource{
				Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization,
				Namespace: "apps", Name: "entry",
			},
			Declaration: generator, Authored: authored, Source: manifest.BootstrapSourceID, File: generatorFile,
		},
	}
	for _, l := range []*Loader{cold, warm} {
		assert.Equal(t, l.Store.GetDeclaration(generator), nil)
		refs := slices.Collect(l.References(generator))
		assert.FatalIf(t, len(refs) != 1, "generator references = %v", refs)
		assert.Diff(t, refs[0].Files, []string{"apps", "copied", "metadata"})
		for _, id := range []manifest.NamedResource{
			{Kind: "Namespace", Name: "delivery"},
			{Kind: "Namespace", Name: "delivery-api"},
			{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "delivery", Name: "bundle-api"},
			{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "local"},
		} {
			assert.Equal(t, l.Store.GetObject(id), nil)
			assert.Equal(t, l.Store.GetArtifact(id, store.ArtifactContent), nil)
			if diff := cmp.Diff(want, l.Store.Productions(id, l.Index)); diff != "" {
				t.Errorf("%s productions (-want +got):\n%s", id, diff)
			}
		}
		for _, tc := range []struct {
			trees [2]fstest.MapFS
			path  string
			reach bool
		}{
			{[2]fstest.MapFS{files, withMetadata}, metadataFile, true},
			{[2]fstest.MapFS{withMetadata, files}, metadataFile, true},
			{[2]fstest.MapFS{files, withUnrelated}, unrelatedFile, false},
		} {
			delta, err := change.Detect(
				t.Context(),
				&store.SourceArtifact{FS: tc.trees[0]},
				&store.SourceArtifact{FS: tc.trees[1]},
				change.DetectConfig{},
			)
			assert.NoError(t, err)
			assert.Diff(t, delta.Paths(), []string{tc.path})
			l.Options.Changes = delta
			proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
			assert.NoError(t, err)
			reached := slices.Contains(proposal.Needed, generator)
			assert.ErrorIf(t, reached != tc.reach,
				"%s reach = %v, wanted %v; needed = %v", tc.path, reached, tc.reach, proposal.Needed)
			proposal.Retire()
		}
	}
}

func restoreGeneratorInputFrame(t *testing.T, cold, warm *Loader, file string) {
	t.Helper()
	input := warm.documents.artifacts[warm.SourceRoot]
	doc := cold.documents.cache[filepath.Join(cold.SourceRoot, file)]
	saved, known, err := cacheInput(file, "generator-digest", &doc)
	assert.NoError(t, err)
	assert.FatalIf(t, !known || len(saved.Objects) != 1, "missing generator occurrence in input frame")
	encoded := encodeInputFrame(t, &inputFrame{Files: []cachedInput{saved}})
	frame, err := readInputFrame(t.Context(), fstest.MapFS{"content": {Data: encoded}})
	assert.NoError(t, err)
	state := &inputCache{
		input:   input,
		digests: map[string]string{file: saved.Digest},
		types:   map[string]fs.FileMode{file: 0o644},
		files:   map[inputCoordinate]inputEvidence{},
	}
	assert.NoError(t, warm.restoreFrameFiles(state, frame))
	_, restored := state.files[inputCoordinate{name: file}]
	assert.Equal(t, restored, true)
}
