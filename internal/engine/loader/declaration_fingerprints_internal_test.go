package loader

import (
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestParsedInputs_DeclarationFingerprints(t *testing.T) {
	t.Parallel()
	for _, conflict := range []bool{false, true} {
		t.Run(strconv.FormatBool(conflict), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			body := "apiVersion: v1\n" +
				"kind: ConfigMap\n" +
				"metadata: {name: data, namespace: apps, labels: {kustomize.toolkit.fluxcd.io/name: owner}}\n" +
				"data: {KEY: 'ENC[AES256_GCM,data:original,iv:iv,tag:tag,type:str]'}\n"
			documents, err := manifest.DecodeDocs(strings.NewReader(body), manifest.IncidentalDocuments)
			assert.NoError(t, err)
			raw := manifest.Fingerprint(manifest.WithoutOwnershipLabels(documents[0].Document))
			want := raw
			if conflict {
				body += "---\n" + strings.ReplaceAll(body, "data:original", "data:different") + "---\n" + body
			}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID,
				&store.SourceArtifact{
					Provider:  store.ProviderInput,
					LocalPath: root,
					FS:        fstest.MapFS{"objects.yaml": {Data: []byte(body)}},
				})
			l := New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: true})
			_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "data", Namespace: "apps"}
			_, found := l.Index.Lookup(id)
			assert.Equal(t, found, true)
			if conflict {
				want = ""
			}
			assert.Equal(t, l.Index.Declarations[manifest.BootstrapSourceID]["objects.yaml"].Objects[id], want)
			position := 0
			observation, err := (&documentReader{compact: true}).prepareStream(t.Context(), strings.NewReader(body),
				manifest.ParseDocOptions{},
				func(object parsedObject) {
					assert.NoError(t, object.outcome.Err)
					assert.Equal(
						t,
						l.DeclarationContents()(object.Doc).Exact,
						manifest.Fingerprint(manifest.WithoutOwnershipLabels(object.Doc)),
					)
					assert.Equal(t, l.DeclarationContents()(object.Doc).Namespace, "apps")
					expected := "ENC[AES256_GCM,data:original,iv:iv,tag:tag,type:str]"
					if conflict && position == 1 {
						expected = "ENC[AES256_GCM,data:different,iv:iv,tag:tag,type:str]"
					}
					assert.Equal(t, object.Doc["data"].(map[string]any)["KEY"], any(expected))
					position++
					assert.FatalIf(
						t,
						l.DeclarationContents()(object.Doc).Exact == "",
						"%s",
						"preparation lost the original declaration fingerprint",
					)
				})
			assert.NoError(t, err)
			assert.Equal(t, observation.Objects[id], want)
			assert.Equal(t, observation.Complete, true)
		})
	}
}

func TestParsedInputs_DeclarationKindsAndRoles(t *testing.T) {
	t.Parallel()
	resource := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: work}\nspec: {replicas: 2}\n"
	custom := "apiVersion: example.io/v1\nkind: Widget\nmetadata: {name: custom}\nspec: {value: authored}\n"
	for _, tc := range []struct {
		name, body string
		complete   bool
		objects    int
	}{
		{name: "work.YAML", body: resource, complete: true, objects: 1},
		{name: "custom.YmL", body: custom, complete: true, objects: 1},
		{name: "list.JSON", body: `{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"work"}},{"apiVersion":"example.io/v1","kind":"Widget","metadata":{"name":"custom"}}]}`, complete: true, objects: 2},
		{name: "empty.yaml", complete: true},
		{name: "null.json", body: "null\n", complete: true},
		{name: "broken.yaml", body: resource + "---\nbroken: [", objects: 1},
		{name: "unresolved.yaml", body: strings.ReplaceAll(resource, "name: work", "name: '${NAME}'")},
		{name: "partial.yaml", body: "apiVersion: v1\nkind: ConfigMap\nmetadata: {}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &declarationReadFS{FS: fstest.MapFS{tc.name: {Data: []byte(tc.body)}}}
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: input},
			)
			l := New(st)
			t.Cleanup(l.Close)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: true})
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			observation, known := l.Index.Declarations[manifest.BootstrapSourceID][tc.name]
			assert.Equal(t, known, true)
			assert.Equal(t, observation.Present, true)
			assert.Equal(t, observation.Complete, tc.complete)
			assert.Equal(t, len(observation.Objects), tc.objects)
			for id := range observation.Objects {
				assert.Equal(t, l.Index.IsFileIndexed(id), false)
				_, published := l.Index.Lookup(id)
				assert.Equal(t, published, false)
			}
			before := input.opens.Load()
			_, err = l.documents.censusFile(t.Context(), filepath.Join(root, tc.name), manifest.ParseDocOptions{})
			assert.NoError(t, err)
			assert.Equal(t, input.opens.Load(), before)
		})
	}
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
			"kustomization.yaml": {Data: []byte("resources: [resource]\n")},
			"resource":           {Data: []byte(custom)},
			"script.sh":          {Data: []byte(resource)},
		}},
	)
	l := New(st)
	t.Cleanup(l.Close)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	assert.Equal(t, len(l.Index.Declarations[manifest.BootstrapSourceID]["resource"].Objects), 1)
	_, observed := l.Index.Declarations[manifest.BootstrapSourceID]["script.sh"]
	assert.Equal(t, observed, false)
}

func TestCompleteDeclarations_RequiredFileRoles(t *testing.T) {
	t.Parallel()
	body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: required}\n"
	for _, mode := range []string{"cached", "parsed", "replaced"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			input := &declarationReadFS{FS: fstest.MapFS{
				"resource": {Data: []byte(body)}, "script.sh": {Data: []byte(body)},
			}}
			l, _ := completionFixture(t, input)
			l.documents.mountSources(l.Store, nil, l.Index)
			l.documents.intent, l.documents.skipDeclarations = manifest.RequiredDocuments, true
			_, err := l.documents.fileObjects(t.Context(), filepath.Join(l.SourceRoot, "resource"),
				manifest.ParseDocOptions{RequiredIdentity: true})
			assert.NoError(t, err)
			l.documents.intent = manifest.IncidentalDocuments
			if mode != "cached" {
				l.documents.parsed = l.ParsedInputs()
				l.documents.cache = map[string]document{}
			}
			if mode == "replaced" {
				input = &declarationReadFS{FS: input.FS}
				l.Store.SetArtifact(manifest.BootstrapSourceID,
					&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: l.SourceRoot, FS: input})
			}
			current := l.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			changes, err := change.Detect(t.Context(), &store.SourceArtifact{FS: fstest.MapFS{}}, current,
				change.DetectConfig{Candidates: map[string]bool{"resource": false, "script.sh": false}})
			assert.NoError(t, err)
			input.opens.Store(0)
			index, err := l.CompleteDeclarations(
				t.Context(),
				selection.DemandDeclarations(manifest.TreeHead, changes, l.Index),
			)
			assert.NoError(t, err)
			want := 1
			if mode == "replaced" {
				want = 0
			}
			assert.Equal(t, len(index.Declarations[manifest.BootstrapSourceID]["resource"].Objects), want)
			assert.Equal(t, input.opens.Load(), int64(want))
			assert.Diff(t, index.Declarations[manifest.BootstrapSourceID]["script.sh"],
				manifest.FileDeclarations{Present: true, Complete: true})
		})
	}
}

func TestCompleteDeclarations_PreservesInventorySnapshots(t *testing.T) {
	t.Parallel()
	for _, content := range []string{"before", "replacement"} {
		t.Run(content, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
			reader := readerForTest(root)
			reader.artifacts = map[string]*store.SourceArtifact{root: input}
			id := manifest.NamedResource{Kind: "ConfigMap", Name: "work"}
			previous := map[string]manifest.FileDeclarations{
				"input.yaml": {Present: true, Complete: true, Objects: map[manifest.NamedResource]string{id: "before"}},
			}
			reader.cache[filepath.Join(root, "input.yaml")] = document{
				sourceRoot: root,
				declarations: &fileObservation{
					Present: true, Complete: true, Objects: map[manifest.NamedResource]string{id: content},
				},
			}
			inventory := declarationInventory{input: input, files: previous, paths: []string{"input.yaml"}}
			changes, err := change.Qualify(
				t.Context(),
				detectChanges(t, []string{"input.yaml"}, []string{"input.yaml"}),
				change.SourcePair{Stores: [2]*store.Store{store.New(), store.New()}},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			err = reader.completeSource(t.Context(), changes,
				manifest.TreeHead, &inventory)
			assert.NoError(t, err)
			assert.Equal(t, len(inventory.files), 0)
			assert.Equal(t, len(previous), 1)
			assert.Equal(t, previous["input.yaml"].Objects[id], "before")
			current, err := reader.sourceDeclarations(input, previous)
			assert.NoError(t, err)
			assert.Equal(t, current["input.yaml"].Objects[id], content)
		})
	}
}
