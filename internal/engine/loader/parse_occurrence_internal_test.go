package loader

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil/assert"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"

	"sigs.k8s.io/kustomize/api/resource"
)

func TestDecodeFileObjects_ExpansionCoordinates(t *testing.T) {
	t.Parallel()
	input := `{}
---
apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: same}
- apiVersion: v1
  kind: ConfigMapList
  items:
  - apiVersion: v1
    kind: ConfigMap
    metadata: {name: same}
  - apiVersion: v1
    kind: ConfigMap
    metadata: {name: same}
- apiVersion: v1
  kind: Secret
  metadata: {name: last}
`
	decoded, err := manifest.DecodeDocs(strings.NewReader(input), manifest.IncidentalDocuments)
	assert.FatalIf(t, err != nil || len(decoded) != 1, "decode = %v, %v", decoded, err)
	root := t.TempDir()
	reader := documentReader{compact: true}
	reader.setSources(
		map[string]*store.SourceArtifact{
			root: {LocalPath: root, FS: fstest.MapFS{"input.yaml": {Data: []byte(input)}}},
		},
	)
	var expanded []parsedObject
	err = reader.decodeFileObjects(t.Context(),
		filepath.Join(root, "input.yaml"),
		manifest.ParseDocOptions{},
		func(child parsedObject) { expanded = append(expanded, child) },
	)
	assert.NoError(t, err)
	data, err := json.Marshal(decoded[0].Document)
	assert.NoError(t, err)
	want, err := resource.NewFactory(nil).RNodesFromBytes(data)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		len(expanded) != len(want) || len(expanded) != 4,
		"leaf count = %d, want %d",
		len(expanded),
		len(want),
	)
	for i, got := range expanded {
		doc, err := want[i].Map()
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			got.outcome.Ordinal != 1 || got.item != i || !reflect.DeepEqual(got.outcome.Document, doc),
			"leaf %d = %+v, want ordinal 1 and upstream map %v",
			i,
			got,
			doc,
		)
	}
}

func TestExpandDocuments_CollectionPolicy(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		doc     map[string]any
		count   int
		failure bool
	}{
		{name: "bare", doc: map[string]any{"kind": "ConfigMap", "data": map[string]any{"key": "value"}}, count: 1},
		{name: "absent items", doc: map[string]any{"kind": "FooList"}, count: 1},
		{name: "null", doc: map[string]any{"apiVersion": "v1", "kind": "List", "items": nil}},
		{name: "empty", doc: map[string]any{"apiVersion": "v1", "kind": "List", "items": []any{}}},
		{name: "malformed", doc: map[string]any{"apiVersion": "v1", "kind": "List", "items": "invalid"}, failure: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, _, err := expandDocuments(manifest.DocOccurrence{Document: tt.doc, Ordinal: 7}, nil)
			if tt.failure {
				assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "error = %v, want ErrInput", err)
				return
			}
			assert.FatalIf(t, err != nil || len(got) != tt.count, "Expand = %v, %v", got, err)
			if len(got) == 1 && got[0].Ordinal != 7 {
				t.Fatalf("coordinates changed: %+v", got[0])
			}
		})
	}
}

func TestOccurrenceObject_CompleteFileCollectionCoordinate(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	file := filepath.Join(root, "input.yaml")
	reader := documentReader{compact: true, cache: map[string]document{}}
	reader.setSources(map[string]*store.SourceArtifact{
		root: {LocalPath: root, FS: fstest.MapFS{"input.yaml": {Data: []byte(`{}
---
apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: same, namespace: apps}
  data: {VALUE: first}
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: same, namespace: apps}
  data: {VALUE: second}
`)}}},
	})
	complete, err := reader.fileObjects(t.Context(), file, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	assert.Equal(t, len(complete), 2)
	reader.coordinate = &occurrenceID{Document: 1, item: 1}
	children, err := reader.parseFile(t.Context(), file, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	assert.FatalIf(t, len(children) != 1, "exact children=%d, want 1", len(children))
	assert.Equal(t, children[0].Obj.(*manifest.ConfigMap).Data["VALUE"], "second")
	assert.Equal(t, len(reader.cache[file].objects), 2)
}

func TestPrepare_DeclarationContent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, namespace, defaultNamespace string
		ownership                         bool
	}{
		{name: "no namespace"},
		{name: "default namespace", defaultNamespace: "apps"},
		{name: "explicit namespace", namespace: "explicit"},
		{name: "ownership labels", namespace: "apps", ownership: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metadata := map[string]any{"name": "settings"}
			if tc.namespace != "" {
				metadata["namespace"] = tc.namespace
			}
			if tc.ownership {
				metadata["labels"] = map[string]any{"kustomize.toolkit.fluxcd.io/name": "sync", "custom": "retained"}
			}
			doc := map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   metadata,
				"data":       map[string]any{"KEY": "value"},
			}
			before := manifest.DeepCopyMap(doc)
			child, outcome, _ := Prepare(
				manifest.DocOccurrence{Document: doc},
				manifest.ParseDocOptions{DefaultNamespace: tc.defaultNamespace},
			)
			assert.NoError(t, outcome.Err)
			exact := manifest.WithoutOwnershipLabels(child.Doc)
			withoutNamespace := manifest.DeepCopyMap(exact)
			delete(withoutNamespace["metadata"].(map[string]any), "namespace")
			assert.Equal(t, new(Loader).DeclarationContents()(child.Doc), store.DeclarationContent{
				Exact:            manifest.Fingerprint(exact),
				WithoutNamespace: manifest.Fingerprint(withoutNamespace),
				Namespace:        manifest.DocMetadata(child.Doc).Namespace,
			})
			assert.Diff(t, doc, before)
			if tc.namespace == "" && tc.defaultNamespace == "" {
				assert.Equal(
					t,
					new(Loader).DeclarationContents()(child.Doc).Exact,
					new(Loader).DeclarationContents()(child.Doc).WithoutNamespace,
				)
			}
		})
	}
	child, _, _ := Prepare(manifest.DocOccurrence{}, manifest.ParseDocOptions{})
	assert.Equal(t, new(Loader).DeclarationContents()(child.Doc), store.DeclarationContent{})
}
