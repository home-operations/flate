package loader

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"

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
	err = reader.decodeFileObjects(
		filepath.Join(root, "input.yaml"),
		manifest.ParseDocOptions{},
		true,
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
			got, _, err := expandDocuments(manifest.DocOccurrence{Document: tt.doc, Ordinal: 7})
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
	complete, err := reader.fileObjects(file, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	assert.Equal(t, len(complete), 2)
	reader.coordinate = &OccurrenceID{Document: 1, item: 1}
	children, err := reader.parseFile(t.Context(), file, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	assert.FatalIf(t, len(children) != 1, "exact children=%d, want 1", len(children))
	assert.Equal(t, children[0].Obj.(*manifest.ConfigMap).Data["VALUE"], "second")
	assert.Equal(t, len(reader.cache[file].objects), 2)
}
