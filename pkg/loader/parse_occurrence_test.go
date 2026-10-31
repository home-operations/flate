package loader

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

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
	if err != nil || len(decoded) != 1 {
		t.Fatalf("decode = %v, %v", decoded, err)
	}
	root := t.TempDir()
	reader := documentReader{compact: true}
	reader.setSources(map[string]*store.SourceArtifact{root: {LocalPath: root, FS: fstest.MapFS{"input.yaml": {Data: []byte(input)}}}})
	var expanded []parsedObject
	err = reader.decodeFileObjects(filepath.Join(root, "input.yaml"), manifest.ParseDocOptions{}, true, func(child parsedObject) { expanded = append(expanded, child) })
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(decoded[0].Document)
	if err != nil {
		t.Fatal(err)
	}
	want, err := resource.NewFactory(nil).RNodesFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(expanded) != len(want) || len(expanded) != 4 {
		t.Fatalf("leaf count = %d, want %d", len(expanded), len(want))
	}
	for i, got := range expanded {
		doc, err := want[i].Map()
		if err != nil {
			t.Fatal(err)
		}
		if got.outcome.Ordinal != 1 || got.item != i || !reflect.DeepEqual(got.outcome.Document, doc) {
			t.Fatalf("leaf %d = %+v, want ordinal 1 and upstream map %v", i, got, doc)
		}
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
				if !errors.Is(err, manifest.ErrInput) {
					t.Fatalf("error = %v, want ErrInput", err)
				}
				return
			}
			if err != nil || len(got) != tt.count {
				t.Fatalf("Expand = %v, %v", got, err)
			}
			if len(got) == 1 && got[0].Ordinal != 7 {
				t.Fatalf("coordinates changed: %+v", got[0])
			}
		})
	}
}
