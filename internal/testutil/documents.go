package testutil

import (
	"bytes"
	"io"
	"reflect"

	"github.com/home-operations/flate/pkg/manifest"
)

// DocumentBatch keeps fixture identities aligned with borrowed payloads.
func DocumentBatch(parent manifest.BaseManifest, documents []map[string]any) manifest.DocumentBatch {
	var policy manifest.EmissionPolicy
	if parent != nil {
		policy = parent.EmissionPolicy()
	}
	identities := make([]manifest.NamedResource, len(documents))
	for i, doc := range documents {
		identities[i] = policy.EmittedIdentity(doc)
	}
	return manifest.DocumentBatch{Parent: parent, Documents: documents, Identities: identities}
}

// DecodeDocs constructs mapping fixtures and rejects retained failures.
func DecodeDocs(r io.Reader) ([]map[string]any, error) {
	docs, err := manifest.DecodeDocs(r, manifest.IncidentalDocuments)
	return documentMaps(docs, err)
}

// SplitDocs constructs mapping fixtures from bytes.
func SplitDocs(data []byte) ([]map[string]any, error) { return DecodeDocs(bytes.NewReader(data)) }

func documentMaps(docs []manifest.DocOccurrence, err error) ([]map[string]any, error) {
	if err == nil {
		err = manifest.DocumentFailure(docs)
	}
	if err != nil {
		return nil, err
	}
	if docs == nil {
		return nil, nil
	}
	maps := make([]map[string]any, len(docs))
	for i, doc := range docs {
		maps[i] = doc.Document
	}
	return maps, nil
}

// RenderMaps projects successful renderer outcomes for map assertions.
func RenderMaps(
	docs []manifest.DocOccurrence,
	receipt *manifest.RenderReceipt,
	err error,
) ([]map[string]any, *manifest.RenderReceipt, error) {
	maps, err := documentMaps(docs, err)
	return maps, receipt, err
}

// EncodeRender encodes ordered mapping fixtures.
func EncodeRender(docs []map[string]any) ([]byte, error) {
	return manifest.EncodeRender(DocumentOccurrences(docs))
}

// DecodeRender projects constructed cache fixtures.
func DecodeRender(data []byte) ([]map[string]any, error) {
	docs, err := manifest.DecodeRender(data)
	return documentMaps(docs, err)
}

// DocumentMaps projects attempted inventory for mapping assertions.
func DocumentMaps(docs []manifest.DocOccurrence) []map[string]any {
	if docs == nil {
		return nil
	}
	out := make([]map[string]any, len(docs))
	for i, doc := range docs {
		out[i] = doc.Document
	}
	return out
}

// DocumentOccurrences assigns ordinals to mapping fixtures.
func DocumentOccurrences(docs []map[string]any) []manifest.DocOccurrence {
	out := make([]manifest.DocOccurrence, len(docs))
	for i, doc := range docs {
		out[i] = manifest.DocOccurrence{Document: doc, Ordinal: i}
	}
	return out
}

// ObjectDocument retains data payloads when constructing authoritative SDK fixtures.
func ObjectDocument(obj manifest.BaseManifest) map[string]any {
	if obj == nil || reflect.ValueOf(obj).IsNil() {
		return nil
	}
	if data, ok := obj.(manifest.Data); ok {
		return data.Snapshot()
	}
	doc, err := manifest.ObjectDocument(obj.Named(), obj)
	if err != nil {
		panic(err)
	}
	return doc
}
