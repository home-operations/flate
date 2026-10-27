package testutil

import (
	"bytes"
	"io"

	"github.com/home-operations/flate/pkg/manifest"
)

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
func RenderMaps(docs []manifest.DocOccurrence, receipt *manifest.RenderReceipt, err error) ([]map[string]any, *manifest.RenderReceipt, error) {
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

// ParseChildren prepares mapping fixtures for inventory assertions.
func ParseChildren[T any](parser interface {
	ParseChildren(manifest.BaseManifest, []manifest.DocOccurrence, manifest.ParseDocOptions) ([]T, []manifest.DocOccurrence, []manifest.DocOccurrence)
}, parent manifest.BaseManifest, docs []map[string]any, opts manifest.ParseDocOptions) ([]T, []map[string]any) {
	children, visible, failures := parser.ParseChildren(parent, DocumentOccurrences(docs), opts)
	if err := manifest.DocumentFailure(failures); err != nil {
		panic(err)
	}
	return children, DocumentMaps(visible)
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
