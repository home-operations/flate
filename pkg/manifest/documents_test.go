package manifest

import (
	"bytes"
	"io"
)

func decodeDocMaps(r io.Reader) ([]map[string]any, error) {
	docs, err := DecodeDocs(r, IncidentalDocuments)
	if err == nil {
		err = DocumentFailure(docs)
	}
	if err != nil {
		return nil, err
	}
	var maps []map[string]any
	for _, doc := range docs {
		maps = append(maps, doc.Document)
	}
	return maps, nil
}

func splitDocMaps(data []byte) ([]map[string]any, error) { return decodeDocMaps(bytes.NewReader(data)) }
func encodeRenderMaps(docs []map[string]any) ([]byte, error) {
	outcomes := make([]DocOccurrence, len(docs))
	for i, doc := range docs {
		outcomes[i] = DocOccurrence{Document: doc, Ordinal: i}
	}
	return EncodeRender(outcomes)
}
func decodeRenderMaps(data []byte) ([]map[string]any, error) {
	docs, err := DecodeRender(data)
	if err != nil {
		return nil, err
	}
	var maps []map[string]any
	for _, doc := range docs {
		maps = append(maps, doc.Document)
	}
	return maps, nil
}
