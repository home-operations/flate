package manifest

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	yaml "go.yaml.in/yaml/v4"
)

// DecodeDocs reads zero or more YAML documents from r and returns each
// as a generic map. Empty / null-scalar documents are skipped.
//
// Decodes directly into map[string]any via the yaml.v4 library — same
// shape sigs.k8s.io/yaml uses behind real Flux. Letting the library
// own the walk means YAML 1.2 features (anchors, aliases — including
// aliases-as-keys, merge keys, tagged scalars) round-trip correctly
// without a hand-rolled node visitor playing catch-up.
func DecodeDocs(r io.Reader) ([]map[string]any, error) {
	dec := yaml.NewDecoder(r)
	var out []map[string]any
	for {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, fmt.Errorf("%w: %v", ErrInput, err)
		}
		if len(m) == 0 {
			continue
		}
		out = append(out, m)
	}
}

// SplitDocs is the byte-slice convenience wrapper for DecodeDocs.
func SplitDocs(data []byte) ([]map[string]any, error) {
	return DecodeDocs(bytes.NewReader(data))
}
