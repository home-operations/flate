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
// We walk yaml.Node directly instead of routing through a YAML→JSON→Go
// round-trip — DecodeDocs is on the hot path for every loaded file plus
// every rendered helm/kustomize output, so skipping the extra
// marshal/unmarshal makes rendering measurably faster.
func DecodeDocs(r io.Reader) ([]map[string]any, error) {
	dec := yaml.NewDecoder(r)
	var out []map[string]any
	for {
		var node yaml.Node
		if err := dec.Decode(&node); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, fmt.Errorf("%w: %v", ErrInput, err)
		}
		m, err := nodeToMap(&node)
		if err != nil {
			return nil, err
		}
		if m == nil {
			continue
		}
		out = append(out, m)
	}
}

// SplitDocs is the byte-slice convenience wrapper for DecodeDocs.
func SplitDocs(data []byte) ([]map[string]any, error) {
	return DecodeDocs(bytes.NewReader(data))
}

// nodeToMap walks a yaml.Node and returns the root mapping as
// map[string]any. A nil / empty / null-scalar root yields (nil, nil) so
// the caller can skip it; a non-mapping root is an error.
func nodeToMap(node *yaml.Node) (map[string]any, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	target := node
	if node.Kind == yaml.DocumentNode {
		if len(node.Content) == 0 {
			return nil, nil
		}
		target = node.Content[0]
	}
	if target.Kind == yaml.ScalarNode && (target.Tag == "!!null" || target.Value == "") {
		return nil, nil
	}
	if target.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%w: expected mapping at document root, got %d", ErrInput, target.Kind)
	}
	v, err := nodeValue(target)
	if err != nil {
		return nil, err
	}
	m, _ := v.(map[string]any)
	if len(m) == 0 {
		return nil, nil
	}
	return m, nil
}

// nodeValue converts an arbitrary yaml.Node into the equivalent Go
// value: MappingNode → map[string]any, SequenceNode → []any,
// ScalarNode → typed scalar (nil, bool, int, float64, or string) per
// yaml.v4's YAML 1.2 core schema resolution.
func nodeValue(n *yaml.Node) (any, error) {
	switch n.Kind {
	case yaml.MappingNode:
		m := make(map[string]any, len(n.Content)/2)
		for i := 0; i+1 < len(n.Content); i += 2 {
			keyNode, valNode := n.Content[i], n.Content[i+1]
			if keyNode.Kind != yaml.ScalarNode {
				return nil, fmt.Errorf("%w: non-scalar mapping key at line %d", ErrInput, keyNode.Line)
			}
			val, err := nodeValue(valNode)
			if err != nil {
				return nil, err
			}
			m[keyNode.Value] = val
		}
		return m, nil
	case yaml.SequenceNode:
		arr := make([]any, len(n.Content))
		for i, c := range n.Content {
			v, err := nodeValue(c)
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	case yaml.ScalarNode:
		// Delegate scalar type resolution to yaml.v4 so untagged values
		// pick up the YAML 1.2 core schema (true/false, integers,
		// floats, null) and explicit tags are honored.
		var v any
		if err := n.Decode(&v); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInput, err)
		}
		return v, nil
	case yaml.AliasNode:
		if n.Alias == nil {
			return nil, fmt.Errorf("%w: unresolved alias", ErrInput)
		}
		return nodeValue(n.Alias)
	}
	return nil, fmt.Errorf("%w: unexpected yaml node kind %d", ErrInput, n.Kind)
}
