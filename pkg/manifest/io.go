package manifest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	yaml "go.yaml.in/yaml/v4"
	"go.yaml.in/yaml/v4/plugin/limit"
)

// decodedMapPool recycles top-level map[string]any documents produced
// by DecodeDocs. yaml.v4's libyaml constructor reuses a pre-populated
// non-nil map when decoding (constructor.go: `if out.IsNil() {
// MakeMap }`), so handing it a cleared pooled map skips the
// 16-bucket map allocation per document — meaningful for multi-doc
// helm output (50+ docs) and tight loader loops.
//
// Pool elements are *map[string]any to avoid escaping a fresh
// interface wrapper per Get / Put. The 16-entry seed mirrors the
// typical Flux CR top-level shape (apiVersion, kind, metadata, spec,
// plus a handful of misc keys).
var decodedMapPool = sync.Pool{
	New: func() any {
		m := make(map[string]any, 16)
		return &m
	},
}

// getDecodedMap returns a cleared, ready-to-use map from the pool.
func getDecodedMap() map[string]any {
	return *decodedMapPool.Get().(*map[string]any)
}

// ReleaseDoc returns a decoded document to the internal pool. Safe to
// call only after the caller is done reading the top-level map.
//
// Calling ReleaseDoc with a map that did not originate from
// DecodeDocs is allowed but pollutes the pool; reserve for the
// internal load path.
func ReleaseDoc(m map[string]any) {
	if m == nil {
		return
	}
	clear(m)
	decodedMapPool.Put(&m)
}

// DecodeDocs reads zero or more YAML documents from r and returns each
// as a generic map. Implicit timestamps retain their scalar text; explicit
// tags and alias/merge construction use the upstream YAML policy. Empty maps
// and recoverable LoadErrors are skipped; other errors wrap ErrInput.
//
// Returned maps belong to the caller. Release them only when no authoritative
// document or render artifact retains them; never copy retained maps
// merely to recycle the originals.
func DecodeDocs(r io.Reader) ([]map[string]any, error) {
	dec := yaml.NewDecoder(r)
	var priorAliases, priorConstructs, lastAliases, lastConstructs int
	guard := limit.New()
	// Node.Load creates a constructor per document; the decoder's alias policy
	// requires cumulative stream counts, including skipped documents.
	constructionOption := yaml.WithPlugin(limit.New(limit.AliasFunc(func(aliases, constructs int) error {
		if constructs == 1 {
			priorAliases += lastAliases
			priorConstructs += lastConstructs
		}
		lastAliases, lastConstructs = aliases, constructs
		return guard.CheckAlias(priorAliases+aliases, priorConstructs+constructs)
	})))
	var out []map[string]any
	for {
		var node yaml.Node
		var m map[string]any
		err := dec.Decode(&node)
		if err == nil {
			preserveTimestampStrings(&node)
			m = getDecodedMap()
			err = node.Load(&m, constructionOption)
		}
		if err != nil {
			ReleaseDoc(m)
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			if _, ok := errors.AsType[*yaml.LoadErrors](err); ok {
				continue
			}
			return nil, fmt.Errorf("%w: %w", ErrInput, err)
		}
		if len(m) == 0 {
			ReleaseDoc(m)
			continue
		}
		out = append(out, m)
	}
}

func preserveTimestampStrings(n *yaml.Node) {
	if n.Kind == yaml.ScalarNode && n.ShortTag() == "!!timestamp" && n.Style&yaml.TaggedStyle == 0 {
		n.Tag = "!!str"
	}
	// Anchored targets occur in Content; following Alias would revisit them.
	for _, child := range n.Content {
		preserveTimestampStrings(child)
	}
}

// SplitDocs is the byte-slice convenience wrapper for DecodeDocs.
func SplitDocs(data []byte) ([]map[string]any, error) {
	return DecodeDocs(bytes.NewReader(data))
}
