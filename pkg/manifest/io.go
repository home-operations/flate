package manifest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	yaml "go.yaml.in/yaml/v4"
	"go.yaml.in/yaml/v4/plugin/limit"
)

// yaml.v4 reuses non-nil maps; only unretained documents may return here.
var decodedMapPool = sync.Pool{
	New: func() any {
		return make(map[string]any, 8)
	},
}

func getDecodedMap() map[string]any {
	return decodedMapPool.Get().(map[string]any)
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
	decodedMapPool.Put(m)
}

// DocOccurrence retains a constructed document or failure at its original stream ordinal.
type DocOccurrence struct {
	Document map[string]any
	Ordinal  int
	Err      error
}

// DocumentRole determines whether nonmapping documents are required manifests.
type DocumentRole bool

const (
	// IncidentalDocuments permits nonmapping repository content.
	IncidentalDocuments DocumentRole = false
	// RequiredDocuments requires each nonempty document to be a mapping.
	RequiredDocuments DocumentRole = true
)

// DecodeDocs retains document and read failures, returning cancellation and deadline errors.
// Returned maps belong to the caller and MUST remain live while retained.
func DecodeDocs(r io.Reader, role DocumentRole) ([]DocOccurrence, error) {
	stream := &documentStream{Reader: r}
	dec := yaml.NewDecoder(stream)
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
	var out []DocOccurrence
	for ordinal := 0; ; ordinal++ {
		var node yaml.Node
		err := dec.Decode(&node)
		if err != nil {
			if stream.err != nil {
				failure := NewError(ErrInput, fmt.Errorf("read documents: %w", stream.err), "")
				if errors.Is(stream.err, context.Canceled) || errors.Is(stream.err, context.DeadlineExceeded) {
					return out, failure
				}
				return append(out, DocOccurrence{Ordinal: ordinal, Err: failure}), nil
			}
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			out = append(out, DocOccurrence{Ordinal: ordinal, Err: NewError(ErrInput, err, "")})
			return out, nil
		}
		preserveTimestampStrings(&node)
		mapping := &node
		if mapping.Kind == yaml.DocumentNode && len(mapping.Content) != 0 {
			mapping = mapping.Content[0]
		}
		var m map[string]any
		shapeFailure := false
		if mapping.Kind == yaml.MappingNode {
			m = getDecodedMap()
			err = node.Load(&m, constructionOption)
		} else {
			var value any
			err = node.Load(&value, constructionOption)
			if err == nil && value != nil && role == RequiredDocuments {
				err = fmt.Errorf("document must be a mapping")
				shapeFailure = true
			}
		}
		if err != nil {
			ReleaseDoc(m)
			out = append(out, DocOccurrence{Ordinal: ordinal, Err: NewError(ErrInput, err, "")})
			if _, recoverable := errors.AsType[*yaml.LoadErrors](err); !recoverable && !shapeFailure {
				return out, nil
			}
			continue
		}
		if len(m) == 0 {
			ReleaseDoc(m)
			continue
		}
		out = append(out, DocOccurrence{Document: m, Ordinal: ordinal})
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

type documentStream struct {
	io.Reader
	err error
}

func (s *documentStream) Read(p []byte) (int, error) {
	n, err := s.Reader.Read(p)
	if err != nil && err != io.EOF {
		s.err = err
	}
	return n, err
}

// DocumentFailure returns the first required failure in stream order.
func DocumentFailure(docs []DocOccurrence) error {
	for _, doc := range docs {
		if doc.Err != nil {
			if id := DocIdentity(doc.Document); id.Name != "" {
				return fmt.Errorf("document %d (%s): %w", doc.Ordinal+1, id, doc.Err)
			}
			return fmt.Errorf("document %d: %w", doc.Ordinal+1, doc.Err)
		}
	}
	return nil
}
