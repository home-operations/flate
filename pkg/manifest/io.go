package manifest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	operatorv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	yaml "go.yaml.in/yaml/v4"
	"go.yaml.in/yaml/v4/plugin/loaderlimits"
)

// yaml.v4 reuses non-nil maps; only unretained documents may return here.
//
//nolint:gochecknoglobals // A process-wide pool reuses decoded document maps.
var decodedMapPool = sync.Pool{
	New: func() any {
		return make(map[string]any, initialDocumentCapacity)
	},
}

func getDecodedMap() map[string]any {
	//nolint:errcheck // The owning path guarantees the concrete type and panics on a broken invariant.
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
	Document map[string]any `json:"Document"`
	Ordinal  int            `json:"Ordinal"`
	Err      error          `json:"Err"`
	Warnings []Warning      `json:"-"`
}

// DocumentRole determines whether nonmapping documents are required manifests.
type DocumentRole uint8

const (
	// IncidentalDocuments permits nonmapping repository content.
	IncidentalDocuments DocumentRole = iota
	// RequiredDocuments requires each nonempty document to be a mapping.
	RequiredDocuments
	// ApplicationDocuments constructs only application identity, source, path and dependencies.
	ApplicationDocuments
)

// DecodeDocs retains document and read failures, returning cancellation and deadline errors.
// Returned maps belong to the caller and MUST remain live while retained.
func DecodeDocs(r io.Reader, role DocumentRole) ([]DocOccurrence, error) {
	stream := &documentStream{Reader: r}
	dec := yaml.NewDecoder(stream)
	var priorAliases, priorConstructs, lastAliases, lastConstructs int
	guard := loaderlimits.New()
	// Node.Load creates a constructor per document; the decoder's alias policy
	// requires cumulative stream counts, including skipped documents.
	constructionOption := yaml.WithPlugin(loaderlimits.New(loaderlimits.AliasFunc(func(aliases, constructs int) error {
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
			return stream.finishDecode(out, err, ordinal)
		}
		preserveTimestampStrings(&node)
		loaded, err := loadDocumentNode(&node, role, constructionOption)
		m, shapeFailure := loaded.document, loaded.shapeFailure
		if err != nil {
			ReleaseDoc(m)
			out = append(out, DocOccurrence{Ordinal: ordinal, Err: NewError(ErrInput, err, "")})
			if _, recoverable := errors.AsType[*yaml.LoadErrors](err); !recoverable && !shapeFailure {
				return out, nil
			}
			continue
		}
		if len(m) == 0 && (m == nil || role != RequiredDocuments) {
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
	//nolint:wrapcheck // Reader errors and EOF retain their identity at this transparent observation boundary.
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

const (
	initialDocumentCapacity = 8
)

func (s *documentStream) finishDecode(out []DocOccurrence, err error, ordinal int) ([]DocOccurrence, error) {
	if s.err != nil {
		failure := NewError(ErrInput, fmt.Errorf("read documents: %w", s.err), "")
		if errors.Is(s.err, context.Canceled) || errors.Is(s.err, context.DeadlineExceeded) {
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

type loadedDocument struct {
	document     map[string]any
	shapeFailure bool
}

func loadDocumentNode(
	node *yaml.Node,
	role DocumentRole,
	constructionOption yaml.Option,
) (loadedDocument, error) {
	mapping := node
	if mapping.Kind == yaml.DocumentNode && len(mapping.Content) != 0 {
		mapping = mapping.Content[0]
	}
	var m map[string]any
	var err error
	shapeFailure := false
	if mapping.Kind == yaml.MappingNode {
		if role == ApplicationDocuments {
			m, err = applicationDocument(node, constructionOption)
		} else {
			m = getDecodedMap()
			err = node.Load(&m, constructionOption)
		}
	} else {
		var value any
		err = node.Load(&value, constructionOption)
		if err == nil && value != nil && role == RequiredDocuments {
			err = fmt.Errorf("document must be a mapping")
			shapeFailure = true
		}
	}
	return loadedDocument{document: m, shapeFailure: shapeFailure}, err
}

func applicationDocument(node *yaml.Node, option yaml.Option) (map[string]any, error) {
	var fields struct {
		APIVersion string        `yaml:"apiVersion"`
		Kind       string        `yaml:"kind"`
		Metadata   NamedResource `yaml:"metadata"`
		Spec       yaml.Node     `yaml:"spec"`
		Items      []yaml.Node   `yaml:"items"`
	}
	if err := node.Load(&fields, option); err != nil {
		return nil, fmt.Errorf("decode application: %w", err)
	}
	doc := map[string]any{
		"apiVersion": fields.APIVersion, "kind": fields.Kind,
		"metadata": map[string]any{"name": fields.Metadata.Name, "namespace": fields.Metadata.Namespace},
	}
	if strings.HasSuffix(fields.Kind, "List") && fields.Items != nil {
		items := make([]any, 0, len(fields.Items))
		for i := range fields.Items {
			item, err := applicationDocument(&fields.Items[i], option)
			if err != nil {
				return nil, fmt.Errorf("decode application: %w", err)
			}
			items = append(items, item)
		}
		doc["items"] = items
		return doc, nil
	}
	id := DocIdentity(doc)
	if id.Group == FluxKustomizeDomain && id.Kind == KindKustomization {
		var spec struct {
			Path      string                                    `yaml:"path"`
			SourceRef kustomizev1.CrossNamespaceSourceReference `yaml:"sourceRef"`
			DependsOn []kustomizev1.DependencyReference         `yaml:"dependsOn"`
		}
		if err := fields.Spec.Load(&spec, option); err != nil {
			return nil, fmt.Errorf("decode application: %w", err)
		}
		object := &Kustomization{Name: id.Name, Namespace: id.Namespace}
		object.Path, object.DependsOn = spec.Path, spec.DependsOn
		object.SourceRef = spec.SourceRef
		projected, err := object.RenderDocument()
		if err != nil {
			return nil, fmt.Errorf("decode application: %w", err)
		}
		projected["apiVersion"] = fields.APIVersion
		return projected, nil
	}
	if id.Group != FluxOperatorDomain || id.Kind != "FluxInstance" {
		return doc, nil
	}
	return applicationSyncDocument(doc, &fields.Spec, option)
}

func applicationSyncDocument(doc map[string]any, node *yaml.Node, option yaml.Option) (map[string]any, error) {
	var spec struct {
		Sync *operatorv1.Sync `yaml:"sync"`
	}
	if err := node.Load(&spec, option); err != nil {
		return nil, fmt.Errorf("decode application: %w", err)
	}
	if spec.Sync != nil {
		data, err := json.Marshal(spec.Sync)
		if err != nil {
			return nil, fmt.Errorf("decode application: %w", err)
		}
		var syncDocument map[string]any
		if err := json.Unmarshal(data, &syncDocument); err != nil {
			return nil, fmt.Errorf("decode application: %w", err)
		}
		doc["spec"] = map[string]any{"sync": syncDocument}
	}
	return doc, nil
}
