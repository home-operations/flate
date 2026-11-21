package manifest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	yaml "go.yaml.in/yaml/v4"
	"go.yaml.in/yaml/v4/plugin/loaderlimits"
)

// yaml.v4 reuses non-nil maps; only unretained documents may return here.
var decodedMapPool = sync.Pool{
	New: func() any {
		return make(map[string]any, initialDocumentCapacity)
	},
}

func getDecodedMap() map[string]any {
	//nolint:errcheck // decodedMapPool.New and ReleaseDoc publish only map[string]any values.
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
	Document           map[string]any `json:"Document"`
	Ordinal            int            `json:"Ordinal"`
	Err                error          `json:"Err"`
	Warnings           []Warning      `json:"-"`
	ConsumedUnreadable []Reference    `json:"-"`
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

// The preset contains immutable options and stateless plugins shared by independent decoders.
//
//nolint:gochecknoglobals // Independent decoders share immutable upstream options.
var documentDecodeOptions = yaml.WithV3Defaults()

// DecodeDocs retains document and read failures, returning cancellation and deadline errors.
// Returned maps belong to the caller and MUST remain live while retained.
func DecodeDocs(r io.Reader, role DocumentRole) ([]DocOccurrence, error) {
	stream := &documentStream{Reader: r}
	// The fixed upstream preset cannot fail option construction.
	dec, _ := yaml.NewLoader(stream, documentDecodeOptions)
	constructionOption := yaml.WithPlugin(&stream.limits)
	var out []DocOccurrence
	for ordinal := 0; ; ordinal++ {
		stream.node = yaml.Node{}
		err := dec.Load(&stream.node)
		if err != nil {
			return stream.finishDecode(out, err, ordinal)
		}
		preserveTimestampStrings(&stream.node)
		loaded, err := stream.loadDocumentNode(role, constructionOption)
		// Clear the borrowed header before a failed document returns to the pool.
		stream.document = nil
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

// Node.Load creates a constructor per document; alias limits MUST count the entire stream.
type documentLimits struct {
	loaderlimits.Plugin

	priorAliases, priorConstructs int
	lastAliases, lastConstructs   int
}

func (l *documentLimits) CheckAlias(aliases, constructs int) error {
	if constructs == 1 {
		l.priorAliases += l.lastAliases
		l.priorConstructs += l.lastConstructs
	}
	l.lastAliases, l.lastConstructs = aliases, constructs
	//nolint:wrapcheck // Preserve upstream diagnostics.
	return l.Plugin.CheckAlias(l.priorAliases+aliases, l.priorConstructs+constructs)
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

	err      error
	document map[string]any
	limits   documentLimits
	node     yaml.Node
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

func (s *documentStream) loadDocumentNode(
	role DocumentRole,
	constructionOption yaml.Option,
) (loadedDocument, error) {
	node := &s.node
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
			s.document = getDecodedMap()
			err = node.Load(&s.document, constructionOption)
			m = s.document
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
		apiVersionKey: fields.APIVersion, kindKey: fields.Kind,
		metadataKey: map[string]any{nameKey: fields.Metadata.Name, "namespace": fields.Metadata.Namespace},
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
	var spec fluxInstanceGenerationSpec
	if err := node.Load(&spec, option); err != nil {
		return nil, fmt.Errorf("decode application: %w", err)
	}
	data, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("decode application: %w", err)
	}
	var compact map[string]any
	if err := json.Unmarshal(data, &compact); err != nil {
		return nil, fmt.Errorf("decode application: %w", err)
	}
	if len(compact) != 0 {
		doc["spec"] = compact
	}
	return doc, nil
}
