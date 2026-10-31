package manifest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"text/template"

	k8syaml "k8s.io/apimachinery/pkg/util/yaml"

	"sigs.k8s.io/kustomize/api/resource"
	kyaml "sigs.k8s.io/kustomize/kyaml/yaml"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/internal/resourcesetinput"
)

// TemplateProjection contains positive inputs and output identities from unrendered documents.
type TemplateProjection struct {
	Inputs  []TemplateInput
	Targets []ProducerTarget
	Objects []BaseManifest
}

// TemplateInput binds one emitted reader to its concrete input reference.
type TemplateInput struct {
	Reader    NamedResource
	Reference Reference
}

// ProducerProjections shares immutable projection facts across concurrent plans.
type ProducerProjections struct {
	locks *keylock.KeyMap[string]
	// The map MUST stay indirect so borrowing the cache does not retain traversal scratch.
	facts *sync.Map
}

// NewProducerProjections constructs a cache owned by one planning invocation.
func NewProducerProjections() *ProducerProjections {
	return &ProducerProjections{locks: keylock.New[string](), facts: new(sync.Map)}
}

// Project derives source-free facts once per producer content.
func (p *ProducerProjections) Project(ctx context.Context, object BaseManifest) (TemplateProjection, error) {
	resourceSet, ok := object.(*ResourceSet)
	if !ok {
		return TemplateProjection{}, nil
	}
	if p == nil {
		return resourceSet.templateProjection(), nil
	}
	key := Fingerprint(resourceSet)
	if key == "" {
		return resourceSet.templateProjection(), nil
	}
	if projection, ok := p.facts.Load(key); ok {
		return projection.(TemplateProjection), nil
	}
	release, err := p.locks.Acquire(ctx, key)
	if err != nil {
		return TemplateProjection{}, err
	}
	defer release()
	if projection, ok := p.facts.Load(key); ok {
		return projection.(TemplateProjection), nil
	}
	projection := resourceSet.templateProjection()
	p.facts.Store(key, projection)
	return projection, nil
}

// Clear releases cached facts and keys after every planning caller finishes.
func (p *ProducerProjections) Clear() {
	p.facts.Clear()
	p.locks = keylock.New[string]()
}

// ProjectProducer derives static emitted identities and inputs from a producer declaration.
func ProjectProducer(object BaseManifest) TemplateProjection {
	if resourceSet, ok := object.(*ResourceSet); ok {
		return resourceSet.templateProjection()
	}
	return TemplateProjection{}
}

// ProjectTemplate derives concrete references and output identities through the upstream YAML reader.
// Unknown template operands grant no positive reference evidence.
func ProjectTemplate(data []byte, namespace string) TemplateProjection {
	var projection TemplateProjection
	if len(data) == 0 {
		return projection
	}
	if bytes.Contains(data, []byte("{{")) {
		compiled, err := template.New("static").Option("missingkey=error").Parse(string(data))
		if err == nil {
			var output bytes.Buffer
			if compiled.Execute(&output, map[string]any{}) == nil {
				data = output.Bytes()
			}
		}
	}
	reader := k8syaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	for {
		document, err := reader.Read()
		if err != nil {
			return projection
		}
		node, err := kyaml.Parse(string(document))
		if err != nil || kyaml.IsMissingOrNull(node) {
			continue
		}
		if _, err := node.GetMeta(); err != nil && !errors.Is(err, kyaml.ErrMissingMetadata) {
			continue
		}
		doc, err := node.Map()
		if err != nil {
			continue
		}
		docs := []map[string]any{doc}
		if strings.HasSuffix(DocKind(doc), "List") {
			nodes, err := resource.NewFactory(nil).RNodesFromBytes(document)
			if err != nil {
				continue
			}
			docs = nil
			for _, node := range nodes {
				doc, err := node.Map()
				if err == nil {
					docs = append(docs, doc)
				}
			}
		}
		for _, doc := range docs {
			_, authoredNamespace := DocMetadata(doc)
			object, _, _, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: namespace})
			if err != nil || object == nil {
				continue
			}
			id := object.Named()
			if concreteTemplateOperand(id.Name) && concreteTemplateOperand(id.Namespace) {
				projection.Targets = append(projection.Targets, ProducerTarget{NamedResource: id})
				projection.Objects = append(projection.Objects, object)
				projection.Targets = append(projection.Targets, ProducerTargets(object)...)
			}
			nested := ProjectProducer(object)
			projection.Inputs = append(projection.Inputs, nested.Inputs...)
			projection.Targets = append(projection.Targets, nested.Targets...)
			projection.Objects = append(projection.Objects, nested.Objects...)
			for ref := range References(object, ReferenceRelations) {
				if !ref.ContentInput() || ref.Ordering || !concreteTemplateOperand(ref.Name) || !concreteTemplateOperand(ref.Namespace) {
					continue
				}
				ref.NamespaceDefault = ref.NamespaceDefault && authoredNamespace == ""
				ref.Gate, ref.ArtifactStages = 0, 0
				ref.Artifact, ref.ChartSource = false, false
				if ref.Use == "source" {
					for root := range SourceRoots(object) {
						if concreteTemplateOperand(root) {
							ref.Files = append(ref.Files, root)
							ref.Subtree = true
						}
					}
				}
				projection.Inputs = append(projection.Inputs, TemplateInput{Reader: id, Reference: ref})
			}
		}
	}
}

func concreteTemplateOperand(value string) bool {
	return !strings.Contains(value, "{{") && !strings.Contains(value, "<<") && !HasEnvsubstReference(value)
}

func (r *ResourceSet) templateProjection() TemplateProjection {
	var projection TemplateProjection
	if len(r.Resources) == 0 && r.ResourcesTemplate == "" && len(r.Steps) == 0 {
		return projection
	}
	var inputs []map[string]any
	inputsKnown := false
	visit := func(data []byte, jsonDocument bool) {
		next := ProjectTemplate(data, r.Namespace)
		projection.Inputs = append(projection.Inputs, next.Inputs...)
		projection.Targets = append(projection.Targets, next.Targets...)
		projection.Objects = append(projection.Objects, next.Objects...)
		if jsonDocument {
			converted, err := yaml.JSONToYAML(data)
			if err != nil {
				return
			}
			data = converted
		}
		if !strings.Contains(string(data), "<<") {
			return
		}
		if !inputsKnown {
			inputsKnown = true
			var err error
			inputs, err = resourcesetinput.Combine([]resourcesetinput.Group{resourcesetinput.Inline(r.Name, r.Namespace, r.Inputs)}, r.InputStrategy)
			if err != nil {
				return
			}
			if len(inputs) == 0 {
				inputs = []map[string]any{nil}
			}
		}
		template, err := resourcesetinput.Parse(string(data))
		if err != nil {
			return
		}
		for _, input := range inputs {
			rendered, err := template.Execute(input)
			if err != nil {
				continue
			}
			next := ProjectTemplate(rendered, r.Namespace)
			projection.Inputs = append(projection.Inputs, next.Inputs...)
			projection.Targets = append(projection.Targets, next.Targets...)
			projection.Objects = append(projection.Objects, next.Objects...)
		}
	}
	for _, resource := range r.Resources {
		visit(resource.Raw, true)
	}
	visit([]byte(r.ResourcesTemplate), false)
	for _, step := range r.Steps {
		for _, resource := range step.Resources {
			visit(resource.Raw, true)
		}
		visit([]byte(step.ResourcesTemplate), false)
	}
	return projection
}
