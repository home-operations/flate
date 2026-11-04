package manifest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"

	yamlv4 "go.yaml.in/yaml/v4"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/kustomize/api/resource"
	kyaml "sigs.k8s.io/kustomize/kyaml/yaml"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/internal/resourcesetinput"
)

// TemplateProjection contains positive inputs, output identities, and applications from unrendered documents.
type TemplateProjection struct {
	Inputs  []TemplateInput
	Targets []ProducerTarget
	Objects []BaseManifest
}

// TemplateInput binds one emitted reader to its concrete input reference.
type TemplateInput struct {
	Reader         NamedResource
	Reference      Reference
	UnresolvedPath bool
}

// ProducerProjections shares immutable projection facts across concurrent plans.
type ProducerProjections struct {
	// Backing state MUST stay indirect so sharing projections does not retain traversal scratch.
	cache atomic.Pointer[producerProjectionCache]
}

type producerProjectionCache struct {
	locks *keylock.KeyMap[string]
	facts sync.Map
}

// NewProducerProjections constructs a cache owned by one planning invocation.
func NewProducerProjections() *ProducerProjections {
	return &ProducerProjections{}
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
	return p.project(ctx, Fingerprint(resourceSet), resourceSet.templateProjection)
}

// ProjectTemplate derives immutable references, identities, and applications once per template content and namespace.
func (p *ProducerProjections) ProjectTemplate(
	ctx context.Context,
	data []byte,
	namespace string,
) (TemplateProjection, error) {
	build := func() TemplateProjection {
		var projection TemplateProjection
		projectTemplate(data, namespace, true, func(object BaseManifest, authoredNamespace string) {
			projection.addObject(object, authoredNamespace, false)
		})
		return projection
	}
	if p == nil {
		return build(), nil
	}
	key := Fingerprint(struct {
		Template  []byte
		Namespace string
	}{data, namespace})
	return p.project(ctx, key, build)
}

func (p *ProducerProjections) project(
	ctx context.Context,
	key string,
	build func() TemplateProjection,
) (TemplateProjection, error) {
	if key == "" {
		return build(), nil
	}
	cache := p.cache.Load()
	if cache == nil {
		cache = &producerProjectionCache{locks: keylock.New[string]()}
		if !p.cache.CompareAndSwap(nil, cache) {
			cache = p.cache.Load()
		}
	}
	if projection, ok := cache.facts.Load(key); ok {
		//nolint:errcheck // The owning path guarantees the concrete type and panics on a broken invariant.
		return projection.(TemplateProjection), nil
	}
	release, err := cache.locks.Acquire(ctx, key)
	if err != nil {
		return TemplateProjection{}, err
	}
	defer release()
	if projection, ok := cache.facts.Load(key); ok {
		//nolint:errcheck // The owning path guarantees the concrete type and panics on a broken invariant.
		return projection.(TemplateProjection), nil
	}
	projection := build()
	cache.facts.Store(key, projection)
	return projection, nil
}

// Clear releases cached facts and keys after every planning caller finishes.
func (p *ProducerProjections) Clear() {
	p.cache.Store(nil)
}

// ProjectProducer derives static emitted identities and inputs from a producer declaration.
func ProjectProducer(object BaseManifest) TemplateProjection {
	if resourceSet, ok := object.(*ResourceSet); ok {
		return resourceSet.templateProjection()
	}
	return TemplateProjection{}
}

func projectTemplate(data []byte, namespace string, projected bool, yield func(BaseManifest, string)) {
	if len(data) == 0 {
		return
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
			return
		}
		projectTemplateDocument(document, namespace, projected, yield)
	}
}

func (p *TemplateProjection) addObject(object BaseManifest, authoredNamespace string, retain bool) {
	keep := func(object BaseManifest) {
		_, application := ApplicationFor(object)
		if retain || application {
			p.Objects = append(p.Objects, object)
		}
	}
	id := object.Named()
	if concreteTemplateOperand(id.Name) && concreteTemplateOperand(id.Namespace) {
		p.Targets = append(p.Targets, ProducerTarget{NamedResource: id})
		keep(object)
		p.Targets = append(p.Targets, ProducerTargets(object)...)
	}
	nested := ProjectProducer(object)
	p.Inputs = append(p.Inputs, nested.Inputs...)
	p.Targets = append(p.Targets, nested.Targets...)
	for _, object := range nested.Objects {
		keep(object)
	}
	p.addObjectInputs(object, authoredNamespace)
}

func concreteTemplateOperand(value string) bool {
	return !strings.Contains(value, "{{") && !strings.Contains(value, "<<") && !HasEnvsubstReference(value)
}

func (r *ResourceSet) templateProjection() TemplateProjection {
	var projection TemplateProjection
	if len(r.Resources) == 0 && r.ResourcesTemplate == "" && len(r.Steps) == 0 {
		return projection
	}
	templates := resourceSetTemplates{winners: map[NamedResource]int{}}
	for _, resource := range r.Resources {
		templates.visit(r, resource.Raw, true)
	}
	templates.visit(r, []byte(r.ResourcesTemplate), false)
	for _, step := range r.Steps {
		for _, resource := range step.Resources {
			templates.visit(r, resource.Raw, true)
		}
		templates.visit(r, []byte(step.ResourcesTemplate), false)
	}
	for _, child := range templates.children {
		projection.addObject(child.object, child.namespace, true)
	}
	return projection
}

func projectTemplateDocument(document []byte, namespace string, projected bool, yield func(BaseManifest, string)) {
	var fields struct {
		APIVersion string `yaml:"apiVersion"`
		Kind       string `yaml:"kind"`
		Metadata   struct {
			Name      string `yaml:"name"`
			Namespace string `yaml:"namespace"`
		} `yaml:"metadata"`
		Spec map[string]any `yaml:"spec"`
	}
	if projected {
		if unmarshalErr := yamlv4.Unmarshal(document, &fields); unmarshalErr != nil {
			return
		}
		if !strings.HasSuffix(fields.Kind, "List") {
			doc := map[string]any{
				apiVersionKey: fields.APIVersion,
				kindKey:       fields.Kind,
				metadataKey: map[string]any{
					nameKey:      fields.Metadata.Name,
					namespaceKey: fields.Metadata.Namespace,
				},
				specKey: fields.Spec,
			}
			parsed, parseDocErr := ParseDoc(doc, ParseDocOptions{DefaultNamespace: namespace})
			object := parsed.Object
			if parseDocErr == nil && object != nil {
				yield(object, fields.Metadata.Namespace)
			}
			return
		}
	}
	node, err := kyaml.Parse(string(document))
	if err != nil || kyaml.IsMissingOrNull(node) {
		return
	}
	if _, getMetaErr := node.GetMeta(); getMetaErr != nil && !errors.Is(getMetaErr, kyaml.ErrMissingMetadata) {
		return
	}
	var header struct {
		Kind string `yaml:"kind"`
	}
	if decodeErr := node.YNode().Decode(&header); decodeErr != nil {
		return
	}
	nodes := []*kyaml.RNode{node}
	if strings.HasSuffix(header.Kind, "List") {
		nodes, err = resource.NewFactory(nil).RNodesFromBytes(document)
		if err != nil {
			return
		}
	}
	for _, node := range nodes {
		projectTemplateNode(node, namespace, projected, yield)
	}
}

func projectTemplateNode(node *kyaml.RNode, namespace string, projected bool, yield func(BaseManifest, string)) {
	var err error
	var doc map[string]any
	if projected {
		var fields struct {
			APIVersion string `yaml:"apiVersion"`
			Kind       string `yaml:"kind"`
			Metadata   struct {
				Name      string `yaml:"name"`
				Namespace string `yaml:"namespace"`
			} `yaml:"metadata"`
			Spec map[string]any `yaml:"spec"`
		}
		if decodeErr2 := node.YNode().Decode(&fields); decodeErr2 != nil {
			return
		}
		doc = map[string]any{
			apiVersionKey: fields.APIVersion, kindKey: fields.Kind,
			metadataKey: map[string]any{nameKey: fields.Metadata.Name, namespaceKey: fields.Metadata.Namespace},
			specKey:     fields.Spec,
		}
	} else {
		doc, err = node.Map()
		if err != nil {
			return
		}
	}
	metadata := DocMetadata(doc)
	authoredNamespace := metadata.Namespace
	parsed2, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: namespace})
	object := parsed2.Object
	if err != nil || object == nil {
		return
	}
	yield(object, authoredNamespace)
}

type projectedTemplateChild struct {
	object    BaseManifest
	namespace string
	evaluated bool
}

type resourceSetTemplates struct {
	children    []projectedTemplateChild
	winners     map[NamedResource]int
	inputs      []map[string]any
	inputsKnown bool
}

func (p *resourceSetTemplates) collect(data []byte, defaultNamespace string, evaluated bool) {
	projectTemplate(data, defaultNamespace, false, func(object BaseManifest, namespace string) {
		id := object.Named()
		if i, known := p.winners[id]; known {
			if evaluated && !p.children[i].evaluated {
				p.children[i] = projectedTemplateChild{object, namespace, true}
			}
			return
		}
		p.winners[id] = len(p.children)
		p.children = append(p.children, projectedTemplateChild{object, namespace, evaluated})
	})
}

func (p *resourceSetTemplates) visit(r *ResourceSet, data []byte, jsonDocument bool) {
	p.collect(data, r.Namespace, false)
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
	if !p.inputsKnown {
		p.inputsKnown = true
		var err error
		p.inputs, err = resourcesetinput.Combine(
			[]resourcesetinput.Group{resourcesetinput.Inline(r.Name, r.Namespace, r.Inputs)},
			r.InputStrategy,
		)
		if err != nil {
			return
		}
		if len(p.inputs) == 0 {
			p.inputs = []map[string]any{nil}
		}
	}
	tmpl, err := resourcesetinput.Parse(string(data))
	if err != nil {
		return
	}
	for _, input := range p.inputs {
		rendered, err := tmpl.Execute(input)
		if err != nil {
			continue
		}
		p.collect(rendered, r.Namespace, true)
	}
}

func (p *TemplateProjection) addObjectInputs(object BaseManifest, authoredNamespace string) {
	id := object.Named()
	for ref := range References(object, ReferenceRelations) {
		if !ref.ContentInput() || ref.Ordering || !concreteTemplateOperand(ref.Name) ||
			!concreteTemplateOperand(ref.Namespace) {
			continue
		}
		ref.NamespaceDefault = ref.NamespaceDefault && authoredNamespace == ""
		ref.Gate, ref.ArtifactStages = 0, 0
		ref.Artifact, ref.ChartSource = false, false
		unresolved := false
		if ref.Use == sourceKey {
			for root := range SourceRoots(object) {
				if concreteTemplateOperand(root) {
					ref.Files = append(ref.Files, root)
					ref.Subtree = true
				} else {
					unresolved = true
				}
			}
		}
		p.Inputs = append(p.Inputs, TemplateInput{Reader: id, Reference: ref, UnresolvedPath: unresolved})
	}
}
