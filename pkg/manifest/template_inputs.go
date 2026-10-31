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

	k8syaml "k8s.io/apimachinery/pkg/util/yaml"

	"sigs.k8s.io/kustomize/api/resource"
	kyaml "sigs.k8s.io/kustomize/kyaml/yaml"
	"sigs.k8s.io/yaml"

	yamlv3 "go.yaml.in/yaml/v3"

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

// ProjectTemplate derives immutable references and identities once per template content and namespace.
func (p *ProducerProjections) ProjectTemplate(ctx context.Context, data []byte, namespace string) (TemplateProjection, error) {
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

func (p *ProducerProjections) project(ctx context.Context, key string, build func() TemplateProjection) (TemplateProjection, error) {
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
		return projection.(TemplateProjection), nil
	}
	release, err := cache.locks.Acquire(ctx, key)
	if err != nil {
		return TemplateProjection{}, err
	}
	defer release()
	if projection, ok := cache.facts.Load(key); ok {
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
			if err := yamlv3.Unmarshal(document, &fields); err != nil {
				continue
			}
			if !strings.HasSuffix(fields.Kind, "List") {
				doc := map[string]any{"apiVersion": fields.APIVersion, "kind": fields.Kind,
					"metadata": map[string]any{"name": fields.Metadata.Name, "namespace": fields.Metadata.Namespace}, "spec": fields.Spec}
				object, _, _, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: namespace})
				if err == nil && object != nil {
					yield(object, fields.Metadata.Namespace)
				}
				continue
			}
		}
		node, err := kyaml.Parse(string(document))
		if err != nil || kyaml.IsMissingOrNull(node) {
			continue
		}
		if _, err := node.GetMeta(); err != nil && !errors.Is(err, kyaml.ErrMissingMetadata) {
			continue
		}
		var header struct {
			Kind string `yaml:"kind"`
		}
		if err := node.YNode().Decode(&header); err != nil {
			continue
		}
		nodes := []*kyaml.RNode{node}
		if strings.HasSuffix(header.Kind, "List") {
			nodes, err = resource.NewFactory(nil).RNodesFromBytes(document)
			if err != nil {
				continue
			}
		}
		for _, node := range nodes {
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
				if err := node.YNode().Decode(&fields); err != nil {
					continue
				}
				doc = map[string]any{
					"apiVersion": fields.APIVersion, "kind": fields.Kind,
					"metadata": map[string]any{"name": fields.Metadata.Name, "namespace": fields.Metadata.Namespace},
					"spec":     fields.Spec,
				}
			} else {
				doc, err = node.Map()
				if err != nil {
					continue
				}
			}
			_, authoredNamespace := DocMetadata(doc)
			object, _, _, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: namespace})
			if err != nil || object == nil {
				continue
			}
			yield(object, authoredNamespace)
		}
	}
}

func (p *TemplateProjection) addObject(object BaseManifest, authoredNamespace string, retain bool) {
	id := object.Named()
	if concreteTemplateOperand(id.Name) && concreteTemplateOperand(id.Namespace) {
		p.Targets = append(p.Targets, ProducerTarget{NamedResource: id})
		if retain {
			p.Objects = append(p.Objects, object)
		}
		p.Targets = append(p.Targets, ProducerTargets(object)...)
	}
	nested := ProjectProducer(object)
	p.Inputs = append(p.Inputs, nested.Inputs...)
	p.Targets = append(p.Targets, nested.Targets...)
	if retain {
		p.Objects = append(p.Objects, nested.Objects...)
	}
	for ref := range References(object, ReferenceRelations) {
		if !ref.ContentInput() || ref.Ordering || !concreteTemplateOperand(ref.Name) || !concreteTemplateOperand(ref.Namespace) {
			continue
		}
		ref.NamespaceDefault = ref.NamespaceDefault && authoredNamespace == ""
		ref.Gate, ref.ArtifactStages = 0, 0
		ref.Artifact, ref.ChartSource = false, false
		unresolved := false
		if ref.Use == "source" {
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

func concreteTemplateOperand(value string) bool {
	return !strings.Contains(value, "{{") && !strings.Contains(value, "<<") && !HasEnvsubstReference(value)
}

func (r *ResourceSet) templateProjection() TemplateProjection {
	var projection TemplateProjection
	if len(r.Resources) == 0 && r.ResourcesTemplate == "" && len(r.Steps) == 0 {
		return projection
	}
	type child struct {
		object    BaseManifest
		namespace string
		evaluated bool
	}
	var children []child
	winners := map[NamedResource]int{}
	collect := func(data []byte, evaluated bool) {
		projectTemplate(data, r.Namespace, false, func(object BaseManifest, namespace string) {
			id := object.Named()
			if i, known := winners[id]; known {
				if evaluated && !children[i].evaluated {
					children[i] = child{object, namespace, true}
				}
				return
			}
			winners[id] = len(children)
			children = append(children, child{object, namespace, evaluated})
		})
	}
	var inputs []map[string]any
	inputsKnown := false
	visit := func(data []byte, jsonDocument bool) {
		collect(data, false)
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
			collect(rendered, true)
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
	for _, child := range children {
		projection.addObject(child.object, child.namespace, true)
	}
	return projection
}
