package manifest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	yamlv4 "go.yaml.in/yaml/v4"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/kustomize/api/resource"
	kyaml "sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/core/application"
	"github.com/home-operations/flate/internal/core/keylock"
	"github.com/home-operations/flate/internal/core/resourcesetinput"
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

// Project derives source-free facts from producer content.
func (p *ProducerProjections) Project(ctx context.Context, object BaseManifest) (TemplateProjection, error) {
	switch object := object.(type) {
	case *HelmRelease:
		return object.templateProjection(true), nil
	case *ResourceSet:
		if p == nil {
			return object.templateProjection(true), nil
		}
		return p.project(ctx, Fingerprint(object), func() TemplateProjection {
			return object.templateProjection(true)
		})
	default:
		return TemplateProjection{}, nil
	}
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
		//nolint:errcheck // project stores only TemplateProjection values in producerProjectionCache.facts.
		return projection.(TemplateProjection), nil
	}
	release, err := cache.locks.Acquire(ctx, key)
	if err != nil {
		return TemplateProjection{}, err
	}
	defer release()
	if projection, ok := cache.facts.Load(key); ok {
		//nolint:errcheck // project stores only TemplateProjection values in producerProjectionCache.facts.
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

// ProjectProducer derives static facts and screens complete producer inputs for entry applications.
func ProjectProducer(object BaseManifest, content iter.Seq[[]byte]) (TemplateProjection, bool) {
	var projection TemplateProjection
	if producer, ok := object.(interface{ templateProjection(bool) TemplateProjection }); ok {
		projection = producer.templateProjection(content == nil)
	}
	if producer, ok := object.(interface{ entryPrecondition(iter.Seq[[]byte]) bool }); ok {
		return projection, producer.entryPrecondition(content)
	}
	return projection, true
}

func (h *HelmRelease) entryPrecondition(content iter.Seq[[]byte]) bool {
	declaration, err := json.Marshal(h)
	return err != nil || application.Contains(declaration) || possibleApplication(content)
}

func (h *HelmRelease) templateProjection(authored bool) TemplateProjection {
	var projection TemplateProjection
	visitValuesSyncSpecs(h.Values, func(spec, sync map[string]any) {
		// Only a Git source can use the bootstrap tree. Helm must prove the kind from prepared values.
		if _, specified := sync[kindKey]; authored && !specified {
			spec, sync = maps.Clone(spec), maps.Clone(sync)
			sync[kindKey] = KindGitRepository
			spec["sync"] = sync
		}
		object := &RawObject{
			Kind: kindFluxInstance, APIVersion: FluxOperatorDomain + "/v1",
			Namespace: h.ReleaseNamespace(), Spec: spec,
		}
		source, err := object.syncSpec()
		if err != nil {
			return
		}
		incomplete := authored && len(h.ValuesFrom) != 0 && hasSyncLocation(source) && source.Path == ""
		if !hasSyncSource(source) && !incomplete {
			return
		}
		if authored || source.Name != "" || object.Namespace != "" {
			projection.Objects = append(projection.Objects, object.catalog(object.Named()))
		}
	})
	return projection
}

func visitValuesSyncSpecs(value any, yield func(map[string]any, map[string]any)) {
	switch value := value.(type) {
	case map[string]any:
		if source, valid := value["sync"].(map[string]any); valid {
			yield(value, source)
		}
		for _, key := range slices.Sorted(maps.Keys(value)) {
			visitValuesSyncSpecs(value[key], yield)
		}
	case []any:
		for _, item := range value {
			visitValuesSyncSpecs(item, yield)
		}
	}
}

func (r *ResourceSet) entryPrecondition(content iter.Seq[[]byte]) bool {
	inputs, err := json.Marshal(r.Inputs)
	if err != nil || application.Contains(inputs) || application.Contains([]byte(r.ResourcesTemplate)) {
		return true
	}
	for _, resource := range r.Resources {
		if application.Contains(resource.Raw) {
			return true
		}
	}
	for _, step := range r.Steps {
		if application.Contains([]byte(step.ResourcesTemplate)) {
			return true
		}
		for _, resource := range step.Resources {
			if application.Contains(resource.Raw) {
				return true
			}
		}
	}
	return possibleApplication(content)
}

func possibleApplication(content iter.Seq[[]byte]) bool {
	if content == nil {
		return true
	}
	for data := range content {
		if application.Contains(data) {
			return true
		}
	}
	return false
}

func projectTemplate(data []byte, namespace string, projected bool, yield func(BaseManifest, string)) bool {
	if len(data) == 0 {
		return false
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
	unresolved := false
	reader := k8syaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	for {
		document, err := reader.Read()
		if err != nil {
			return unresolved
		}
		known := false
		projectTemplateDocument(document, namespace, projected, func(object BaseManifest, authoredNamespace string) {
			known = true
			yield(object, authoredNamespace)
		})
		unresolved = unresolved || !known && bytes.Contains(document, []byte("<<"))
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
	nested, _ := ProjectProducer(object, nil)
	p.Inputs = append(p.Inputs, nested.Inputs...)
	p.Targets = append(p.Targets, nested.Targets...)
	for _, object := range nested.Objects {
		keep(object)
	}
	p.addObjectInputs(object, authoredNamespace, false)
}

func concreteTemplateOperand(value string) bool {
	return !strings.Contains(value, "{{") && !strings.Contains(value, "<<") && !HasEnvsubstReference(value)
}

func (r *ResourceSet) templateProjection(_ bool) TemplateProjection {
	var projection TemplateProjection
	if len(r.Resources) == 0 && r.ResourcesTemplate == "" && len(r.Steps) == 0 {
		return projection
	}
	docs, complete := r.evaluateTemplates()
	seen := make(map[NamedResource]struct{})
	for _, doc := range docs {
		parsed, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: r.Namespace})
		if err != nil || parsed.Object == nil {
			continue
		}
		object := parsed.Object
		id := object.Named()
		if _, duplicate := seen[id]; !duplicate {
			projection.addObject(object, DocMetadata(doc).Namespace, true)
			seen[id] = struct{}{}
		}
	}
	if complete {
		return projection
	}
	steps := r.Steps
	if len(steps) == 0 {
		steps = []fluxopv1.ResourceSetStep{{Resources: r.Resources, ResourcesTemplate: r.ResourcesTemplate}}
	}
	for i := range steps {
		for _, resource := range steps[i].Resources {
			if resource != nil {
				projection.addResourceSetDraft(resource.Raw, r.Namespace, seen)
			}
		}
		projection.addResourceSetDraft([]byte(steps[i].ResourcesTemplate), r.Namespace, seen)
	}
	return projection
}

func (r *ResourceSet) evaluateTemplates() ([]map[string]any, bool) {
	if resourcesetinput.ValidateSteps(&r.ResourceSetSpec) != nil {
		return nil, false
	}
	inputs, err := resourcesetinput.Combine(
		[]resourcesetinput.Group{resourcesetinput.Inline(r.Name, r.Namespace, r.Inputs)},
		r.InputStrategy,
	)
	if err != nil || len(inputs) == 0 && len(r.InputsFrom) != 0 {
		return nil, false
	}
	docs, err := resourcesetinput.Build(&r.ResourceSetSpec, inputs, true)
	return docs, err == nil && len(r.InputsFrom) == 0
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

func (p *TemplateProjection) addResourceSetDraft(data []byte, namespace string, seen map[NamedResource]struct{}) {
	dynamicKind := false
	unresolved := projectTemplate(data, namespace, false, func(object BaseManifest, authoredNamespace string) {
		id := object.Named()
		if raw, ok := object.(*RawObject); ok && !raw.hasLiteralTemplateKind() {
			dynamicKind = true
			return
		}
		if _, known := seen[id]; !known {
			p.addObject(object, authoredNamespace, true)
			seen[id] = struct{}{}
		} else {
			p.addObjectInputs(object, authoredNamespace, true)
		}
	})
	if dynamicKind || unresolved && !opaqueResourceSetDraft(data) {
		p.Inputs = append(p.Inputs, TemplateInput{UnresolvedPath: true})
	}
}

func opaqueResourceSetDraft(data []byte) bool {
	tmpl, err := resourcesetinput.Parse(string(data))
	if err != nil {
		return false
	}
	reader := k8syaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(tmpl.Draft())))
	found := false
	for {
		document, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return found
		}
		var doc map[string]any
		if err != nil || yamlv4.Unmarshal(document, &doc) != nil {
			return false
		}
		if doc == nil {
			continue
		}
		raw := RawObject{APIVersion: DocAPIVersion(doc), Kind: DocKind(doc)}
		if !raw.hasLiteralTemplateKind() || raw.draftMayReadSource() {
			return false
		}
		found = true
	}
}

func (r *RawObject) hasLiteralTemplateKind() bool {
	return r.APIVersion != "" && r.Kind != "" &&
		concreteTemplateOperand(r.APIVersion) && concreteTemplateOperand(r.Kind)
}

func (r *RawObject) draftMayReadSource() bool {
	if strings.HasSuffix(r.Kind, "List") {
		return true
	}
	id := r.Named()
	return id.KindInfo().Tracked || id.CanDeclareGeneratedEntry()
}

func (p *TemplateProjection) addObjectInputs(object BaseManifest, authoredNamespace string, pendingOnly bool) {
	id := object.Named()
	for ref := range References(object, ReferenceRelations) {
		if !ref.ContentInput() || ref.Ordering {
			continue
		}
		if !concreteTemplateOperand(ref.Name) || !concreteTemplateOperand(ref.Namespace) {
			if ref.BindsSource() {
				p.Inputs = append(p.Inputs, TemplateInput{Reader: id, UnresolvedPath: true})
			}
			continue
		}
		ref.NamespaceDefault = ref.NamespaceDefault && authoredNamespace == ""
		ref.Gate, ref.ArtifactStages = 0, 0
		ref.Artifact, ref.ChartSource = false, false
		unresolved := templateInputPaths(object, &ref)
		if !pendingOnly || unresolved {
			p.Inputs = append(p.Inputs, TemplateInput{Reader: id, Reference: ref, UnresolvedPath: unresolved})
		}
	}
}

func templateInputPaths(object BaseManifest, ref *Reference) bool {
	unresolved := !concreteTemplateOperand(ref.ChartPath)
	if unresolved {
		ref.ChartPath = ""
	}
	if slices.ContainsFunc(ref.Files, func(file string) bool { return !concreteTemplateOperand(file) }) {
		ref.Files = slices.DeleteFunc(slices.Clone(ref.Files), func(file string) bool {
			return !concreteTemplateOperand(file)
		})
		unresolved = true
	}
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
	return unresolved
}
