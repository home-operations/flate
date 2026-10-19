package manifest

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	kjson "k8s.io/apimachinery/pkg/util/json"
)

// Kustomization is the Flux Kustomization CR. It bundles the path of a
// local kustomize tree together with the in-cluster materials it produces
// (HelmReleases, HelmRepositories, ConfigMaps, ...).
type Kustomization struct {
	Name      string `json:"name"                yaml:"name"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`

	kustomizev1.KustomizationSpec `json:",inline" yaml:",inline"`

	shape      *kustomizationShape
	projection *atomic.Pointer[kustomizationProjection]

	Labels      map[string]string `json:"-" yaml:"-"`
	Annotations map[string]string `json:"-" yaml:"-"`
}

// GetLabels returns the Kustomization's metadata.labels. Implements
// the shared accessor pkg/depwait projectObject uses for CEL exposure.
func (k *Kustomization) GetLabels() map[string]string { return k.Labels }

// GetAnnotations returns the Kustomization's metadata.annotations.
func (k *Kustomization) GetAnnotations() map[string]string { return k.Annotations }

// Suspended reports whether reconciliation is paused.
func (k *Kustomization) Suspended() bool { return k.Suspend }

// Named identifies the Kustomization.
func (k *Kustomization) Named() NamedResource {
	return NamedResource{Group: kindTable[kindKustomization].Group, Kind: KindKustomization, Namespace: k.Namespace, Name: k.Name}
}

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (k *Kustomization) WithNamespace(namespace string) BaseManifest {
	out := *k
	out.Namespace = namespace
	return &out
}

// Clone deep-copies mutable fields; store-owned specs MUST remain immutable.
func (k *Kustomization) Clone() *Kustomization {
	out := *k
	k.DeepCopyInto(&out.KustomizationSpec)
	if k.shape != nil {
		shape := *k.shape
		shape.Fields = DeepCopyMap(shape.Fields)
		shape.NullElements = maps.Clone(shape.NullElements)
		for field, positions := range shape.NullElements {
			shape.NullElements[field] = slices.Clone(positions)
		}
		shape.NullCommonLabels = slices.Clone(shape.NullCommonLabels)
		shape.NullCommonAnnotations = slices.Clone(shape.NullCommonAnnotations)
		out.shape = &shape
	}
	out.Labels = maps.Clone(k.Labels)
	out.Annotations = maps.Clone(k.Annotations)
	return &out
}

func parseKustomization(doc map[string]any) (*Kustomization, error) {
	var cr kustomizev1.Kustomization
	if err := decodeCR(doc, &cr, "Kustomization", FluxKustomizeDomain); err != nil {
		return nil, err
	}
	cr.Spec.Path = resolveEnvsubstDefaults(cr.Spec.Path)
	cr.Spec.SourceRef.Name = resolveEnvsubstDefaults(cr.Spec.SourceRef.Name)
	cr.Spec.SourceRef.Namespace = resolveEnvsubstDefaults(cr.Spec.SourceRef.Namespace)
	for i := range cr.Spec.DependsOn {
		cr.Spec.DependsOn[i].Namespace = resolveEnvsubstDefaults(cr.Spec.DependsOn[i].Namespace)
	}
	// sourceRef validation catches the truncated-YAML case where
	// `name:` (no value, e.g. file ends mid-mapping) decodes silently
	// as empty string. Without this, the failure surfaces as a
	// misleading "kustomization path does not exist" 30 lines later
	// in the reconcile flow.
	if cr.Spec.SourceRef.Kind != "" && cr.Spec.SourceRef.Name == "" {
		return nil, inputf("Kustomization %s/%s: spec.sourceRef.kind=%q but spec.sourceRef.name is empty (check for truncated YAML)",
			cr.Namespace, cr.Name, cr.Spec.SourceRef.Kind)
	}

	for _, dep := range cr.Spec.DependsOn {
		if dep.Name == "" {
			return nil, inputf("Kustomization missing dependsOn.name")
		}
	}

	out := &Kustomization{
		Name:              cr.Name,
		Namespace:         cr.Namespace,
		KustomizationSpec: cr.Spec,
		Labels:            cr.Labels,
		Annotations:       cr.Annotations,
		projection:        new(atomic.Pointer[kustomizationProjection]),
	}
	spec, _ := doc["spec"].(map[string]any)
	var shape *kustomizationShape
	for _, field := range []string{"targetNamespace", "namePrefix", "nameSuffix", "patches", "components", "images", "buildMetadata", "patchesStrategicMerge", "patchesJson6902"} {
		value, exists := spec[field]
		if !exists {
			continue
		}
		override := field == "targetNamespace" || field == "namePrefix" || field == "nameSuffix"
		generatorOnly := field == "patchesStrategicMerge" || field == "patchesJson6902"
		if value == nil || (override && value == "") || generatorOnly {
			if shape == nil {
				shape = &kustomizationShape{}
			}
			if shape.Fields == nil {
				shape.Fields = make(map[string]any)
			}
			shape.Fields[field] = deepCopyValue(value)
		}
		if field == "patches" || field == "images" || field == "components" || field == "buildMetadata" {
			items, _ := value.([]any)
			for i, item := range items {
				if item == nil {
					if shape == nil {
						shape = &kustomizationShape{}
					}
					if shape.NullElements == nil {
						shape.NullElements = make(map[string][]int)
					}
					shape.NullElements[field] = append(shape.NullElements[field], i)
				}
			}
		}
	}
	metadata, _ := spec["commonMetadata"].(map[string]any)
	for _, field := range []string{"labels", "annotations"} {
		values, _ := metadata[field].(map[string]any)
		var keys []string
		for key, value := range values {
			if value == nil {
				keys = append(keys, key)
			}
		}
		if len(keys) == 0 {
			continue
		}
		if shape == nil {
			shape = &kustomizationShape{}
		}
		slices.Sort(keys)
		if field == "labels" {
			shape.NullCommonLabels = keys
		} else {
			shape.NullCommonAnnotations = keys
		}
	}
	out.shape = shape
	return out, nil
}

// SourceID resolves the authored source against the effective namespace.
func (k *Kustomization) SourceID() NamedResource {
	return Resolve(NamedResource{Group: referenceGroup(k.SourceRef.Kind, k.SourceRef.APIVersion), Kind: k.SourceRef.Kind, Namespace: k.SourceRef.Namespace, Name: k.SourceRef.Name}, "", k.Namespace)
}

// IsFragment identifies patch fragments without a render path or source name.
func (k *Kustomization) IsFragment() bool { return k.Path == "" && k.SourceRef.Name == "" }

func (k *Kustomization) mapDependsOn(name, namespace func(string) string) *Kustomization {
	var out *Kustomization
	for i, dep := range k.DependsOn {
		resolvedName := name(dep.Name)
		resolvedNamespace := dep.Namespace
		if namespace != nil {
			resolvedNamespace = namespace(dep.Namespace)
		}
		if resolvedName == dep.Name && resolvedNamespace == dep.Namespace {
			continue
		}
		if out == nil {
			out = k.Clone()
		}
		out.DependsOn[i].Name = resolvedName
		out.DependsOn[i].Namespace = resolvedNamespace
	}
	if out == nil {
		return k
	}
	return out
}

func (k *Kustomization) references(yield func(Reference) bool) {
	if k.SourceRef.Kind != "" || k.SourceRef.Name != "" {
		src := k.SourceID()
		if !yield(Reference{
			NamedResource: src,
			Use:           "source", Content: src.Kind != "" && src.Name != "",
		}) {
			return
		}
	}
	for i := range k.DependsOn {
		dep := &k.DependsOn[i]
		if !yield(Reference{
			NamedResource: Resolve(NamedResource{Group: kindTable[kindKustomization].Group, Kind: KindKustomization, Namespace: dep.Namespace, Name: dep.Name}, "", k.Namespace),
			Use:           "dependsOn", Position: i, ReadyExpr: dep.ReadyExpr,
		}) {
			return
		}
	}
	if k.PostBuild == nil {
		return
	}
	for i, ref := range k.PostBuild.SubstituteFrom {
		if ref.Kind != KindConfigMap && ref.Kind != KindSecret {
			continue
		}
		content := !ref.Optional && ref.Name != ""
		if !yield(Reference{
			NamedResource: Resolve(NamedResource{Kind: ref.Kind, Name: ref.Name}, "", k.Namespace),
			Use:           "substituteFrom", Position: i, Optional: ref.Optional, Content: content,
		}) {
			return
		}
	}
}

func (k *Kustomization) kind() KindInfo { return kindTable[kindKustomization].KindInfo }

// Content returns prepared render inputs without emission labels or annotations.
func (k *Kustomization) Content(source any) any {
	return struct {
		Source          any
		Name, Namespace string
		Spec            *kustomizev1.KustomizationSpec
		Shape           *kustomizationShape
	}{source, k.Name, k.Namespace, &k.KustomizationSpec, k.effectiveShape()}
}

type kustomizationShape struct {
	Fields                                  map[string]any
	NullElements                            map[string][]int
	NullCommonLabels, NullCommonAnnotations []string
}

// Authored shapes apply only while the effective typed value remains zero.
func (k *Kustomization) effectiveShape() *kustomizationShape {
	if k.shape == nil {
		return nil
	}
	var shape kustomizationShape
	for field, value := range k.shape.Fields {
		var empty bool
		switch field {
		case "targetNamespace":
			empty = k.TargetNamespace == ""
		case "namePrefix":
			empty = k.NamePrefix == ""
		case "nameSuffix":
			empty = k.NameSuffix == ""
		case "patches":
			empty = len(k.Patches) == 0
		case "images":
			empty = len(k.Images) == 0
		case "components":
			empty = len(k.Components) == 0
		case "buildMetadata":
			empty = len(k.BuildMetadata) == 0
		case "patchesStrategicMerge", "patchesJson6902":
			empty = true
		}
		if empty {
			if shape.Fields == nil {
				shape.Fields = make(map[string]any)
			}
			shape.Fields[field] = value
		}
	}
	for field, positions := range k.shape.NullElements {
		for _, i := range positions {
			var empty bool
			switch field {
			case "patches":
				empty = i < len(k.Patches) && k.Patches[i].Patch == "" && k.Patches[i].Target == nil
			case "images":
				empty = i < len(k.Images) && k.Images[i].Name == "" && k.Images[i].NewName == "" && k.Images[i].NewTag == "" && k.Images[i].Digest == ""
			case "components":
				empty = i < len(k.Components) && k.Components[i] == ""
			case "buildMetadata":
				empty = i < len(k.BuildMetadata) && k.BuildMetadata[i] == ""
			}
			if empty {
				if shape.NullElements == nil {
					shape.NullElements = make(map[string][]int)
				}
				shape.NullElements[field] = append(shape.NullElements[field], i)
			}
		}
	}
	if k.CommonMetadata != nil {
		for _, key := range k.shape.NullCommonLabels {
			if value, ok := k.CommonMetadata.Labels[key]; ok && value == "" {
				shape.NullCommonLabels = append(shape.NullCommonLabels, key)
			}
		}
		for _, key := range k.shape.NullCommonAnnotations {
			if value, ok := k.CommonMetadata.Annotations[key]; ok && value == "" {
				shape.NullCommonAnnotations = append(shape.NullCommonAnnotations, key)
			}
		}
	}
	if len(shape.Fields)+len(shape.NullElements)+len(shape.NullCommonLabels)+len(shape.NullCommonAnnotations) == 0 {
		return nil
	}
	return &shape
}

type kustomizationProjection struct {
	raw      []byte
	document map[string]any
}

type kustomizationEncoder struct {
	buffer  bytes.Buffer
	encoder *jsontext.Encoder
	object  struct {
		kustomizev1.Kustomization
		Shape *kustomizationShape `json:"_flateShape,omitempty"`
	}
}

var kustomizationEncoders = sync.Pool{New: func() any {
	state := new(kustomizationEncoder)
	state.encoder = jsontext.NewEncoder(&state.buffer)
	return state
}}

// RenderDocument derives a Flux generator document; callers MUST keep it immutable.
func (k *Kustomization) RenderDocument() (map[string]any, error) {
	shape := k.effectiveShape()
	state := kustomizationEncoders.Get().(*kustomizationEncoder)
	state.buffer.Reset()
	state.encoder.Reset(&state.buffer)
	state.object.Kustomization = kustomizev1.Kustomization{
		APIVersion: FluxKustomizeDomain + "/v1", Kind: KindKustomization,
		Name: k.Name, Namespace: k.Namespace, Labels: k.Labels, Annotations: k.Annotations,
		Spec: k.KustomizationSpec,
	}
	state.object.Shape = shape
	defer func() {
		state.object.Kustomization = kustomizev1.Kustomization{}
		state.object.Shape = nil
		kustomizationEncoders.Put(state)
	}()
	err := jsonv2.MarshalEncode(state.encoder, &state.object, json.DefaultOptionsV1())
	raw := state.buffer.Bytes()
	if err != nil {
		return nil, inputf("project Kustomization spec: %w", err)
	}
	if k.projection != nil {
		if cached := k.projection.Load(); cached != nil && bytes.Equal(raw, cached.raw) {
			return cached.document, nil
		}
	}
	var doc map[string]any
	if err := kjson.Unmarshal(raw, &doc); err != nil {
		return nil, inputf("project Kustomization spec: %w", err)
	}
	delete(doc, "_flateShape")
	spec := doc["spec"].(map[string]any)
	if shape != nil {
		for field, value := range shape.Fields {
			spec[field] = deepCopyValue(value)
		}
		for field, positions := range shape.NullElements {
			items := spec[field].([]any)
			for _, i := range positions {
				items[i] = nil
			}
		}
		metadata, _ := spec["commonMetadata"].(map[string]any)
		for field, keys := range map[string][]string{"labels": shape.NullCommonLabels, "annotations": shape.NullCommonAnnotations} {
			values, _ := metadata[field].(map[string]any)
			for _, key := range keys {
				values[key] = nil
			}
		}
	}
	if k.projection != nil {
		k.projection.Store(&kustomizationProjection{raw: bytes.Clone(raw), document: doc})
	}
	return doc, nil
}
