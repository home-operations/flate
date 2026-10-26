package manifest

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"maps"
	"reflect"
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

	projection *atomic.Pointer[kustomizationProjection]

	Labels      map[string]string `json:"-" yaml:"-"`
	Annotations map[string]string `json:"-" yaml:"-"`
}

// GetLabels returns the Kustomization's metadata.labels.
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

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (k *Kustomization) ProjectionCompatible(previous BaseManifest) bool {
	return k.TargetNamespace == previous.(*Kustomization).TargetNamespace
}

// Clone deep-copies mutable fields; store-owned specs MUST remain immutable.
func (k *Kustomization) Clone() *Kustomization {
	out := *k
	k.DeepCopyInto(&out.KustomizationSpec)
	out.Labels = maps.Clone(k.Labels)
	out.Annotations = maps.Clone(k.Annotations)
	return &out
}

func (k *Kustomization) equal(other *Kustomization) bool {
	if k == other {
		return true
	}
	return k != nil && other != nil && k.Name == other.Name && k.Namespace == other.Namespace &&
		reflect.DeepEqual(&k.KustomizationSpec, &other.KustomizationSpec) &&
		reflect.DeepEqual(k.Labels, other.Labels) && reflect.DeepEqual(k.Annotations, other.Annotations)
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

	return &Kustomization{
		Name:              cr.Name,
		Namespace:         cr.Namespace,
		KustomizationSpec: cr.Spec,
		Labels:            cr.Labels,
		Annotations:       cr.Annotations,
		projection:        new(atomic.Pointer[kustomizationProjection]),
	}, nil
}

// SourceID resolves the authored source against the effective namespace.
func (k *Kustomization) SourceID() NamedResource {
	return Resolve(NamedResource{Group: referenceGroup(k.SourceRef.Kind, k.SourceRef.APIVersion), Kind: k.SourceRef.Kind, Namespace: k.SourceRef.Namespace, Name: k.SourceRef.Name}, "", k.Namespace)
}

// IsFragment identifies patch fragments without a render path or source name.
func (k *Kustomization) IsFragment() bool { return k.Path == "" && k.SourceRef.Name == "" }

// SubstituteStrategy returns the upstream post-build substitution policy.
func (k *Kustomization) SubstituteStrategy() kustomizev1.SubstituteStrategy {
	return (kustomizev1.Kustomization{Spec: k.KustomizationSpec}).GetSubstituteStrategy()
}

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

func (k *Kustomization) references(yield func(Reference) bool, view ReferenceView) {
	authoredSource := k.SourceRef.Kind != "" || k.SourceRef.Name != ""
	implicitSource := view == ReferenceAll && k.Path != ""
	if authoredSource || implicitSource {
		src := k.SourceID()
		content := src.Kind != "" && src.Name != ""
		use := "source"
		if !content && k.Path != "" {
			src = BootstrapSourceID
			use = ""
		}
		if (view == ReferenceAll || use != "") && !yield(Reference{
			NamedResource: src,
			Use:           use, Content: content, Gate: GateInputs | GateArtifacts, Artifact: true,
			ArtifactStages:      GateInputs | GateArtifacts,
			UnknownArtifactRoot: src.Group == SourceDomain && src.Kind == KindOCIRepository,
		}) {
			return
		}
	}
	for i := range k.DependsOn {
		dep := &k.DependsOn[i]
		if !yield(Reference{
			NamedResource: Resolve(NamedResource{Group: kindTable[kindKustomization].Group, Kind: KindKustomization, Namespace: dep.Namespace, Name: dep.Name}, "", k.Namespace),
			Use:           "dependsOn", Position: i, ReadyExpr: referenceExpression(&dep.ReadyExpr), Gate: GateInputs, Ordering: true,
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
	}{source, k.Name, k.Namespace, &k.KustomizationSpec}
}

type kustomizationProjection struct {
	raw      []byte
	document map[string]any
}

type kustomizationEncoder struct {
	buffer  bytes.Buffer
	encoder *jsontext.Encoder
	object  kustomizev1.Kustomization
}

var kustomizationEncoders = sync.Pool{New: func() any {
	state := new(kustomizationEncoder)
	state.encoder = jsontext.NewEncoder(&state.buffer)
	return state
}}

// RenderDocument derives a Flux desired document; callers MUST keep it immutable.
func (k *Kustomization) RenderDocument() (map[string]any, error) {
	state := kustomizationEncoders.Get().(*kustomizationEncoder)
	state.buffer.Reset()
	state.encoder.Reset(&state.buffer)
	state.object = kustomizev1.Kustomization{
		APIVersion: FluxKustomizeDomain + "/v1", Kind: KindKustomization,
		Name: k.Name, Namespace: k.Namespace, Labels: k.Labels, Annotations: k.Annotations,
		Spec: k.KustomizationSpec,
	}
	defer func() {
		state.object = kustomizev1.Kustomization{}
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
	if k.projection != nil {
		k.projection.Store(&kustomizationProjection{raw: bytes.Clone(raw), document: doc})
	}
	return doc, nil
}
