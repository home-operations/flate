package manifest

import (
	"maps"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
)

// Kustomization is the Flux Kustomization CR. It bundles the path of a
// local kustomize tree together with the in-cluster materials it produces
// (HelmReleases, HelmRepositories, ConfigMaps, ...).
//
// The embedded kustomizev1.KustomizationSpec promotes Path, Suspend,
// TargetNamespace, Components, SourceRef, PostBuild, Patches, Images,
// CommonMetadata, NamePrefix, NameSuffix, Wait, Prune, Force,
// HealthChecks, etc. to the top level. Shadowed by flate-only fields
// where the projected shape differs from upstream (PostBuildSubstitute).
type Kustomization struct {
	Name      string `json:"name"                yaml:"name"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`

	kustomizev1.KustomizationSpec `json:",inline" yaml:",inline"`

	// Contents is the raw decoded YAML document. Retained because
	// RenderFlux still feeds it to fluxcd/pkg/kustomize.NewGenerator
	// as an unstructured.Unstructured.
	Contents map[string]any `json:"-" yaml:"-"`

	// PostBuildSubstitute holds the resolved substitution variables
	// (after evaluating spec.postBuild.substituteFrom against the
	// store). Shadows the upstream Spec.PostBuild.Substitute (which
	// holds the spec'd unresolved values).
	PostBuildSubstitute map[string]any `json:"-" yaml:"-"`

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
	out.Contents = DeepCopyMap(k.Contents)
	out.PostBuildSubstitute = maps.Clone(k.PostBuildSubstitute)
	out.Labels = maps.Clone(k.Labels)
	out.Annotations = maps.Clone(k.Annotations)
	return &out
}

// UpdatePostBuildSubstitutions merges the given map into the substitution
// table AND into the raw contents doc, mirroring upstream behavior so the
// raw document is consistent for serialization.
// Callers must not mutate subs after this call.
func (k *Kustomization) UpdatePostBuildSubstitutions(subs map[string]any) {
	if k.PostBuildSubstitute == nil {
		k.PostBuildSubstitute = subs
	} else {
		maps.Copy(k.PostBuildSubstitute, subs)
	}
	if k.Contents == nil {
		return
	}
	spec := childMap(k.Contents, "spec")
	post := childMap(spec, "postBuild")
	sub := childMap(post, "substitute")
	maps.Copy(sub, subs)
}

// childMap returns parent[key] as a map[string]any, lazily creating one
// when absent (or present as a non-map). Used by the Contents dual-write
// helpers to walk into spec.postBuild.substitute without re-spelling the
// type-assert-and-create dance at each level.
func childMap(parent map[string]any, key string) map[string]any {
	child, _ := parent[key].(map[string]any)
	if child == nil {
		child = make(map[string]any)
		parent[key] = child
	}
	return child
}

// SetTargetNamespace sets spec.targetNamespace on both the typed field
// and the raw Contents doc so RenderFlux (which feeds Contents to
// kustomize) and the typed readers stay consistent. Mirrors
// UpdatePostBuildSubstitutions' dual-write contract.
func (k *Kustomization) SetTargetNamespace(ns string) {
	k.TargetNamespace = ns
	if k.Contents == nil {
		return
	}
	childMap(k.Contents, "spec")["targetNamespace"] = ns
}

// parseKustomization decodes a Flux Kustomization CR via the
// kustomize-controller typed schema. The raw doc is retained in
// Contents because RenderFlux still feeds it to fluxcd/pkg/kustomize
// as an unstructured.Unstructured.
func parseKustomization(doc map[string]any) (*Kustomization, error) {
	var cr kustomizev1.Kustomization
	if err := decodeCR(doc, &cr, "Kustomization", FluxKustomizeDomain); err != nil {
		return nil, err
	}
	// Pre-resolve envsubst defaults so flate's path / dep resolution
	// matches what real Flux's postBuild.substitute would produce.
	// Bare ${VAR} (no default) is left for the postBuild step to
	// fill — only ${VAR:=default} and ${VAR:-default} collapse here.
	cr.Spec.Path = resolveEnvsubstDefaults(cr.Spec.Path)
	cr.Spec.SourceRef.Name = resolveEnvsubstDefaults(cr.Spec.SourceRef.Name)
	cr.Spec.SourceRef.Namespace = resolveEnvsubstDefaults(cr.Spec.SourceRef.Namespace)
	for i := range cr.Spec.DependsOn {
		cr.Spec.DependsOn[i].Name = resolveEnvsubstDefaults(cr.Spec.DependsOn[i].Name)
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
	var subst map[string]any
	if pb := cr.Spec.PostBuild; pb != nil {
		if len(pb.Substitute) > 0 {
			subst = make(map[string]any, len(pb.Substitute))
			for k, v := range pb.Substitute {
				subst[k] = v
			}
		}
	}

	for _, dep := range cr.Spec.DependsOn {
		if dep.Name == "" {
			return nil, inputf("Kustomization missing dependsOn.name")
		}
	}

	return &Kustomization{
		Name:                cr.Name,
		Namespace:           cr.Namespace,
		KustomizationSpec:   cr.Spec,
		Contents:            doc,
		PostBuildSubstitute: subst,
		Labels:              cr.Labels,
		Annotations:         cr.Annotations,
	}, nil
}

// SourceID resolves the authored source against the effective namespace.
func (k *Kustomization) SourceID() NamedResource {
	return Resolve(NamedResource{Group: referenceGroup(k.SourceRef.Kind, k.SourceRef.APIVersion), Kind: k.SourceRef.Kind, Namespace: k.SourceRef.Namespace, Name: k.SourceRef.Name}, "", k.Namespace)
}

// IsFragment identifies patch fragments without a render path or source name.
func (k *Kustomization) IsFragment() bool { return k.Path == "" && k.SourceRef.Name == "" }

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
