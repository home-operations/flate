package manifest

import (
	"log/slog"
	"maps"
	"slices"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
)

// Kustomization is the Flux Kustomization CR. It bundles the path of a
// local kustomize tree together with the in-cluster materials it produces
// (HelmReleases, HelmRepositories, ConfigMaps, Secrets, ...).
//
// The embedded kustomizev1.KustomizationSpec promotes Path, Suspend,
// TargetNamespace, Components, SourceRef, PostBuild, Patches, Images,
// CommonMetadata, NamePrefix, NameSuffix, Wait, Prune, Force,
// HealthChecks, etc. to the top level. Shadowed by flate-only fields
// where the projected shape differs from the upstream (DependsOn,
// PostBuildSubstitute, PostBuildSubstituteFrom — see field docs).
type Kustomization struct {
	Name      string `json:"name"                yaml:"name"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`

	kustomizev1.KustomizationSpec `json:",inline" yaml:",inline"`

	HelmRepos        []*HelmRepository  `json:"helmRepos,omitempty"        yaml:"helmRepos,omitempty"`
	OCIRepos         []*OCIRepository   `json:"ociRepos,omitempty"         yaml:"ociRepos,omitempty"`
	HelmReleases     []*HelmRelease     `json:"helmReleases,omitempty"     yaml:"helmReleases,omitempty"`
	ConfigMaps       []*ConfigMap       `json:"configMaps,omitempty"       yaml:"configMaps,omitempty"`
	Secrets          []*Secret          `json:"secrets,omitempty"          yaml:"secrets,omitempty"`
	HelmChartSources []*HelmChartSource `json:"helmChartSources,omitempty" yaml:"helmChartSources,omitempty"`

	// SourcePath is the location on disk this Kustomization was loaded
	// from (config.kubernetes.io/path annotation).
	SourcePath string `json:"-" yaml:"-"`

	// SourceKind / SourceName / SourceNamespace mirror the embedded
	// Spec.SourceRef with the namespace defaulted to the Kustomization's
	// own namespace (matching Flux's behavior). The embedded struct's
	// SourceRef is accessible at k.SourceRef when callers need the raw
	// upstream typed form.
	SourceKind      string `json:"-" yaml:"-"`
	SourceName      string `json:"-" yaml:"-"`
	SourceNamespace string `json:"-" yaml:"-"`

	// Contents is the raw decoded YAML document. Retained because
	// RenderFlux still feeds it to fluxcd/pkg/kustomize.NewGenerator
	// as an unstructured.Unstructured.
	Contents map[string]any `json:"-" yaml:"-"`

	// PostBuildSubstitute holds the resolved substitution variables
	// (after evaluating spec.postBuild.substituteFrom against the
	// store). Shadows the upstream Spec.PostBuild.Substitute (which
	// holds the spec'd unresolved values).
	PostBuildSubstitute map[string]any `json:"-" yaml:"-"`

	// PostBuildSubstituteFrom captures the typed substituteFrom refs
	// from spec.postBuild.
	PostBuildSubstituteFrom []SubstituteReference `json:"-" yaml:"-"`

	// DependsOn is the normalized form of spec.dependsOn carrying any
	// ReadyExpr. Shadows the embedded Spec.DependsOn ([]DependencyReference).
	DependsOn []DependencyRef `json:"-" yaml:"-"`

	Labels      map[string]string `json:"-" yaml:"-"`
	Annotations map[string]string `json:"-" yaml:"-"`
}

// GetLabels returns the Kustomization's metadata.labels. Implements
// the shared accessor pkg/depwait projectObject uses for CEL exposure.
func (k *Kustomization) GetLabels() map[string]string { return k.Labels }

// GetAnnotations returns the Kustomization's metadata.annotations.
func (k *Kustomization) GetAnnotations() map[string]string { return k.Annotations }

// Named identifies the Kustomization.
func (k *Kustomization) Named() NamedResource {
	return NamedResource{Kind: KindKustomization, Namespace: k.Namespace, Name: k.Name}
}

// Clone returns a copy of k safe for in-place mutation during a single
// reconcile pass. Deep-copies the maps reconcile writes to (Contents,
// PostBuildSubstitute) so the canonical store-owned object is never
// observed mid-mutation by other goroutines.
func (k *Kustomization) Clone() *Kustomization {
	out := *k
	out.Contents = DeepCopyMap(k.Contents)
	out.PostBuildSubstitute = maps.Clone(k.PostBuildSubstitute)
	return &out
}

// NamespacedName is "<namespace>/<name>".
func (k *Kustomization) NamespacedName() string { return k.Namespace + "/" + k.Name }

// FilterDependsOn returns a copy of deps with any entries whose target
// is not present in known removed. known is a set of "namespace/name"
// identifiers. The second return value is the count of dropped
// entries. Pure function — does not mutate deps. Callers updating a
// stored object should follow the Store's immutability contract:
// shallow-copy the object, set the new DependsOn on the copy, then
// re-AddObject the copy.
func FilterDependsOn(deps []DependencyRef, known map[string]struct{}) ([]DependencyRef, int) {
	if len(deps) == 0 {
		return deps, 0
	}
	kept := slices.DeleteFunc(slices.Clone(deps), func(dep DependencyRef) bool {
		_, ok := known[dep.NamespacedName()]
		return !ok
	})
	dropped := len(deps) - len(kept)
	if dropped > 0 {
		// Demoted to Debug: dependsOn references often dangle in a
		// statically-loaded view because parent-Kustomization
		// targetNamespace inheritance happens lazily. Real Flux resolves
		// them at apply time, and dropping them here only affects the
		// wait order during flate's reconcile.
		slog.Debug("dependsOn entries dropped",
			"dropped", dropped, "kept", len(kept))
	}
	return kept, dropped
}

// UpdatePostBuildSubstitutions merges the given map into the substitution
// table AND into the raw contents doc, mirroring upstream behavior so the
// raw document is consistent for serialization.
func (k *Kustomization) UpdatePostBuildSubstitutions(subs map[string]any) {
	if k.PostBuildSubstitute == nil {
		k.PostBuildSubstitute = make(map[string]any, len(subs))
	}
	maps.Copy(k.PostBuildSubstitute, subs)
	if k.Contents == nil {
		return
	}
	spec, _ := k.Contents["spec"].(map[string]any)
	if spec == nil {
		spec = make(map[string]any)
		k.Contents["spec"] = spec
	}
	post, _ := spec["postBuild"].(map[string]any)
	if post == nil {
		post = make(map[string]any)
		spec["postBuild"] = post
	}
	sub, _ := post["substitute"].(map[string]any)
	if sub == nil {
		sub = make(map[string]any)
		post["substitute"] = sub
	}
	maps.Copy(sub, subs)
}

// parseKustomization decodes a Flux Kustomization CR via the
// kustomize-controller typed schema. The raw doc is retained in
// Contents because RenderFlux still feeds it to fluxcd/pkg/kustomize
// as an unstructured.Unstructured.
func parseKustomization(doc map[string]any) (*Kustomization, error) {
	if err := checkAPIVersion(doc, FluxKustomizeDomain); err != nil {
		return nil, err
	}
	var cr kustomizev1.Kustomization
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("Kustomization decode: %w", err)
	}
	if cr.Name == "" {
		return nil, inputf("Kustomization missing metadata.name")
	}
	// Pre-resolve envsubst defaults so flate's path / dep resolution
	// matches what real Flux's postBuild.substitute would produce.
	// Bare ${VAR} (no default) is left for the postBuild step to
	// fill — only ${VAR:=default} and ${VAR:-default} collapse here.
	cr.Spec.Path = ResolveEnvsubstDefaults(cr.Spec.Path)
	cr.Spec.SourceRef.Name = ResolveEnvsubstDefaults(cr.Spec.SourceRef.Name)
	cr.Spec.SourceRef.Namespace = ResolveEnvsubstDefaults(cr.Spec.SourceRef.Namespace)
	for i := range cr.Spec.DependsOn {
		cr.Spec.DependsOn[i].Name = ResolveEnvsubstDefaults(cr.Spec.DependsOn[i].Name)
		cr.Spec.DependsOn[i].Namespace = ResolveEnvsubstDefaults(cr.Spec.DependsOn[i].Namespace)
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
	// namespace is optional — a parent Kustomization's
	// spec.targetNamespace may fill it in at apply time.
	ns := cr.Namespace
	srcNamespace := cr.Spec.SourceRef.Namespace
	if srcNamespace == "" {
		srcNamespace = ns
	}

	var substituteFrom []SubstituteReference
	var subst map[string]any
	if pb := cr.Spec.PostBuild; pb != nil {
		substituteFrom = slices.Clone(pb.SubstituteFrom)
		if len(pb.Substitute) > 0 {
			subst = make(map[string]any, len(pb.Substitute))
			for k, v := range pb.Substitute {
				subst[k] = v
			}
		}
	}

	var dependsOn []DependencyRef
	for _, dep := range cr.Spec.DependsOn {
		if dep.Name == "" {
			return nil, inputf("Kustomization missing dependsOn.name")
		}
		depNS := dep.Namespace
		if depNS == "" {
			depNS = ns
		}
		dependsOn = append(dependsOn, DependencyRef{
			NamedResource: NamedResource{Kind: KindKustomization, Namespace: depNS, Name: dep.Name},
			ReadyExpr:     dep.ReadyExpr,
		})
	}

	return &Kustomization{
		Name:                    cr.Name,
		Namespace:               ns,
		KustomizationSpec:       cr.Spec,
		SourcePath:              cr.Annotations["config.kubernetes.io/path"],
		SourceKind:              cr.Spec.SourceRef.Kind,
		SourceName:              cr.Spec.SourceRef.Name,
		SourceNamespace:         srcNamespace,
		Contents:                doc,
		PostBuildSubstitute:     subst,
		PostBuildSubstituteFrom: substituteFrom,
		DependsOn:               dependsOn,
		Labels:                  cr.Labels,
		Annotations:             cr.Annotations,
	}, nil
}
