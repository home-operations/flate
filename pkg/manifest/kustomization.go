package manifest

import (
	"log/slog"
	"maps"
	"slices"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
)

// SubstituteReference contains a reference to a resource supplying the
// variable name/value pairs used by postBuild.substitute.
type SubstituteReference struct {
	Kind     string `json:"kind" yaml:"kind"`
	Name     string `json:"name" yaml:"name"`
	Optional bool   `json:"optional,omitempty" yaml:"optional,omitempty"`
}

// Kustomization is the Flux Kustomization CR. It bundles the path of a
// local kustomize tree together with the in-cluster materials it produces
// (HelmReleases, HelmRepositories, ConfigMaps, Secrets, ...).
type Kustomization struct {
	Name      string `json:"name" yaml:"name"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Path      string `json:"path" yaml:"path"`

	HelmRepos        []*HelmRepository  `json:"helmRepos,omitempty" yaml:"helmRepos,omitempty"`
	OCIRepos         []*OCIRepository   `json:"ociRepos,omitempty" yaml:"ociRepos,omitempty"`
	HelmReleases     []*HelmRelease     `json:"helmReleases,omitempty" yaml:"helmReleases,omitempty"`
	ConfigMaps       []*ConfigMap       `json:"configMaps,omitempty" yaml:"configMaps,omitempty"`
	Secrets          []*Secret          `json:"secrets,omitempty" yaml:"secrets,omitempty"`
	HelmChartSources []*HelmChartSource `json:"helmChartSources,omitempty" yaml:"helmChartSources,omitempty"`

	// Internal-only fields (not emitted to YAML output).
	SourcePath              string                `json:"-" yaml:"-"`
	SourceKind              string                `json:"-" yaml:"-"`
	SourceName              string                `json:"-" yaml:"-"`
	SourceNamespace         string                `json:"-" yaml:"-"`
	TargetNamespace         string                `json:"-" yaml:"-"`
	Contents                map[string]any        `json:"-" yaml:"-"`
	PostBuildSubstitute     map[string]any        `json:"-" yaml:"-"`
	PostBuildSubstituteFrom []SubstituteReference `json:"-" yaml:"-"`
	DependsOn               []string              `json:"-" yaml:"-"`
	Labels                  map[string]string     `json:"-" yaml:"-"`
	// Components is Flux v1's spec.components — paths to kustomize
	// components injected on top of spec.path at reconcile time.
	Components []string `json:"-" yaml:"-"`
	// Suspend mirrors spec.suspend — controllers skip suspended objects.
	Suspend bool `json:"-" yaml:"-"`

	Images []string `json:"images,omitempty" yaml:"images,omitempty"`
}

// Named identifies the Kustomization.
func (k *Kustomization) Named() NamedResource {
	return NamedResource{Kind: KindKustomization, Namespace: k.Namespace, Name: k.Name}
}

// IDName is the test-friendly identifier (the path).
func (k *Kustomization) IDName() string { return k.Path }

// NamespacedName is "<namespace>/<name>".
func (k *Kustomization) NamespacedName() string { return k.Namespace + "/" + k.Name }

// ValidateDependsOn drops any dependency that is not present in allKS.
// allKS is a set of "namespace/name" identifiers.
func (k *Kustomization) ValidateDependsOn(allKS map[string]struct{}) {
	if len(k.DependsOn) == 0 {
		return
	}
	kept := slices.DeleteFunc(slices.Clone(k.DependsOn), func(dep string) bool {
		_, ok := allKS[dep]
		return !ok
	})
	if missing := len(k.DependsOn) - len(kept); missing > 0 {
		// Demoted to Debug: dependsOn references often dangle in a
		// statically-loaded view because parent-Kustomization
		// targetNamespace inheritance happens lazily. Real Flux resolves
		// them at apply time, and dropping them here only affects the
		// wait order during flate's reconcile.
		slog.Debug("kustomization dependsOn entries dropped",
			"kustomization", k.NamespacedName(),
			"dropped", missing, "kept", len(kept))
	}
	k.DependsOn = kept
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

// ParseKustomization decodes a Flux Kustomization CR via the
// kustomize-controller typed schema. The raw doc is retained in
// Contents because RenderFlux still feeds it to fluxcd/pkg/kustomize
// as an unstructured.Unstructured.
func ParseKustomization(doc map[string]any) (*Kustomization, error) {
	if err := checkAPIVersion(doc, FluxKustomizeDomain); err != nil {
		return nil, err
	}
	var cr kustomizev1.Kustomization
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("Kustomization decode: %v", err)
	}
	if cr.Name == "" {
		return nil, inputf("Kustomization missing metadata.name")
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
		for _, ref := range pb.SubstituteFrom {
			substituteFrom = append(substituteFrom, SubstituteReference{
				Kind: ref.Kind, Name: ref.Name, Optional: ref.Optional,
			})
		}
		if len(pb.Substitute) > 0 {
			subst = make(map[string]any, len(pb.Substitute))
			for k, v := range pb.Substitute {
				subst[k] = v
			}
		}
	}

	var dependsOn []string
	for _, dep := range cr.Spec.DependsOn {
		if dep.Name == "" {
			return nil, inputf("Kustomization missing dependsOn.name")
		}
		depNS := dep.Namespace
		if depNS == "" {
			depNS = ns
		}
		dependsOn = append(dependsOn, depNS+"/"+dep.Name)
	}

	return &Kustomization{
		Name:                    cr.Name,
		Namespace:               ns,
		Path:                    cr.Spec.Path,
		SourcePath:              cr.Annotations["config.kubernetes.io/path"],
		SourceKind:              cr.Spec.SourceRef.Kind,
		SourceName:              cr.Spec.SourceRef.Name,
		SourceNamespace:         srcNamespace,
		TargetNamespace:         cr.Spec.TargetNamespace,
		Contents:                doc,
		PostBuildSubstitute:     subst,
		PostBuildSubstituteFrom: substituteFrom,
		DependsOn:               dependsOn,
		Labels:                  cr.Labels,
		Components:              cr.Spec.Components,
		Suspend:                 cr.Spec.Suspend,
	}, nil
}
