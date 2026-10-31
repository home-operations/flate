package manifest

import (
	"maps"
	"slices"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
)

// CatalogObject projects immutable reporting and traversal facts, never execution authority or a complete spec.
func CatalogObject(obj BaseManifest, id NamedResource) BaseManifest {
	return obj.catalog(id)
}

func (k *Kustomization) catalog(id NamedResource) BaseManifest {
	out := &Kustomization{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(k.Labels)}
	out.Path, out.SourceRef, out.TargetNamespace = k.Path, k.SourceRef, k.TargetNamespace
	out.Prune, out.Wait, out.Suspend = k.Prune, k.Wait, k.Suspend
	out.Components, out.IgnoreMissingComponents = slices.Clone(k.Components), k.IgnoreMissingComponents
	if k.PostBuild != nil {
		postBuild := *k.PostBuild
		postBuild.SubstituteFrom = nil
		postBuild.Substitute = maps.Clone(k.PostBuild.Substitute)
		out.PostBuild = &postBuild
	}
	return out
}

func (h *HelmRelease) catalog(id NamedResource) BaseManifest {
	out := &HelmRelease{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(h.Labels)}
	out.Suspend, out.TargetNamespace, out.HelmReleaseSpec.ReleaseName = h.Suspend, h.TargetNamespace, h.HelmReleaseSpec.ReleaseName
	out.ChartRef = h.ChartRef
	if template := h.HelmReleaseSpec.Chart; template != nil {
		out.HelmReleaseSpec.Chart = &helmv2.HelmChartTemplate{Kind: template.Kind}
		out.HelmReleaseSpec.Chart.Spec.Chart, out.HelmReleaseSpec.Chart.Spec.Version = template.Spec.Chart, template.Spec.Version
		out.HelmReleaseSpec.Chart.Spec.SourceRef = template.Spec.SourceRef
		out.HelmReleaseSpec.Chart.Spec.Reference = template.Spec.Reference
	}
	source := out.deriveChartSource()
	out.source = source.declared
	out.Chart = HelmChart{Name: h.Chart.Name, Version: h.Chart.Version, Source: source.declared}
	if source.backing.hasSourceChartPath() {
		out.Chart.Source = source.backing.Source
	}
	return out
}

func (g *GitRepository) catalog(id NamedResource) BaseManifest {
	return &GitRepository{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(g.Labels)}
}

func (o *OCIRepository) catalog(id NamedResource) BaseManifest {
	out := &OCIRepository{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(o.Labels)}
	out.Reference = o.Reference
	return out
}

func (*HelmRepository) catalog(id NamedResource) BaseManifest {
	return &HelmRepository{Name: id.Name, Namespace: id.Namespace}
}

func (h *HelmChartSource) catalog(id NamedResource) BaseManifest {
	out := &HelmChartSource{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(h.Labels)}
	out.Chart, out.Version, out.SourceRef = h.Chart, h.Version, h.SourceRef
	return out
}

func (e *ExternalArtifact) catalog(id NamedResource) BaseManifest {
	return &ExternalArtifact{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(e.Labels)}
}

func (b *Bucket) catalog(id NamedResource) BaseManifest {
	return &Bucket{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(b.Labels)}
}

func (r *ResourceSet) catalog(id NamedResource) BaseManifest {
	return &ResourceSet{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(r.Labels), Suspend: r.Suspend}
}

func (p *ResourceSetInputProvider) catalog(id NamedResource) BaseManifest {
	return &ResourceSetInputProvider{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(p.Labels)}
}

func (*ConfigMap) catalog(id NamedResource) BaseManifest {
	return &ConfigMap{Name: id.Name, Namespace: id.Namespace}
}

func (*Secret) catalog(id NamedResource) BaseManifest {
	return &Secret{Name: id.Name, Namespace: id.Namespace}
}

func (r *RawObject) catalog(id NamedResource) BaseManifest {
	out := &RawObject{Kind: r.Kind, APIVersion: r.APIVersion, Name: id.Name, Namespace: id.Namespace}
	if _, valid := r.EntryApplication(); valid {
		out.Spec = map[string]any{"sync": r.Spec["sync"]}
	}
	return out
}
