package manifest

import (
	"maps"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
)

// CatalogObject borrows immutable reporting and traversal facts without granting execution authority.
//
//nolint:ireturn // The manifest contract requires polymorphic results.
func CatalogObject(obj BaseManifest, id NamedResource) BaseManifest {
	return obj.catalog(id)
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (k *Kustomization) catalog(id NamedResource) BaseManifest {
	if k.Named() == id {
		return k
	}
	out := *k
	out.Name, out.Namespace = id.Name, id.Namespace
	return &out
}

//nolint:ireturn // The manifest contract requires polymorphic results.
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

//nolint:ireturn // The manifest contract requires polymorphic results.
func (g *GitRepository) catalog(id NamedResource) BaseManifest {
	return &GitRepository{
		Name:              id.Name,
		Namespace:         id.Namespace,
		Labels:            maps.Clone(g.Labels),
		GitRepositorySpec: g.GitRepositorySpec,
	}
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (o *OCIRepository) catalog(id NamedResource) BaseManifest {
	out := &OCIRepository{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(o.Labels)}
	out.Reference = o.Reference
	return out
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (*HelmRepository) catalog(id NamedResource) BaseManifest {
	return &HelmRepository{Name: id.Name, Namespace: id.Namespace}
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (h *HelmChartSource) catalog(id NamedResource) BaseManifest {
	out := &HelmChartSource{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(h.Labels)}
	out.Chart, out.Version, out.SourceRef = h.Chart, h.Version, h.SourceRef
	return out
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (e *ExternalArtifact) catalog(id NamedResource) BaseManifest {
	return &ExternalArtifact{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(e.Labels)}
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (b *Bucket) catalog(id NamedResource) BaseManifest {
	return &Bucket{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(b.Labels)}
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (r *ResourceSet) catalog(id NamedResource) BaseManifest {
	return &ResourceSet{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(r.Labels), Suspend: r.Suspend}
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (p *ResourceSetInputProvider) catalog(id NamedResource) BaseManifest {
	return &ResourceSetInputProvider{Name: id.Name, Namespace: id.Namespace, Labels: maps.Clone(p.Labels)}
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (*ConfigMap) catalog(id NamedResource) BaseManifest {
	return &ConfigMap{Name: id.Name, Namespace: id.Namespace}
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (*Secret) catalog(id NamedResource) BaseManifest {
	return &Secret{Name: id.Name, Namespace: id.Namespace}
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (r *RawObject) catalog(id NamedResource) BaseManifest {
	out := &RawObject{Kind: r.Kind, APIVersion: r.APIVersion, Name: id.Name, Namespace: id.Namespace}
	if r.isArtifactGenerator() {
		out.Spec = map[string]any{"artifacts": r.Spec["artifacts"], "pathPattern": r.Spec["pathPattern"]}
	} else if _, valid := r.EntryApplication(); valid {
		out.Spec = map[string]any{"sync": r.Spec["sync"]}
	}
	return out
}
