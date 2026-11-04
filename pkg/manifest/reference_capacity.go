package manifest

// ReferenceLimits bounds preallocation without resolving identities.
type ReferenceLimits struct {
	References int
	DataInputs int
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (k *Kustomization) ReferenceCapacity() ReferenceLimits {
	data := 0
	n := len(k.DependsOn)
	if k.SourceRef.Kind != "" || k.SourceRef.Name != "" || k.Path != "" {
		n++
	}
	if k.PostBuild != nil {
		data = len(k.PostBuild.SubstituteFrom)
		n += data
	}
	return ReferenceLimits{References: n, DataInputs: data}
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (h *HelmRelease) ReferenceCapacity() ReferenceLimits {
	n := len(h.DependsOn) + len(h.ValuesFrom)
	if h.ChartRef != nil {
		n++
		if h.chartDeclaration != nil {
			n++
		}
		return ReferenceLimits{References: n, DataInputs: len(h.ValuesFrom)}
	}
	template := h.HelmReleaseSpec.Chart
	if template == nil {
		return ReferenceLimits{References: n, DataInputs: len(h.ValuesFrom)}
	}
	if template.Kind == KindOCIRepository {
		n++
		for _, ref := range []*LocalObjectReference{template.Spec.SecretRef, template.Spec.CertSecretRef, template.Spec.ProxySecretRef} {
			if ref != nil {
				n++
			}
		}
	} else {
		n += chartTemplateSourceCapacity
	}
	return ReferenceLimits{References: n, DataInputs: len(h.ValuesFrom)}
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (r *ResourceSet) ReferenceCapacity() ReferenceLimits {
	return ReferenceLimits{References: len(r.DependsOn) + len(r.InputsFrom)}
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*GitRepository) ReferenceCapacity() ReferenceLimits {
	return ReferenceLimits{References: gitCredentialCapacity}
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*OCIRepository) ReferenceCapacity() ReferenceLimits {
	return ReferenceLimits{References: ociCredentialCapacity}
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*Bucket) ReferenceCapacity() ReferenceLimits {
	return ReferenceLimits{References: bucketCredentialCapacity}
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*HelmRepository) ReferenceCapacity() ReferenceLimits {
	return ReferenceLimits{References: helmCredentialCapacity}
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*HelmChartSource) ReferenceCapacity() ReferenceLimits {
	return ReferenceLimits{References: 1}
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (e *ExternalArtifact) ReferenceCapacity() ReferenceLimits {
	if e.SourceRef != nil {
		return ReferenceLimits{References: 1}
	}
	return ReferenceLimits{}
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (p *ResourceSetInputProvider) ReferenceCapacity() ReferenceLimits {
	if p.inputKind() == ProviderInputRegistry {
		return ReferenceLimits{References: registryCredentialCapacity}
	}
	return ReferenceLimits{}
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*ConfigMap) ReferenceCapacity() ReferenceLimits {
	return ReferenceLimits{}
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*Secret) ReferenceCapacity() ReferenceLimits {
	return ReferenceLimits{}
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (r *RawObject) ReferenceCapacity() ReferenceLimits {
	if r.isArtifactGenerator() {
		sources, _ := r.Spec["sources"].([]any)
		return ReferenceLimits{References: len(sources)}
	}
	return ReferenceLimits{}
}

const (
	chartTemplateSourceCapacity = 2
	gitCredentialCapacity       = 2
	ociCredentialCapacity       = 3
	bucketCredentialCapacity    = 3
	helmCredentialCapacity      = 2
	registryCredentialCapacity  = 2
)
