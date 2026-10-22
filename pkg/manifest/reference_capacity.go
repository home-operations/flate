package manifest

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (k *Kustomization) ReferenceCapacity() (int, int) {
	data := 0
	n := len(k.DependsOn)
	if k.SourceRef.Kind != "" || k.SourceRef.Name != "" || k.Path != "" {
		n++
	}
	if k.PostBuild != nil {
		data = len(k.PostBuild.SubstituteFrom)
		n += data
	}
	return n, data
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (h *HelmRelease) ReferenceCapacity() (int, int) {
	n := len(h.DependsOn) + len(h.ValuesFrom)
	if h.hasChartReferences() {
		n += 2
	}
	return n, len(h.ValuesFrom)
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (r *ResourceSet) ReferenceCapacity() (int, int) {
	return len(r.DependsOn) + len(r.InputsFrom), 0
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*GitRepository) ReferenceCapacity() (int, int) { return 2, 0 }

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*OCIRepository) ReferenceCapacity() (int, int) { return 3, 0 }

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*Bucket) ReferenceCapacity() (int, int) { return 3, 0 }

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*HelmRepository) ReferenceCapacity() (int, int) { return 2, 0 }

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*HelmChartSource) ReferenceCapacity() (int, int) { return 1, 0 }

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (e *ExternalArtifact) ReferenceCapacity() (int, int) {
	if e.SourceRef != nil {
		return 1, 0
	}
	return 0, 0
}

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*ResourceSetInputProvider) ReferenceCapacity() (int, int) { return 0, 0 }

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*ConfigMap) ReferenceCapacity() (int, int) { return 0, 0 }

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (*Secret) ReferenceCapacity() (int, int) { return 0, 0 }

// ReferenceCapacity bounds all references and data input uses without resolving identities.
func (r *RawObject) ReferenceCapacity() (int, int) {
	if r.isArtifactGenerator() {
		sources, _ := r.Spec["sources"].([]any)
		return len(sources), 0
	}
	return 0, 0
}

func (h *HelmRelease) hasChartReferences() bool {
	return h.ChartRef != nil || h.HelmReleaseSpec.Chart != nil || h.Chart.Source.Kind != ""
}
