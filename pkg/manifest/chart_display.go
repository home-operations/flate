package manifest

// ChartDisplaySource returns the effective chart input selected by the declaration.
func (h *HelmRelease) ChartDisplaySource() (NamedResource, bool) {
	if h.HelmReleaseSpec.Chart != nil && h.chartProjection().declared.Kind == KindHelmChart {
		return NamedResource{}, false
	}
	source := h.ChartSource()
	return source, source.Name != ""
}

// ChartVersion returns the chart template's declared version or OCI reference.
func (h *HelmRelease) ChartVersion() string { return h.chartProjection().version }

// ChartDisplaySource reports no chart display source for this manifest.
func (*Kustomization) ChartDisplaySource() (NamedResource, bool) {
	return NamedResource{}, false
}

// ChartDisplaySource reports no chart display source for this manifest.
func (*HelmRepository) ChartDisplaySource() (NamedResource, bool) {
	return NamedResource{}, false
}

// ChartDisplaySource reports no chart display source for this manifest.
func (*HelmChartSource) ChartDisplaySource() (NamedResource, bool) {
	return NamedResource{}, false
}

// ChartDisplaySource reports no chart display source for this manifest.
func (*GitRepository) ChartDisplaySource() (NamedResource, bool) {
	return NamedResource{}, false
}

// ChartDisplaySource reports no chart display source for this manifest.
func (*OCIRepository) ChartDisplaySource() (NamedResource, bool) {
	return NamedResource{}, false
}

// ChartDisplaySource reports no chart display source for this manifest.
func (*ExternalArtifact) ChartDisplaySource() (NamedResource, bool) {
	return NamedResource{}, false
}

// ChartDisplaySource reports no chart display source for this manifest.
func (*Bucket) ChartDisplaySource() (NamedResource, bool) {
	return NamedResource{}, false
}

// ChartDisplaySource reports no chart display source for this manifest.
func (*ResourceSet) ChartDisplaySource() (NamedResource, bool) {
	return NamedResource{}, false
}

// ChartDisplaySource reports no chart display source for this manifest.
func (*ResourceSetInputProvider) ChartDisplaySource() (NamedResource, bool) {
	return NamedResource{}, false
}

// ChartDisplaySource reports no chart display source for this manifest.
func (*ConfigMap) ChartDisplaySource() (NamedResource, bool) {
	return NamedResource{}, false
}

// ChartDisplaySource reports no chart display source for this manifest.
func (*Secret) ChartDisplaySource() (NamedResource, bool) {
	return NamedResource{}, false
}

// ChartDisplaySource reports no chart display source for this manifest.
func (*RawObject) ChartDisplaySource() (NamedResource, bool) {
	return NamedResource{}, false
}
