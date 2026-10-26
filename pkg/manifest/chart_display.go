package manifest

// ChartDisplaySource returns the resolved chart source when no explicit version is set.
func (h *HelmRelease) ChartDisplaySource() (NamedResource, bool) {
	if h.Chart.Version != "" {
		return NamedResource{}, false
	}
	return h.ChartSource(), true
}

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
