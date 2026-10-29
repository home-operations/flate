package manifest

// SourcePath returns the resolved source identity and authored path, including an empty path.
func (k *Kustomization) SourcePath() (NamedResource, string, bool) {
	return k.SourceID(), k.Path, !k.standalone
}

// SourcePath reports no source-qualified path for this manifest.
func (*HelmRelease) SourcePath() (NamedResource, string, bool) {
	return NamedResource{}, "", false
}

// SourcePath reports no source-qualified path for this manifest.
func (*HelmRepository) SourcePath() (NamedResource, string, bool) {
	return NamedResource{}, "", false
}

// SourcePath reports no source-qualified path for this manifest.
func (*HelmChartSource) SourcePath() (NamedResource, string, bool) {
	return NamedResource{}, "", false
}

// SourcePath reports no source-qualified path for this manifest.
func (*GitRepository) SourcePath() (NamedResource, string, bool) {
	return NamedResource{}, "", false
}

// SourcePath reports no source-qualified path for this manifest.
func (*OCIRepository) SourcePath() (NamedResource, string, bool) {
	return NamedResource{}, "", false
}

// SourcePath reports no source-qualified path for this manifest.
func (*ExternalArtifact) SourcePath() (NamedResource, string, bool) {
	return NamedResource{}, "", false
}

// SourcePath reports no source-qualified path for this manifest.
func (*Bucket) SourcePath() (NamedResource, string, bool) {
	return NamedResource{}, "", false
}

// SourcePath reports no source-qualified path for this manifest.
func (*ResourceSet) SourcePath() (NamedResource, string, bool) {
	return NamedResource{}, "", false
}

// SourcePath reports no source-qualified path for this manifest.
func (*ResourceSetInputProvider) SourcePath() (NamedResource, string, bool) {
	return NamedResource{}, "", false
}

// SourcePath reports no source-qualified path for this manifest.
func (*ConfigMap) SourcePath() (NamedResource, string, bool) {
	return NamedResource{}, "", false
}

// SourcePath reports no source-qualified path for this manifest.
func (*Secret) SourcePath() (NamedResource, string, bool) {
	return NamedResource{}, "", false
}

// SourcePath returns the source-qualified path of a FluxInstance sync.
func (r *RawObject) SourcePath() (NamedResource, string, bool) {
	application, ok := r.EntryApplication()
	return application.Source, application.Path, ok
}
