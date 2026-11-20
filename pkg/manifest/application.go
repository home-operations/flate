package manifest

// EntryApplication describes the source and path applied by a declaration.
type EntryApplication struct {
	Source NamedResource
	Path   string
}

// EntryApplication returns the authored application of a non-internal Kustomization.
func (k *Kustomization) EntryApplication() (EntryApplication, bool) {
	if k.IsFragment() || k.Named().ReportHidden() {
		return EntryApplication{}, false
	}
	return EntryApplication{Source: k.EffectiveSourceID(), Path: k.Path}, true
}

// EntryApplication reports construction inputs before FluxInstance patches.
func (r *RawObject) EntryApplication() (EntryApplication, bool) {
	sync, err := r.syncSpec()
	return r.syncApplication(sync, err)
}

// ApplicationFor returns the application described by an entry applier.
func ApplicationFor(obj BaseManifest) (EntryApplication, bool) {
	switch obj := obj.(type) {
	case *Kustomization:
		return obj.EntryApplication()
	case *RawObject:
		return obj.EntryApplication()
	default:
		return EntryApplication{}, false
	}
}
