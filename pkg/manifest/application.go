package manifest

import (
	"encoding/json"

	operatorv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
)

// EntryApplication describes the source and path applied by a declaration.
type EntryApplication struct {
	Source      NamedResource
	Path        string
	Declaration BaseManifest
}

// EntryApplication returns the authored application of a non-internal Kustomization.
func (k *Kustomization) EntryApplication() (EntryApplication, bool) {
	if k.IsFragment() || k.Named().ReportHidden() {
		return EntryApplication{}, false
	}
	source := k.SourceID()
	if source.Name == "" || source.Kind == "" {
		source = BootstrapSourceID
	}
	return EntryApplication{Source: source, Path: k.Path}, true
}

// EntryApplication projects a FluxInstance sync through the operator source contract.
func (r *RawObject) EntryApplication() (EntryApplication, bool) {
	if r.Named().Group != FluxOperatorDomain || r.Kind != "FluxInstance" {
		return EntryApplication{}, false
	}
	data, err := json.Marshal(r.Spec["sync"])
	if err != nil {
		return EntryApplication{}, false
	}
	var sync operatorv1.Sync
	if err := json.Unmarshal(data, &sync); err != nil {
		return EntryApplication{}, false
	}
	source, _ := r.syncSource(sync)
	if source == nil {
		return EntryApplication{}, false
	}
	return EntryApplication{Source: source.Named(), Path: sync.Path, Declaration: source}, true
}
