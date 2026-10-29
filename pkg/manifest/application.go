package manifest

import (
	"cmp"
	"encoding/json"

	operatorv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
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
	if err := json.Unmarshal(data, &sync); err != nil || sync.URL == "" || sync.Ref == "" || sync.Path == "" {
		return EntryApplication{}, false
	}
	name := cmp.Or(sync.Name, r.Namespace)
	if name == "" {
		return EntryApplication{}, false
	}
	var secret *meta.LocalObjectReference
	if sync.PullSecret != "" {
		secret = &meta.LocalObjectReference{Name: sync.PullSecret}
	}
	var declaration BaseManifest
	switch sync.Kind {
	case KindGitRepository:
		declaration = NewGitRepository(sourcev1.GitRepository{Name: name, Namespace: r.Namespace, Spec: sourcev1.GitRepositorySpec{
			URL: sync.URL, Reference: &sourcev1.GitRepositoryRef{Name: sync.Ref}, SecretRef: secret, Provider: sync.Provider,
		}})
	case KindOCIRepository:
		declaration = NewOCIRepository(sourcev1.OCIRepository{Name: name, Namespace: r.Namespace, Spec: sourcev1.OCIRepositorySpec{
			URL: sync.URL, Reference: &sourcev1.OCIRepositoryRef{Tag: sync.Ref}, SecretRef: secret, Provider: sync.Provider,
		}})
	case KindBucket:
		declaration = NewBucket(sourcev1.Bucket{Name: name, Namespace: r.Namespace, Spec: sourcev1.BucketSpec{
			Endpoint: sync.URL, BucketName: sync.Ref, SecretRef: secret, Provider: sync.Provider,
		}})
	default:
		return EntryApplication{}, false
	}
	return EntryApplication{Source: declaration.Named(), Path: sync.Path, Declaration: declaration}, true
}
