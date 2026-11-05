package manifest

import (
	"cmp"
	"encoding/json"
	"time"

	operatorv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxapis "github.com/fluxcd/pkg/apis/kustomize"
	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const operatorSyncInterval = 10 * time.Minute

// EntryApplication keeps the bootstrap specification separate from the authored owner.
type EntryApplication struct {
	Source       NamedResource
	Path         string
	Declaration  BaseManifest
	Spec         *Kustomization
	Patches      []fluxapis.Patch
	Transformers [][]byte
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
	return EntryApplication{Source: source, Path: k.Path, Spec: &Kustomization{
		Name: k.Name, Namespace: k.Namespace,
		Interval: k.Interval, Path: k.Path, Prune: k.Prune, SourceRef: k.SourceRef,
	}}, true
}

// EntryApplication projects a FluxInstance sync through the operator source contract.
func (r *RawObject) EntryApplication() (EntryApplication, bool) {
	if r.Named().Group != FluxOperatorDomain || r.Kind != "FluxInstance" {
		return EntryApplication{}, false
	}
	data, err := json.Marshal(r.Spec)
	if err != nil {
		return EntryApplication{}, false
	}
	var spec operatorv1.FluxInstanceSpec
	if err := json.Unmarshal(data, &spec); err != nil || spec.Sync == nil {
		return EntryApplication{}, false
	}
	sync := spec.Sync
	if sync.URL == "" || sync.Ref == "" || sync.Path == "" {
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
	interval := metav1.Duration{Duration: time.Minute}
	if sync.Interval != nil {
		interval = *sync.Interval
	}
	var declaration BaseManifest
	switch sync.Kind {
	case KindGitRepository:
		declaration = NewGitRepository(
			sourcev1.GitRepository{Name: name, Namespace: r.Namespace, Spec: sourcev1.GitRepositorySpec{
				Interval:  interval,
				URL:       sync.URL,
				Reference: &sourcev1.GitRepositoryRef{Name: sync.Ref},
				SecretRef: secret,
				Provider:  sync.Provider,
			}},
		)
	case KindOCIRepository:
		declaration = NewOCIRepository(
			sourcev1.OCIRepository{Name: name, Namespace: r.Namespace, Spec: sourcev1.OCIRepositorySpec{
				Interval:  &interval,
				URL:       sync.URL,
				Reference: &sourcev1.OCIRepositoryRef{Tag: sync.Ref},
				SecretRef: secret,
				Provider:  sync.Provider,
			}},
		)
	case KindBucket:
		declaration = NewBucket(sourcev1.Bucket{Name: name, Namespace: r.Namespace, Spec: sourcev1.BucketSpec{
			Interval: interval, Endpoint: sync.URL, BucketName: sync.Ref, SecretRef: secret, Provider: sync.Provider,
		}})
	default:
		return EntryApplication{}, false
	}
	id := declaration.Named()
	application := EntryApplication{
		Source: id, Path: sync.Path, Declaration: declaration,
		Spec: &Kustomization{
			Name: name, Namespace: r.Namespace,
			Interval: metav1.Duration{Duration: operatorSyncInterval}, Path: sync.Path, Prune: true,
			SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: id.Kind, Name: id.Name},
		},
	}
	if spec.Kustomize != nil {
		application.Patches = spec.Kustomize.Patches
	}
	// flux-operator v0.62.1 applies external transformers after builtin patches.
	for _, transformer := range []struct {
		name, kind, field string
		values            map[string]string
	}{
		{name: "annotations", kind: "AnnotationsTransformer", field: "annotations", values: map[string]string{
			FluxKustomizeDomain + "/ssa": "Ignore", FluxKustomizeDomain + "/prune": "Disabled",
		}},
		{name: "labels", kind: "LabelTransformer", field: "labels", values: map[string]string{
			"app.kubernetes.io/managed-by": "flux-operator",
			"app.kubernetes.io/instance":   r.Namespace,
			"app.kubernetes.io/version":    spec.Distribution.Version,
			"app.kubernetes.io/part-of":    "flux",
		}},
	} {
		raw, err := json.Marshal(map[string]any{
			"apiVersion": "builtin", "kind": transformer.kind,
			"metadata": map[string]string{nameKey: transformer.name}, transformer.field: transformer.values,
			"fieldSpecs": []map[string]any{{"path": "metadata/" + transformer.field, "create": true}},
		})
		if err != nil {
			return EntryApplication{}, false
		}
		application.Transformers = append(application.Transformers, raw)
	}
	return application, true
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
