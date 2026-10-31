package manifest

import (
	"bytes"
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

// SyncProjection contains the operator sync documents and their build composition.
type SyncProjection struct {
	Documents    []byte
	Patches      []fluxapis.Patch
	Namespace    string
	Transformers [][]byte
}

// SyncProjection derives immutable sync input without rendering an installation.
func (r *RawObject) SyncProjection() (*SyncProjection, error) {
	var spec operatorv1.FluxInstanceSpec
	if err := decodeInto(r.Spec, &spec); err != nil {
		return nil, inputf("decode FluxInstance spec: %w", err)
	}
	if spec.Sync == nil {
		return nil, nil
	}
	source, sourceSpec := r.syncSource(*spec.Sync)
	if source == nil {
		return nil, inputf("invalid FluxInstance sync")
	}
	id := source.Named()
	metadata := map[string]string{"name": id.Name, "namespace": id.Namespace}
	rootSpec := kustomizev1.KustomizationSpec{
		Interval: metav1.Duration{Duration: 10 * time.Minute},
		Path:     spec.Sync.Path, Prune: true,
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: id.Kind, Name: id.Name},
	}
	projection := &SyncProjection{Namespace: r.Namespace}
	if spec.Kustomize != nil {
		projection.Patches = spec.Kustomize.Patches
	}
	var documents bytes.Buffer
	for i, object := range []map[string]any{
		{"apiVersion": sourcev1.GroupVersion.String(), "kind": id.Kind, "metadata": metadata, "spec": sourceSpec},
		{"apiVersion": kustomizev1.GroupVersion.String(), "kind": KindKustomization, "metadata": metadata, "spec": rootSpec},
	} {
		raw, err := json.Marshal(object)
		if err != nil {
			return nil, inputf("marshal FluxInstance sync: %w", err)
		}
		if i > 0 {
			documents.WriteString("\n---\n")
		}
		documents.Write(raw)
	}
	projection.Documents = documents.Bytes()
	// flux-operator v0.61.0 applies these external transformers after builtin patches.
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
			"metadata":        map[string]string{"name": transformer.name},
			transformer.field: transformer.values,
			"fieldSpecs":      []map[string]any{{"path": "metadata/" + transformer.field, "create": true}},
		})
		if err != nil {
			return nil, inputf("marshal FluxInstance transformer: %w", err)
		}
		projection.Transformers = append(projection.Transformers, raw)
	}
	return projection, nil
}

func (r *RawObject) syncSource(sync operatorv1.Sync) (BaseManifest, any) {
	name := cmp.Or(sync.Name, r.Namespace)
	if name == "" || sync.URL == "" || sync.Ref == "" || sync.Path == "" {
		return nil, nil
	}
	interval := metav1.Duration{Duration: time.Minute}
	if sync.Interval != nil {
		interval = *sync.Interval
	}
	var secret *meta.LocalObjectReference
	if sync.PullSecret != "" {
		secret = &meta.LocalObjectReference{Name: sync.PullSecret}
	}
	switch sync.Kind {
	case KindGitRepository:
		cr := sourcev1.GitRepository{
			Name: name, Namespace: r.Namespace,
			Spec: sourcev1.GitRepositorySpec{
				Interval: interval, URL: sync.URL, Reference: &sourcev1.GitRepositoryRef{Name: sync.Ref},
				SecretRef: secret, Provider: sync.Provider,
			},
		}
		return NewGitRepository(cr), cr.Spec
	case KindOCIRepository:
		cr := sourcev1.OCIRepository{
			Name: name, Namespace: r.Namespace,
			Spec: sourcev1.OCIRepositorySpec{
				Interval: &interval, URL: sync.URL, Reference: &sourcev1.OCIRepositoryRef{Tag: sync.Ref},
				SecretRef: secret, Provider: sync.Provider,
			},
		}
		return NewOCIRepository(cr), cr.Spec
	case KindBucket:
		cr := sourcev1.Bucket{
			Name: name, Namespace: r.Namespace,
			Spec: sourcev1.BucketSpec{
				Interval: interval, Endpoint: sync.URL, BucketName: sync.Ref, SecretRef: secret, Provider: sync.Provider,
			},
		}
		return NewBucket(cr), cr.Spec
	default:
		return nil, nil
	}
}
