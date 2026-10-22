package resourceset

import (
	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/home-operations/flate/pkg/manifest"
)

// DedupKey identifies a document by its authored API group, kind, namespace and name.
func DedupKey(doc map[string]any) manifest.NamedResource {
	id := manifest.DocIdentity(doc)
	if _, err := schema.ParseGroupVersion(manifest.DocAPIVersion(doc)); err != nil {
		id.Group = ""
	}
	return id
}

func disabledByReconcileAnnotation(doc map[string]any) bool {
	md, _ := doc["metadata"].(map[string]any)
	ann, _ := md["annotations"].(map[string]any)
	v, _ := ann[fluxopv1.ReconcileAnnotation].(string)
	return v == fluxopv1.DisabledValue
}
