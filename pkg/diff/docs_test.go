package diff

import (
	"reflect"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestSelectedDocs_PreservesProducerAndEmissionOrder(t *testing.T) {
	a := &manifest.Kustomization{Name: "a", Namespace: "ns"}
	a.Path = "./apps/a"
	b := &manifest.Kustomization{Name: "b", Namespace: "ns"}
	hr := &manifest.HelmRelease{Name: "hr", Namespace: "ns"}
	objects := []manifest.BaseManifest{a, b, hr}
	manifests := map[manifest.NamedResource][]map[string]any{
		b.Named(): {{"kind": "ConfigMap", "metadata": map[string]any{"name": "b1"}}},
		a.Named(): {
			{"kind": "ConfigMap", "metadata": map[string]any{"name": "a1"}},
			{"kind": "ConfigMap", "metadata": map[string]any{"name": "a2"}},
		},
		hr.Named(): {{"kind": "Service"}},
	}
	docs := selectedDocs(objects, manifests)
	if len(docs) != 4 {
		t.Fatalf("want 4 docs, got %d", len(docs))
	}
	if docs[0].Parent.Name != "a" || docs[0].Parent.Path != "apps/a" {
		t.Errorf("doc[0] parent = %+v; want a / apps/a", docs[0].Parent)
	}
	if name, _ := manifest.DocMetadata(docs[1].Manifest); name != "a2" {
		t.Errorf("within-parent order not preserved; doc[1] = %v", docs[1].Manifest)
	}
	if docs[2].Parent.Name != "b" || docs[2].Parent.Path != "" {
		t.Errorf("doc[2] parent = %+v; want b / empty path", docs[2].Parent)
	}
	if docs[3].Parent.Kind != manifest.KindHelmRelease || docs[3].Parent.Path != "" {
		t.Errorf("doc[3] parent = %+v; want HelmRelease / empty path", docs[3].Parent)
	}
	if got := selectedDocs(objects, manifests); !reflect.DeepEqual(docs, got) {
		t.Error("selectedDocs must preserve the completed selection order")
	}
}
