package diff

import (
	"reflect"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestSelectedDocs_PreservesProducerAndEmissionOrder(t *testing.T) {
	t.Parallel()
	a := &manifest.Kustomization{Name: "a", Namespace: "ns"}
	a.Path = "./apps/a"
	b := &manifest.Kustomization{Name: "b", Namespace: "ns"}
	hr := &manifest.HelmRelease{Name: "hr", Namespace: "ns"}
	manifests := []manifest.DocumentBatch{
		{Parent: a, Documents: []map[string]any{
			{"kind": "ConfigMap", "metadata": map[string]any{"name": "a1"}},
			{"kind": "ConfigMap", "metadata": map[string]any{"name": "a2"}},
		}},
		{Parent: b, Documents: []map[string]any{{"kind": "ConfigMap", "metadata": map[string]any{"name": "b1"}}}},
		{Parent: hr, Documents: []map[string]any{{"kind": "Service"}}},
		{Documents: []map[string]any{{"kind": "Namespace", "metadata": map[string]any{"name": "root"}}}},
	}
	docs := selectedDocs(manifests)
	if len(docs) != 5 {
		t.Fatalf("want 5 docs, got %d", len(docs))
	}
	if docs[0].Parent.NamedResource != a.Named() || docs[0].Parent.Path != "apps/a" {
		t.Errorf("doc[0] parent = %+v; want a / apps/a", docs[0].Parent)
	}
	if name, _ := manifest.DocMetadata(docs[1].Manifest); name != "a2" {
		t.Errorf("within-parent order not preserved; doc[1] = %v", docs[1].Manifest)
	}
	if docs[2].Parent.NamedResource != b.Named() || docs[2].Parent.Path != "" {
		t.Errorf("doc[2] parent = %+v; want b / empty path", docs[2].Parent)
	}
	if docs[3].Parent.NamedResource != hr.Named() || docs[3].Parent.Path != "" {
		t.Errorf("doc[3] parent = %+v; want HelmRelease.helm.toolkit.fluxcd.io / empty path", docs[3].Parent)
	}
	if docs[4].Parent != (Parent{}) || manifest.DocIdentity(docs[4].Manifest).Name != "root" {
		t.Fatalf("ownerless identity or attribution lost: %+v", docs[4])
	}
	if got := selectedDocs(manifests); !reflect.DeepEqual(docs, got) {
		t.Error("selectedDocs must preserve the completed selection order")
	}
}
