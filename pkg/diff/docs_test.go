package diff

import (
	"reflect"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

// TestDocsFromManifests_GroupsByParentDeterministically locks parent sort
// order (so dyff/unified input is stable across the map's random
// iteration), within-parent emission order, and the pathOf wiring.
func TestDocsFromManifests_GroupsByParentDeterministically(t *testing.T) {
	ksA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "a"}
	ksB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "b"}
	manifests := map[manifest.NamedResource][]map[string]any{
		ksB: {{"kind": "ConfigMap", "metadata": map[string]any{"name": "b1"}}},
		ksA: {
			{"kind": "ConfigMap", "metadata": map[string]any{"name": "a1"}},
			{"kind": "ConfigMap", "metadata": map[string]any{"name": "a2"}},
		},
	}
	pathOf := func(id manifest.NamedResource) string {
		if id.Name == "a" {
			return "apps/a"
		}
		return ""
	}

	docs := DocsFromManifests(manifests, pathOf)
	if len(docs) != 3 {
		t.Fatalf("want 3 docs, got %d", len(docs))
	}
	// ksA sorts before ksB; a1 before a2 (slice order preserved).
	if docs[0].Parent.Name != "a" || docs[0].Parent.Path != "apps/a" {
		t.Errorf("doc[0] parent = %+v; want a / apps/a", docs[0].Parent)
	}
	if name, _ := docs[1].Manifest["metadata"].(map[string]any)["name"].(string); name != "a2" {
		t.Errorf("within-parent order not preserved; doc[1] = %v", docs[1].Manifest)
	}
	if docs[2].Parent.Name != "b" || docs[2].Parent.Path != "" {
		t.Errorf("doc[2] parent = %+v; want b / empty path", docs[2].Parent)
	}

	if docs2 := DocsFromManifests(manifests, pathOf); !reflect.DeepEqual(docs, docs2) {
		t.Errorf("DocsFromManifests must be deterministic across map iteration")
	}
}

// TestDocsFromManifests_NilPathOf confirms a nil pathOf leaves Path empty
// (the common consumer case — HelmRelease parents, or callers that don't
// need overlay disambiguation).
func TestDocsFromManifests_NilPathOf(t *testing.T) {
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "hr"}
	docs := DocsFromManifests(map[manifest.NamedResource][]map[string]any{
		id: {{"kind": "Service"}},
	}, nil)
	if len(docs) != 1 || docs[0].Parent.Path != "" {
		t.Errorf("nil pathOf should leave Path empty; got %+v", docs)
	}
}
