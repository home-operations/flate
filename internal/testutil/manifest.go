package testutil

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

// DepRefs wraps NamedResources as bare DependencyRefs (no ReadyExpr),
// the shape dependency-ordering tests use to express dependsOn edges.
func DepRefs(ids ...manifest.NamedResource) []manifest.DependencyRef {
	out := make([]manifest.DependencyRef, len(ids))
	for i, id := range ids {
		out[i] = manifest.DependencyRef{NamedResource: id}
	}
	return out
}

// MustYAML decodes a single YAML document literal into a generic map,
// failing the test on parse error or multi-document input.
func MustYAML(t testing.TB, doc string) map[string]any {
	t.Helper()
	docs, err := manifest.SplitDocs([]byte(doc))
	if err != nil {
		t.Fatalf("SplitDocs: %v\n%s", err, doc)
	}
	if len(docs) != 1 {
		t.Fatalf("expected single doc, got %d", len(docs))
	}
	return docs[0]
}
