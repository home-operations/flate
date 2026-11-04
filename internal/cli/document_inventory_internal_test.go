package cli

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCompareDocs_OrdersByIdentity(t *testing.T) {
	t.Parallel()
	mkDoc := func(apiVersion, kind, ns, name string) map[string]any {
		return map[string]any{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata":   map[string]any{"namespace": ns, "name": name},
		}
	}
	cases := []struct {
		name string
		a, b map[string]any
		want int
	}{
		{"kind", mkDoc("v1", "ConfigMap", "a", "x"), mkDoc("v1", "Secret", "a", "x"), -1},
		{"group", mkDoc("a.example/v1", "CM", "z", "x"), mkDoc("z.example/v1", "CM", "a", "x"), -1},
		{"core", mkDoc("v1", "CM", "z", "x"), mkDoc("a.example/v1", "CM", "a", "x"), -1},
		{"namespace", mkDoc("v1", "CM", "a", "x"), mkDoc("v1", "CM", "b", "x"), -1},
		{"name", mkDoc("v1", "CM", "a", "x"), mkDoc("v1", "CM", "a", "y"), -1},
		{"same", mkDoc("v1", "CM", "a", "x"), mkDoc("v1", "CM", "a", "x"), 0},
		{"version", mkDoc("a.example/v1", "CM", "a", "x"), mkDoc("a.example/v2", "CM", "a", "x"), 0},
		{"permissive group", mkDoc("a.example/v1/extra", "CM", "a", "x"), mkDoc("a.example/v2", "CM", "a", "x"), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := compareDocs(tc.a, tc.b)
			assert.ErrorIf(
				t,
				(got < 0) != (tc.want < 0) || (got == 0) != (tc.want == 0),
				"compareDocs(%v, %v) = %d, want sign of %d",
				tc.a,
				tc.b,
				got,
				tc.want,
			)
		})
	}
	first := mkDoc("a.example/v1", "CM", "ns", "shared")
	second := mkDoc("a.example/v2", "CM", "ns", "shared")
	foreign := mkDoc("z.example/v1", "CM", "ns", "shared")
	core := mkDoc("v1", "CM", "ns", "shared")
	docs := []map[string]any{foreign, second, core, first}
	for range 2 {
		assert.Diff(t, emissionDocs(docs, &buildFlags{}), []map[string]any{core, second, first, foreign})
		assert.Diff(t, docs, []map[string]any{foreign, second, core, first})
	}
}

// TestEmissionDocs_OnlyKinds drops every non-CRD doc.
func TestEmissionDocs_OnlyKinds(t *testing.T) {
	t.Parallel()
	docs := []map[string]any{
		{"kind": "ConfigMap"},
		{"kind": "CustomResourceDefinition"},
		{"kind": "Secret"},
		{"kind": "CustomResourceDefinition"},
	}
	out := emissionDocs(docs, &buildFlags{onlyKinds: []string{manifest.KindCustomResourceDefinition}})
	assert.ErrorIf(t, docs[2]["kind"] != "Secret", "emissionDocs mutated the input: %+v", docs)
	assert.ErrorIf(t, len(out) != 2, "expected 2 CRDs, got %d: %+v", len(out), out)
	for _, d := range out {
		assert.ErrorIf(t, d["kind"] != "CustomResourceDefinition", "non-CRD slipped through: %v", d)
	}
}

func TestEmissionDocs_EmptyOnNoCRDs(t *testing.T) {
	t.Parallel()
	docs := []map[string]any{{"kind": "ConfigMap"}, {"kind": "Secret"}}
	{
		out := emissionDocs(
			docs,
			&buildFlags{onlyKinds: []string{manifest.KindCustomResourceDefinition}},
		)
		assert.ErrorIf(t, len(
			out,
		) != 0, "expected no docs for no-CRD input, got %+v", out)
	}
}
