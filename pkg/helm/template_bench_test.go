package helm

import (
	"os"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func BenchmarkTemplate_SchemaFailure(b *testing.B) {
	dir := stageBenchChartDir(b)
	testutil.WriteFile(b, dir, "mychart/values.schema.json", `{
  "type":"object","additionalProperties":false,
  "properties":{"alpha":{"type":"string"},"zeta":{"type":"string"},
    "nested":{"allOf":[{"type":"object","additionalProperties":false}]}}
}`)
	cli := NewClient(cachetest.New(b, "", sourcecache.New))
	input := RenderOptions{Sources: benchChartResolver(b, dir, os.DirFS(dir))}
	hr := newHR()
	values := map[string]any{"alpha": nil, "zeta": nil, "nested": map[string]any{"last": true, "first": true}}
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := cli.Template(b.Context(), hr, values, input); err == nil {
			b.Fatal("expected schema validation failure")
		}
	}
}
