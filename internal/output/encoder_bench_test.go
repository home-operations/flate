package output

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
)

func BenchmarkEncoder_Documents(b *testing.B) {
	small := make([]map[string]any, 100)
	deployments := make([]map[string]any, 20)
	for i := range small {
		small[i] = map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": fmt.Sprintf("config-%03d", i), "namespace": "apps"},
			"data":     map[string]any{"mode": "enabled", "count": i},
		}
	}
	for i := range deployments {
		deployments[i] = map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": fmt.Sprintf("app-%03d", i), "namespace": "apps"},
			"spec": map[string]any{
				"replicas": 3, "selector": map[string]any{"matchLabels": map[string]any{"app": "demo"}},
				"template": map[string]any{
					"metadata": map[string]any{"labels": map[string]any{"app": "demo"}},
					"spec": map[string]any{"containers": []any{map[string]any{
						"name": "app", "image": "example.test/demo:v1",
						"ports": []any{map[string]any{"containerPort": 8080}},
						"env":   []any{map[string]any{"name": "MODE", "value": "enabled"}},
					}}},
				},
			},
		}
	}
	large := []map[string]any{{
		"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "dashboard"},
		"data": map[string]any{
			"dashboard.json": "{\n" + strings.Repeat("  \"panel\": {\"z\": 1, \"a\": \"雪\"},\n", 2048) + "}\n",
			"script.sh":      strings.Repeat("echo 'a multiline configuration value'\n", 2048),
		},
	}}

	wide := map[string]any{}
	for i := range 512 {
		wide[fmt.Sprintf("key%03d", i)] = i
	}
	keyDoc := map[string]any{"value": map[any]any{int(1): "int---", int64(1): "int64-", uint64(1): "uint64", time.Date(2026, 10, 28, 12, 34, 56, 0, time.UTC): "utc---", time.Date(2026, 10, 28, 12, 34, 56, 0, time.FixedZone("", 0)): "offset", "\xff": "first-", "\xfe": "second"}}
	scalarDoc := map[string]any{"fraction": 1.25, "small": 1e-7, "large": 1e20, "time": time.Date(2026, 10, 28, 12, 34, 56, 123000000, time.UTC), "strings": []any{"true", "1e999", "a: b", "a\u0085\u0085b", "a\u2028b", "a\xffb", "first 🚨\nsecond\n", strings.Repeat("α ", 42) + "tail"}}
	keys, scalars := make([]map[string]any, 100), make([]map[string]any, 100)
	for i := range keys {
		keys[i] = keyDoc
		scalars[i] = scalarDoc
	}
	for _, tc := range []struct {
		name   string
		format Format
		groups []DocumentGroup
		direct bool
	}{
		{"small", FormatYAML, []DocumentGroup{{Documents: small}}, false},
		{"direct", FormatYAML, []DocumentGroup{{Documents: small}}, true},
		{"deployments", FormatYAML, []DocumentGroup{{Documents: deployments}}, false},
		{"large", FormatYAML, []DocumentGroup{{Documents: large}}, false},
		{"groups", FormatYAML, []DocumentGroup{{Cluster: "alpha", Documents: small}, {Cluster: "empty"}, {Cluster: "omega", Documents: deployments}}, false},
		{"json", FormatJSON, []DocumentGroup{{Documents: small}}, false},
		{"wide", FormatYAML, []DocumentGroup{{Documents: []map[string]any{wide}}}, false},
		{"keys", FormatYAML, []DocumentGroup{{Documents: keys}}, false},
		{"scalars", FormatYAML, []DocumentGroup{{Documents: scalars}}, false},
	} {
		for _, destination := range []string{"discard", "buffer"} {
			b.Run(tc.name+"/"+destination, func(b *testing.B) {
				var buf bytes.Buffer
				if err := (Encoder{Writer: &buf, Format: tc.format}).DocumentGroups(tc.groups); err != nil {
					b.Fatal(err)
				}
				length := buf.Len()
				count := 0
				for _, group := range tc.groups {
					count += len(group.Documents)
				}
				writer := io.Discard
				if destination == "buffer" {
					writer = &buf
				}
				encoder := Encoder{Writer: writer, Format: tc.format}
				b.ReportAllocs()
				b.SetBytes(int64(length))
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				for b.Loop() {
					buf.Reset()
					var err error
					if tc.direct {
						err = encoder.Documents(tc.groups[0].Documents)
					} else {
						err = encoder.DocumentGroups(tc.groups)
					}
					if err != nil {
						b.Fatal(err)
					}
					if destination == "buffer" && buf.Len() != length {
						b.Fatalf("output length = %d, want %d", buf.Len(), length)
					}
				}
				runtime.ReadMemStats(&after)
				allocations := float64(after.Mallocs-before.Mallocs) / float64(b.N)
				b.ReportMetric(allocations, "allocs/call")
				b.ReportMetric(allocations/float64(count), "allocs/doc")
				b.ReportMetric(float64(count), "docs/op")
				b.ReportMetric(float64(length), "output-B/op")
			})
		}
	}
}
