package kustomization

import "testing"

func BenchmarkSubstituteDoc(b *testing.B) {
	for _, tc := range []struct {
		name  string
		key   string
		value string
	}{
		{"no_substitution", "script", "echo literal"},
		{"bare_dollar", "script", "echo $PID"},
		{"double_dollar", "script", "echo $$PID"},
		{"key_reference", "${VAR}", "literal"},
		{"value_reference", "script", "${VAR}"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			doc := map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "example", "namespace": "default"},
				"data":       map[string]any{tc.key: tc.value},
			}
			vars := map[string]string{"VAR": "example"}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := substituteDoc(doc, vars, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
