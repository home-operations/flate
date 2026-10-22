package artifactgenerator

import (
	"fmt"
	"strings"
	"testing"
)

func TestArtifactGenerator_Validation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		edit  func(map[string]any, map[string]any, map[string]any, map[string]any)
		valid bool
	}{
		{"valid", func(_, _, _, _ map[string]any) {}, true},
		{"sources_required", func(spec, _, _, _ map[string]any) { delete(spec, "sources") }, false},
		{"sources_empty", func(spec, _, _, _ map[string]any) { spec["sources"] = []any{} }, false},
		{"sources_type", func(spec, _, _, _ map[string]any) { spec["sources"] = "repo" }, false},
		{"source_type", func(spec, _, _, _ map[string]any) { spec["sources"] = []any{"repo"} }, false},
		{"path_pattern_type", func(spec, _, _, _ map[string]any) { spec["pathPattern"] = 12 }, false},
		{"sources_at_cap", func(spec, _, _, _ map[string]any) {
			var sources []any
			for i := range 1000 {
				sources = append(sources, map[string]any{"alias": fmt.Sprintf("repo%d", i), "name": "repo", "kind": "GitRepository"})
			}
			spec["sources"] = sources
		}, true},
		{"sources_over_cap", func(spec, _, _, _ map[string]any) {
			var sources []any
			for i := range 1001 {
				sources = append(sources, map[string]any{"alias": fmt.Sprintf("repo%d", i), "name": "repo", "kind": "GitRepository"})
			}
			spec["sources"] = sources
		}, false},
		{"duplicate_alias", func(spec, source, _, _ map[string]any) { spec["sources"] = []any{source, source} }, false},
		{"alias_at_cap", func(_, source, _, _ map[string]any) { source["alias"] = strings.Repeat("a", 63) }, true},
		{"alias_over_cap", func(_, source, _, _ map[string]any) { source["alias"] = strings.Repeat("a", 64) }, false},
		{"alias_required", func(_, source, _, _ map[string]any) { delete(source, "alias") }, false},
		{"alias_syntax", func(_, source, _, _ map[string]any) { source["alias"] = "UPPER" }, false},
		{"source_name_at_cap", func(_, source, _, _ map[string]any) { source["name"] = strings.Repeat("a", 253) }, true},
		{"source_name_over_cap", func(_, source, _, _ map[string]any) { source["name"] = strings.Repeat("a", 254) }, false},
		{"source_name_required", func(_, source, _, _ map[string]any) { delete(source, "name") }, false},
		{"namespace_at_cap", func(_, source, _, _ map[string]any) { source["namespace"] = strings.Repeat("a", 63) }, true},
		{"namespace_over_cap", func(_, source, _, _ map[string]any) { source["namespace"] = strings.Repeat("a", 64) }, false},
		{"explicit_empty_namespace", func(_, source, _, _ map[string]any) { source["namespace"] = "" }, false},
		{"unsupported_kind", func(_, source, _, _ map[string]any) { source["kind"] = "HelmRepository" }, false},
		{"oci_source", func(_, source, _, _ map[string]any) { source["kind"] = "OCIRepository" }, true},
		{"bucket_source", func(_, source, _, _ map[string]any) { source["kind"] = "Bucket" }, true},
		{"chart_source", func(_, source, _, _ map[string]any) { source["kind"] = "HelmChart" }, true},
		{"artifact_source", func(_, source, _, _ map[string]any) { source["kind"] = "ExternalArtifact" }, true},
		{"path_pattern_at_cap", func(spec, _, _, _ map[string]any) { spec["pathPattern"] = "@repo/" + strings.Repeat("a", 1018) }, true},
		{"path_pattern_over_cap", func(spec, _, _, _ map[string]any) { spec["pathPattern"] = strings.Repeat("a", 1025) }, false},
		{"artifacts_required", func(spec, _, _, _ map[string]any) { delete(spec, "artifacts") }, false},
		{"artifacts_type", func(spec, _, _, _ map[string]any) { spec["artifacts"] = "output" }, false},
		{"artifacts_at_cap", func(spec, _, _, op map[string]any) {
			var outputs []any
			for i := range 1000 {
				outputs = append(outputs, map[string]any{"name": fmt.Sprintf("output%d", i), "copy": []any{op}})
			}
			spec["artifacts"] = outputs
		}, true},
		{"artifacts_over_cap", func(spec, _, _, op map[string]any) {
			var outputs []any
			for i := range 1001 {
				outputs = append(outputs, map[string]any{"name": fmt.Sprintf("output%d", i), "copy": []any{op}})
			}
			spec["artifacts"] = outputs
		}, false},
		{"duplicate_template_name", func(spec, _, output, _ map[string]any) { spec["artifacts"] = []any{output, output} }, false},
		{"output_name_required", func(_, _, output, _ map[string]any) { delete(output, "name") }, false},
		{"output_name_at_cap", func(_, _, output, _ map[string]any) { output["name"] = strings.Repeat("a", 253) }, true},
		{"output_name_over_cap", func(_, _, output, _ map[string]any) { output["name"] = strings.Repeat("a", 254) }, false},
		{"copy_required", func(_, _, output, _ map[string]any) { delete(output, "copy") }, false},
		{"selector_at_cap", func(_, source, output, _ map[string]any) {
			source["alias"] = strings.Repeat("a", 63)
			output["revision"] = "@" + strings.Repeat("a", 63)
		}, true},
		{"selector_over_cap", func(_, _, output, _ map[string]any) { output["originRevision"] = "@" + strings.Repeat("a", 64) }, false},
		{"selector_prefix", func(_, _, output, _ map[string]any) { output["revision"] = "repo" }, false},
		{"unknown_revision", func(_, _, output, _ map[string]any) { output["revision"] = "@missing" }, false},
		{"absent_origin_alias", func(_, _, output, _ map[string]any) { output["originRevision"] = "@missing" }, true},
		{"from_required", func(_, _, _, op map[string]any) { delete(op, "from") }, false},
		{"from_at_cap", func(_, _, _, op map[string]any) { op["from"] = "@repo/" + strings.Repeat("a", 1018) }, true},
		{"from_over_cap", func(_, _, _, op map[string]any) { op["from"] = "@repo/" + strings.Repeat("a", 1019) }, false},
		{"to_required", func(_, _, _, op map[string]any) { delete(op, "to") }, false},
		{"to_at_cap", func(_, _, _, op map[string]any) { op["to"] = "@artifact/" + strings.Repeat("a", 1014) }, true},
		{"to_over_cap", func(_, _, _, op map[string]any) { op["to"] = "@artifact/" + strings.Repeat("a", 1015) }, false},
		{"exclude_at_cap", func(_, _, _, op map[string]any) { op["exclude"] = make([]string, 100) }, true},
		{"exclude_over_cap", func(_, _, _, op map[string]any) { op["exclude"] = make([]string, 101) }, false},
		{"exclude_pattern_at_cap", func(_, _, _, op map[string]any) { op["exclude"] = []string{strings.Repeat("a", 1024)} }, true},
		{"exclude_pattern_over_cap", func(_, _, _, op map[string]any) { op["exclude"] = []string{strings.Repeat("a", 1025)} }, false},
		{"exclude_glob_invalid", func(_, _, _, op map[string]any) { op["exclude"] = []string{"["} }, false},
		{"strategy_invalid", func(_, _, _, op map[string]any) { op["strategy"] = "Append" }, false},
		{"explicit_overwrite", func(_, _, _, op map[string]any) { op["strategy"] = "Overwrite" }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := map[string]any{"alias": "repo", "name": "repo", "kind": "GitRepository"}
			op := map[string]any{"from": "@repo/file", "to": "@artifact/file"}
			output := map[string]any{"name": "output", "copy": []any{op}}
			spec := map[string]any{"sources": []any{source}, "artifacts": []any{output}}
			tc.edit(spec, source, output, op)
			_, err := decodeSpec(spec)
			if (err == nil) != tc.valid {
				t.Fatalf("decodeSpec() = %v; valid=%v", err, tc.valid)
			}
			if (tc.name == "from_required" || tc.name == "to_required") && !strings.Contains(err.Error(), "copy operation outside API bounds") {
				t.Fatalf("required path diagnostic = %v", err)
			}
		})
	}
}

func TestValidateGlob_BraceBound(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, pattern string
		valid         bool
	}{
		{"at_cap", "{" + strings.Repeat("a,", 20) + "z}", true},
		{"over_cap", "{" + strings.Repeat("a,", 21) + "z}", false},
		{"escaped_commas", "{" + strings.Repeat(`a\,`, 21) + "z}", true},
		{"outside_braces", strings.Repeat("a,", 21), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := validateGlob(tc.pattern); (err == nil) != tc.valid {
				t.Fatalf("validateGlob() = %v; valid=%v", err, tc.valid)
			}
		})
	}
}
