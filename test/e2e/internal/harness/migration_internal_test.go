package harness

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// Each migration removes its files from this fixed list. New files receive no exemption.
const unmigratedFiles = `artifactgenerator_input_test.go
artifactgenerator_test.go
crds/metadata_test.go
crds/missing_crds_test.go
crds/resourceset_test.go
scope/authoritative_manifest_failures_test.go
scope/bootstrap_namespace_projection_test.go
scope/cache_diff_test.go
scope/child_kustomization_identity_test.go
scope/component_propagation_test.go
scope/disabled_application_sides_test.go
scope/disabled_apps_test.go
scope/entry_packages_test.go
scope/entry_prerequisite_sides_test.go
scope/git_fixture_test.go
scope/ignored_substitution_test.go
scope/inputs_test.go
scope/literal_root_layers_test.go
scope/local_chart_inputs_test.go
scope/malformed_manifest_scope_test.go
scope/malformed_manifest_transitions_test.go
scope/namespace_diff_test.go
scope/namespace_orphan_authority_test.go
scope/namespace_orphans_test.go
scope/optional_input_test.go
scope/orphan_order_test.go
scope/orphan_selection_test.go
scope/runtime_owner_namespace_test.go
scope/selected_root_classification_test.go
scope/selected_root_diff_test.go
scope/selected_root_ownership_test.go
scope/selection_auxiliary_test.go
scope/selection_test.go
scope/skip_kind_output_test.go
scope/unclaimed_test.go
scope/unowned_entry_authority_test.go
scope/unreadable_manifest_scope_test.go
sourceref/checkout_preservation_test.go
sourceref/discovery_index_test.go
sourceref/empty_source_path_test.go
sourceref/held_source_failures_test.go
sourceref/inputs_test.go
sourceref/late_source_discovery_test.go
sourceref/missing_source_kinds_test.go
sourceref/path_origin_store_test.go
sourceref/pinned_diff_revisions_test.go
sourceref/pinned_producer_inputs_test.go
sourceref/pinned_root_diff_test.go
sourceref/pinned_source_ownership_test.go
sourceref/source_diff_assertions_test.go
sourceref/source_fixture_test.go
sourceref/source_ref_test.go
sourceref/source_transport_fixtures_test.go
sourceref/warnings_test.go
substitution/data_reader_test.go
substitution/depends_on_test.go
substitution/patches_sops_internal_test.go
substitution/patches_test.go
substitution/produced_dependency_edits_test.go
substitution/sops_sops_internal_test.go
substitution/substitution_producer_test.go`

func TestMigrationGuard_E2EBoundary(t *testing.T) {
	t.Parallel()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate end-to-end sources")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..")
	pending := make(map[string]bool)
	for file := range strings.FieldsSeq(unmigratedFiles) {
		pending[file] = true
	}
	err := filepath.WalkDir(root, func(file string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() == "internal" {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(file, ".go") {
			return nil
		}
		assertMigrationFile(t, root, file, pending)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for file := range pending {
		t.Errorf("remove absent file %s from unmigratedFiles", file)
	}
}

func assertMigrationFile(t *testing.T, root, file string, pending map[string]bool) {
	t.Helper()
	rel, err := filepath.Rel(root, file)
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.ToSlash(rel)
	set := token.NewFileSet()
	source, err := parser.ParseFile(set, file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	violations := migrationViolations(source)
	if pending[name] {
		delete(pending, name)
		if len(violations) == 0 {
			t.Errorf("remove migrated file %s from unmigratedFiles", name)
		}
		return
	}
	for _, violation := range violations {
		t.Errorf("%s: %s", name, violation)
	}
}

const harnessPath = "github.com/home-operations/flate/test/e2e/internal/harness"

func migrationViolations(file *ast.File) []string {
	imports := make(map[string]string)
	var violations []string
	for _, spec := range file.Imports {
		imported, _ := strconv.Unquote(spec.Path.Value)
		name := path.Base(imported)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = imported
		first, _, _ := strings.Cut(imported, "/")
		external := strings.Contains(first, ".") && imported != harnessPath
		stateful := imported == "os" || strings.HasPrefix(imported, "os/") || imported == "io/fs" ||
			imported == "net" || strings.HasPrefix(imported, "net/") || imported == "syscall" || imported == "unsafe"
		if name == "." || name == "_" || external || stateful {
			violations = append(violations, "fixture state and execution belong to the harness: "+imported)
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if violation := migrationNodeViolation(node, imports); violation != "" {
			violations = append(violations, violation)
		}
		return true
	})
	return violations
}

func migrationNodeViolation(node ast.Node, imports map[string]string) string {
	switch n := node.(type) {
	case *ast.BasicLit:
		if n.Kind != token.STRING {
			break
		}
		value, _ := strconv.Unquote(n.Value)
		if strings.Contains(value, "apiVersion:") || strings.Contains(value, `"apiVersion"`) ||
			strings.Contains(value, "{{") {
			return "manifest and template bodies belong in testdata"
		}
	case *ast.SelectorExpr:
		base, ok := n.X.(*ast.Ident)
		if !ok {
			break
		}
		imported := imports[base.Name]
		if imported == harnessPath && n.Sel.Name != "Open" && n.Sel.Name != "Result" {
			return "use Open, Case.Run and Case.Want"
		}
		if (imported == "strings" || imported == "bytes") && strings.HasPrefix(n.Sel.Name, "Contains") {
			return "compare complete result bytes"
		}
		switch n.Sel.Name {
		case "TempDir", "Chdir", "Setenv":
			return "fixture state belongs to the materializer"
		}
	}
	return ""
}

func TestMigrationGuard_Rules(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, source string
		valid        bool
	}{
		{"harness alias", `package example; import h "github.com/home-operations/flate/test/e2e/internal/harness"; func f() { c := h.Open(nil,"entry-root","literal-app"); c.Run(nil); c.Want(nil) }`, true},
		{"writer alias", `package example; import files "os"; func f() { files.WriteFile("repo",nil,0) }`, false},
		{"direct CLI", `package example; import app "github.com/home-operations/flate/internal/cli"; var run = app.Run`, false},
		{"inline YAML", "package example; const manifest = `apiVersion: v1\\nkind: ConfigMap`", false},
		{"inline JSON", "package example; const manifest = `{" + `"apiVersion":"v1","kind":"ConfigMap"` + "}`", false},
		{"template", `package example; const template = "{{ .Values.value }}"`, false},
		{"process", `package example; import "os/exec"; var command = exec.Command`, false},
		{"old harness", `package example; import h "github.com/home-operations/flate/test/e2e/internal/harness"; var run = h.RunCLI`, false},
		{"substring", `package example; import "strings"; var compare = strings.Contains`, false},
		{"mutation", `package example; func f(t *T) { t.Setenv("FLATE_PATH", "repo") }`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			file, err := parser.ParseFile(token.NewFileSet(), "case.go", tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(migrationViolations(file)) == 0; got != tc.valid {
				t.Fatalf("accepted=%t, want %t", got, tc.valid)
			}
		})
	}
}
