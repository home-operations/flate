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
const unmigratedFiles = ""

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
