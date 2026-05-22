package source_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/pkg/source"
)

func writeFile(t *testing.T, dir, rel string) {
	t.Helper()
	p := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(p), err)
	}
	if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
}

func exists(t *testing.T, p string) bool {
	t.Helper()
	_, err := os.Stat(p)
	if err == nil {
		return true
	}
	if !os.IsNotExist(err) {
		t.Fatalf("stat %s: %v", p, err)
	}
	return false
}

func TestApplyIgnore_NilOrEmptyIsNoop(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "keep.yaml")
	writeFile(t, root, "docs/readme.md")

	for _, ig := range []*string{nil, ptr("")} {
		if err := source.ApplyIgnore(root, ig); err != nil {
			t.Fatalf("ApplyIgnore: %v", err)
		}
	}
	for _, rel := range []string{"keep.yaml", "docs/readme.md"} {
		if !exists(t, filepath.Join(root, rel)) {
			t.Errorf("expected %s to remain", rel)
		}
	}
}

func TestApplyIgnore_DeletesMatching(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "app/deployment.yaml")
	writeFile(t, root, "app/values.yaml")
	writeFile(t, root, "docs/readme.md")
	writeFile(t, root, "README.md")

	patterns := "*.md\ndocs/\n"
	if err := source.ApplyIgnore(root, &patterns); err != nil {
		t.Fatalf("ApplyIgnore: %v", err)
	}
	if exists(t, filepath.Join(root, "README.md")) {
		t.Errorf("*.md should be removed")
	}
	if exists(t, filepath.Join(root, "docs")) {
		t.Errorf("docs/ should be removed as a tree")
	}
	if !exists(t, filepath.Join(root, "app/deployment.yaml")) {
		t.Errorf("unmatched files should remain")
	}
	if !exists(t, filepath.Join(root, "app/values.yaml")) {
		t.Errorf("unmatched files should remain")
	}
}

func TestApplyIgnore_GitignoreSyntax(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "infra/vendor/bigfile")
	writeFile(t, root, "infra/important.yaml")
	writeFile(t, root, ".git/HEAD")

	patterns := "vendor/\n.git/\n"
	if err := source.ApplyIgnore(root, &patterns); err != nil {
		t.Fatalf("ApplyIgnore: %v", err)
	}
	if exists(t, filepath.Join(root, "infra/vendor")) {
		t.Errorf("vendor/ should be removed")
	}
	if exists(t, filepath.Join(root, ".git")) {
		t.Errorf(".git/ should be removed")
	}
	if !exists(t, filepath.Join(root, "infra/important.yaml")) {
		t.Errorf("non-ignored file should remain")
	}
}

func TestApplyIgnore_CommentsAndBlankLines(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.tmp")
	writeFile(t, root, "b.yaml")

	patterns := "# strip temp files\n*.tmp\n\n   \n"
	if err := source.ApplyIgnore(root, &patterns); err != nil {
		t.Fatalf("ApplyIgnore: %v", err)
	}
	if exists(t, filepath.Join(root, "a.tmp")) {
		t.Errorf("a.tmp should be removed")
	}
	if !exists(t, filepath.Join(root, "b.yaml")) {
		t.Errorf("b.yaml should remain")
	}
}

func ptr(s string) *string { return &s }
