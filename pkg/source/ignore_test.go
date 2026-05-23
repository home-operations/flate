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

// TestApplyIgnore_AppliesDefaultsWhenIgnoreNil asserts the source-
// controller default exclusions (VCS + ExcludeExt + ExcludeCI +
// ExcludeExtra) fire even when spec.ignore is nil, matching real
// Flux's artifact-build behavior.
func TestApplyIgnore_AppliesDefaultsWhenIgnoreNil(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "keep.yaml")
	writeFile(t, root, "docs/readme.md")
	writeFile(t, root, ".git/HEAD")
	writeFile(t, root, ".github/workflows/ci.yml")
	writeFile(t, root, "img.png")
	writeFile(t, root, ".sops.yaml")

	if err := source.ApplyIgnore(root, nil); err != nil {
		t.Fatalf("ApplyIgnore: %v", err)
	}
	for _, gone := range []string{".git", ".github", "img.png", ".sops.yaml"} {
		if exists(t, filepath.Join(root, gone)) {
			t.Errorf("default exclude should remove %s", gone)
		}
	}
	for _, kept := range []string{"keep.yaml", "docs/readme.md"} {
		if !exists(t, filepath.Join(root, kept)) {
			t.Errorf("expected %s to remain", kept)
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

func TestApplyIgnore_DoubleStarMatchesAtAnyDepth(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "vendor/lib/x")
	writeFile(t, root, "apps/a/vendor/lib/x")
	writeFile(t, root, "apps/b/manifest.yaml")

	patterns := "**/vendor/\n"
	if err := source.ApplyIgnore(root, &patterns); err != nil {
		t.Fatalf("ApplyIgnore: %v", err)
	}
	if exists(t, filepath.Join(root, "vendor")) {
		t.Errorf("**/vendor must remove root-level vendor")
	}
	if exists(t, filepath.Join(root, "apps/a/vendor")) {
		t.Errorf("**/vendor must remove nested vendor")
	}
	if !exists(t, filepath.Join(root, "apps/b/manifest.yaml")) {
		t.Errorf("non-matching path should remain")
	}
}

func TestApplyIgnore_LeadingSlashAnchorsToRoot(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "config.yaml")
	writeFile(t, root, "subdir/config.yaml")

	// /config.yaml should match only the root-level file, not the
	// nested one. gitignore semantics: a leading "/" anchors.
	patterns := "/config.yaml\n"
	if err := source.ApplyIgnore(root, &patterns); err != nil {
		t.Fatalf("ApplyIgnore: %v", err)
	}
	if exists(t, filepath.Join(root, "config.yaml")) {
		t.Errorf("root-anchored /config.yaml must remove root file")
	}
	if !exists(t, filepath.Join(root, "subdir/config.yaml")) {
		t.Errorf("root-anchored /config.yaml must NOT remove nested file")
	}
}
