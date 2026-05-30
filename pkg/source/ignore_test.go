package source_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source"
)

// writeFile drops a one-byte marker file at dir/rel for ignore-walk
// fixtures — contents are irrelevant, only presence matters.
func writeFile(t *testing.T, dir, rel string) {
	t.Helper()
	testutil.WriteFile(t, dir, rel, "x")
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

// TestApplyIgnoreNoDefaults_PreservesVCSAndExtensions locks the
// Bucket-flavored ignore variant: when ApplyIgnoreNoDefaults runs
// with a nil spec.ignore, files that the default-matcher would
// exclude (VCS-named, image extensions, .flux.yaml) are left in
// place. Mirrors upstream source-controller's bucket_controller.go
// using sourceignore.NewMatcher instead of NewDefaultMatcher —
// buckets can legitimately carry these as regular objects.
func TestApplyIgnoreNoDefaults_PreservesVCSAndExtensions(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "image.jpg")
	writeFile(t, root, "kept/.flux.yaml")
	writeFile(t, root, "kept/keep.txt")
	writeFile(t, root, ".sops.yaml")

	if err := source.ApplyIgnoreNoDefaults(root, nil); err != nil {
		t.Fatalf("ApplyIgnoreNoDefaults: %v", err)
	}

	for _, rel := range []string{"image.jpg", "kept/.flux.yaml", "kept/keep.txt", ".sops.yaml"} {
		if !exists(t, filepath.Join(root, rel)) {
			t.Errorf("ApplyIgnoreNoDefaults stripped %q which Bucket should preserve", rel)
		}
	}
}

// TestApplyIgnoreNoDefaults_HonorsUserSpecIgnore confirms that the
// no-defaults variant still applies user-supplied spec.ignore.
func TestApplyIgnoreNoDefaults_HonorsUserSpecIgnore(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "image.jpg")
	writeFile(t, root, "kept.txt")
	patterns := "*.jpg"

	if err := source.ApplyIgnoreNoDefaults(root, &patterns); err != nil {
		t.Fatalf("ApplyIgnoreNoDefaults: %v", err)
	}
	if exists(t, filepath.Join(root, "image.jpg")) {
		t.Error("user-supplied *.jpg pattern was not honored")
	}
	if !exists(t, filepath.Join(root, "kept.txt")) {
		t.Error("kept.txt was stripped despite not matching user pattern")
	}
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

// Regression: m00nwtchr's tekton-operator / garage GitRepositories use
//
//	/*
//	!/charts/tekton-operator/
//
// to extract a single chart subdir from a larger repo. The matcher
// returns match=true for `charts/` but match=false for files under
// `charts/tekton-operator/`. The previous walk SkipDir'd on `charts/`
// and never gave the descendants a chance to be re-included, wiping
// the chart subpath entirely.
func TestApplyIgnore_ReIncludeUnderExcludedParent(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "README.md")
	writeFile(t, root, "docs/index.md")
	writeFile(t, root, "charts/tekton-operator/Chart.yaml")
	writeFile(t, root, "charts/tekton-operator/values.yaml")
	writeFile(t, root, "charts/tekton-other/Chart.yaml")

	patterns := "/*\n!/charts/tekton-operator/\n"
	if err := source.ApplyIgnore(root, &patterns); err != nil {
		t.Fatalf("ApplyIgnore: %v", err)
	}
	if !exists(t, filepath.Join(root, "charts/tekton-operator/Chart.yaml")) {
		t.Errorf("re-included file must survive")
	}
	if !exists(t, filepath.Join(root, "charts/tekton-operator/values.yaml")) {
		t.Errorf("re-included sibling file must survive")
	}
	if exists(t, filepath.Join(root, "README.md")) {
		t.Errorf("root-level file should be removed")
	}
	if exists(t, filepath.Join(root, "docs/index.md")) {
		t.Errorf("non-re-included subtree should be removed")
	}
	if exists(t, filepath.Join(root, "charts/tekton-other/Chart.yaml")) {
		t.Errorf("sibling subdir of re-included path should be removed")
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
