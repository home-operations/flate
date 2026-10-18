package harness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestdataPath locates a fixture beneath the repository's testdata directory.
func TestdataPath(t *testing.T, sub string) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for cur := wd; cur != "/" && cur != "."; cur = filepath.Dir(cur) {
		if _, err := os.Stat(filepath.Join(cur, "go.mod")); err == nil {
			return filepath.Join(cur, "testdata", sub)
		}
	}
	t.Fatal("could not locate repo root from " + wd)
	return ""
}

// MutateFile replaces the first oldStr in path and fails if the sentinel is absent.
func MutateFile(t *testing.T, path, oldStr, newStr string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mutated := strings.Replace(string(body), oldStr, newStr, 1)
	if mutated == string(body) {
		t.Fatalf("sentinel %q not found in %s", oldStr, path)
	}
	if err := os.WriteFile(path, []byte(mutated), 0o600); err != nil {
		t.Fatal(err)
	}
}
