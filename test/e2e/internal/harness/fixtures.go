package harness

import (
	"os"
	"strings"
	"testing"
)

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
