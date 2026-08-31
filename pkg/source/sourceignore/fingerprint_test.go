package sourceignore

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, root, rel, body string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func fingerprint(t *testing.T, root string) string {
	t.Helper()
	fp, err := Fingerprint(root)
	if err != nil {
		t.Fatalf("Fingerprint: %v", err)
	}
	return fp
}

func TestFingerprint(t *testing.T) {
	root := t.TempDir()
	write(t, root, "a.yaml", "a: 1\n")
	base := fingerprint(t, root)

	if fingerprint(t, root) != base {
		t.Fatal("fingerprint must be stable across calls")
	}

	write(t, root, "unrelated.yaml", "b: 2\n")
	if fingerprint(t, root) != base {
		t.Fatal("a file that is not a .sourceignore must not change the fingerprint")
	}

	write(t, root, ".sourceignore", "b.yaml\n")
	withRoot := fingerprint(t, root)
	if withRoot == base {
		t.Fatal("adding a root .sourceignore must change the fingerprint")
	}

	write(t, root, ".sourceignore", "c.yaml\n")
	edited := fingerprint(t, root)
	if edited == withRoot {
		t.Fatal("editing .sourceignore in place must change the fingerprint")
	}

	write(t, root, "sub/.sourceignore", "d.yaml\n")
	nested := fingerprint(t, root)
	if nested == edited {
		t.Fatal("adding a nested .sourceignore must change the fingerprint")
	}

	write(t, root, ".git/.sourceignore", "ignored\n")
	if fingerprint(t, root) != nested {
		t.Fatal(".git/ must be skipped, as LoadIgnorePatterns skips it")
	}

	if err := os.Remove(filepath.Join(root, "sub", ".sourceignore")); err != nil {
		t.Fatal(err)
	}
	if fingerprint(t, root) != edited {
		t.Fatal("removing a .sourceignore must restore the earlier fingerprint")
	}
}
