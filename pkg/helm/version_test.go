package helm

import (
	"os"
	"slices"
	"strings"
	"testing"
)

func TestBundledKubeVersion_MatchesDependency(t *testing.T) {
	data, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(data))
	i := slices.Index(fields, "k8s.io/api")
	if i < 0 || i+1 >= len(fields) {
		t.Fatal("k8s.io/api dependency missing")
	}
	want := "1." + strings.TrimPrefix(fields[i+1], "v0.")
	if fallbackKubeVersion != want {
		t.Fatalf("fallback=%q, want %q", fallbackKubeVersion, want)
	}
}
