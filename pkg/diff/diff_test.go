package diff

import (
	"strings"
	"testing"
)

func cm(name, ns, key, value string) Doc {
	return Doc{
		Manifest: map[string]any{
			"kind": "ConfigMap",
			"metadata": map[string]any{
				"name": name, "namespace": ns,
			},
			"data": map[string]any{"k": value},
		},
		Parent: Parent{Kind: "HelmRelease", Namespace: ns, Name: key},
	}
}

func unified(t *testing.T, left, right []Doc) string {
	t.Helper()
	out, err := RenderDocs(left, right, Options{Format: FormatDiff})
	if err != nil {
		t.Fatalf("RenderDocs(diff): %v", err)
	}
	return string(out)
}

// TestUnified_ValueChange pins the FormatDiff body shape: a standard
// unified diff with `--- `/`+++ ` labels naming the resource and the
// changed value on `-`/`+` lines.
func TestUnified_ValueChange(t *testing.T) {
	s := unified(t, []Doc{cm("a", "ns", "owner", "v1")}, []Doc{cm("a", "ns", "owner", "v2")})
	for _, want := range []string{"--- ConfigMap ns/a", "+++ ConfigMap ns/a", "@@ -", "-  k: v1", "+  k: v2"} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in:\n%s", want, s)
		}
	}
}

func TestUnified_NoChange(t *testing.T) {
	d := cm("a", "ns", "owner", "v1")
	if s := unified(t, []Doc{d}, []Doc{d}); s != "" {
		t.Errorf("identical inputs should render empty, got:\n%s", s)
	}
}

// TestUnified_AddedAndRemoved pins that a resource present on only one
// side renders as a wholesale add/remove block, each attributed to its
// own resource label.
func TestUnified_AddedAndRemoved(t *testing.T) {
	s := unified(t, []Doc{cm("a", "", "owner", "v")}, []Doc{cm("b", "", "owner", "v")})
	if !strings.Contains(s, "ConfigMap a") || !strings.Contains(s, "ConfigMap b") {
		t.Errorf("both resource labels should appear:\n%s", s)
	}
	if !strings.Contains(s, "-kind: ConfigMap") {
		t.Errorf("removed resource should show full content with leading `-`:\n%s", s)
	}
	if !strings.Contains(s, "+kind: ConfigMap") {
		t.Errorf("added resource should show full content with leading `+`:\n%s", s)
	}
}

// TestUnified_BlankLineBetweenDocs pins that consecutive document bodies
// are separated by a blank line so the next `--- ` header is easy to
// spot when scanning multi-resource output.
func TestUnified_BlankLineBetweenDocs(t *testing.T) {
	left := []Doc{cm("a", "ns", "owner", "v1"), cm("b", "ns", "owner", "v1")}
	right := []Doc{cm("a", "ns", "owner", "v2"), cm("b", "ns", "owner", "v2")}
	s := unified(t, left, right)
	if !strings.Contains(s, "\n\n--- ConfigMap ns/b") {
		t.Errorf("second doc header should be preceded by a blank line:\n%s", s)
	}
	if strings.HasPrefix(s, "\n") {
		t.Errorf("output should not start with a blank line:\n%s", s)
	}
	if strings.HasSuffix(s, "\n\n") {
		t.Errorf("output should end with a single newline, not a trailing blank line:\n%s", s)
	}
}

// TestUnified_Deletion verifies a wholly-removed resource shows its full
// content (so reviewers don't have to chase the original separately).
func TestUnified_Deletion(t *testing.T) {
	s := unified(t, []Doc{cm("a", "ns", "owner", "v1")}, nil)
	for _, want := range []string{"--- ConfigMap ns/a", "-kind: ConfigMap", "-  k: v1"} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in:\n%s", want, s)
		}
	}
}
