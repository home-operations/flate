package diff

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
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
		Parent: Parent{NamedResource: testutil.HelmReleaseID(ns, key)},
	}
}

func unified(t *testing.T, left, right []Doc) string {
	t.Helper()
	out, _, err := RenderDocs(left, right, Options{Format: FormatDiff})
	assert.FatalIf(t, err != nil, "RenderDocs(diff): %v", err)
	return string(out)
}

// TestUnified_ValueChange pins the FormatDiff body shape: a standard
// unified diff with `--- `/`+++ ` labels naming the resource and the
// changed value on `-`/`+` lines.
func TestUnified_ValueChange(t *testing.T) {
	t.Parallel()
	s := unified(t, []Doc{cm("a", "ns", "owner", "v1")}, []Doc{cm("a", "ns", "owner", "v2")})
	for _, want := range []string{"--- ConfigMap ns/a", "+++ ConfigMap ns/a", "@@ -", "-  k: v1", "+  k: v2"} {
		assert.ErrorIf(t, !strings.Contains(s, want), "missing %q in:\n%s", want, s)
	}
}

func TestUnified_NoChange(t *testing.T) {
	t.Parallel()
	d := cm("a", "ns", "owner", "v1")
	{
		s := unified(t, []Doc{d}, []Doc{d})
		assert.ErrorIf(t, s != "", "identical inputs should render empty, got:\n%s", s)
	}
}

// TestUnified_AddedAndRemoved pins that a resource present on only one
// side renders as a wholesale add/remove block, each attributed to its
// own resource label.
func TestUnified_AddedAndRemoved(t *testing.T) {
	t.Parallel()
	s := unified(t, []Doc{cm("a", "", "owner", "v")}, []Doc{cm("b", "", "owner", "v")})
	assert.ErrorIf(
		t,
		!strings.Contains(s, "ConfigMap a") || !strings.Contains(s, "ConfigMap b"),
		"both resource labels should appear:\n%s",
		s,
	)
	assert.ErrorIf(
		t,
		!strings.Contains(s, "-kind: ConfigMap"),
		"removed resource should show full content with leading `-`:\n%s",
		s,
	)
	assert.ErrorIf(
		t,
		!strings.Contains(s, "+kind: ConfigMap"),
		"added resource should show full content with leading `+`:\n%s",
		s,
	)
}

// TestUnified_BlankLineBetweenDocs pins that consecutive document bodies
// are separated by a blank line so the next `--- ` header is easy to
// spot when scanning multi-resource output.
func TestUnified_BlankLineBetweenDocs(t *testing.T) {
	t.Parallel()
	left := []Doc{cm("a", "ns", "owner", "v1"), cm("b", "ns", "owner", "v1")}
	right := []Doc{cm("a", "ns", "owner", "v2"), cm("b", "ns", "owner", "v2")}
	s := unified(t, left, right)
	assert.ErrorIf(
		t,
		!strings.Contains(s, "\n\n--- ConfigMap ns/b"),
		"second doc header should be preceded by a blank line:\n%s",
		s,
	)
	assert.ErrorIf(t, strings.HasPrefix(s, "\n"), "output should not start with a blank line:\n%s", s)
	assert.ErrorIf(
		t,
		strings.HasSuffix(s, "\n\n"),
		"output should end with a single newline, not a trailing blank line:\n%s",
		s,
	)
}

// TestUnified_Deletion verifies a wholly-removed resource shows its full
// content (so reviewers don't have to chase the original separately).
func TestUnified_Deletion(t *testing.T) {
	t.Parallel()
	s := unified(t, []Doc{cm("a", "ns", "owner", "v1")}, nil)
	for _, want := range []string{"--- ConfigMap ns/a", "-kind: ConfigMap", "-  k: v1"} {
		assert.ErrorIf(t, !strings.Contains(s, want), "missing %q in:\n%s", want, s)
	}
}
