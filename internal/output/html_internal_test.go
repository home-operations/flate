package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestEncoderLines_PreservesStatus(t *testing.T) {
	t.Parallel()
	lines := []LineChange{
		{
			Resource: Subject{
				ID: manifest.NamedResource{Group: "example.test", Kind: "Widget", Namespace: "ns", Name: "same"},
			},
			Parent: Subject{
				ID: manifest.NamedResource{Group: "parents.test", Kind: "Producer", Namespace: "ns", Name: "same"},
			},
			From: "key: old\n",
			To:   "key: new\n",
		},
	}
	names := PrepareNames([]string{""}, [][]manifest.NamedResource{LineSubjects(lines, FormatHTML)}, nil)
	NameLines(lines, names[0])
	var buf bytes.Buffer
	assert.NoError(t, (Encoder{Writer: &buf, Format: FormatHTML}).Lines(lines))
	{
		out := buf.String()
		assert.FatalIf(t, !strings.Contains(out, "1 changed") ||
			!strings.Contains(out, `<span class="status status-"></span>`) ||
			!strings.Contains(out, "Widget ns/same") ||
			!strings.Contains(
				out,
				"Producer ns/same",
			), "HTML must preserve prepared empty status for unequal YAML: %s", out)
	}
}

func TestRuneDiffRange(t *testing.T) {
	t.Parallel()
	// a value edit in the middle: only the differing run is flagged on each side.
	if lo, hi, _, _ := runeDiffRange("v1.19.6", "v1.20.0"); string([]rune("v1.19.6")[lo:hi]) != "19.6" {
		t.Errorf(`middle edit: got %q, want "19.6"`, string([]rune("v1.19.6")[lo:hi]))
	}
	// a suffix removed: the left flags the removed run, the right flags nothing.
	aLo, aHi, bLo, bHi := runeDiffRange("name: ceph-csi-controller-manager", "name: ceph-csi")
	{
		got := string([]rune("name: ceph-csi-controller-manager")[aLo:aHi])
		assert.ErrorIf(
			t,
			got != "-controller-manager",
			`suffix removed (left): got %q, want "-controller-manager"`,
			got,
		)
	}
	assert.ErrorIf(t, bHi != bLo, "suffix removed (right): want empty range, got [%d,%d)", bLo, bHi)
	// no shared prefix or suffix: nothing flagged, so the line is just tinted.
	{
		lo, hi, _, _ := runeDiffRange("abc", "xyz")
		assert.ErrorIf(t, lo != 0 || hi != 0, "unrelated lines: want empty range, got [%d,%d)", lo, hi)
	}
}
