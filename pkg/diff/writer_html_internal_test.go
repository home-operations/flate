package diff

import (
	"bytes"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEncoderLines_PreservesStatus(t *testing.T) {
	t.Parallel()
	lines := []lineChange{
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
	names := PrepareNames([]string{""}, [][]manifest.NamedResource{lineInventory(lines, FormatHTML).Subjects}, nil)
	nameLines(lines, names.Groups[0])
	var buf bytes.Buffer
	assert.NoError(t, (encoder{Writer: &buf, Format: FormatHTML}).lines(lines))
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
	spans := runeDiffRange("v1.19.6", "v1.20.0")

	if string([]rune("v1.19.6")[spans.origLow:spans.origHigh]) != "19.6" {
		t.Errorf(`middle edit: got %q, want "19.6"`, string([]rune("v1.19.6")[spans.origLow:spans.origHigh]))
	}
	// a suffix removed: the left flags the removed run, the right flags nothing.
	spans2 := runeDiffRange("name: ceph-csi-controller-manager", "name: ceph-csi")

	{
		got := string([]rune("name: ceph-csi-controller-manager")[spans2.origLow:spans2.origHigh])
		assert.ErrorIf(
			t,
			got != "-controller-manager",
			`suffix removed (left): got %q, want "-controller-manager"`,
			got,
		)
	}
	assert.ErrorIf(
		t,
		spans2.currentHigh != spans2.currentLow,
		"suffix removed (right): want empty range, got [%d,%d)",
		spans2.currentLow,
		spans2.currentHigh,
	)
	// no shared prefix or suffix: nothing flagged, so the line is just tinted.
	{
		spans3 := runeDiffRange("abc", "xyz")

		assert.ErrorIf(
			t,
			spans3.origLow != 0 || spans3.origHigh != 0,
			"unrelated lines: want empty range, got [%d,%d)",
			spans3.origLow,
			spans3.origHigh,
		)
	}
}
