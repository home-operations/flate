package output

import (
	"bytes"
	"html"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEncoderLines_PreservesStatus(t *testing.T) {
	t.Parallel()
	changes := func(yield func(LineChange, error) bool) {
		yield(LineChange{Resource: manifest.NamedResource{Group: "example.test", Kind: "Widget", Namespace: "ns", Name: "same"}, Parent: manifest.NamedResource{Group: "parents.test", Kind: "Producer", Namespace: "ns", Name: "same"}, From: "key: old\n", To: "key: new\n"}, nil)
	}
	var buf bytes.Buffer
	if err := (Encoder{Writer: &buf, Format: FormatHTML}).Lines(changes); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); !strings.Contains(out, "1 changed") || !strings.Contains(out, `<span class="status status-"></span>`) || !strings.Contains(out, "Widget.example.test ns/same") || !strings.Contains(out, "Producer.parents.test ns/same") {
		t.Fatalf("HTML must preserve prepared empty status for unequal YAML: %s", out)
	}
	var singleton, grouped bytes.Buffer
	body := bytes.Clone(buf.Bytes())
	if err := (Encoder{Writer: &singleton, Format: FormatHTML}).DiffGroups([]DiffGroup{{Cluster: "a", Body: body}}); err != nil {
		t.Fatal(err)
	}
	assert.Diff(t, singleton.String(), buf.String())
	if err := (Encoder{Writer: &grouped, Format: FormatHTML}).DiffGroups([]DiffGroup{{Cluster: `a"<&`, Body: body}, {Cluster: "b", Added: true}, {Cluster: "c", Removed: true}}); err != nil {
		t.Fatal(err)
	}
	out := grouped.String()
	if strings.Count(out, "<!doctype html>") != 1 || !strings.Contains(out, `srcdoc="`+html.EscapeString(string(body))+`"`) ||
		!strings.Contains(out, "<h2>Cluster: a&#34;&lt;&amp;</h2>") || !strings.Contains(out, "<h2>Cluster: b (added)</h2>") || !strings.Contains(out, "<h2>Cluster: c (removed)</h2>") {
		t.Fatalf("grouped HTML lost escaped cluster sections or original body: %s", out)
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
	if got := string([]rune("name: ceph-csi-controller-manager")[aLo:aHi]); got != "-controller-manager" {
		t.Errorf(`suffix removed (left): got %q, want "-controller-manager"`, got)
	}
	if bHi != bLo {
		t.Errorf("suffix removed (right): want empty range, got [%d,%d)", bLo, bHi)
	}
	// no shared prefix or suffix: nothing flagged, so the line is just tinted.
	if lo, hi, _, _ := runeDiffRange("abc", "xyz"); lo != 0 || hi != 0 {
		t.Errorf("unrelated lines: want empty range, got [%d,%d)", lo, hi)
	}
}
