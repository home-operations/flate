package output

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEncoder_TestVerdictGlyphAndElapsed(t *testing.T) {
	t.Parallel()
	ok := testutil.KustomizationID("ns", "ok")
	pass := TestReport{Cases: []Case{{ID: ok, Outcome: OutcomePassed}}, Passed: 1}
	var pb bytes.Buffer
	if err := writeTestReport(Encoder{Writer: &pb}, pass, 1500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if out := pb.String(); !strings.Contains(out, GlyphPass) || strings.Contains(out, GlyphFail) {
		t.Errorf("all-pass summary wants ✓ and no ✗:\n%s", out)
	} else if !strings.Contains(out, "1.5s") {
		t.Errorf("summary should show the elapsed clock:\n%s", out)
	}

	bad := testutil.KustomizationID("ns", "bad")
	fail := TestReport{Cases: []Case{{ID: bad, Outcome: OutcomeFailed, Reason: "boom"}}, Failed: 1}
	var fb bytes.Buffer
	_ = writeTestReport(Encoder{Writer: &fb}, fail, 0)
	if out := fb.String(); !strings.Contains(out, GlyphFail) {
		t.Errorf("failing summary wants ✗:\n%s", out)
	} else if strings.Contains(out, "0.0s") {
		t.Errorf("elapsed=0 should be omitted, not rendered:\n%s", out)
	}
	var singleton bytes.Buffer
	if err := writeTestGroups(Encoder{Writer: &singleton}, []ReportGroup{{Cluster: "entry", Test: &pass}}, 1500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	assert.Diff(t, singleton.String(), pb.String())
}

func TestEncoder_TestGroupedVerdicts(t *testing.T) {
	t.Parallel()
	groups := []ReportGroup{
		{Cluster: "a", Test: &TestReport{Passed: 1, Skipped: 1, Cases: []Case{
			{ID: testutil.KustomizationID("ns", "same"), Outcome: OutcomePassed},
			{ID: testutil.KustomizationID("ns", "skip"), Outcome: OutcomeSkipped},
		}}},
		{Cluster: "b", Test: &TestReport{Failed: 1, Cases: []Case{
			{ID: testutil.KustomizationID("ns", "same"), Outcome: OutcomeFailed, Reason: "boom"},
		}, Blocked: []Case{{ID: testutil.KustomizationID("ns", "child"), Outcome: OutcomeBlocked}}}},
	}
	var b bytes.Buffer
	if err := writeTestGroups(Encoder{Writer: &b}, groups, 1500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	assert.Diff(t, b.String(), "\n  ✓  Kustomization  a: ns/same\n"+
		"  ‒  Kustomization  a: ns/skip\n\n"+
		"  ✗  Kustomization  b: ns/same  boom\n"+
		"  ⊘  Kustomization  b: ns/child\n\n"+
		"  a: ✓ 1 passed · 1 skipped\n\n"+
		"  b: ✗ 0 passed · 1 failed · 1 blocked\n\nInvocation totals\n"+
		"  ✗ 1 passed · 1 skipped · 1 failed · 1 blocked   1.5s\n")
}

func TestEncoder_TestShowsSkippedByDefault(t *testing.T) {
	t.Parallel()
	passed := testutil.KustomizationID("ns", "changed")
	skipped := testutil.KustomizationID("ns", "untouched")
	rep := TestReport{
		Cases:  []Case{{ID: passed, Outcome: OutcomePassed}, {ID: skipped, Outcome: OutcomeSkipped, Reason: "unchanged"}},
		Passed: 1, Skipped: 1,
	}

	var b bytes.Buffer
	if err := writeTestReport(Encoder{Writer: &b}, rep, 0); err != nil {
		t.Fatalf("Test: %v", err)
	}
	out := b.String()

	if !strings.Contains(out, "changed") {
		t.Errorf("PASSED row must appear: %s", out)
	}
	if !strings.Contains(out, "untouched") {
		t.Errorf("skipped row must appear by default: %s", out)
	}
	if !strings.Contains(out, "‒") || !strings.Contains(out, "unchanged") {
		t.Errorf("skipped row should carry its glyph and reason: %s", out)
	}
	if !strings.Contains(out, "1 skipped") {
		t.Errorf("summary must report the skipped count: %s", out)
	}
}

func TestEncoder_TestFailedNeverHidden(t *testing.T) {
	t.Parallel()
	failed := testutil.KustomizationID("ns", "broken")
	rep := TestReport{Cases: []Case{{ID: failed, Outcome: OutcomeFailed, Reason: "boom"}}, Failed: 1}
	var b bytes.Buffer
	if err := writeTestReport(Encoder{Writer: &b}, rep, 0); err != nil {
		t.Fatalf("Test: %v", err)
	}
	if out := b.String(); !strings.Contains(out, "✗") || !strings.Contains(out, "broken") {
		t.Errorf("failed row must appear by default: %s", out)
	}
}

func TestEncoder_TestColor(t *testing.T) {
	t.Parallel()
	pass := testutil.KustomizationID("ns", "ok")
	fail := testutil.HelmReleaseID("ns", "bad")
	rep := TestReport{
		Cases:  []Case{{ID: fail, Outcome: OutcomeFailed, Reason: "boom"}, {ID: pass, Outcome: OutcomePassed}},
		Passed: 1, Failed: 1,
	}

	var colored, plain bytes.Buffer
	if err := writeTestReport(Encoder{Writer: &colored, Color: true}, rep, 0); err != nil {
		t.Fatalf("Test(color): %v", err)
	}
	if err := writeTestReport(Encoder{Writer: &plain}, rep, 0); err != nil {
		t.Fatalf("Test(plain): %v", err)
	}
	if !strings.Contains(colored.String(), "\x1b") {
		t.Errorf("colored output emitted no ANSI: %q", colored.String())
	}
	if strings.Contains(plain.String(), "\x1b") {
		t.Errorf("plain output leaked an escape code: %q", plain.String())
	}
	if !strings.Contains(colored.String(), "\n\n  "+Fail(GlyphFail, true)+" ") {
		t.Errorf("colored verdict lost its painted glyph: %q", colored.String())
	}
	if !strings.Contains(plain.String(), "\n\n  "+GlyphFail+" ") {
		t.Errorf("plain verdict lost its glyph framing: %q", plain.String())
	}
}

func TestEncoder_TestReturnsWriterError(t *testing.T) {
	t.Parallel()
	want := errors.New("write failed")
	w := &verdictFailWriter{err: want}
	rep := Report{Test: true,
		Groups:   []ReportGroup{{Test: &TestReport{Cases: []Case{{ID: testutil.KustomizationID("ns", "apps")}}, Passed: 1}}},
		Warnings: []Warning{{Warning: manifest.Warning{Message: "advisory"}}},
	}
	rep.Prepare(nil)
	e := Encoder{Writer: w}
	if err := e.Report(rep, e); !errors.Is(err, want) {
		t.Fatalf("Test error = %v, want %v", err, want)
	}
	for _, token := range []string{"ns/apps", "advisory"} {
		assert.Equal(t, strings.Count(w.output.String(), token), 1)
	}
}

type verdictFailWriter struct {
	output bytes.Buffer
	err    error
}

func (w *verdictFailWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("1 passed")) {
		return 0, w.err
	}
	return w.output.Write(p)
}

func TestEncoder_TestMultilineColor(t *testing.T) {
	t.Parallel()
	root := testutil.KustomizationID("ns", "root")
	child := testutil.HelmReleaseID("ns", "child")
	r := TestReport{
		Cases:   []Case{{ID: root, Outcome: OutcomeFailed, Reason: " first \n\t\n second\n third "}},
		Blocked: []Case{{ID: child, Outcome: OutcomeBlocked, Reason: "blocked by ns/root"}},
		Failed:  1,
	}
	for _, tc := range []struct {
		name  string
		color bool
	}{{name: "plain"}, {name: "color", color: true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			color := tc.color
			want := "\n  " + Fail(GlyphFail, color) + "  " + Dim("Kustomization", color) + "  ns/root  " + Dim("first", color) +
				"\n         " + Dim("second", color) + "\n         " + Dim("third", color) +
				"\n  " + Dim(GlyphBlocked, color) + "  " + Dim("HelmRelease  ", color) + "  ns/child  " + Dim("blocked by ns/root", color) +
				"\n\n  " + Fail(GlyphFail, color) + " " + Pass("0 passed", color) + " · " + Fail("1 failed", color) +
				" · " + Dim("1 blocked", color) + "   " + Dim("1.5s", color) + "\n"
			var b bytes.Buffer
			if err := writeTestReport(Encoder{Writer: &b, Color: color}, r, 1500*time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if got := b.String(); got != want {
				t.Fatalf("report = %q, want %q", got, want)
			}
		})
	}
}

func writeTestReport(e Encoder, report TestReport, elapsed time.Duration) error {
	return writeTestGroups(e, []ReportGroup{{Test: &report}}, elapsed)
}

func writeTestGroups(e Encoder, groups []ReportGroup, elapsed time.Duration) error {
	r := Report{Groups: groups, Test: true, Elapsed: elapsed}
	r.Prepare(nil)
	return e.Report(r, e)
}
