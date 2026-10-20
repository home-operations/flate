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

func TestEncoder_TestNotesFooter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		notes []Note
		want  string
	}{
		{name: "empty", want: "\n  ✓ 0 passed\n"},
		{
			name:  "single",
			notes: []Note{{Text: "resource orphaned id=x"}},
			want:  "\n  notes (1)\n    resource orphaned id=x\n\n  ✓ 0 passed\n",
		},
		{
			name:  "collapsed",
			notes: []Note{{Text: "resource orphaned id=x", Count: 3}, {Text: "source fetched", Count: 1}},
			want:  "\n  notes (4)\n    resource orphaned id=x (×3)\n    source fetched\n\n  ✓ 0 passed\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b}).Test(TestReport{}, nil, tc.notes, 0); err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_TestVerdictGlyphAndElapsed(t *testing.T) {
	t.Parallel()
	ok := testutil.KustomizationID("ns", "ok")
	pass := TestReport{Cases: []Case{{ID: ok, Outcome: OutcomePassed}}, Passed: 1}
	var pb bytes.Buffer
	if err := (Encoder{Writer: &pb}).Test(pass, nil, nil, 1500*time.Millisecond); err != nil {
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
	_ = (Encoder{Writer: &fb}).Test(fail, nil, nil, 0)
	if out := fb.String(); !strings.Contains(out, GlyphFail) {
		t.Errorf("failing summary wants ✗:\n%s", out)
	} else if strings.Contains(out, "0.0s") {
		t.Errorf("elapsed=0 should be omitted, not rendered:\n%s", out)
	}
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
	if err := (Encoder{Writer: &b}).Test(rep, nil, nil, 0); err != nil {
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
	if err := (Encoder{Writer: &b}).Test(rep, nil, nil, 0); err != nil {
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
	if err := (Encoder{Writer: &colored, Color: true}).Test(rep, nil, nil, 0); err != nil {
		t.Fatalf("Test(color): %v", err)
	}
	if err := (Encoder{Writer: &plain}).Test(rep, nil, nil, 0); err != nil {
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
	rep := TestReport{Cases: []Case{{ID: testutil.KustomizationID("ns", "apps")}}}

	if err := (Encoder{Writer: errorWriter{want}}).Test(rep, nil, nil, 0); !errors.Is(err, want) {
		t.Fatalf("Test error = %v, want %v", err, want)
	}
}

func TestEncoder_TestMultilineColorAndNotes(t *testing.T) {
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
			want := "\n  " + Fail(GlyphFail, color) + "  " + Dim("Kustomization.kustomize.toolkit.fluxcd.io", color) + "  ns/root  " + Dim("first", color) +
				"\n         " + Dim("second", color) + "\n         " + Dim("third", color) +
				"\n  " + Dim(GlyphBlocked, color) + "  " + Dim("HelmRelease.helm.toolkit.fluxcd.io       ", color) + "  ns/child  " + Dim("blocked by ns/root", color) +
				"\n\n  " + Warn("⚠ warnings (1)", color) + "\n    " + Warn("advisory (×2)", color) +
				"\n\n  " + Dim("notes (3)", color) + "\n    " + Dim("repeated (×3)", color) +
				"\n\n  " + Fail(GlyphFail, color) + " " + Pass("0 passed", color) + " · " + Fail("1 failed", color) +
				" · " + Dim("1 blocked", color) + "   " + Dim("1.5s", color) + "\n"
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Color: color}).Test(r, []manifest.Warning{{Message: "advisory", Count: 2}}, []Note{{Text: "repeated", Count: 3}}, 1500*time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if got := b.String(); got != want {
				t.Fatalf("report = %q, want %q", got, want)
			}
		})
	}
}
