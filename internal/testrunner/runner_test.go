package testrunner

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/style"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestWrite_VerdictGlyphAndElapsed: the summary leads with a green ✓ when all
// pass and a red ✗ when anything fails, and shows the elapsed clock only when
// non-zero.
func TestWrite_VerdictGlyphAndElapsed(t *testing.T) {
	pass := store.New()
	ok := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "ok"}
	pass.AddObject(&manifest.Kustomization{Name: ok.Name, Namespace: ok.Namespace})
	pass.UpdateStatus(ok, store.StatusReady, "")
	var pb bytes.Buffer
	if err := Run(Job{Store: pass}).Write(&pb, false, 1500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if out := pb.String(); !strings.Contains(out, style.GlyphPass) || strings.Contains(out, style.GlyphFail) {
		t.Errorf("all-pass summary wants ✓ and no ✗:\n%s", out)
	} else if !strings.Contains(out, "1.5s") {
		t.Errorf("summary should show the elapsed clock:\n%s", out)
	}

	fail := store.New()
	bad := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "bad"}
	fail.AddObject(&manifest.Kustomization{Name: bad.Name, Namespace: bad.Namespace})
	fail.UpdateStatus(bad, store.StatusFailed, "boom")
	var fb bytes.Buffer
	_ = Run(Job{Store: fail}).Write(&fb, false, 0)
	if out := fb.String(); !strings.Contains(out, style.GlyphFail) {
		t.Errorf("failing summary wants ✗:\n%s", out)
	} else if strings.Contains(out, "0.0s") {
		t.Errorf("elapsed=0 should be omitted, not rendered:\n%s", out)
	}
}

func TestRun_AllPass(t *testing.T) {
	s := store.New()
	s.AddObject(&manifest.Kustomization{Name: "apps", Namespace: "flux-system"})
	s.AddObject(&manifest.HelmRelease{Name: "demo", Namespace: "apps"})
	s.UpdateStatus(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}, store.StatusReady, "")
	s.UpdateStatus(manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "demo"}, store.StatusReady, "")

	rep := Run(Job{Store: s})
	if rep.AnyFailed() || rep.Passed != 2 {
		t.Errorf("expected 2 passed, got %+v", rep)
	}
	var b bytes.Buffer
	if err := rep.Write(&b, false, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !strings.Contains(b.String(), "2 passed") {
		t.Errorf("missing summary: %s", b.String())
	}
}

func TestRun_OneFailed(t *testing.T) {
	s := store.New()
	s.AddObject(&manifest.Kustomization{Name: "apps", Namespace: "flux-system"})
	s.UpdateStatus(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}, store.StatusFailed, "boom")

	rep := Run(Job{Store: s})
	if !rep.AnyFailed() {
		t.Errorf("expected failure: %+v", rep)
	}
	if rep.Cases[0].Reason != "boom" {
		t.Errorf("reason: %q", rep.Cases[0].Reason)
	}
}

func TestRun_SkippedReasonReportedAsSkipped(t *testing.T) {
	// A KS that soft-skipped (e.g. source was --allow-missing-secrets'd)
	// reports as SKIPPED, not PASSED or FAILED.
	s := store.New()
	s.AddObject(&manifest.Kustomization{Name: "apps", Namespace: "flux-system"})
	s.UpdateStatus(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"},
		store.StatusReady, "skipped: source GitRepository/flux-system/sealed missing auth")

	rep := Run(Job{Store: s})
	if rep.AnyFailed() {
		t.Errorf("skipped-reason case must not fail: %+v", rep)
	}
	if rep.Skipped != 1 || rep.Passed != 0 {
		t.Errorf("expected 1 skipped, 0 passed; got %+v", rep)
	}
	if !strings.Contains(rep.Cases[0].Reason, "missing auth") {
		t.Errorf("skip reason should carry the underlying message; got %q", rep.Cases[0].Reason)
	}
}

func TestRun_NoStatus(t *testing.T) {
	s := store.New()
	s.AddObject(&manifest.Kustomization{Name: "x", Namespace: "ns"})
	rep := Run(Job{Store: s})
	if !rep.AnyFailed() {
		t.Errorf("expected failure for no-status case")
	}
}

func TestRun_IncludePredicate(t *testing.T) {
	s := store.New()
	alpha := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "alpha", Name: "apps"}
	beta := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "beta", Name: "apps"}
	s.AddObject(&manifest.Kustomization{Name: alpha.Name, Namespace: alpha.Namespace})
	s.AddObject(&manifest.Kustomization{Name: beta.Name, Namespace: beta.Namespace})
	s.UpdateStatus(alpha, store.StatusReady, "")
	s.UpdateStatus(beta, store.StatusReady, "")

	rep := Run(Job{
		Store: s,
		Include: func(id manifest.NamedResource) bool {
			return id.Namespace == "alpha"
		},
	})
	if rep.Passed != 1 || len(rep.Cases) != 1 || rep.Cases[0].ID != alpha {
		t.Errorf("Include predicate report = %+v, want only %s", rep, alpha)
	}
}

func TestWrite_ShowsSkippedByDefault(t *testing.T) {
	s := store.New()
	passed := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "changed"}
	skipped := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "untouched"}
	s.AddObject(&manifest.Kustomization{Name: passed.Name, Namespace: passed.Namespace})
	s.AddObject(&manifest.Kustomization{Name: skipped.Name, Namespace: skipped.Namespace})
	s.UpdateStatus(passed, store.StatusReady, "")
	s.UpdateStatus(skipped, store.StatusReady, store.MsgUnchanged)

	rep := Run(Job{Store: s})

	var b bytes.Buffer
	if err := rep.Write(&b, false, 0); err != nil {
		t.Fatalf("Write: %v", err)
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

func TestWrite_FailedNeverHidden(t *testing.T) {
	s := store.New()
	failed := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "broken"}
	s.AddObject(&manifest.Kustomization{Name: failed.Name, Namespace: failed.Namespace})
	s.UpdateStatus(failed, store.StatusFailed, "boom")

	rep := Run(Job{Store: s})
	var b bytes.Buffer
	if err := rep.Write(&b, false, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if out := b.String(); !strings.Contains(out, "✗") || !strings.Contains(out, "broken") {
		t.Errorf("failed row must appear by default: %s", out)
	}
}

// TestWrite_Color: color=true emits ANSI codes (green pass, red fail); color=
// false renders none, so piped / NO_COLOR output stays plain.
func TestWrite_Color(t *testing.T) {
	s := store.New()
	pass := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "ok"}
	fail := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "bad"}
	s.AddObject(&manifest.Kustomization{Name: pass.Name, Namespace: pass.Namespace})
	s.AddObject(&manifest.HelmRelease{Name: fail.Name, Namespace: fail.Namespace})
	s.UpdateStatus(pass, store.StatusReady, "")
	s.UpdateStatus(fail, store.StatusFailed, "boom")
	rep := Run(Job{Store: s})

	var colored, plain bytes.Buffer
	if err := rep.Write(&colored, true, 0); err != nil {
		t.Fatalf("Write(color): %v", err)
	}
	if err := rep.Write(&plain, false, 0); err != nil {
		t.Fatalf("Write(plain): %v", err)
	}
	if !strings.Contains(colored.String(), "\x1b") {
		t.Errorf("colored output emitted no ANSI: %q", colored.String())
	}
	if strings.Contains(plain.String(), "\x1b") {
		t.Errorf("plain output leaked an escape code: %q", plain.String())
	}
}

func TestRun_DeterministicOrderAndMatchCount(t *testing.T) {
	s := store.New()
	for _, id := range []manifest.NamedResource{
		{Kind: manifest.KindKustomization, Namespace: "z", Name: "last"},
		{Kind: manifest.KindKustomization, Namespace: "a", Name: "first"},
		{Kind: manifest.KindKustomization, Namespace: "m", Name: "middle"},
	} {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		s.UpdateStatus(id, store.StatusReady, "")
	}

	rep := Run(Job{Store: s, Kinds: []string{manifest.KindKustomization}})

	if rep.Matched != 3 {
		t.Fatalf("Matched = %d, want 3", rep.Matched)
	}
	got := []manifest.NamedResource{rep.Cases[0].ID, rep.Cases[1].ID, rep.Cases[2].ID}
	want := []manifest.NamedResource{
		{Kind: manifest.KindKustomization, Namespace: "a", Name: "first"},
		{Kind: manifest.KindKustomization, Namespace: "m", Name: "middle"},
		{Kind: manifest.KindKustomization, Namespace: "z", Name: "last"},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("case order = %v, want %v", got, want)
		}
	}
}

func TestWrite_ReturnsWriterError(t *testing.T) {
	want := errors.New("write failed")
	rep := Report{Cases: []Case{{ID: manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "apps"}}}}

	if err := rep.Write(errWriter{err: want}, false, 0); !errors.Is(err, want) {
		t.Fatalf("Write error = %v, want %v", err, want)
	}
}

type errWriter struct {
	err error
}

func (w errWriter) Write(_ []byte) (int, error) {
	return 0, w.err
}
