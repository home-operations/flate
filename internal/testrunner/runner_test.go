package testrunner

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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
	if err := rep.Write(&b); err != nil {
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
	if err := rep.Write(&b); err != nil {
		t.Fatalf("Write: %v", err)
	}
	out := b.String()

	if !strings.Contains(out, "changed") {
		t.Errorf("PASSED row must appear: %s", out)
	}
	if !strings.Contains(out, "untouched") {
		t.Errorf("SKIPPED row must appear by default: %s", out)
	}
	if !strings.Contains(out, "SKIPPED (unchanged)") {
		t.Errorf("SKIPPED row should carry its reason: %s", out)
	}
	if !strings.Contains(out, "1 skipped") {
		t.Errorf("summary must report SKIPPED count: %s", out)
	}
	if !strings.Contains(out, "collected 2 items") {
		t.Errorf("collected-count must include SKIPPED rows: %s", out)
	}
}

func TestWrite_FailedNeverHidden(t *testing.T) {
	s := store.New()
	failed := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "broken"}
	s.AddObject(&manifest.Kustomization{Name: failed.Name, Namespace: failed.Namespace})
	s.UpdateStatus(failed, store.StatusFailed, "boom")

	rep := Run(Job{Store: s})
	var b bytes.Buffer
	if err := rep.Write(&b); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !strings.Contains(b.String(), "FAILED") {
		t.Errorf("FAILED row must appear by default: %s", b.String())
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

	if err := rep.Write(errWriter{err: want}); !errors.Is(err, want) {
		t.Fatalf("Write error = %v, want %v", err, want)
	}
}

type errWriter struct {
	err error
}

func (w errWriter) Write(_ []byte) (int, error) {
	return 0, w.err
}
