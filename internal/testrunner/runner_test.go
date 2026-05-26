package testrunner

import (
	"bytes"
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
	rep.Write(&b)
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

// TestWrite_HidesSkippedByDefault pins the output-consistency
// behavior: under changed-only mode, unchanged resources are
// SKIPPED — they have no per-resource information worth surfacing
// (the summary count is enough). `flate diff` already follows this
// discipline (no diff line for unchanged); `flate test` matches.
func TestWrite_HidesSkippedByDefault(t *testing.T) {
	s := store.New()
	passed := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "changed"}
	skipped := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "untouched"}
	s.AddObject(&manifest.Kustomization{Name: passed.Name, Namespace: passed.Namespace})
	s.AddObject(&manifest.Kustomization{Name: skipped.Name, Namespace: skipped.Namespace})
	s.UpdateStatus(passed, store.StatusReady, "")
	s.UpdateStatus(skipped, store.StatusReady, store.MsgUnchanged)

	rep := Run(Job{Store: s})

	var b bytes.Buffer
	rep.Write(&b)
	out := b.String()

	if !strings.Contains(out, "changed") {
		t.Errorf("PASSED row must appear: %s", out)
	}
	if strings.Contains(out, "untouched") {
		t.Errorf("SKIPPED row must be hidden by default; got:\n%s", out)
	}
	// Summary line still surfaces the SKIPPED count so callers
	// can see how many resources the change-filter excluded.
	if !strings.Contains(out, "1 skipped") {
		t.Errorf("summary must report SKIPPED count even when rows are hidden: %s", out)
	}
	// "collected" reflects the rendered row count, not the raw
	// case count — otherwise users see "collected 2" but only one
	// row of output, which reads as a bug.
	if !strings.Contains(out, "collected 1 items") {
		t.Errorf("collected-count must match visible rows: %s", out)
	}
}

// TestWrite_ShowSkippedSurfacesEverything pins the opt-in path:
// callers that DO want the full breakdown (debugging unexpected
// skips, verifying changed-only mode is working as intended) get
// every row back when ShowSkipped is true.
func TestWrite_ShowSkippedSurfacesEverything(t *testing.T) {
	s := store.New()
	passed := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "changed"}
	skipped := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "untouched"}
	s.AddObject(&manifest.Kustomization{Name: passed.Name, Namespace: passed.Namespace})
	s.AddObject(&manifest.Kustomization{Name: skipped.Name, Namespace: skipped.Namespace})
	s.UpdateStatus(passed, store.StatusReady, "")
	s.UpdateStatus(skipped, store.StatusReady, store.MsgUnchanged)

	rep := Run(Job{Store: s})
	rep.ShowSkipped = true

	var b bytes.Buffer
	rep.Write(&b)
	out := b.String()

	if !strings.Contains(out, "untouched") {
		t.Errorf("SKIPPED row must appear with ShowSkipped: %s", out)
	}
	if !strings.Contains(out, "SKIPPED (unchanged)") {
		t.Errorf("SKIPPED row should carry its reason: %s", out)
	}
}

// TestWrite_FailedNeverHidden ensures the suppression is scoped to
// SKIPPED only — a FAILED row must always print regardless of
// ShowSkipped, since that's the row the user reads to fix things.
func TestWrite_FailedNeverHidden(t *testing.T) {
	s := store.New()
	failed := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "broken"}
	s.AddObject(&manifest.Kustomization{Name: failed.Name, Namespace: failed.Namespace})
	s.UpdateStatus(failed, store.StatusFailed, "boom")

	rep := Run(Job{Store: s})
	var b bytes.Buffer
	rep.Write(&b)
	if !strings.Contains(b.String(), "FAILED") {
		t.Errorf("FAILED row must appear by default: %s", b.String())
	}
}
