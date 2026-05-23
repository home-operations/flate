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
